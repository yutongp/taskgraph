package integration

import (
	"log"
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/go-distributed/meritop/controller"
	"github.com/go-distributed/meritop/framework"
	"github.com/go-distributed/meritop/pkg/etcdutil"
)

// TestFailedMasterTask checks if a master task failed,
// 1. a new boostrap will be created to take over
// 2. continue what's left;
// 3. finish the job with the same result.
func TestRegressFailedMaster(t *testing.T) {
	t.Skip()
	m := etcdutil.StartNewEtcdServer(t, "regression_failmaster_test")
	defer m.Terminate(t)

	job := "framework_regression_test"
	etcdURLs := []string{m.URL()}
	numOfTasks := uint64(2)

	// controller start first to setup task directories in etcd
	controller := controller.New(job, etcd.NewClient(etcdURLs), numOfTasks)
	controller.InitEtcdLayout()
	defer controller.DestroyEtcdLayout()

	// We need to set etcd so that nodes know what to do.
	taskBuilder := &framework.SimpleTaskBuilder{
		GDataChan:    make(chan int32, 10),
		FinishChan:   make(chan struct{}),
		TaskStopChan: make(chan bool, 1),
		// this task master will fail
		Config: map[string]string{
			"failmaster": "yes",
			"failepoch":  "1",
		},
	}
	for i := uint64(0); i < numOfTasks; i++ {
		go drive(t, job, etcdURLs, numOfTasks, taskBuilder)
	}
	if <-taskBuilder.TaskStopChan {
		// assuming health key expire after this.
		time.Sleep(5 * time.Second)
		taskBuilder.Config = map[string]string{}
		// this time we start a new bootstrap whose task master doesn't fail.
		go drive(t, job, etcdURLs, numOfTasks, taskBuilder)
	}

	// wait for last number to comeback.
	wantData := []int32{0, 105, 210, 315, 420, 525, 630, 735, 840, 945, 1050}
	getData := make([]int32, framework.NumOfIterations+1)
	for i := uint64(0); i <= framework.NumOfIterations; i++ {
		getData[i] = <-taskBuilder.GDataChan
		log.Println("iteration:", i)
	}

	for i := range wantData {
		if wantData[i] != getData[i] {
			t.Errorf("#%d: data want = %d, get = %d\n", i, wantData[i], getData[i])
		}
	}
	<-taskBuilder.FinishChan
}
