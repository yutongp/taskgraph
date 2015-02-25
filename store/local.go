package store

import (
	"os"
	ph "path"
)

type LocalStore struct {
}

func (ls *LocalStore) Open(path, name string) (File, error) {
	return os.OpenFile(ph.Join(path, name), os.O_RDWR, 0777)
}

func NewLocal() *LocalStore {
	return new(LocalStore)
}
