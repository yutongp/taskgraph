package store

import (
	"bytes"
	"errors"

	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
)

type S3File struct {
	s3.Bucket
	keyPath string
	buf     *bytes.Buffer
}

type S3Store struct {
	s3.S3
}

func (s3f *S3File) Read(p []byte) (int, error) {
	return s3f.buf.Read(p)
}

func (s3f *S3File) Write(p []byte) (int, error) {
	return s3f.buf.Write(p)
}

func (s3f *S3File) Sync() error {
	return s3f.Put(s3f.keyPath, s3f.buf.Bytes(), "binary", s3.BucketOwnerFull)
}

func (s3s *S3Store) Open(path, name string) (File, error) {
	bkt := s3s.Bucket(path)
	if bkt == nil {
		return nil, errors.New("not found")
	}
	b, err := bkt.Get(name)
	if err != nil {
		return nil, err
	}

	return &S3File{*bkt, name, bytes.NewBuffer(b)}, nil
}

func NewS3(auth aws.Auth, region aws.Region) *S3Store {
	return &S3Store{*s3.New(auth, region)}
}
