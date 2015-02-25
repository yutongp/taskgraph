package store

import "io"

type File interface {
	io.ReadWriter
	Sync() error
}

type Store interface {
	Open(path, name string) (File, error)
}
