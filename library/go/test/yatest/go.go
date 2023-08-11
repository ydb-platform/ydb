package yatest

import (
	"os"
)

func PrepareGOPATH() error {
	return preparePath("GOPATH")
}

func PrepareGOCACHE() error {
	return preparePath("GOCACHE")
}

func preparePath(name string) error {
	p, err := os.MkdirTemp(WorkPath(""), "name")
	if err != nil {
		return err
	}
	return os.Setenv(name, p)
}
