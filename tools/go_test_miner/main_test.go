package main

import (
	"testing"
)

func TestOk(t *testing.T) {
}

func Test1(a *testing.T) {
}

func Test_Function(tt *testing.T) {
}

func Test(t *testing.T) {
}

//nolint:tests
func Testfail(t *testing.T) {
	panic("Not a test function!")
}
