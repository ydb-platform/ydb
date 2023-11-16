package json_test

import "testing"

func assertErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%+v", err)
	}
}

func assertEq(t *testing.T, msg string, exp interface{}, act interface{}) {
	t.Helper()
	if exp != act {
		t.Fatalf("failed to test for %s. exp=[%v] but act=[%v]", msg, exp, act)
	}
}

func assertNeq(t *testing.T, msg string, exp interface{}, act interface{}) {
	t.Helper()
	if exp == act {
		t.Fatalf("failed to test for %s. expected value is not [%v] but got same value", msg, act)
	}
}
