package proto

import "testing"

func TestClientData_EncodeAware(t *testing.T) {
	Gold(t, ClientData{
		TableName: "Foo",
	})
}
