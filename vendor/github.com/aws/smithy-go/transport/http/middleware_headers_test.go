package http_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/go-cmp/cmp"
)

func TestAddHeaderValue(t *testing.T) {
	stack := middleware.NewStack("stack", smithyhttp.NewStackRequest)
	err := smithyhttp.AddHeaderValue("foo", "fooValue")(stack)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}
	err = smithyhttp.AddHeaderValue("bar", "firstValue")(stack)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}
	err = smithyhttp.AddHeaderValue("bar", "secondValue")(stack)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}

	handler := middleware.DecorateHandler(middleware.HandlerFunc(func(ctx context.Context, input interface{}) (output interface{}, metadata middleware.Metadata, err error) {
		req := input.(*smithyhttp.Request)
		if diff := cmp.Diff(req.Header, http.Header{
			"Foo": []string{"fooValue"},
			"Bar": []string{"firstValue", "secondValue"},
		}); len(diff) > 0 {
			t.Errorf(diff)
		}
		return output, metadata, err
	}), stack)
	_, _, err = handler.Handle(context.Background(), nil)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}
}

func TestSetHeaderValue(t *testing.T) {
	stack := middleware.NewStack("stack", smithyhttp.NewStackRequest)
	err := smithyhttp.SetHeaderValue("foo", "firstValue")(stack)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}
	err = smithyhttp.SetHeaderValue("foo", "secondValue")(stack)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}

	handler := middleware.DecorateHandler(middleware.HandlerFunc(func(ctx context.Context, input interface{}) (output interface{}, metadata middleware.Metadata, err error) {
		req := input.(*smithyhttp.Request)
		if diff := cmp.Diff(req.Header, http.Header{
			"Foo": []string{"secondValue"},
		}); len(diff) > 0 {
			t.Errorf(diff)
		}
		return output, metadata, err
	}), stack)
	_, _, err = handler.Handle(context.Background(), nil)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}
}
