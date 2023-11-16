package json_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/goccy/go-json"
)

type queryTestX struct {
	XA int
	XB string
	XC *queryTestY
	XD bool
	XE float32
}

type queryTestY struct {
	YA int
	YB string
	YC *queryTestZ
	YD bool
	YE float32
}

type queryTestZ struct {
	ZA string
	ZB bool
	ZC int
}

func (z *queryTestZ) MarshalJSON(ctx context.Context) ([]byte, error) {
	type _queryTestZ queryTestZ
	return json.MarshalContext(ctx, (*_queryTestZ)(z))
}

func TestFieldQuery(t *testing.T) {
	query, err := json.BuildFieldQuery(
		"XA",
		"XB",
		json.BuildSubFieldQuery("XC").Fields(
			"YA",
			"YB",
			json.BuildSubFieldQuery("YC").Fields(
				"ZA",
				"ZB",
			),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(query, &json.FieldQuery{
		Fields: []*json.FieldQuery{
			{
				Name: "XA",
			},
			{
				Name: "XB",
			},
			{
				Name: "XC",
				Fields: []*json.FieldQuery{
					{
						Name: "YA",
					},
					{
						Name: "YB",
					},
					{
						Name: "YC",
						Fields: []*json.FieldQuery{
							{
								Name: "ZA",
							},
							{
								Name: "ZB",
							},
						},
					},
				},
			},
		},
	}) {
		t.Fatal("cannot get query")
	}
	queryStr, err := query.QueryString()
	if err != nil {
		t.Fatal(err)
	}
	if queryStr != `["XA","XB",{"XC":["YA","YB",{"YC":["ZA","ZB"]}]}]` {
		t.Fatalf("failed to create query string. %s", queryStr)
	}
	ctx := json.SetFieldQueryToContext(context.Background(), query)
	b, err := json.MarshalContext(ctx, &queryTestX{
		XA: 1,
		XB: "xb",
		XC: &queryTestY{
			YA: 2,
			YB: "yb",
			YC: &queryTestZ{
				ZA: "za",
				ZB: true,
				ZC: 3,
			},
			YD: true,
			YE: 4,
		},
		XD: true,
		XE: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	expected := `{"XA":1,"XB":"xb","XC":{"YA":2,"YB":"yb","YC":{"ZA":"za","ZB":true}}}`
	got := string(b)
	if expected != got {
		t.Fatalf("failed to encode with field query: expected %q but got %q", expected, got)
	}
}
