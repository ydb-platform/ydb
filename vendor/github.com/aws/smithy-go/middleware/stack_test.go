package middleware

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestStackList(t *testing.T) {
	s := NewStack("fooStack", func() interface{} { return struct{}{} })

	s.Initialize.Add(mockInitializeMiddleware("first"), After)
	s.Serialize.Add(mockSerializeMiddleware("second"), After)
	s.Build.Add(mockBuildMiddleware("third"), After)
	s.Finalize.Add(mockFinalizeMiddleware("fourth"), After)
	s.Deserialize.Add(mockDeserializeMiddleware("fifth"), After)

	actual := s.List()

	expect := []string{
		"fooStack",
		(*InitializeStep)(nil).ID(),
		"first",
		(*SerializeStep)(nil).ID(),
		"second",
		(*BuildStep)(nil).ID(),
		"third",
		(*FinalizeStep)(nil).ID(),
		"fourth",
		(*DeserializeStep)(nil).ID(),
		"fifth",
	}

	if diff := cmp.Diff(expect, actual); len(diff) != 0 {
		t.Errorf("expect and actual stack list differ\n%s", diff)
	}
}

func TestStackString(t *testing.T) {
	s := NewStack("fooStack", func() interface{} { return struct{}{} })

	s.Initialize.Add(mockInitializeMiddleware("first"), After)
	s.Serialize.Add(mockSerializeMiddleware("second"), After)
	s.Build.Add(mockBuildMiddleware("third"), After)
	s.Finalize.Add(mockFinalizeMiddleware("fourth"), After)
	s.Deserialize.Add(mockDeserializeMiddleware("fifth"), After)

	actual := s.String()

	expect := strings.Join([]string{
		"fooStack",
		"\t" + (*InitializeStep)(nil).ID(),
		"\t\t" + "first",
		"\t" + (*SerializeStep)(nil).ID(),
		"\t\t" + "second",
		"\t" + (*BuildStep)(nil).ID(),
		"\t\t" + "third",
		"\t" + (*FinalizeStep)(nil).ID(),
		"\t\t" + "fourth",
		"\t" + (*DeserializeStep)(nil).ID(),
		"\t\t" + "fifth",
		"",
	}, "\n")

	if diff := cmp.Diff(expect, actual); len(diff) != 0 {
		t.Errorf("expect and actual stack list differ\n%s", diff)
	}
}
