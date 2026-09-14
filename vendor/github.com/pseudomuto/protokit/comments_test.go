package protokit_test

import (
	"github.com/stretchr/testify/suite"

	"fmt"
	"testing"

	"github.com/pseudomuto/protokit"
	"github.com/pseudomuto/protokit/utils"
)

type CommentsTest struct {
	suite.Suite
	comments protokit.Comments
}

func TestComments(t *testing.T) {
	suite.Run(t, new(CommentsTest))
}

func (assert *CommentsTest) SetupSuite() {
	pf, err := utils.LoadDescriptor("todo.proto", "fixtures", "fileset.pb")
	assert.NoError(err)

	assert.comments = protokit.ParseComments(pf)
}

func (assert *CommentsTest) TestComments() {
	tests := []struct {
		key      string
		leading  string
		trailing string
	}{
		{"6.0.2.1", "Add an item to your list\n\nAdds a new item to the specified list.", ""}, // leading commend
		{"4.0.2.0", "", "The id of the list."},                                                // tailing comment
	}

	for _, test := range tests {
		assert.Equal(test.leading, assert.comments[test.key].GetLeading())
		assert.Equal(test.trailing, assert.comments[test.key].GetTrailing())
		assert.Len(assert.comments[test.key].GetDetached(), 0)
	}

	assert.NotNil(assert.comments.Get("WONTBETHERE"))
	assert.Equal("", assert.comments.Get("WONTBETHERE").String())
}

// Join the leading and trailing comments together
func ExampleComment_String() {
	c := &protokit.Comment{Leading: "Some leading comment", Trailing: "Some trailing comment"}
	fmt.Println(c.String())
	// Output: Some leading comment
	//
	// Some trailing comment
}
