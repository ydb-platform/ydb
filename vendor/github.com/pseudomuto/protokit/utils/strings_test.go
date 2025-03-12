package utils_test

import (
	"github.com/stretchr/testify/suite"

	"testing"

	"github.com/pseudomuto/protokit/utils"
)

type StringsTest struct {
	suite.Suite
}

func TestStrings(t *testing.T) {
	suite.Run(t, new(StringsTest))
}

func (assert *StringsTest) TestInStringSlice() {
	vals := []string{"val1", "val2"}
	assert.True(utils.InStringSlice(vals, "val1"))
	assert.False(utils.InStringSlice(vals, "wat"))
}
