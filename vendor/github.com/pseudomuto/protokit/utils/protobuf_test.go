package utils_test

import (
	"github.com/stretchr/testify/suite"

	"testing"

	"github.com/pseudomuto/protokit/utils"
)

type UtilsTest struct {
	suite.Suite
}

func TestUtils(t *testing.T) {
	suite.Run(t, new(UtilsTest))
}

func (assert *UtilsTest) TestCreateGenRequest() {
	fds, err := utils.LoadDescriptorSet("..", "fixtures", "fileset.pb")
	assert.NoError(err)

	req := utils.CreateGenRequest(fds, "booking.proto", "todo.proto")
	assert.Equal([]string{"booking.proto", "todo.proto"}, req.GetFileToGenerate())

	expectedProtos := []string{
		"booking.proto",
		"google/protobuf/any.proto",
		"google/protobuf/descriptor.proto",
		"google/protobuf/timestamp.proto",
		"extend.proto",
		"todo.proto",
		"todo_import.proto",
	}

	for _, pf := range req.GetProtoFile() {
		assert.True(utils.InStringSlice(expectedProtos, pf.GetName()))
	}
}

func (assert *UtilsTest) TestFilesToGenerate() {
	fds, err := utils.LoadDescriptorSet("..", "fixtures", "fileset.pb")
	assert.NoError(err)

	req := utils.CreateGenRequest(fds, "booking.proto")
	protos := utils.FilesToGenerate(req)
	assert.Len(protos, 1)
	assert.Equal("booking.proto", protos[0].GetName())
}

func (assert *UtilsTest) TestLoadDescriptorSet() {
	set, err := utils.LoadDescriptorSet("..", "fixtures", "fileset.pb")
	assert.NoError(err)
	assert.Len(set.GetFile(), 7)

	assert.NotNil(utils.FindDescriptor(set, "todo.proto"))
	assert.Nil(utils.FindDescriptor(set, "whodis.proto"))
}

func (assert *UtilsTest) TestLoadDescriptorSetFileNotFound() {
	set, err := utils.LoadDescriptorSet("..", "fixtures", "notgonnadoit.pb")
	assert.Nil(set)
	assert.EqualError(err, "open ../fixtures/notgonnadoit.pb: no such file or directory")
}

func (assert *UtilsTest) TestLoadDescriptorSetMarshalError() {
	set, err := utils.LoadDescriptorSet("..", "fixtures", "todo.proto")
	assert.Nil(set)
	assert.EqualError(err, "proto: can't skip unknown wire type 7 for descriptor.FileDescriptorSet")
}

func (assert *UtilsTest) TestLoadDescriptor() {
	proto, err := utils.LoadDescriptor("todo.proto", "..", "fixtures", "fileset.pb")
	assert.NotNil(proto)
	assert.NoError(err)
}

func (assert *UtilsTest) TestLoadDescriptorFileNotFound() {
	proto, err := utils.LoadDescriptor("todo.proto", "..", "fixtures", "notgonnadoit.pb")
	assert.Nil(proto)
	assert.EqualError(err, "open ../fixtures/notgonnadoit.pb: no such file or directory")
}

func (assert *UtilsTest) TestLoadDescriptorMarshalError() {
	proto, err := utils.LoadDescriptor("todo.proto", "..", "fixtures", "todo.proto")
	assert.Nil(proto)
	assert.EqualError(err, "proto: can't skip unknown wire type 7 for descriptor.FileDescriptorSet")
}

func (assert *UtilsTest) TestLoadDescriptorDescriptorNotFound() {
	proto, err := utils.LoadDescriptor("nothere.proto", "..", "fixtures", "fileset.pb")
	assert.Nil(proto)
	assert.EqualError(err, "FileDescriptor not found")
}
