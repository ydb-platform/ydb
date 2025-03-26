package protokit_test

import (
	"github.com/stretchr/testify/suite"

	"context"
	"testing"

	"github.com/pseudomuto/protokit"
)

type ContextTest struct {
	suite.Suite
}

func TestContext(t *testing.T) {
	suite.Run(t, new(ContextTest))
}

func (assert *ContextTest) TestContextWithFileDescriptor() {
	ctx := context.Background()

	val, found := protokit.FileDescriptorFromContext(ctx)
	assert.Nil(val)
	assert.False(found)

	ctx = protokit.ContextWithFileDescriptor(ctx, new(protokit.FileDescriptor))
	val, found = protokit.FileDescriptorFromContext(ctx)
	assert.NotNil(val)
	assert.True(found)
}

func (assert *ContextTest) TestContextWithEnumDescriptor() {
	ctx := context.Background()

	val, found := protokit.EnumDescriptorFromContext(ctx)
	assert.Nil(val)
	assert.False(found)

	ctx = protokit.ContextWithEnumDescriptor(ctx, new(protokit.EnumDescriptor))
	val, found = protokit.EnumDescriptorFromContext(ctx)
	assert.NotNil(val)
	assert.True(found)
}

func (assert *ContextTest) TestContextWithDescriptor() {
	ctx := context.Background()

	val, found := protokit.DescriptorFromContext(ctx)
	assert.Nil(val)
	assert.False(found)

	ctx = protokit.ContextWithDescriptor(ctx, new(protokit.Descriptor))
	val, found = protokit.DescriptorFromContext(ctx)
	assert.NotNil(val)
	assert.True(found)
}

func (assert *ContextTest) TestContextWithServiceDescriptor() {
	ctx := context.Background()

	val, found := protokit.ServiceDescriptorFromContext(ctx)
	assert.Empty(val)
	assert.False(found)

	ctx = protokit.ContextWithServiceDescriptor(ctx, new(protokit.ServiceDescriptor))
	val, found = protokit.ServiceDescriptorFromContext(ctx)
	assert.NotNil(val)
	assert.True(found)
}
