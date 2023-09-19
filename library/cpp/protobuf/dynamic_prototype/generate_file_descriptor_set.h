#pragma once

#include <google/protobuf/descriptor.pb.h>

NProtoBuf::FileDescriptorSet GenerateFileDescriptorSet(const NProtoBuf::Descriptor* desc);
