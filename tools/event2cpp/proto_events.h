#pragma once

#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/stubs/common.h>

namespace NProtoBuf::NCompiler::NPlugins {

class TProtoEventExtensionGenerator : public google::protobuf::compiler::CodeGenerator {
 public:
  TProtoEventExtensionGenerator() {}
  ~TProtoEventExtensionGenerator() override {}

  bool Generate(const google::protobuf::FileDescriptor* file,
      const TProtoStringType& parameter,
      google::protobuf::compiler::OutputDirectory* output_directory,
      TProtoStringType* error) const override;
};

} // namespace NProtoBuf::NCompiler::NPlugins
