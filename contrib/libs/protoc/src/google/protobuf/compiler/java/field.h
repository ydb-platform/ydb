// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// Author: kenton@google.com (Kenton Varda)
//  Based on original Protocol Buffers design by
//  Sanjay Ghemawat, Jeff Dean, and others.

#ifndef GOOGLE_PROTOBUF_COMPILER_JAVA_FIELD_H__
#define GOOGLE_PROTOBUF_COMPILER_JAVA_FIELD_H__

#include <cstdint>
#include <memory>
#include <string>

#include "y_absl/container/flat_hash_map.h"
#include "y_absl/log/absl_check.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/port.h"

namespace google {
namespace protobuf {
namespace compiler {
namespace java {
class Context;            // context.h
class ClassNameResolver;  // name_resolver.h
}  // namespace java
}  // namespace compiler
namespace io {
class Printer;  // printer.h
}
}  // namespace protobuf
}  // namespace google

namespace google {
namespace protobuf {
namespace compiler {
namespace java {

class ImmutableFieldGenerator {
 public:
  ImmutableFieldGenerator() {}
  ImmutableFieldGenerator(const ImmutableFieldGenerator&) = delete;
  ImmutableFieldGenerator& operator=(const ImmutableFieldGenerator&) = delete;
  virtual ~ImmutableFieldGenerator();

  virtual int GetMessageBitIndex() const = 0;
  virtual int GetBuilderBitIndex() const = 0;
  virtual int GetNumBitsForMessage() const = 0;
  virtual int GetNumBitsForBuilder() const = 0;
  virtual void GenerateInterfaceMembers(io::Printer* printer) const = 0;
  virtual void GenerateMembers(io::Printer* printer) const = 0;
  virtual void GenerateBuilderMembers(io::Printer* printer) const = 0;
  virtual void GenerateInitializationCode(io::Printer* printer) const = 0;
  virtual void GenerateBuilderClearCode(io::Printer* printer) const = 0;
  virtual void GenerateMergingCode(io::Printer* printer) const = 0;
  virtual void GenerateBuildingCode(io::Printer* printer) const = 0;
  virtual void GenerateBuilderParsingCode(io::Printer* printer) const = 0;
  virtual void GenerateBuilderParsingCodeFromPacked(io::Printer* printer) const;
  virtual void GenerateSerializationCode(io::Printer* printer) const = 0;
  virtual void GenerateSerializedSizeCode(io::Printer* printer) const = 0;
  virtual void GenerateFieldBuilderInitializationCode(
      io::Printer* printer) const = 0;
  virtual void GenerateKotlinDslMembers(io::Printer* printer) const = 0;

  virtual void GenerateEqualsCode(io::Printer* printer) const = 0;
  virtual void GenerateHashCode(io::Printer* printer) const = 0;

  virtual TProtoStringType GetBoxedType() const = 0;
};

class ImmutableFieldLiteGenerator {
 public:
  ImmutableFieldLiteGenerator() {}
  ImmutableFieldLiteGenerator(const ImmutableFieldLiteGenerator&) = delete;
  ImmutableFieldLiteGenerator& operator=(const ImmutableFieldLiteGenerator&) =
      delete;
  virtual ~ImmutableFieldLiteGenerator();

  virtual int GetNumBitsForMessage() const = 0;
  virtual void GenerateInterfaceMembers(io::Printer* printer) const = 0;
  virtual void GenerateMembers(io::Printer* printer) const = 0;
  virtual void GenerateBuilderMembers(io::Printer* printer) const = 0;
  virtual void GenerateInitializationCode(io::Printer* printer) const = 0;
  virtual void GenerateFieldInfo(io::Printer* printer,
                                 std::vector<uint16_t>* output) const = 0;
  virtual void GenerateKotlinDslMembers(io::Printer* printer) const = 0;

  virtual TProtoStringType GetBoxedType() const = 0;
};


// Convenience class which constructs FieldGenerators for a Descriptor.
template <typename FieldGeneratorType>
class FieldGeneratorMap {
 public:
  explicit FieldGeneratorMap(const Descriptor* descriptor, Context* context);
  FieldGeneratorMap(const FieldGeneratorMap&) = delete;
  FieldGeneratorMap& operator=(const FieldGeneratorMap&) = delete;
  ~FieldGeneratorMap();

  const FieldGeneratorType& get(const FieldDescriptor* field) const;

 private:
  const Descriptor* descriptor_;
  std::vector<std::unique_ptr<FieldGeneratorType>> field_generators_;
};

template <typename FieldGeneratorType>
inline const FieldGeneratorType& FieldGeneratorMap<FieldGeneratorType>::get(
    const FieldDescriptor* field) const {
  Y_ABSL_CHECK_EQ(field->containing_type(), descriptor_);
  return *field_generators_[field->index()];
}

// Instantiate template for mutable and immutable maps.
template <>
FieldGeneratorMap<ImmutableFieldGenerator>::FieldGeneratorMap(
    const Descriptor* descriptor, Context* context);

template <>
FieldGeneratorMap<ImmutableFieldGenerator>::~FieldGeneratorMap();


template <>
FieldGeneratorMap<ImmutableFieldLiteGenerator>::FieldGeneratorMap(
    const Descriptor* descriptor, Context* context);

template <>
FieldGeneratorMap<ImmutableFieldLiteGenerator>::~FieldGeneratorMap();


// Field information used in FieldGenerators.
struct FieldGeneratorInfo {
  TProtoStringType name;
  TProtoStringType capitalized_name;
  TProtoStringType disambiguated_reason;
};

// Oneof information used in OneofFieldGenerators.
struct OneofGeneratorInfo {
  TProtoStringType name;
  TProtoStringType capitalized_name;
};

// Set some common variables used in variable FieldGenerators.
void SetCommonFieldVariables(
    const FieldDescriptor* descriptor, const FieldGeneratorInfo* info,
    y_absl::flat_hash_map<y_absl::string_view, TProtoStringType>* variables);

// Set some common oneof variables used in OneofFieldGenerators.
void SetCommonOneofVariables(
    const FieldDescriptor* descriptor, const OneofGeneratorInfo* info,
    y_absl::flat_hash_map<y_absl::string_view, TProtoStringType>* variables);

// Print useful comments before a field's accessors.
void PrintExtraFieldInfo(
    const y_absl::flat_hash_map<y_absl::string_view, TProtoStringType>& variables,
    io::Printer* printer);

}  // namespace java
}  // namespace compiler
}  // namespace protobuf
}  // namespace google

#endif  // GOOGLE_PROTOBUF_COMPILER_JAVA_FIELD_H__
