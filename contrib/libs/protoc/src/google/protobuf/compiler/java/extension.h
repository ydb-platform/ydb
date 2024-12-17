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

#ifndef GOOGLE_PROTOBUF_COMPILER_JAVA_EXTENSION_H__
#define GOOGLE_PROTOBUF_COMPILER_JAVA_EXTENSION_H__

#include <string>

#include "y_absl/container/flat_hash_map.h"
#include "google/protobuf/port.h"

namespace google {
namespace protobuf {
class FieldDescriptor;  // descriptor.h
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

// Generates code for an extension, which may be within the scope of some
// message or may be at file scope.  This is much simpler than FieldGenerator
// since extensions are just simple identifiers with interesting types.
class ExtensionGenerator {
 public:
  explicit ExtensionGenerator() {}
  ExtensionGenerator(const ExtensionGenerator&) = delete;
  ExtensionGenerator& operator=(const ExtensionGenerator&) = delete;
  virtual ~ExtensionGenerator() {}

  virtual void Generate(io::Printer* printer) = 0;

  // Returns an estimate of the number of bytes the printed code will compile
  // to
  virtual int GenerateNonNestedInitializationCode(io::Printer* printer) = 0;

  // Returns an estimate of the number of bytes the printed code will compile
  // to
  virtual int GenerateRegistrationCode(io::Printer* printer) = 0;

 protected:
  static void InitTemplateVars(
      const FieldDescriptor* descriptor, const TProtoStringType& scope,
      bool immutable, ClassNameResolver* name_resolver,
      y_absl::flat_hash_map<y_absl::string_view, TProtoStringType>* vars_pointer,
      Context* context);
};

class ImmutableExtensionGenerator : public ExtensionGenerator {
 public:
  explicit ImmutableExtensionGenerator(const FieldDescriptor* descriptor,
                                       Context* context);
  ImmutableExtensionGenerator(const ImmutableExtensionGenerator&) = delete;
  ImmutableExtensionGenerator& operator=(const ImmutableExtensionGenerator&) =
      delete;
  ~ImmutableExtensionGenerator() override;

  void Generate(io::Printer* printer) override;
  int GenerateNonNestedInitializationCode(io::Printer* printer) override;
  int GenerateRegistrationCode(io::Printer* printer) override;

 protected:
  const FieldDescriptor* descriptor_;
  ClassNameResolver* name_resolver_;
  TProtoStringType scope_;
  Context* context_;
};

}  // namespace java
}  // namespace compiler
}  // namespace protobuf
}  // namespace google

#endif  // GOOGLE_PROTOBUF_COMPILER_JAVA_EXTENSION_H__
