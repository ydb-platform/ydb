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

#include "google/protobuf/compiler/objectivec/enum_field.h"

#include <string>

#include "y_absl/container/flat_hash_map.h"
#include "google/protobuf/compiler/objectivec/helpers.h"
#include "google/protobuf/compiler/objectivec/names.h"
#include "google/protobuf/io/printer.h"

namespace google {
namespace protobuf {
namespace compiler {
namespace objectivec {

namespace {

void SetEnumVariables(
    const FieldDescriptor* descriptor,
    y_absl::flat_hash_map<y_absl::string_view, TProtoStringType>* variables) {
  const TProtoStringType type = EnumName(descriptor->enum_type());
  const TProtoStringType enum_desc_func = y_absl::StrCat(type, "_EnumDescriptor");
  (*variables)["storage_type"] = type;
  // For non repeated fields, if it was defined in a different file, the
  // property decls need to use "enum NAME" rather than just "NAME" to support
  // the forward declaration of the enums.
  if (!descriptor->is_repeated() &&
      (descriptor->file() != descriptor->enum_type()->file())) {
    (*variables)["property_type"] = y_absl::StrCat("enum ", type);
  }
  (*variables)["enum_verifier"] = y_absl::StrCat(type, "_IsValidValue");
  (*variables)["enum_desc_func"] = enum_desc_func;

  (*variables)["dataTypeSpecific_name"] = "enumDescFunc";
  (*variables)["dataTypeSpecific_value"] = enum_desc_func;

  const Descriptor* msg_descriptor = descriptor->containing_type();
  (*variables)["owning_message_class"] = ClassName(msg_descriptor);
}
}  // namespace

EnumFieldGenerator::EnumFieldGenerator(const FieldDescriptor* descriptor)
    : SingleFieldGenerator(descriptor) {
  SetEnumVariables(descriptor, &variables_);
}

void EnumFieldGenerator::GenerateCFunctionDeclarations(
    io::Printer* printer) const {
  if (descriptor_->enum_type()->is_closed()) {
    return;
  }

  // clang-format off
  printer->Print(
      variables_,
      "/**\n"
      " * Fetches the raw value of a @c $owning_message_class$'s @c $name$ property, even\n"
      " * if the value was not defined by the enum at the time the code was generated.\n"
      " **/\n"
      "arc_i32 $owning_message_class$_$capitalized_name$_RawValue($owning_message_class$ *message);\n"
      "/**\n"
      " * Sets the raw value of an @c $owning_message_class$'s @c $name$ property, allowing\n"
      " * it to be set to a value that was not defined by the enum at the time the code\n"
      " * was generated.\n"
      " **/\n"
      "void Set$owning_message_class$_$capitalized_name$_RawValue($owning_message_class$ *message, arc_i32 value);\n"
      "\n");
  // clang-format on
}

void EnumFieldGenerator::GenerateCFunctionImplementations(
    io::Printer* printer) const {
  if (descriptor_->enum_type()->is_closed()) {
    return;
  }

  // clang-format off
  printer->Print(
      variables_,
      "arc_i32 $owning_message_class$_$capitalized_name$_RawValue($owning_message_class$ *message) {\n"
      "  GPBDescriptor *descriptor = [$owning_message_class$ descriptor];\n"
      "  GPBFieldDescriptor *field = [descriptor fieldWithNumber:$field_number_name$];\n"
      "  return GPBGetMessageRawEnumField(message, field);\n"
      "}\n"
      "\n"
      "void Set$owning_message_class$_$capitalized_name$_RawValue($owning_message_class$ *message, arc_i32 value) {\n"
      "  GPBDescriptor *descriptor = [$owning_message_class$ descriptor];\n"
      "  GPBFieldDescriptor *field = [descriptor fieldWithNumber:$field_number_name$];\n"
      "  GPBSetMessageRawEnumField(message, field, value);\n"
      "}\n"
      "\n");
  // clang-format on
}

void EnumFieldGenerator::DetermineForwardDeclarations(
    y_absl::btree_set<TProtoStringType>* fwd_decls,
    bool include_external_types) const {
  SingleFieldGenerator::DetermineForwardDeclarations(fwd_decls,
                                                     include_external_types);
  // If it is an enum defined in a different file (and not a WKT), then we'll
  // need a forward declaration for it.  When it is in our file, all the enums
  // are output before the message, so it will be declared before it is needed.
  if (include_external_types &&
      descriptor_->file() != descriptor_->enum_type()->file() &&
      !IsProtobufLibraryBundledProtoFile(descriptor_->enum_type()->file())) {
    // Enum name is already in "storage_type".
    const TProtoStringType& name = variable("storage_type");
    fwd_decls->insert(y_absl::StrCat("GPB_ENUM_FWD_DECLARE(", name, ");"));
  }
}

RepeatedEnumFieldGenerator::RepeatedEnumFieldGenerator(
    const FieldDescriptor* descriptor)
    : RepeatedFieldGenerator(descriptor) {
  SetEnumVariables(descriptor, &variables_);
  variables_["array_storage_type"] = "GPBEnumArray";
}

void RepeatedEnumFieldGenerator::FinishInitialization() {
  RepeatedFieldGenerator::FinishInitialization();
  TProtoStringType name = variables_["name"];
  TProtoStringType storage_type = variables_["storage_type"];
  variables_["array_comment"] =
      y_absl::StrCat("// |", name, "| contains |", storage_type, "|\n");
}

// NOTE: RepeatedEnumFieldGenerator::DetermineForwardDeclarations isn't needed
// because `GPBEnumArray` isn't generic (like `NSArray` would be for messages)
// and thus doesn't reference the type in the header.

}  // namespace objectivec
}  // namespace compiler
}  // namespace protobuf
}  // namespace google
