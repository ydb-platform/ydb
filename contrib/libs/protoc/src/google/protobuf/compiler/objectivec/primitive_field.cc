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

#include "google/protobuf/compiler/objectivec/primitive_field.h"

#include <string>

#include "y_absl/container/flat_hash_map.h"
#include "y_absl/log/absl_log.h"
#include "y_absl/strings/str_cat.h"
#include "google/protobuf/compiler/objectivec/helpers.h"
#include "google/protobuf/io/printer.h"

namespace google {
namespace protobuf {
namespace compiler {
namespace objectivec {

namespace {

const char* PrimitiveTypeName(const FieldDescriptor* descriptor) {
  ObjectiveCType type = GetObjectiveCType(descriptor);
  switch (type) {
    case OBJECTIVECTYPE_INT32:
      return "arc_i32";
    case OBJECTIVECTYPE_UINT32:
      return "arc_ui32";
    case OBJECTIVECTYPE_INT64:
      return "arc_i64";
    case OBJECTIVECTYPE_UINT64:
      return "arc_ui64";
    case OBJECTIVECTYPE_FLOAT:
      return "float";
    case OBJECTIVECTYPE_DOUBLE:
      return "double";
    case OBJECTIVECTYPE_BOOLEAN:
      return "BOOL";
    case OBJECTIVECTYPE_STRING:
      return "NSString";
    case OBJECTIVECTYPE_DATA:
      return "NSData";
    case OBJECTIVECTYPE_ENUM:
      return "arc_i32";
    case OBJECTIVECTYPE_MESSAGE:
      return nullptr;  // Messages go through message_field.cc|h.
  }

  // Some compilers report reaching end of function even though all cases of
  // the enum are handed in the switch.
  Y_ABSL_LOG(FATAL) << "Can't get here.";
  return nullptr;
}

const char* PrimitiveArrayTypeName(const FieldDescriptor* descriptor) {
  ObjectiveCType type = GetObjectiveCType(descriptor);
  switch (type) {
    case OBJECTIVECTYPE_INT32:
      return "Int32";
    case OBJECTIVECTYPE_UINT32:
      return "UInt32";
    case OBJECTIVECTYPE_INT64:
      return "Int64";
    case OBJECTIVECTYPE_UINT64:
      return "UInt64";
    case OBJECTIVECTYPE_FLOAT:
      return "Float";
    case OBJECTIVECTYPE_DOUBLE:
      return "Double";
    case OBJECTIVECTYPE_BOOLEAN:
      return "Bool";
    case OBJECTIVECTYPE_STRING:
      return "";  // Want NSArray
    case OBJECTIVECTYPE_DATA:
      return "";  // Want NSArray
    case OBJECTIVECTYPE_ENUM:
      return "Enum";
    case OBJECTIVECTYPE_MESSAGE:
      // Want NSArray (but goes through message_field.cc|h).
      return "";
  }

  // Some compilers report reaching end of function even though all cases of
  // the enum are handed in the switch.
  Y_ABSL_LOG(FATAL) << "Can't get here.";
  return nullptr;
}

void SetPrimitiveVariables(
    const FieldDescriptor* descriptor,
    y_absl::flat_hash_map<y_absl::string_view, TProtoStringType>* variables) {
  TProtoStringType primitive_name = PrimitiveTypeName(descriptor);
  (*variables)["type"] = primitive_name;
  (*variables)["storage_type"] = primitive_name;
}

}  // namespace

PrimitiveFieldGenerator::PrimitiveFieldGenerator(
    const FieldDescriptor* descriptor)
    : SingleFieldGenerator(descriptor) {
  SetPrimitiveVariables(descriptor, &variables_);
}

void PrimitiveFieldGenerator::GenerateFieldStorageDeclaration(
    io::Printer* printer) const {
  if (GetObjectiveCType(descriptor_) == OBJECTIVECTYPE_BOOLEAN) {
    // Nothing, BOOLs are stored in the has bits.
  } else {
    SingleFieldGenerator::GenerateFieldStorageDeclaration(printer);
  }
}

int PrimitiveFieldGenerator::ExtraRuntimeHasBitsNeeded() const {
  if (GetObjectiveCType(descriptor_) == OBJECTIVECTYPE_BOOLEAN) {
    // Reserve a bit for the storage of the boolean.
    return 1;
  }
  return 0;
}

void PrimitiveFieldGenerator::SetExtraRuntimeHasBitsBase(int index_base) {
  if (GetObjectiveCType(descriptor_) == OBJECTIVECTYPE_BOOLEAN) {
    // Set into the offset the has bit to use for the actual value.
    variables_["storage_offset_value"] = y_absl::StrCat(index_base);
    variables_["storage_offset_comment"] =
        "  // Stored in _has_storage_ to save space.";
  }
}

PrimitiveObjFieldGenerator::PrimitiveObjFieldGenerator(
    const FieldDescriptor* descriptor)
    : ObjCObjFieldGenerator(descriptor) {
  SetPrimitiveVariables(descriptor, &variables_);
  variables_["property_storage_attribute"] = "copy";
}

RepeatedPrimitiveFieldGenerator::RepeatedPrimitiveFieldGenerator(
    const FieldDescriptor* descriptor)
    : RepeatedFieldGenerator(descriptor) {
  SetPrimitiveVariables(descriptor, &variables_);

  TProtoStringType base_name = PrimitiveArrayTypeName(descriptor);
  if (base_name.length()) {
    variables_["array_storage_type"] = y_absl::StrCat("GPB", base_name, "Array");
  } else {
    TProtoStringType storage_type = variables_["storage_type"];
    variables_["array_storage_type"] = "NSMutableArray";
    variables_["array_property_type"] =
        y_absl::StrCat("NSMutableArray<", storage_type, "*>");
  }
}

}  // namespace objectivec
}  // namespace compiler
}  // namespace protobuf
}  // namespace google
