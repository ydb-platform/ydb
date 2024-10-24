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

#include "google/protobuf/compiler/java/context.h"

#include <string>

#include "y_absl/log/absl_log.h"
#include "y_absl/strings/str_cat.h"
#include "y_absl/strings/string_view.h"
#include "y_absl/strings/strip.h"
#include "google/protobuf/compiler/java/field.h"
#include "google/protobuf/compiler/java/helpers.h"
#include "google/protobuf/compiler/java/name_resolver.h"
#include "google/protobuf/descriptor.h"

namespace google {
namespace protobuf {
namespace compiler {
namespace java {

Context::Context(const FileDescriptor* file, const Options& options)
    : name_resolver_(new ClassNameResolver(options)), options_(options) {
  InitializeFieldGeneratorInfo(file);
}

Context::~Context() {}

ClassNameResolver* Context::GetNameResolver() const {
  return name_resolver_.get();
}

namespace {
bool EqualWithSuffix(y_absl::string_view name1, y_absl::string_view suffix,
                     y_absl::string_view name2) {
  if (!y_absl::ConsumeSuffix(&name2, suffix)) return false;
  return name1 == name2;
}

// Whether two fields have conflicting accessors (assuming name1 and name2
// are different). name1 and name2 are field1 and field2's camel-case name
// respectively.
bool IsConflicting(const FieldDescriptor* field1, y_absl::string_view name1,
                   const FieldDescriptor* field2, y_absl::string_view name2,
                   TProtoStringType* info) {
  if (field1->is_repeated()) {
    if (field2->is_repeated()) {
      // Both fields are repeated.
      return false;
    } else {
      // field1 is repeated, and field2 is not.
      if (EqualWithSuffix(name1, "Count", name2)) {
        *info = y_absl::StrCat("both repeated field \"", field1->name(),
                             "\" and singular ", "field \"", field2->name(),
                             "\" generate the method \"", "get", name1,
                             "Count()\"");
        return true;
      }
      if (EqualWithSuffix(name1, "List", name2)) {
        *info =
            y_absl::StrCat("both repeated field \"", field1->name(),
                         "\" and singular ", "field \"", field2->name(),
                         "\" generate the method \"", "get", name1, "List()\"");
        return true;
      }
      // Well, there are obviously many more conflicting cases, but it probably
      // doesn't worth the effort to exhaust all of them because they rarely
      // happen and as we are continuing adding new methods/changing existing
      // methods the number of different conflicting cases will keep growing.
      // We can just add more cases here when they are found in the real world.
      return false;
    }
  } else {
    if (field2->is_repeated()) {
      return IsConflicting(field2, name2, field1, name1, info);
    } else {
      // None of the two fields are repeated.
      return false;
    }
  }
}
}  // namespace

void Context::InitializeFieldGeneratorInfo(const FileDescriptor* file) {
  for (int i = 0; i < file->message_type_count(); ++i) {
    InitializeFieldGeneratorInfoForMessage(file->message_type(i));
  }
}

void Context::InitializeFieldGeneratorInfoForMessage(
    const Descriptor* message) {
  for (int i = 0; i < message->nested_type_count(); ++i) {
    InitializeFieldGeneratorInfoForMessage(message->nested_type(i));
  }
  std::vector<const FieldDescriptor*> fields;
  fields.reserve(message->field_count());
  for (int i = 0; i < message->field_count(); ++i) {
    fields.push_back(message->field(i));
  }
  InitializeFieldGeneratorInfoForFields(fields);

  for (int i = 0; i < message->oneof_decl_count(); ++i) {
    const OneofDescriptor* oneof = message->oneof_decl(i);
    OneofGeneratorInfo info;
    info.name = UnderscoresToCamelCase(oneof->name(), false);
    info.capitalized_name = UnderscoresToCamelCase(oneof->name(), true);
    oneof_generator_info_map_[oneof] = info;
  }
}

void Context::InitializeFieldGeneratorInfoForFields(
    const std::vector<const FieldDescriptor*>& fields) {
  // Find out all fields that conflict with some other field in the same
  // message.
  std::vector<bool> is_conflict(fields.size());
  std::vector<TProtoStringType> conflict_reason(fields.size());
  for (int i = 0; i < fields.size(); ++i) {
    const FieldDescriptor* field = fields[i];
    const TProtoStringType& name = CapitalizedFieldName(field);
    for (int j = i + 1; j < fields.size(); ++j) {
      const FieldDescriptor* other = fields[j];
      const TProtoStringType& other_name = CapitalizedFieldName(other);
      if (name == other_name) {
        is_conflict[i] = is_conflict[j] = true;
        conflict_reason[i] = conflict_reason[j] =
            y_absl::StrCat("capitalized name of field \"", field->name(),
                         "\" conflicts with field \"", other->name(), "\"");
      } else if (IsConflicting(field, name, other, other_name,
                               &conflict_reason[j])) {
        is_conflict[i] = is_conflict[j] = true;
        conflict_reason[i] = conflict_reason[j];
      }
    }
    if (is_conflict[i]) {
      Y_ABSL_LOG(WARNING) << "field \"" << field->full_name()
                        << "\" is conflicting "
                        << "with another field: " << conflict_reason[i];
    }
  }
  for (int i = 0; i < fields.size(); ++i) {
    const FieldDescriptor* field = fields[i];
    FieldGeneratorInfo info;
    info.name = CamelCaseFieldName(field);
    info.capitalized_name = CapitalizedFieldName(field);
    // For fields conflicting with some other fields, we append the field
    // number to their field names in generated code to avoid conflicts.
    if (is_conflict[i]) {
      y_absl::StrAppend(&info.name, field->number());
      y_absl::StrAppend(&info.capitalized_name, field->number());
      info.disambiguated_reason = conflict_reason[i];
    }
    field_generator_info_map_[field] = info;
  }
}

const FieldGeneratorInfo* Context::GetFieldGeneratorInfo(
    const FieldDescriptor* field) const {
  auto it = field_generator_info_map_.find(field);
  if (it == field_generator_info_map_.end()) {
    Y_ABSL_LOG(FATAL) << "Can not find FieldGeneratorInfo for field: "
                    << field->full_name();
  }
  return &it->second;
}

const OneofGeneratorInfo* Context::GetOneofGeneratorInfo(
    const OneofDescriptor* oneof) const {
  auto it = oneof_generator_info_map_.find(oneof);
  if (it == oneof_generator_info_map_.end()) {
    Y_ABSL_LOG(FATAL) << "Can not find OneofGeneratorInfo for oneof: "
                    << oneof->name();
  }
  return &it->second;
}

// Does this message class have generated parsing, serialization, and other
// standard methods for which reflection-based fallback implementations exist?
bool Context::HasGeneratedMethods(const Descriptor* descriptor) const {
  return options_.enforce_lite ||
         descriptor->file()->options().optimize_for() != FileOptions::CODE_SIZE;
}

}  // namespace java
}  // namespace compiler
}  // namespace protobuf
}  // namespace google
