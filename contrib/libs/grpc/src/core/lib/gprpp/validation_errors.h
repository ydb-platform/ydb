// Copyright 2020 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef GRPC_CORE_LIB_GPRPP_VALIDATION_ERRORS_H
#define GRPC_CORE_LIB_GPRPP_VALIDATION_ERRORS_H

#include <grpc/support/port_platform.h>

#include <stddef.h>

#include <map>
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <vector>

#include "y_absl/status/status.h"
#include "y_absl/strings/string_view.h"

namespace grpc_core {

// Tracks errors that occur during validation of a data structure (e.g.,
// a JSON object or protobuf message).  Errors are tracked based on
// which field they are associated with.  If at least one error occurs
// during validation, the validation failed.
//
// Example usage:
//
// y_absl::StatusOr<TString> GetFooBar(const Json::Object& json) {
//   ValidationErrors errors;
//   {
//     ValidationErrors::ScopedField field("foo");
//     auto it = json.object_value().find("foo");
//     if (it == json.object_value().end()) {
//       errors.AddError("field not present");
//     } else if (it->second.type() != Json::Type::OBJECT) {
//       errors.AddError("must be a JSON object");
//     } else {
//       const Json& foo = it->second;
//       ValidationErrors::ScopedField field(".bar");
//       auto it = foo.object_value().find("bar");
//       if (it == json.object_value().end()) {
//         errors.AddError("field not present");
//       } else if (it->second.type() != Json::Type::STRING) {
//         errors.AddError("must be a JSON string");
//       } else {
//         return it->second.string_value();
//       }
//     }
//   }
//   return errors.status("errors validating foo.bar");
// }
class ValidationErrors {
 public:
  // Pushes a field name onto the stack at construction and pops it off
  // of the stack at destruction.
  class ScopedField {
   public:
    ScopedField(ValidationErrors* errors, y_absl::string_view field_name)
        : errors_(errors) {
      errors_->PushField(field_name);
    }
    ~ScopedField() { errors_->PopField(); }

   private:
    ValidationErrors* errors_;
  };

  // Records that we've encountered an error associated with the current
  // field.
  void AddError(y_absl::string_view error) GPR_ATTRIBUTE_NOINLINE;

  // Returns true if the current field has errors.
  bool FieldHasErrors() const GPR_ATTRIBUTE_NOINLINE;

  // Returns the resulting status of parsing.
  y_absl::Status status(y_absl::string_view prefix) const;

  // Returns true if there are no errors.
  bool ok() const { return field_errors_.empty(); }

  size_t size() const { return field_errors_.size(); }

 private:
  // Pushes a field name onto the stack.
  void PushField(y_absl::string_view ext) GPR_ATTRIBUTE_NOINLINE;
  // Pops a field name off of the stack.
  void PopField() GPR_ATTRIBUTE_NOINLINE;

  // Errors that we have encountered so far, keyed by field name.
  // TODO(roth): If we don't actually have any fields for which we
  // report more than one error, simplify this data structure.
  std::map<TString /*field_name*/, std::vector<TString>> field_errors_;
  // Stack of field names indicating the field that we are currently
  // validating.
  std::vector<TString> fields_;
};

}  // namespace grpc_core

#endif  // GRPC_CORE_LIB_GPRPP_VALIDATION_ERRORS_H
