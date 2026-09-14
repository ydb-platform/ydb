//
//
// Copyright 2023 gRPC authors.
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
//
//

#ifndef GRPC_GRPC_AUDIT_LOGGING_H
#define GRPC_GRPC_AUDIT_LOGGING_H

#include <grpc/support/port_platform.h>

#include <memory>
#include <util/generic/string.h>
#include <util/string/cast.h>

#include "y_absl/status/statusor.h"
#include "y_absl/strings/string_view.h"

#include <grpc/support/json.h>

namespace grpc_core {
namespace experimental {

// The class containing the context for an audited RPC.
class AuditContext {
 public:
  AuditContext(y_absl::string_view rpc_method, y_absl::string_view principal,
               y_absl::string_view policy_name, y_absl::string_view matched_rule,
               bool authorized)
      : rpc_method_(rpc_method),
        principal_(principal),
        policy_name_(policy_name),
        matched_rule_(matched_rule),
        authorized_(authorized) {}

  y_absl::string_view rpc_method() const { return rpc_method_; }
  y_absl::string_view principal() const { return principal_; }
  y_absl::string_view policy_name() const { return policy_name_; }
  y_absl::string_view matched_rule() const { return matched_rule_; }
  bool authorized() const { return authorized_; }

 private:
  y_absl::string_view rpc_method_;
  y_absl::string_view principal_;
  y_absl::string_view policy_name_;
  y_absl::string_view matched_rule_;
  bool authorized_;
};

// This base class for audit logger implementations.
class AuditLogger {
 public:
  virtual ~AuditLogger() = default;
  virtual y_absl::string_view name() const = 0;
  virtual void Log(const AuditContext& audit_context) = 0;
};

// This is the base class for audit logger factory implementations.
class AuditLoggerFactory {
 public:
  class Config {
   public:
    virtual ~Config() = default;
    virtual y_absl::string_view name() const = 0;
    virtual TString ToString() const = 0;
  };

  virtual ~AuditLoggerFactory() = default;
  virtual y_absl::string_view name() const = 0;

  virtual y_absl::StatusOr<std::unique_ptr<Config>> ParseAuditLoggerConfig(
      const Json& json) = 0;

  virtual std::unique_ptr<AuditLogger> CreateAuditLogger(
      std::unique_ptr<AuditLoggerFactory::Config>) = 0;
};

// Registers an audit logger factory. This should only be called during
// initialization.
void RegisterAuditLoggerFactory(std::unique_ptr<AuditLoggerFactory> factory);

}  // namespace experimental
}  // namespace grpc_core

#endif  // GRPC_GRPC_AUDIT_LOGGING_H
