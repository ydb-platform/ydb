// Copyright 2022 gRPC authors.
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

#ifndef GRPC_SRC_CORE_LIB_CONFIG_LOAD_CONFIG_H
#define GRPC_SRC_CORE_LIB_CONFIG_LOAD_CONFIG_H

#include <grpc/support/port_platform.h>

#include <stdint.h>

#include <util/generic/string.h>
#include <util/string/cast.h>

#include "y_absl/flags/flag.h"
#include "y_absl/strings/string_view.h"
#include "y_absl/types/optional.h"

namespace grpc_core {

TString LoadConfigFromEnv(y_absl::string_view environment_variable,
                              const char* default_value);
int32_t LoadConfigFromEnv(y_absl::string_view environment_variable,
                          int32_t default_value);
bool LoadConfigFromEnv(y_absl::string_view environment_variable,
                       bool default_value);

template <typename T, typename D>
T LoadConfig(const y_absl::Flag<y_absl::optional<T>>& flag,
             y_absl::string_view environment_variable,
             const y_absl::optional<T>& override, D default_value) {
  if (override.has_value()) return *override;
  auto from_flag = y_absl::GetFlag(flag);
  if (from_flag.has_value()) return std::move(*from_flag);
  return LoadConfigFromEnv(environment_variable, default_value);
}

}  // namespace grpc_core

#endif  // GRPC_SRC_CORE_LIB_CONFIG_LOAD_CONFIG_H
