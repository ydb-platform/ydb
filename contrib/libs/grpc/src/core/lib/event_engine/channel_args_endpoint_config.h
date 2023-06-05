// Copyright 2021 The gRPC Authors
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
#ifndef GRPC_CORE_LIB_EVENT_ENGINE_CHANNEL_ARGS_ENDPOINT_CONFIG_H
#define GRPC_CORE_LIB_EVENT_ENGINE_CHANNEL_ARGS_ENDPOINT_CONFIG_H

#include <grpc/support/port_platform.h>

#include "y_absl/strings/string_view.h"
#include "y_absl/types/optional.h"

#include <grpc/event_engine/endpoint_config.h>

#include "src/core/lib/channel/channel_args.h"

namespace grpc_event_engine {
namespace experimental {

class ChannelArgsEndpointConfig : public EndpointConfig {
 public:
  ChannelArgsEndpointConfig() = default;
  explicit ChannelArgsEndpointConfig(const grpc_core::ChannelArgs& args)
      : args_(args) {}
  ChannelArgsEndpointConfig(const ChannelArgsEndpointConfig& config) = default;
  ChannelArgsEndpointConfig& operator=(const ChannelArgsEndpointConfig& other) =
      default;
  y_absl::optional<int> GetInt(y_absl::string_view key) const override;
  y_absl::optional<y_absl::string_view> GetString(
      y_absl::string_view key) const override;
  void* GetVoidPointer(y_absl::string_view key) const override;

 private:
  grpc_core::ChannelArgs args_;
};

}  // namespace experimental
}  // namespace grpc_event_engine

#endif  // GRPC_CORE_LIB_EVENT_ENGINE_CHANNEL_ARGS_ENDPOINT_CONFIG_H
