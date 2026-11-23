// Copyright 2023 The gRPC Authors.
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

#include <grpc/support/port_platform.h>

#include "src/core/lib/iomgr/port.h"

#ifdef GRPC_POSIX_SOCKET_RESOLVE_ADDRESS

#include <netdb.h>
#include <string.h>
#include <sys/socket.h>

#include <util/generic/string.h>
#include <util/string/cast.h>
#include <type_traits>
#include <utility>
#include <vector>

#include "y_absl/functional/any_invocable.h"
#include "y_absl/status/status.h"
#include "y_absl/status/statusor.h"
#include "y_absl/strings/str_cat.h"
#include "y_absl/strings/str_format.h"

#include "src/core/lib/event_engine/posix_engine/native_posix_dns_resolver.h"
#include "src/core/lib/gpr/useful.h"
#include "src/core/lib/gprpp/host_port.h"

namespace grpc_event_engine {
namespace experimental {
namespace {

y_absl::StatusOr<std::vector<EventEngine::ResolvedAddress>>
LookupHostnameBlocking(y_absl::string_view name, y_absl::string_view default_port) {
  struct addrinfo hints;
  struct addrinfo *result = nullptr, *resp;
  TString host;
  TString port;
  // parse name, splitting it into host and port parts
  grpc_core::SplitHostPort(name, &host, &port);
  if (host.empty()) {
    return y_absl::InvalidArgumentError(y_absl::StrCat("Unparseable name: ", name));
  }
  if (port.empty()) {
    if (default_port.empty()) {
      return y_absl::InvalidArgumentError(
          y_absl::StrFormat("No port in name %s or default_port argument", name));
    }
    port = TString(default_port);
  }
  // Call getaddrinfo
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;      // ipv4 or ipv6
  hints.ai_socktype = SOCK_STREAM;  // stream socket
  hints.ai_flags = AI_PASSIVE;      // for wildcard IP address
  int s = getaddrinfo(host.c_str(), port.c_str(), &hints, &result);
  if (s != 0) {
    // Retry if well-known service name is recognized
    const char* svc[][2] = {{"http", "80"}, {"https", "443"}};
    for (size_t i = 0; i < GPR_ARRAY_SIZE(svc); i++) {
      if (port == svc[i][0]) {
        s = getaddrinfo(host.c_str(), svc[i][1], &hints, &result);
        break;
      }
    }
  }
  if (s != 0) {
    return y_absl::UnknownError(y_absl::StrFormat(
        "Address lookup failed for %s os_error: %s syscall: getaddrinfo", name,
        gai_strerror(s)));
  }
  // Success path: fill in addrs
  std::vector<EventEngine::ResolvedAddress> addresses;
  for (resp = result; resp != nullptr; resp = resp->ai_next) {
    addresses.emplace_back(resp->ai_addr, resp->ai_addrlen);
  }
  if (result) {
    freeaddrinfo(result);
  }
  return addresses;
}

}  // namespace

NativePosixDNSResolver::NativePosixDNSResolver(
    std::shared_ptr<EventEngine> event_engine)
    : event_engine_(std::move(event_engine)) {}

void NativePosixDNSResolver::LookupHostname(
    EventEngine::DNSResolver::LookupHostnameCallback on_resolved,
    y_absl::string_view name, y_absl::string_view default_port) {
  event_engine_->Run(
      [name, default_port, on_resolved = std::move(on_resolved)]() mutable {
        on_resolved(LookupHostnameBlocking(name, default_port));
      });
}

void NativePosixDNSResolver::LookupSRV(
    EventEngine::DNSResolver::LookupSRVCallback on_resolved,
    y_absl::string_view /* name */) {
  // Not supported
  event_engine_->Run([on_resolved = std::move(on_resolved)]() mutable {
    on_resolved(y_absl::UnimplementedError(
        "The Native resolver does not support looking up SRV records"));
  });
}

void NativePosixDNSResolver::LookupTXT(
    EventEngine::DNSResolver::LookupTXTCallback on_resolved,
    y_absl::string_view /* name */) {
  // Not supported
  event_engine_->Run([on_resolved = std::move(on_resolved)]() mutable {
    on_resolved(y_absl::UnimplementedError(
        "The Native resolver does not support looking up TXT records"));
  });
}

}  // namespace experimental
}  // namespace grpc_event_engine

#endif  // GRPC_POSIX_SOCKET_RESOLVE_ADDRESS
