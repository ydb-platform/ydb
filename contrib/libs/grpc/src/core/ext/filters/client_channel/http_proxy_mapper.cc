//
//
// Copyright 2016 gRPC authors.
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

#include <grpc/support/port_platform.h>

#include "src/core/ext/filters/client_channel/http_proxy_mapper.h"

#include <stdint.h>
#include <string.h>

#include <memory>
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <utility>

#include "y_absl/status/status.h"
#include "y_absl/status/statusor.h"
#include "y_absl/strings/ascii.h"
#include "y_absl/strings/match.h"
#include "y_absl/strings/numbers.h"
#include "y_absl/strings/str_cat.h"
#include "y_absl/strings/str_split.h"
#include "y_absl/strings/string_view.h"
#include "y_absl/strings/strip.h"
#include "y_absl/types/optional.h"

#include <grpc/impl/channel_arg_names.h>
#include <grpc/support/alloc.h>
#include <grpc/support/log.h>

#include "src/core/lib/address_utils/parse_address.h"
#include "src/core/lib/address_utils/sockaddr_utils.h"
#include "src/core/lib/channel/channel_args.h"
#include "src/core/lib/gpr/string.h"
#include "src/core/lib/gprpp/env.h"
#include "src/core/lib/gprpp/host_port.h"
#include "src/core/lib/gprpp/memory.h"
#include "src/core/lib/iomgr/resolve_address.h"
#include "src/core/lib/slice/b64.h"
#include "src/core/lib/transport/http_connect_handshaker.h"
#include "src/core/lib/uri/uri_parser.h"

namespace grpc_core {
namespace {

bool ServerInCIDRRange(const grpc_resolved_address& server_address,
                       y_absl::string_view cidr_range) {
  std::pair<y_absl::string_view, y_absl::string_view> possible_cidr =
      y_absl::StrSplit(cidr_range, y_absl::MaxSplits('/', 1), y_absl::SkipEmpty());
  if (possible_cidr.first.empty() || possible_cidr.second.empty()) {
    return false;
  }
  auto proxy_address = StringToSockaddr(possible_cidr.first, 0);
  if (!proxy_address.ok()) {
    return false;
  }
  uint32_t mask_bits = 0;
  if (y_absl::SimpleAtoi(possible_cidr.second, &mask_bits)) {
    grpc_sockaddr_mask_bits(&*proxy_address, mask_bits);
    return grpc_sockaddr_match_subnet(&server_address, &*proxy_address,
                                      mask_bits);
  }
  return false;
}

bool ExactMatchOrSubdomain(y_absl::string_view host_name,
                           y_absl::string_view host_name_or_domain) {
  return y_absl::EndsWithIgnoreCase(host_name, host_name_or_domain);
}

// Parses the list of host names, addresses or subnet masks and returns true if
// the target address or host matches any value.
bool AddressIncluded(
    const y_absl::optional<grpc_resolved_address>& target_address,
    y_absl::string_view host_name, y_absl::string_view addresses_and_subnets) {
  for (y_absl::string_view entry :
       y_absl::StrSplit(addresses_and_subnets, ',', y_absl::SkipEmpty())) {
    y_absl::string_view sanitized_entry = y_absl::StripAsciiWhitespace(entry);
    if (ExactMatchOrSubdomain(host_name, sanitized_entry) ||
        (target_address.has_value() &&
         ServerInCIDRRange(*target_address, sanitized_entry))) {
      return true;
    }
  }
  return false;
}

///
/// Parses the 'https_proxy' env var (fallback on 'http_proxy') and returns the
/// proxy hostname to resolve or nullopt on error. Also sets 'user_cred' to user
/// credentials if present in the 'http_proxy' env var, otherwise leaves it
/// unchanged.
///
y_absl::optional<TString> GetHttpProxyServer(
    const ChannelArgs& args, y_absl::optional<TString>* user_cred) {
  GPR_ASSERT(user_cred != nullptr);
  y_absl::StatusOr<URI> uri;
  // We check the following places to determine the HTTP proxy to use, stopping
  // at the first one that is set:
  // 1. GRPC_ARG_HTTP_PROXY channel arg
  // 2. grpc_proxy environment variable
  // 3. https_proxy environment variable
  // 4. http_proxy environment variable
  // If none of the above are set, then no HTTP proxy will be used.
  //
  y_absl::optional<TString> uri_str =
      args.GetOwnedString(GRPC_ARG_HTTP_PROXY);
  if (!uri_str.has_value()) uri_str = GetEnv("grpc_proxy");
  if (!uri_str.has_value()) uri_str = GetEnv("https_proxy");
  if (!uri_str.has_value()) uri_str = GetEnv("http_proxy");
  if (!uri_str.has_value()) return y_absl::nullopt;
  // an empty value means "don't use proxy"
  if (uri_str->empty()) return y_absl::nullopt;
  uri = URI::Parse(*uri_str);
  if (!uri.ok() || uri->authority().empty()) {
    gpr_log(GPR_ERROR, "cannot parse value of 'http_proxy' env var. Error: %s",
            uri.status().ToString().c_str());
    return y_absl::nullopt;
  }
  if (uri->scheme() != "http") {
    gpr_log(GPR_ERROR, "'%s' scheme not supported in proxy URI",
            uri->scheme().c_str());
    return y_absl::nullopt;
  }
  // Split on '@' to separate user credentials from host
  char** authority_strs = nullptr;
  size_t authority_nstrs;
  gpr_string_split(uri->authority().c_str(), "@", &authority_strs,
                   &authority_nstrs);
  GPR_ASSERT(authority_nstrs != 0);  // should have at least 1 string
  y_absl::optional<TString> proxy_name;
  if (authority_nstrs == 1) {
    // User cred not present in authority
    proxy_name = authority_strs[0];
  } else if (authority_nstrs == 2) {
    // User cred found
    *user_cred = authority_strs[0];
    proxy_name = authority_strs[1];
    gpr_log(GPR_DEBUG, "userinfo found in proxy URI");
  } else {
    // Bad authority
    proxy_name = y_absl::nullopt;
  }
  for (size_t i = 0; i < authority_nstrs; i++) {
    gpr_free(authority_strs[i]);
  }
  gpr_free(authority_strs);
  return proxy_name;
}

// Adds the default port if target does not contain a port.
TString MaybeAddDefaultPort(y_absl::string_view target) {
  y_absl::string_view host;
  y_absl::string_view port;
  SplitHostPort(target, &host, &port);
  if (port.empty()) {
    return JoinHostPort(host, kDefaultSecurePortInt);
  }
  return TString(target);
}

y_absl::optional<TString> GetChannelArgOrEnvVarValue(
    const ChannelArgs& args, y_absl::string_view channel_arg,
    const char* env_var) {
  auto arg_value = args.GetOwnedString(channel_arg);
  if (arg_value.has_value()) {
    return arg_value;
  }
  return GetEnv(env_var);
}

y_absl::optional<grpc_resolved_address> GetAddressProxyServer(
    const ChannelArgs& args) {
  auto address_value = GetChannelArgOrEnvVarValue(
      args, GRPC_ARG_ADDRESS_HTTP_PROXY, HttpProxyMapper::kAddressProxyEnvVar);
  if (!address_value.has_value()) {
    return y_absl::nullopt;
  }
  auto address = StringToSockaddr(*address_value);
  if (!address.ok()) {
    gpr_log(GPR_ERROR, "cannot parse value of '%s' env var. Error: %s",
            HttpProxyMapper::kAddressProxyEnvVar,
            address.status().ToString().c_str());
    return y_absl::nullopt;
  }
  return *address;
}

}  // namespace

y_absl::optional<TString> HttpProxyMapper::MapName(
    y_absl::string_view server_uri, ChannelArgs* args) {
  if (!args->GetBool(GRPC_ARG_ENABLE_HTTP_PROXY).value_or(true)) {
    return y_absl::nullopt;
  }
  y_absl::optional<TString> user_cred;
  auto name_to_resolve = GetHttpProxyServer(*args, &user_cred);
  if (!name_to_resolve.has_value()) return name_to_resolve;
  y_absl::StatusOr<URI> uri = URI::Parse(server_uri);
  if (!uri.ok() || uri->path().empty()) {
    gpr_log(GPR_ERROR,
            "'http_proxy' environment variable set, but cannot "
            "parse server URI '%s' -- not using proxy. Error: %s",
            TString(server_uri).c_str(), uri.status().ToString().c_str());
    return y_absl::nullopt;
  }
  if (uri->scheme() == "unix") {
    gpr_log(GPR_INFO, "not using proxy for Unix domain socket '%s'",
            TString(server_uri).c_str());
    return y_absl::nullopt;
  }
  if (uri->scheme() == "vsock") {
    gpr_log(GPR_INFO, "not using proxy for VSock '%s'",
            TString(server_uri).c_str());
    return y_absl::nullopt;
  }
  // Prefer using 'no_grpc_proxy'. Fallback on 'no_proxy' if it is not set.
  auto no_proxy_str = GetEnv("no_grpc_proxy");
  if (!no_proxy_str.has_value()) {
    no_proxy_str = GetEnv("no_proxy");
  }
  if (no_proxy_str.has_value()) {
    TString server_host;
    TString server_port;
    if (!SplitHostPort(y_absl::StripPrefix(uri->path(), "/"), &server_host,
                       &server_port)) {
      gpr_log(GPR_INFO,
              "unable to split host and port, not checking no_proxy list for "
              "host '%s'",
              TString(server_uri).c_str());
    } else {
      auto address = StringToSockaddr(server_host, 0);
      if (AddressIncluded(address.ok()
                              ? y_absl::optional<grpc_resolved_address>(*address)
                              : y_absl::nullopt,
                          server_host, *no_proxy_str)) {
        gpr_log(GPR_INFO, "not using proxy for host in no_proxy list '%s'",
                TString(server_uri).c_str());
        return y_absl::nullopt;
      }
    }
  }
  *args = args->Set(GRPC_ARG_HTTP_CONNECT_SERVER,
                    MaybeAddDefaultPort(y_absl::StripPrefix(uri->path(), "/")));
  if (user_cred.has_value()) {
    // Use base64 encoding for user credentials as stated in RFC 7617
    auto encoded_user_cred = UniquePtr<char>(
        grpc_base64_encode(user_cred->data(), user_cred->length(), 0, 0));
    *args = args->Set(
        GRPC_ARG_HTTP_CONNECT_HEADERS,
        y_absl::StrCat("Proxy-Authorization:Basic ", encoded_user_cred.get()));
  }
  return name_to_resolve;
}

y_absl::optional<grpc_resolved_address> HttpProxyMapper::MapAddress(
    const grpc_resolved_address& address, ChannelArgs* args) {
  auto proxy_address = GetAddressProxyServer(*args);
  if (!proxy_address.has_value()) {
    return y_absl::nullopt;
  }
  auto address_string = grpc_sockaddr_to_string(&address, true);
  if (!address_string.ok()) {
    gpr_log(GPR_ERROR, "Unable to convert address to string: %s",
            TString(address_string.status().message()).c_str());
    return y_absl::nullopt;
  }
  TString host_name, port;
  if (!SplitHostPort(*address_string, &host_name, &port)) {
    gpr_log(GPR_ERROR, "Address %s cannot be split in host and port",
            address_string->c_str());
    return y_absl::nullopt;
  }
  auto enabled_addresses = GetChannelArgOrEnvVarValue(
      *args, GRPC_ARG_ADDRESS_HTTP_PROXY_ENABLED_ADDRESSES,
      kAddressProxyEnabledAddressesEnvVar);
  if (!enabled_addresses.has_value() ||
      !AddressIncluded(address, host_name, *enabled_addresses)) {
    return y_absl::nullopt;
  }
  *args = args->Set(GRPC_ARG_HTTP_CONNECT_SERVER, *address_string);
  return proxy_address;
}

void RegisterHttpProxyMapper(CoreConfiguration::Builder* builder) {
  builder->proxy_mapper_registry()->Register(
      true /* at_start */,
      std::unique_ptr<ProxyMapperInterface>(new HttpProxyMapper()));
}

}  // namespace grpc_core
