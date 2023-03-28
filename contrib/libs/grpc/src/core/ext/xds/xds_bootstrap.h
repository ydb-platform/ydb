//
// Copyright 2019 gRPC authors.
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

#ifndef GRPC_CORE_EXT_XDS_XDS_BOOTSTRAP_H
#define GRPC_CORE_EXT_XDS_XDS_BOOTSTRAP_H

#include <grpc/support/port_platform.h>

#include <memory>
#include <set>
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <vector>

#include "y_absl/container/inlined_vector.h"

#include <grpc/slice.h>

#include "src/core/ext/xds/certificate_provider_store.h"
#include "src/core/lib/gprpp/memory.h"
#include "src/core/lib/gprpp/ref_counted_ptr.h"
#include "src/core/lib/iomgr/error.h"
#include "src/core/lib/json/json.h"
#include "src/core/lib/security/credentials/credentials.h"

namespace grpc_core {

class XdsClient;

class XdsBootstrap {
 public:
  struct Node {
    TString id;
    TString cluster;
    TString locality_region;
    TString locality_zone;
    TString locality_sub_zone;
    Json metadata;
  };

  struct XdsServer {
    TString server_uri;
    TString channel_creds_type;
    Json channel_creds_config;
    std::set<TString> server_features;

    static XdsServer Parse(const Json& json, grpc_error_handle* error);

    bool operator==(const XdsServer& other) const {
      return (server_uri == other.server_uri &&
              channel_creds_type == other.channel_creds_type &&
              channel_creds_config == other.channel_creds_config &&
              server_features == other.server_features);
    }

    bool operator<(const XdsServer& other) const {
      if (server_uri < other.server_uri) return true;
      if (channel_creds_type < other.channel_creds_type) return true;
      if (channel_creds_config.Dump() < other.channel_creds_config.Dump()) {
        return true;
      }
      if (server_features < other.server_features) return true;
      return false;
    }

    Json::Object ToJson() const;

    bool ShouldUseV3() const;
  };

  struct Authority {
    TString client_listener_resource_name_template;
    y_absl::InlinedVector<XdsServer, 1> xds_servers;
  };

  // Creates bootstrap object from json_string.
  // If *error is not GRPC_ERROR_NONE after returning, then there was an
  // error parsing the contents.
  static std::unique_ptr<XdsBootstrap> Create(y_absl::string_view json_string,
                                              grpc_error_handle* error);

  // Do not instantiate directly -- use Create() above instead.
  XdsBootstrap(Json json, grpc_error_handle* error);

  TString ToString() const;

  // TODO(roth): We currently support only one server. Fix this when we
  // add support for fallback for the xds channel.
  const XdsServer& server() const { return servers_[0]; }
  const Node* node() const { return node_.get(); }
  const TString& client_default_listener_resource_name_template() const {
    return client_default_listener_resource_name_template_;
  }
  const TString& server_listener_resource_name_template() const {
    return server_listener_resource_name_template_;
  }
  const std::map<TString, Authority>& authorities() const {
    return authorities_;
  }
  const Authority* LookupAuthority(const TString& name) const;
  const CertificateProviderStore::PluginDefinitionMap& certificate_providers()
      const {
    return certificate_providers_;
  }
  // A util method to check that an xds server exists in this bootstrap file.
  bool XdsServerExists(const XdsServer& server) const;

 private:
  grpc_error_handle ParseXdsServerList(
      Json* json, y_absl::InlinedVector<XdsServer, 1>* servers);
  grpc_error_handle ParseAuthorities(Json* json);
  grpc_error_handle ParseAuthority(Json* json, const TString& name);
  grpc_error_handle ParseNode(Json* json);
  grpc_error_handle ParseLocality(Json* json);
  grpc_error_handle ParseCertificateProviders(Json* json);
  grpc_error_handle ParseCertificateProvider(const TString& instance_name,
                                             Json* certificate_provider_json);

  y_absl::InlinedVector<XdsServer, 1> servers_;
  std::unique_ptr<Node> node_;
  TString client_default_listener_resource_name_template_;
  TString server_listener_resource_name_template_;
  std::map<TString, Authority> authorities_;
  CertificateProviderStore::PluginDefinitionMap certificate_providers_;
};

}  // namespace grpc_core

#endif /* GRPC_CORE_EXT_XDS_XDS_BOOTSTRAP_H */
