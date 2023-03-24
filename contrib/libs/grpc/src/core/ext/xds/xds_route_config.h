//
// Copyright 2018 gRPC authors.
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

#ifndef GRPC_CORE_EXT_XDS_XDS_ROUTE_CONFIG_H
#define GRPC_CORE_EXT_XDS_XDS_ROUTE_CONFIG_H

#include <grpc/support/port_platform.h>

#include <map>
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <vector>

#include "y_absl/types/optional.h"
#include "y_absl/types/variant.h"
#include "envoy/config/route/v3/route.upb.h"
#include "envoy/config/route/v3/route.upbdefs.h"
#include "re2/re2.h"

#include "src/core/ext/xds/xds_client.h"
#include "src/core/ext/xds/xds_common_types.h"
#include "src/core/ext/xds/xds_http_filters.h"
#include "src/core/ext/xds/xds_resource_type_impl.h"
#include "src/core/lib/channel/status_util.h"
#include "src/core/lib/matchers/matchers.h"

namespace grpc_core {

bool XdsRbacEnabled();

struct XdsRouteConfigResource {
  using TypedPerFilterConfig =
      std::map<TString, XdsHttpFilterImpl::FilterConfig>;

  struct RetryPolicy {
    internal::StatusCodeSet retry_on;
    uint32_t num_retries;

    struct RetryBackOff {
      Duration base_interval;
      Duration max_interval;

      bool operator==(const RetryBackOff& other) const {
        return base_interval == other.base_interval &&
               max_interval == other.max_interval;
      }
      TString ToString() const;
    };
    RetryBackOff retry_back_off;

    bool operator==(const RetryPolicy& other) const {
      return (retry_on == other.retry_on && num_retries == other.num_retries &&
              retry_back_off == other.retry_back_off);
    }
    TString ToString() const;
  };

  // TODO(donnadionne): When we can use y_absl::variant<>, consider using that
  // for: PathMatcher, HeaderMatcher, cluster_name and weighted_clusters
  struct Route {
    // Matchers for this route.
    struct Matchers {
      StringMatcher path_matcher;
      std::vector<HeaderMatcher> header_matchers;
      y_absl::optional<uint32_t> fraction_per_million;

      bool operator==(const Matchers& other) const {
        return path_matcher == other.path_matcher &&
               header_matchers == other.header_matchers &&
               fraction_per_million == other.fraction_per_million;
      }
      TString ToString() const;
    };

    Matchers matchers;

    struct UnknownAction {
      bool operator==(const UnknownAction& /* other */) const { return true; }
    };

    struct RouteAction {
      struct HashPolicy {
        enum Type { HEADER, CHANNEL_ID };
        Type type;
        bool terminal = false;
        // Fields used for type HEADER.
        TString header_name;
        std::unique_ptr<RE2> regex = nullptr;
        TString regex_substitution;

        HashPolicy() {}

        // Copyable.
        HashPolicy(const HashPolicy& other);
        HashPolicy& operator=(const HashPolicy& other);

        // Moveable.
        HashPolicy(HashPolicy&& other) noexcept;
        HashPolicy& operator=(HashPolicy&& other) noexcept;

        bool operator==(const HashPolicy& other) const;
        TString ToString() const;
      };

      struct ClusterWeight {
        TString name;
        uint32_t weight;
        TypedPerFilterConfig typed_per_filter_config;

        bool operator==(const ClusterWeight& other) const {
          return name == other.name && weight == other.weight &&
                 typed_per_filter_config == other.typed_per_filter_config;
        }
        TString ToString() const;
      };

      std::vector<HashPolicy> hash_policies;
      y_absl::optional<RetryPolicy> retry_policy;

      // Action for this route.
      // TODO(roth): When we can use y_absl::variant<>, consider using that
      // here, to enforce the fact that only one of the two fields can be set.
      TString cluster_name;
      std::vector<ClusterWeight> weighted_clusters;
      // Storing the timeout duration from route action:
      // RouteAction.max_stream_duration.grpc_timeout_header_max or
      // RouteAction.max_stream_duration.max_stream_duration if the former is
      // not set.
      y_absl::optional<Duration> max_stream_duration;

      bool operator==(const RouteAction& other) const {
        return hash_policies == other.hash_policies &&
               retry_policy == other.retry_policy &&
               cluster_name == other.cluster_name &&
               weighted_clusters == other.weighted_clusters &&
               max_stream_duration == other.max_stream_duration;
      }
      TString ToString() const;
    };

    struct NonForwardingAction {
      bool operator==(const NonForwardingAction& /* other */) const {
        return true;
      }
    };

    y_absl::variant<UnknownAction, RouteAction, NonForwardingAction> action;
    TypedPerFilterConfig typed_per_filter_config;

    bool operator==(const Route& other) const {
      return matchers == other.matchers && action == other.action &&
             typed_per_filter_config == other.typed_per_filter_config;
    }
    TString ToString() const;
  };

  struct VirtualHost {
    std::vector<TString> domains;
    std::vector<Route> routes;
    TypedPerFilterConfig typed_per_filter_config;

    bool operator==(const VirtualHost& other) const {
      return domains == other.domains && routes == other.routes &&
             typed_per_filter_config == other.typed_per_filter_config;
    }
  };

  std::vector<VirtualHost> virtual_hosts;

  bool operator==(const XdsRouteConfigResource& other) const {
    return virtual_hosts == other.virtual_hosts;
  }
  TString ToString() const;

  static grpc_error_handle Parse(
      const XdsEncodingContext& context,
      const envoy_config_route_v3_RouteConfiguration* route_config,
      XdsRouteConfigResource* rds_update);
};

class XdsRouteConfigResourceType
    : public XdsResourceTypeImpl<XdsRouteConfigResourceType,
                                 XdsRouteConfigResource> {
 public:
  y_absl::string_view type_url() const override {
    return "envoy.config.route.v3.RouteConfiguration";
  }
  y_absl::string_view v2_type_url() const override {
    return "envoy.api.v2.RouteConfiguration";
  }

  y_absl::StatusOr<DecodeResult> Decode(const XdsEncodingContext& context,
                                      y_absl::string_view serialized_resource,
                                      bool /*is_v2*/) const override;

  void InitUpbSymtab(upb_symtab* symtab) const override {
    envoy_config_route_v3_RouteConfiguration_getmsgdef(symtab);
  }
};

}  // namespace grpc_core

#endif  // GRPC_CORE_EXT_XDS_XDS_ROUTE_CONFIG_H
