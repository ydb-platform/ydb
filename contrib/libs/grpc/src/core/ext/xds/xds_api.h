/*
 *
 * Copyright 2018 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#ifndef GRPC_CORE_EXT_XDS_XDS_API_H
#define GRPC_CORE_EXT_XDS_XDS_API_H

#include <grpc/support/port_platform.h>

#include <stdint.h>

#include <set>

#include "y_absl/container/inlined_vector.h"
#include "y_absl/types/optional.h"
#include "re2/re2.h"

#include "upb/def.hpp"

#include <grpc/slice_buffer.h>

#include "envoy/admin/v3/config_dump.upb.h"
#include "src/core/ext/filters/client_channel/server_address.h"
#include "src/core/ext/xds/xds_bootstrap.h"
#include "src/core/ext/xds/xds_client_stats.h"
#include "src/core/ext/xds/xds_http_filters.h"
#include "src/core/lib/matchers/matchers.h"

namespace grpc_core {

// TODO(yashykt): Check to see if xDS security is enabled. This will be
// removed once this feature is fully integration-tested and enabled by
// default.
bool XdsSecurityEnabled();

class XdsClient;

class XdsApi {
 public:
  static const char* kLdsTypeUrl;
  static const char* kRdsTypeUrl;
  static const char* kCdsTypeUrl;
  static const char* kEdsTypeUrl;

  struct Duration {
    int64_t seconds = 0;
    int32_t nanos = 0;
    bool operator==(const Duration& other) const {
      return seconds == other.seconds && nanos == other.nanos;
    }
    TString ToString() const {
      return y_absl::StrFormat("Duration seconds: %ld, nanos %d", seconds, nanos);
    }
  };

  using TypedPerFilterConfig =
      std::map<TString, XdsHttpFilterImpl::FilterConfig>;

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

    Matchers matchers;
    std::vector<HashPolicy> hash_policies;

    // Action for this route.
    // TODO(roth): When we can use y_absl::variant<>, consider using that
    // here, to enforce the fact that only one of the two fields can be set.
    TString cluster_name;
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
    std::vector<ClusterWeight> weighted_clusters;
    // Storing the timeout duration from route action:
    // RouteAction.max_stream_duration.grpc_timeout_header_max or
    // RouteAction.max_stream_duration.max_stream_duration if the former is
    // not set.
    y_absl::optional<Duration> max_stream_duration;

    TypedPerFilterConfig typed_per_filter_config;

    bool operator==(const Route& other) const {
      return matchers == other.matchers && cluster_name == other.cluster_name &&
             weighted_clusters == other.weighted_clusters &&
             max_stream_duration == other.max_stream_duration &&
             typed_per_filter_config == other.typed_per_filter_config;
    }
    TString ToString() const;
  };

  struct RdsUpdate {
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

    bool operator==(const RdsUpdate& other) const {
      return virtual_hosts == other.virtual_hosts;
    }
    TString ToString() const;
    VirtualHost* FindVirtualHostForDomain(const TString& domain);
  };

  struct CommonTlsContext {
    struct CertificateValidationContext {
      std::vector<StringMatcher> match_subject_alt_names;

      bool operator==(const CertificateValidationContext& other) const {
        return match_subject_alt_names == other.match_subject_alt_names;
      }

      TString ToString() const;
      bool Empty() const;
    };

    struct CertificateProviderInstance {
      TString instance_name;
      TString certificate_name;

      bool operator==(const CertificateProviderInstance& other) const {
        return instance_name == other.instance_name &&
               certificate_name == other.certificate_name;
      }

      TString ToString() const;
      bool Empty() const;
    };

    struct CombinedCertificateValidationContext {
      CertificateValidationContext default_validation_context;
      CertificateProviderInstance
          validation_context_certificate_provider_instance;

      bool operator==(const CombinedCertificateValidationContext& other) const {
        return default_validation_context == other.default_validation_context &&
               validation_context_certificate_provider_instance ==
                   other.validation_context_certificate_provider_instance;
      }

      TString ToString() const;
      bool Empty() const;
    };

    CertificateProviderInstance tls_certificate_certificate_provider_instance;
    CombinedCertificateValidationContext combined_validation_context;

    bool operator==(const CommonTlsContext& other) const {
      return tls_certificate_certificate_provider_instance ==
                 other.tls_certificate_certificate_provider_instance &&
             combined_validation_context == other.combined_validation_context;
    }

    TString ToString() const;
    bool Empty() const;
  };

  struct DownstreamTlsContext {
    CommonTlsContext common_tls_context;
    bool require_client_certificate = false;

    bool operator==(const DownstreamTlsContext& other) const {
      return common_tls_context == other.common_tls_context &&
             require_client_certificate == other.require_client_certificate;
    }

    TString ToString() const;
    bool Empty() const;
  };

  // TODO(roth): When we can use y_absl::variant<>, consider using that
  // here, to enforce the fact that only one of the two fields can be set.
  struct LdsUpdate {
    enum class ListenerType {
      kTcpListener = 0,
      kHttpApiListener,
    } type;

    struct HttpConnectionManager {
      // The name to use in the RDS request.
      TString route_config_name;
      // Storing the Http Connection Manager Common Http Protocol Option
      // max_stream_duration
      Duration http_max_stream_duration;
      // The RouteConfiguration to use for this listener.
      // Present only if it is inlined in the LDS response.
      y_absl::optional<RdsUpdate> rds_update;

      struct HttpFilter {
        TString name;
        XdsHttpFilterImpl::FilterConfig config;

        bool operator==(const HttpFilter& other) const {
          return name == other.name && config == other.config;
        }

        TString ToString() const;
      };
      std::vector<HttpFilter> http_filters;

      bool operator==(const HttpConnectionManager& other) const {
        return route_config_name == other.route_config_name &&
               http_max_stream_duration == other.http_max_stream_duration &&
               rds_update == other.rds_update &&
               http_filters == other.http_filters;
      }

      TString ToString() const;
    };

    // Populated for type=kHttpApiListener.
    HttpConnectionManager http_connection_manager;

    // Populated for type=kTcpListener.
    // host:port listening_address set when type is kTcpListener
    TString address;

    struct FilterChainData {
      DownstreamTlsContext downstream_tls_context;
      // This is in principle the filter list.
      // We currently require exactly one filter, which is the HCM.
      HttpConnectionManager http_connection_manager;

      bool operator==(const FilterChainData& other) const {
        return downstream_tls_context == other.downstream_tls_context &&
               http_connection_manager == other.http_connection_manager;
      }

      TString ToString() const;
    } filter_chain_data;

    // A multi-level map used to determine which filter chain to use for a given
    // incoming connection. Determining the right filter chain for a given
    // connection checks the following properties, in order:
    // - destination port (never matched, so not present in map)
    // - destination IP address
    // - server name (never matched, so not present in map)
    // - transport protocol (allows only "raw_buffer" or unset, prefers the
    //   former, so only one of those two types is present in map)
    // - application protocol (never matched, so not present in map)
    // - connection source type (any, local or external)
    // - source IP address
    // - source port
    // https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/listener/v3/listener_components.proto#config-listener-v3-filterchainmatch
    // for more details
    struct FilterChainMap {
      struct FilterChainDataSharedPtr {
        std::shared_ptr<FilterChainData> data;
        bool operator==(const FilterChainDataSharedPtr& other) const {
          return *data == *other.data;
        }
      };
      struct CidrRange {
        grpc_resolved_address address;
        uint32_t prefix_len;

        bool operator==(const CidrRange& other) const {
          return memcmp(&address, &other.address, sizeof(address)) == 0 &&
                 prefix_len == other.prefix_len;
        }

        TString ToString() const;
      };
      using SourcePortsMap = std::map<uint16_t, FilterChainDataSharedPtr>;
      struct SourceIp {
        y_absl::optional<CidrRange> prefix_range;
        SourcePortsMap ports_map;

        bool operator==(const SourceIp& other) const {
          return prefix_range == other.prefix_range &&
                 ports_map == other.ports_map;
        }
      };
      using SourceIpVector = std::vector<SourceIp>;
      enum class ConnectionSourceType {
        kAny = 0,
        kSameIpOrLoopback,
        kExternal
      };
      using ConnectionSourceTypesArray = std::array<SourceIpVector, 3>;
      struct DestinationIp {
        y_absl::optional<CidrRange> prefix_range;
        // We always fail match on server name, so those filter chains are not
        // included here.
        ConnectionSourceTypesArray source_types_array;

        bool operator==(const DestinationIp& other) const {
          return prefix_range == other.prefix_range &&
                 source_types_array == other.source_types_array;
        }
      };
      // We always fail match on destination ports map
      using DestinationIpVector = std::vector<DestinationIp>;
      DestinationIpVector destination_ip_vector;

      bool operator==(const FilterChainMap& other) const {
        return destination_ip_vector == other.destination_ip_vector;
      }

      TString ToString() const;
    } filter_chain_map;

    y_absl::optional<FilterChainData> default_filter_chain;

    bool operator==(const LdsUpdate& other) const {
      return http_connection_manager == other.http_connection_manager &&
             address == other.address &&
             filter_chain_map == other.filter_chain_map &&
             default_filter_chain == other.default_filter_chain;
    }

    TString ToString() const;
  };

  struct LdsResourceData {
    LdsUpdate resource;
    TString serialized_proto;
  };

  using LdsUpdateMap = std::map<TString /*server_name*/, LdsResourceData>;

  struct RdsResourceData {
    RdsUpdate resource;
    TString serialized_proto;
  };

  using RdsUpdateMap =
      std::map<TString /*route_config_name*/, RdsResourceData>;

  struct CdsUpdate {
    enum ClusterType { EDS, LOGICAL_DNS, AGGREGATE };
    ClusterType cluster_type;
    // For cluster type EDS.
    // The name to use in the EDS request.
    // If empty, the cluster name will be used.
    TString eds_service_name;
    // Tls Context used by clients
    CommonTlsContext common_tls_context;
    // The LRS server to use for load reporting.
    // If not set, load reporting will be disabled.
    // If set to the empty string, will use the same server we obtained the CDS
    // data from.
    y_absl::optional<TString> lrs_load_reporting_server_name;
    // The LB policy to use (e.g., "ROUND_ROBIN" or "RING_HASH").
    TString lb_policy;
    // Used for RING_HASH LB policy only.
    uint64_t min_ring_size = 1024;
    uint64_t max_ring_size = 8388608;
    enum HashFunction { XX_HASH, MURMUR_HASH_2 };
    HashFunction hash_function;
    // Maximum number of outstanding requests can be made to the upstream
    // cluster.
    uint32_t max_concurrent_requests = 1024;
    // For cluster type AGGREGATE.
    // The prioritized list of cluster names.
    std::vector<TString> prioritized_cluster_names;

    bool operator==(const CdsUpdate& other) const {
      return cluster_type == other.cluster_type &&
             eds_service_name == other.eds_service_name &&
             common_tls_context == other.common_tls_context &&
             lrs_load_reporting_server_name ==
                 other.lrs_load_reporting_server_name &&
             prioritized_cluster_names == other.prioritized_cluster_names &&
             max_concurrent_requests == other.max_concurrent_requests;
    }

    TString ToString() const;
  };

  struct CdsResourceData {
    CdsUpdate resource;
    TString serialized_proto;
  };

  using CdsUpdateMap = std::map<TString /*cluster_name*/, CdsResourceData>;

  struct EdsUpdate {
    struct Priority {
      struct Locality {
        RefCountedPtr<XdsLocalityName> name;
        uint32_t lb_weight;
        ServerAddressList endpoints;

        bool operator==(const Locality& other) const {
          return *name == *other.name && lb_weight == other.lb_weight &&
                 endpoints == other.endpoints;
        }
        bool operator!=(const Locality& other) const {
          return !(*this == other);
        }
        TString ToString() const;
      };

      std::map<XdsLocalityName*, Locality, XdsLocalityName::Less> localities;

      bool operator==(const Priority& other) const;
      TString ToString() const;
    };
    using PriorityList = y_absl::InlinedVector<Priority, 2>;

    // There are two phases of accessing this class's content:
    // 1. to initialize in the control plane combiner;
    // 2. to use in the data plane combiner.
    // So no additional synchronization is needed.
    class DropConfig : public RefCounted<DropConfig> {
     public:
      struct DropCategory {
        bool operator==(const DropCategory& other) const {
          return name == other.name &&
                 parts_per_million == other.parts_per_million;
        }

        TString name;
        const uint32_t parts_per_million;
      };

      using DropCategoryList = y_absl::InlinedVector<DropCategory, 2>;

      void AddCategory(TString name, uint32_t parts_per_million) {
        drop_category_list_.emplace_back(
            DropCategory{std::move(name), parts_per_million});
        if (parts_per_million == 1000000) drop_all_ = true;
      }

      // The only method invoked from outside the WorkSerializer (used in
      // the data plane).
      bool ShouldDrop(const TString** category_name) const;

      const DropCategoryList& drop_category_list() const {
        return drop_category_list_;
      }

      bool drop_all() const { return drop_all_; }

      bool operator==(const DropConfig& other) const {
        return drop_category_list_ == other.drop_category_list_;
      }
      bool operator!=(const DropConfig& other) const {
        return !(*this == other);
      }

      TString ToString() const;

     private:
      DropCategoryList drop_category_list_;
      bool drop_all_ = false;
    };

    PriorityList priorities;
    RefCountedPtr<DropConfig> drop_config;

    bool operator==(const EdsUpdate& other) const {
      return priorities == other.priorities &&
             *drop_config == *other.drop_config;
    }
    TString ToString() const;
  };

  struct EdsResourceData {
    EdsUpdate resource;
    TString serialized_proto;
  };

  using EdsUpdateMap =
      std::map<TString /*eds_service_name*/, EdsResourceData>;

  struct ClusterLoadReport {
    XdsClusterDropStats::Snapshot dropped_requests;
    std::map<RefCountedPtr<XdsLocalityName>, XdsClusterLocalityStats::Snapshot,
             XdsLocalityName::Less>
        locality_stats;
    grpc_millis load_report_interval;
  };
  using ClusterLoadReportMap = std::map<
      std::pair<TString /*cluster_name*/, TString /*eds_service_name*/>,
      ClusterLoadReport>;

  // The metadata of the xDS resource; used by the xDS config dump.
  struct ResourceMetadata {
    // Resource status from the view of a xDS client, which tells the
    // synchronization status between the xDS client and the xDS server.
    enum ClientResourceStatus {
      // Client requested this resource but hasn't received any update from
      // management server. The client will not fail requests, but will queue
      // them
      // until update arrives or the client times out waiting for the resource.
      REQUESTED = 1,
      // This resource has been requested by the client but has either not been
      // delivered by the server or was previously delivered by the server and
      // then subsequently removed from resources provided by the server.
      DOES_NOT_EXIST,
      // Client received this resource and replied with ACK.
      ACKED,
      // Client received this resource and replied with NACK.
      NACKED
    };

    // The client status of this resource.
    ClientResourceStatus client_status = REQUESTED;
    // The serialized bytes of the last successfully updated raw xDS resource.
    TString serialized_proto;
    // The timestamp when the resource was last successfully updated.
    grpc_millis update_time = 0;
    // The last successfully updated version of the resource.
    TString version;
    // The rejected version string of the last failed update attempt.
    TString failed_version;
    // Details about the last failed update attempt.
    TString failed_details;
    // Timestamp of the last failed update attempt.
    grpc_millis failed_update_time = 0;
  };
  using ResourceMetadataMap =
      std::map<y_absl::string_view /*resource_name*/, const ResourceMetadata*>;
  struct ResourceTypeMetadata {
    y_absl::string_view version;
    ResourceMetadataMap resource_metadata_map;
  };
  using ResourceTypeMetadataMap =
      std::map<y_absl::string_view /*type_url*/, ResourceTypeMetadata>;
  static_assert(static_cast<ResourceMetadata::ClientResourceStatus>(
                    envoy_admin_v3_REQUESTED) ==
                    ResourceMetadata::ClientResourceStatus::REQUESTED,
                "");
  static_assert(static_cast<ResourceMetadata::ClientResourceStatus>(
                    envoy_admin_v3_DOES_NOT_EXIST) ==
                    ResourceMetadata::ClientResourceStatus::DOES_NOT_EXIST,
                "");
  static_assert(static_cast<ResourceMetadata::ClientResourceStatus>(
                    envoy_admin_v3_ACKED) ==
                    ResourceMetadata::ClientResourceStatus::ACKED,
                "");
  static_assert(static_cast<ResourceMetadata::ClientResourceStatus>(
                    envoy_admin_v3_NACKED) ==
                    ResourceMetadata::ClientResourceStatus::NACKED,
                "");

  // If the response can't be parsed at the top level, the resulting
  // type_url will be empty.
  // If there is any other type of validation error, the parse_error
  // field will be set to something other than GRPC_ERROR_NONE and the
  // resource_names_failed field will be populated.
  // Otherwise, one of the *_update_map fields will be populated, based
  // on the type_url field.
  struct AdsParseResult {
    grpc_error* parse_error = GRPC_ERROR_NONE;
    TString version;
    TString nonce;
    TString type_url;
    LdsUpdateMap lds_update_map;
    RdsUpdateMap rds_update_map;
    CdsUpdateMap cds_update_map;
    EdsUpdateMap eds_update_map;
    std::set<TString> resource_names_failed;
  };

  XdsApi(XdsClient* client, TraceFlag* tracer, const XdsBootstrap::Node* node);

  // Creates an ADS request.
  // Takes ownership of \a error.
  grpc_slice CreateAdsRequest(const XdsBootstrap::XdsServer& server,
                              const TString& type_url,
                              const std::set<y_absl::string_view>& resource_names,
                              const TString& version,
                              const TString& nonce, grpc_error* error,
                              bool populate_node);

  // Parses an ADS response.
  AdsParseResult ParseAdsResponse(
      const XdsBootstrap::XdsServer& server, const grpc_slice& encoded_response,
      const std::set<y_absl::string_view>& expected_listener_names,
      const std::set<y_absl::string_view>& expected_route_configuration_names,
      const std::set<y_absl::string_view>& expected_cluster_names,
      const std::set<y_absl::string_view>& expected_eds_service_names);

  // Creates an initial LRS request.
  grpc_slice CreateLrsInitialRequest(const XdsBootstrap::XdsServer& server);

  // Creates an LRS request sending a client-side load report.
  grpc_slice CreateLrsRequest(ClusterLoadReportMap cluster_load_report_map);

  // Parses the LRS response and returns \a
  // load_reporting_interval for client-side load reporting. If there is any
  // error, the output config is invalid.
  grpc_error* ParseLrsResponse(const grpc_slice& encoded_response,
                               bool* send_all_clusters,
                               std::set<TString>* cluster_names,
                               grpc_millis* load_reporting_interval);

  // Assemble the client config proto message and return the serialized result.
  TString AssembleClientConfig(
      const ResourceTypeMetadataMap& resource_type_metadata_map);

 private:
  XdsClient* client_;
  TraceFlag* tracer_;
  const XdsBootstrap::Node* node_;  // Do not own.
  upb::SymbolTable symtab_;
  const TString build_version_;
  const TString user_agent_name_;
};

}  // namespace grpc_core

#endif /* GRPC_CORE_EXT_XDS_XDS_API_H */
