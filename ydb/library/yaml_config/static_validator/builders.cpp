#include "builders.h"

#include <ydb/library/yaml_config/validator/validator_checks.h>
#include <ydb/library/yaml_config/validator/configurators.h>

namespace NKikimr {

using namespace NYamlConfig::NValidator;
using namespace NYamlConfig::NValidator::Configurators;

TArrayBuilder HostConfigBuilder() {
  return TArrayBuilder([](auto& hostsConfig) {
    hostsConfig
    .Configure(uniqueByPath("host_config_id"))
    .MapItem([](auto& host) {
      host
      .Int64("host_config_id", nonNegative()) // >= 0?
      .Array("drive", [](auto& drive) {
        drive
        .Configure(uniqueByPath("path"))
        .MapItem([](auto& driveItem) {
          driveItem
          .String("path")
          .Enum("type", {"ssd", "nvme", "rot"});
        });
      });
    });
  });
}

TArrayBuilder HostsBuilder() {
  return TArrayBuilder([](auto& hosts){
    hosts
    .MapItem([](auto& host) {
      host
      .String("host")
      .Int64("host_config_id", nonNegative())
      .String("node_id", [](auto& nodeId){
        nodeId.Optional();
      })
      .Int64("port", [](auto& port){
        port
        .Range(0, 65535)
        .Optional();
      });
    })
    .Configure(uniqueByPath("node_id"))
    .AddCheck("Must not have two hosts with same host name and port", [](auto& hostsContext) {
      auto array = hostsContext.Node();
      THashMap<std::pair<TString, i64>, i64> itemsToIndex;

      for (int i = 0; i < array.Length(); ++i) {
        TString host = array[i].Map()["host"].String();
        auto portNode = array[i].Map()["port"];
        i64 defaultPort = 19001;
        i64 port = portNode.Exists() ? portNode.Int64() : defaultPort;
        auto item = std::pair{host, port};

        if (itemsToIndex.contains(item)) {
          TString i1 = ToString(itemsToIndex[item]);
          TString i2 = ToString(i);

          hostsContext.Fail("items with indexes " + i1 + " and " + i2 + " are conflicting");
        }
        itemsToIndex[item] = i;
      }
    });
  });
}

TMapBuilder DomainsConfigBuilder() {
  return TMapBuilder([](auto& domainsConfig){
    domainsConfig
    .Array("domain", [](auto& domain){
      domain
      .MapItem([](auto& domainItem){
        domainItem
        .String("name")
        .Field("storage_pool_types", StoragePoolTypesConfigBuilder());
      });
    })
    .Field("state_storage", StateStorageBuilder())
    .Field("security_config", SecurityConfigBuilder());
  });
}

TArrayBuilder StoragePoolTypesConfigBuilder() {
  return TArrayBuilder([](auto& storagePoolTypes){
    storagePoolTypes
    .MapItem([](auto& storagePoolType){
      storagePoolType
      .String("kind")
      .Map("pool_config", [](auto& poolConfig){
        poolConfig
        .Int64("box_id", nonNegative())
        .Int64("encryption", [](auto&encryption){
          encryption
          .Range(0, 1)
          .Optional();
        })
        .Enum("erasure_species", {"none", "block-4-2", "mirror-3-dc", "mirror-3dc-3-nodes"})
        .String("kind")
        .Array("pdisk_filter", [](auto& pdiskFilter){
          pdiskFilter
          .MapItem([](auto& pdiskFilterItem){
            pdiskFilterItem
            .Array("property", [](auto& property){
              property
              .MapItem([](auto& propertyItem){
                propertyItem
                .Enum("type", {"ssd", "nvme", "rot"});
              });
            });
          });
        });
      });
      storagePoolType.Configure(mustBeEqual("kind", "pool_config/kind"));
    });
  });
}

TArrayBuilder StateStorageBuilder() {
  return TArrayBuilder([](auto& stateStorage){
    stateStorage
    .MapItem([](auto& stateStorageItem){
      stateStorageItem
      .Map("ring", [](auto& ring){
        ring
        .Array("node", [](auto& node){
          node.Int64Item(nonNegative());
        })
        .Int64("nto_select", nonNegative())
        .AddCheck("nto_select must not be greater, than node array size", [](auto& ringContext){
          i64 nto_select = ringContext.Node()["nto_select"].Int64();
          i64 arr_size = ringContext.Node()["node"].Array().Length();
          ringContext.Expect(nto_select <= arr_size);
        })
        .AddCheck("nto_select should be odd, because even number usage doesn't improve fault tolerance", [](auto& ringContext){
          ringContext.Expect(ringContext.Node()["nto_select"].Int64() % 2 == 1);
        });
      })
      .Int64("ssid", nonNegative());
    });
  });
}

TMapBuilder SecurityConfigBuilder() {
  return TMapBuilder([](auto& securityConfig){
    securityConfig
    .Bool("enforce_user_token_requirement", [](auto& enforceUserTokenRequirements){
      enforceUserTokenRequirements.Optional();
    })
    .Optional();
  });
}

TMapBuilder ActorSystemConfigBuilder() {
  return TMapBuilder([](auto& actorSystem){
    actorSystem
    .Bool("use_auto_config", [](auto& useAutoConfig){
      useAutoConfig.Optional();
    })
    .Enum("node_type", [](auto& nodeType){
      nodeType
      .SetItems({"STORAGE", "COMPUTE", "HYBRID"})
      .Optional();
    })
    .Int64("cpu_count", [](auto& cpuCount){
      cpuCount
      .Min(0)
      .Optional();
    })
    .Array("executor", [](auto& executor){
      executor
      .Optional()
      .MapItem([](auto& executorItem){
        executorItem
        .Enum("name", {"System", "User", "Batch", "IO", "IC"})
        .Int64("spin_threshold", [](auto& spinThreshold){
          spinThreshold
          .Min(0)
          .Optional();
        })
        .Int64("threads", nonNegative())
        .Int64("max_threads", [](auto& maxThreads){
          maxThreads
          .Optional()
          .Min(0);
        })
        .Int64("max_avg_ping_deviation", [](auto& maxAvgPingDeviation){
          maxAvgPingDeviation
          .Min(0)
          .Optional();
        })
        .Int64("time_per_mailbox_micro_secs", [](auto& timePerMailboxMicroSecs){
          timePerMailboxMicroSecs
          .Optional()
          .Min(0);
        })
        .Enum("type", {"IO", "BASIC"});
      });
    })
    .Map("scheduler", [](auto& scheduler){
      scheduler
      .Optional()
      .Int64("progress_threshold", nonNegative())
      .Int64("resolution", nonNegative())
      .Int64("spin_threshold", nonNegative());
    })
    .AddCheck("Must either be auto config or manual config", [](auto& actorSystemContext){
      bool autoConfig = false;
      auto node = actorSystemContext.Node();
      if (node["use_auto_config"].Exists() && node["use_auto_config"].Bool()) {
        autoConfig = true;
      }
      if (autoConfig) {
        actorSystemContext.Expect(node["node_type"].Exists(), "node_type must exist when using auto congfig");
        actorSystemContext.Expect(node["cpu_count"].Exists(), "cpu_count must exist when using auto congfig");

        actorSystemContext.Expect(!node["executor"].Exists(), "executor must not exist when using auto congfig");
        actorSystemContext.Expect(!node["scheduler"].Exists(), "scheduler must not exist when using auto congfig");
      } else {
        actorSystemContext.Expect(node["executor"].Exists(), "executor must exist when not using auto congfig");
        actorSystemContext.Expect(node["scheduler"].Exists(), "scheduler must exist when not using auto congfig");

        actorSystemContext.Expect(!node["node_type"].Exists(), "node_type must not exist when not using auto congfig");
        actorSystemContext.Expect(!node["cpu_count"].Exists(), "cpu_count must not exist when not using auto congfig");
      }
    });
  });
}

NYamlConfig::NValidator::TMapBuilder BlobStorageConfigBuilder() {
  return TMapBuilder([](auto& blobStorageConfig){
    blobStorageConfig
    .Map("service_set", [](auto& serviceSet){
      serviceSet
      .Array("groups", [](auto& groups){
        groups
        .MapItem([](auto& group){
          group
          .Enum("erasure_species", {"none", "block-4-2", "mirror-3-dc", "mirror-3dc-3-nodes"})
          .Array("rings", [](auto& rings){
            rings
            .MapItem([](auto& ring){
              ring
              .Array("fail_domains", [](auto& failDomains){
                failDomains
                .MapItem([](auto& failDomain){
                  failDomain
                  .Array("vdisk_locations", [](auto& vdiskLocations){
                    vdiskLocations
                    .MapItem([](auto& vdiskLocation){
                      vdiskLocation
                      .String("node_id")
                      .String("path")
                      .Enum("pdisk_category", {"ssd", "nvme", "rot"});
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });
}

NYamlConfig::NValidator::TMapBuilder ChannelProfileConfigBuilder() {
  return TMapBuilder([](auto& channelProfileConfig){
    channelProfileConfig
    .Array("profile", [](auto& profile){
      profile
      .MapItem([](auto& profileItem){
        profileItem
        .Array("channel", [](auto& channel){
          channel
          .MapItem([](auto& channelItem){
            channelItem
            .Enum("erasure_species", {"none", "block-4-2", "mirror-3-dc", "mirror-3dc-3-nodes"})
            .Int64("pdisk_category")
            .Enum("storage_pool_kind", {"nvme", "ssd", "rot"});
          });
        })
        .Int64("profile_id", nonNegative());
      });
    });
  });
}

NYamlConfig::NValidator::TMapBuilder StaticConfigBuilder() {
  return TMapBuilder([](auto& staticConfig){
    staticConfig
    .Enum("static_erasure", [](auto& staticErasure){
      staticErasure
      .SetItems({"none", "block-4-2", "mirror-3-dc", "mirror-3dc-3-nodes"})
      .Optional();
    })
    .Field("host_configs", HostConfigBuilder())
    .Field("hosts", HostsBuilder())
    .Field("domains_config", DomainsConfigBuilder())
    .Map("table_service_config", [](auto& tableServiceConfig){
      tableServiceConfig
      .Optional()
      .String("sql_version");
    })
    .Field("actor_system_config", ActorSystemConfigBuilder())
    .Field("blob_storage_config", BlobStorageConfigBuilder())
    .Field("channel_profile_config", ChannelProfileConfigBuilder());
  });
}

} // namespace NKikimr
