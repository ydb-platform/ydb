#include "builders.h"

#include <ydb/library/yaml_config/validator/validator_checks.h>
#include <ydb/library/yaml_config/validator/configurators.h>

using namespace NYamlConfig::NValidator;
using namespace NYamlConfig::NValidator::Configurators;

TArrayBuilder HostConfigBuilder() {
  return TArrayBuilder([](auto& b) {b
    .Configure(uniqueByPath("host_config_id"))
    .MapItem([](auto& b) {b
      .Int64("host_config_id", nonNegative()) // >= 0?
      .Array("drive", [](auto& b) {b
        .Configure(uniqueByPath("path"))
        .MapItem([](auto& b) {b
          .String("path")
          .Enum("type", {"ssd", "nvme", "rot"});
        });
      });
    });
  });
}

TArrayBuilder HostsBuilder() {
  return TArrayBuilder([](auto& b){b
    .MapItem([](auto& b) {b
      .String("host")
      .Int64("host_config_id", nonNegative())
      .Int64("node_id", nonNegative())
      .Int64("port", 0, 65535)
      .Map("location", [](auto& b){b
        .String("unit")
        .String("data_center")
        .String("rack");
      });
    })
    .Configure(uniqueByPath("node_id"))
    .AddCheck("Must not have to hosts with same host name and port", [](auto& c) {
      auto array = c.Node();
      THashMap<std::pair<TString, i64>, i64> itemsToIndex;

      for (int i = 0; i < array.Length(); ++i) {
        TString host = array[i].Map()["host"].String();
        i64 port = array[i].Map()["port"].Int64();
        auto item = std::pair{host, port};

        if (itemsToIndex.contains(item)) {
          TString i1 = ToString(itemsToIndex[item]);
          TString i2 = ToString(i);

          c.Fail("items with indexes " + i1 + " and " + i2 + " are conflicting");
        }
        itemsToIndex[item] = i;
      }
    });
  });
}

TMapBuilder DomainsConfigBuilder() {
  return TMapBuilder([](auto& b){b
    .Array("domain", [](auto& b){b
      .MapItem([](auto& b){b
        .String("name")
        .Field("storage_pool_types", StoragePoolTypesConfigBuilder());
      });
    })
    .Field("state_storage", StateStorageBuilder())
    .Field("security_config", SecurityConfigBuilder());
  });
}

TArrayBuilder StoragePoolTypesConfigBuilder() {
  return TArrayBuilder([](auto& b){b
    .MapItem([](auto& b){b
      .String("kind")
      .Map("pool_config", [](auto& b){b
        .Int64("box_id", nonNegative())
        .Int64("encryption", [](auto&b){b
          .Range(0, 1)
          .Optional();
        })
        .Enum("erasure_species", {"none", "block-4-2", "mirror-3-dc", "mirror-3dc-3-nodes"})
        .String("kind")
        .Array("pdisk_filter", [](auto& b){b
          .MapItem([](auto& b){b
            .Array("property", [](auto& b){b
              .MapItem([](auto& b){b
                .Enum("type", {"ssd", "nvme", "rot"});
              });
            });
          });
        });
      })
      .Configure(mustBeEqual("kind", "pool_config/kind"));
    });
  });
}

TArrayBuilder StateStorageBuilder() {
  return TArrayBuilder([](auto& b){b
    .MapItem([](auto& b){b
      .Map("ring", [](auto& b){b
        .Array("node", [](auto& b){b
          .Int64Item(nonNegative());
        })
        .Int64("nto_select", nonNegative())
        .AddCheck("nto_select must not be greater, than node array size", [](auto& c){
          i64 nto_select = c.Node()["nto_select"].Int64();
          i64 arr_size = c.Node()["node"].Array().Length();
          c.Expect(nto_select <= arr_size);
        });
      })
      .Int64("ssid", nonNegative());
    });
  });
}

TMapBuilder SecurityConfigBuilder() {
  return TMapBuilder([](auto& b){b
    .Bool("enforce_user_token_requirement", [](auto& b){
      b.Optional();
    })
    .Optional();
  });
}
