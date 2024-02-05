#include <iostream>
#include <map>
#include <set>
#include <vector>

#include <util/generic/string.h>
#include <library/cpp/protobuf/json/util.h>

#include <ydb/library/yaml_config/new/protos/message.pb.h>

using namespace NProtoBuf;

class TConfigTransformer {
public:
    void AddEphemeralInputField(TString str) {
        Y_UNUSED(str);
    }
};

std::vector<TString> ListEphemeralFields() {
    std::vector<TString> result;

    auto& inst = NKikimrConfig::TEphemeralInputFields::default_instance();
    const Descriptor* descriptor = inst.GetDescriptor();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const FieldDescriptor* fieldDescriptor = descriptor->field(i);
        result.push_back(fieldDescriptor->name());
    }
    for (int i = 0; i < descriptor->reserved_name_count(); ++i) {
        result.push_back(descriptor->reserved_name(i));
    }

    for (auto& str : result) {
        NProtobufJson::ToSnakeCaseDense(&str);
    }

    return result;
}

std::set<TString> InOutFields = {
  "blob_storage_config",
  "bootstrap_config",
  "domains_config",
  "nameservice_config", // depends on hosts, nameservice_config (can be explicit if _.node is defined)
};

/*
PrepareNameserviceConfig(json); // in: nameservice_config, [hosts] -- out: nameservice_config

PrepareStaticGroup(json); // in: blob_storage_config, nameservice_config.node -- out: blob_storage_config

PrepareBlobStorageConfig(json); // in: blob_storage_config, nameservice_config, [host_configs] -- out: blob_storage_config

PrepareSystemTabletsInfo(json, relaxed); // in: nameservice_config, [system_tablets] -- out: system_tablets

PrepareBasicDomainsConfig(json, relaxed); //  in: domains_config -- out: domains_config // in-out: [system_tablets]

PrepareBootstrapConfig(json, relaxed); // in: bootstrap_config, [static_erasure] -- out: bootstrap_config // in-out: [system_tablets]
*/

std::set<TString> IsolatedRewriteFields = {
  "log_config",
  "actor_system_config",
  "interconnect_config",
};

auto main(int argc, char *argv[]) -> int {
    Y_UNUSED(argc, argv);

    TConfigTransformer ct;
    for (auto& field : ListEphemeralFields()) {
        ct.AddEphemeralInputField(field);
        std::cout << field << std::endl;
    }

    return 0;
}
