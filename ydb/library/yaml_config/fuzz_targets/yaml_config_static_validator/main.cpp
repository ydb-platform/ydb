#include <ydb/library/yaml_config/static_validator/builders.h>
#include <ydb/library/yaml_config/validator/validator.h>
#include <ydb/library/fyamlcpp/fyamlcpp.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

namespace {

void ConsumeResult(NKikimr::NYamlConfig::NValidator::TValidationResult result) {
    (void)result.Ok();
    for (const auto& issue : result.Issues) {
        (void)issue.NodePath;
        (void)issue.Problem;
    }
}

void ValidateNode(
    const NKikimr::NFyaml::TNodeRef& node,
    NKikimr::NYamlConfig::NValidator::TValidator& validator)
{
    try {
        ConsumeResult(validator.Validate(node));
    } catch (...) {
    }
}

void FuzzStaticValidator(const TString& input) {
    auto staticValidator = NKikimr::StaticConfigBuilder().CreateValidator();
    auto hostConfigValidator = NKikimr::HostConfigBuilder().CreateValidator();
    auto hostsValidator = NKikimr::HostsBuilder().CreateValidator();
    auto domainsValidator = NKikimr::DomainsConfigBuilder().CreateValidator();
    auto actorSystemValidator = NKikimr::ActorSystemConfigBuilder().CreateValidator();
    auto blobStorageValidator = NKikimr::BlobStorageConfigBuilder().CreateValidator();
    auto channelProfileValidator = NKikimr::ChannelProfileConfigBuilder().CreateValidator();

    try {
        ConsumeResult(staticValidator.Validate(input));
    } catch (...) {
    }

    try {
        auto doc = NKikimr::NFyaml::TDocument::Parse(input);
        auto root = doc.Root();
        if (root.Type() != NKikimr::NFyaml::ENodeType::Mapping) {
            return;
        }

        auto map = root.Map();
        if (map.Has("config") && map.at("config").Type() == NKikimr::NFyaml::ENodeType::Mapping) {
            root = map.at("config");
            map = root.Map();
        }

        ValidateNode(root, staticValidator);

        if (map.Has("host_configs")) {
            ValidateNode(map.at("host_configs"), hostConfigValidator);
        }
        if (map.Has("hosts")) {
            ValidateNode(map.at("hosts"), hostsValidator);
        }
        if (map.Has("domains_config")) {
            ValidateNode(map.at("domains_config"), domainsValidator);
        }
        if (map.Has("actor_system_config")) {
            ValidateNode(map.at("actor_system_config"), actorSystemValidator);
        }
        if (map.Has("blob_storage_config")) {
            ValidateNode(map.at("blob_storage_config"), blobStorageValidator);
        }
        if (map.Has("channel_profile_config")) {
            ValidateNode(map.at("channel_profile_config"), channelProfileValidator);
        }
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    try {
        FuzzStaticValidator(TString(reinterpret_cast<const char*>(data), size));
    } catch (...) {
    }

    return 0;
}
