#include <ydb/library/yaml_config/validator/validator_builder.h>

namespace NKikimr {

NYamlConfig::NValidator::TArrayBuilder HostConfigBuilder();
NYamlConfig::NValidator::TArrayBuilder HostsBuilder();
NYamlConfig::NValidator::TMapBuilder DomainsConfigBuilder();

NYamlConfig::NValidator::TArrayBuilder StoragePoolTypesConfigBuilder();
NYamlConfig::NValidator::TArrayBuilder StateStorageBuilder();
NYamlConfig::NValidator::TMapBuilder SecurityConfigBuilder();

NYamlConfig::NValidator::TMapBuilder ActorSystemConfigBuilder();
NYamlConfig::NValidator::TMapBuilder BlobStorageConfigBuilder();

NYamlConfig::NValidator::TMapBuilder ChannelProfileConfigBuilder();

NYamlConfig::NValidator::TMapBuilder StaticConfigBuilder();

} // namespace NKikimr
