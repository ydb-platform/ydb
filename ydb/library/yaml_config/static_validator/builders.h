#include <ydb/library/yaml_config/validator/validator_builder.h>

NYamlConfig::NValidator::TArrayBuilder HostConfigBuilder();
NYamlConfig::NValidator::TArrayBuilder HostsBuilder();
NYamlConfig::NValidator::TMapBuilder DomainsConfigBuilder();

NYamlConfig::NValidator::TArrayBuilder StoragePoolTypesConfigBuilder();
NYamlConfig::NValidator::TArrayBuilder StateStorageBuilder();
NYamlConfig::NValidator::TMapBuilder SecurityConfigBuilder();
