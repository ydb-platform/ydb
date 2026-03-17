#pragma once
#include <boost/noncopyable.hpp>
#include <Disks/ObjectStorages/IMetadataStorage.h>

namespace DB_CHDB
{

class MetadataStorageFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<MetadataStoragePtr(
        const std::string & name,
        const CHDBPoco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ObjectStoragePtr object_storage)>;

    static MetadataStorageFactory & instance();

    void registerMetadataStorageType(const std::string & metadata_type, Creator creator);

    MetadataStoragePtr create(
        const std::string & name,
        const CHDBPoco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ObjectStoragePtr object_storage,
        const std::string & compatibility_type_hint) const;

    static std::string getMetadataType(
        const CHDBPoco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const std::string & compatibility_type_hint = "");

    static std::string getCompatibilityMetadataTypeHint(const ObjectStorageType & type);

private:
    using Registry = std::unordered_map<String, Creator>;
    Registry registry;
};

}
