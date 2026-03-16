#pragma once
#include <boost/noncopyable.hpp>
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DB_CHDB
{

class ObjectStorageFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<ObjectStoragePtr(
        const std::string & name,
        const CHDBPoco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool skip_access_check)>;

    static ObjectStorageFactory & instance();

    void registerObjectStorageType(const std::string & type, Creator creator);

    ObjectStoragePtr create(
        const std::string & name,
        const CHDBPoco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool skip_access_check) const;

private:
    using Registry = std::unordered_map<String, Creator>;
    Registry registry;
};

}
