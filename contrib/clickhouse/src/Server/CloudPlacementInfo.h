#pragma once

#include <string>
#include <boost/core/noncopyable.hpp>
#include <DBPoco/Util/AbstractConfiguration.h>
#include <Common/Logger.h>

namespace DB
{

namespace PlacementInfo
{

static constexpr auto PLACEMENT_CONFIG_PREFIX = "placement";
static constexpr auto DEFAULT_AZ_FILE_PATH = "/run/instance-metadata/node-zone";

/// A singleton providing information on where in cloud server is running.
class PlacementInfo : private boost::noncopyable
{
public:
    static PlacementInfo & instance();

    void initialize(const DBPoco::Util::AbstractConfiguration & config);

    std::string getAvailabilityZone() const;

private:
    PlacementInfo() = default;

    LoggerPtr log = getLogger("CloudPlacementInfo");

    bool initialized;

    bool use_imds;
    std::string availability_zone;
};

}
}
