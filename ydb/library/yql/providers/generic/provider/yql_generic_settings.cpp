#include "yql_generic_settings.h"

namespace NYql {

    using namespace NCommon;

    TGenericConfiguration::TGenericConfiguration()
    {
    }

    TGenericSettings::TConstPtr TGenericConfiguration::Snapshot() const {
        return std::make_shared<const TGenericSettings>(*this);
    }

    bool TGenericConfiguration::HasCluster(TStringBuf cluster) const {
        return ValidClusters.contains(cluster);
    }

}
