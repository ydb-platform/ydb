#include "yql_solomon_config.h"

namespace NYql {

using namespace NCommon;

TSolomonConfiguration::TSolomonConfiguration()
{
}

TSolomonSettings::TConstPtr TSolomonConfiguration::Snapshot() const {
    return std::make_shared<const TSolomonSettings>(*this);
}

} // NYql
