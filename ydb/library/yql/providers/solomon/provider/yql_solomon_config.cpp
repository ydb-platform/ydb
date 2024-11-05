#include "yql_solomon_config.h"

namespace NYql {

using namespace NCommon;

TSolomonConfiguration::TSolomonConfiguration()
{
    REGISTER_SETTING(*this, _EnableReading);
}

TSolomonSettings::TConstPtr TSolomonConfiguration::Snapshot() const {
    return std::make_shared<const TSolomonSettings>(*this);
}

} // NYql
