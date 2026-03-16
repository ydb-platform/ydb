#pragma once

namespace DB_CHDB
{

struct SerializationInfoSettings
{
    const double ratio_of_defaults_for_sparse = 1.0;
    const bool choose_kind = false;

    bool isAlwaysDefault() const { return ratio_of_defaults_for_sparse >= 1.0; }
};

}
