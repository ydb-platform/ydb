#include "table.h"

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TIndexDescription, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TChangefeedDescription, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TValueSinceUnixEpochModeSettings::EUnit, o, x) {
    return NYdb::NTable::TValueSinceUnixEpochModeSettings::Out(o, x);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TTxSettings, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TCreateSessionResult, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TDescribeTableResult, o, x) {
    return x.Out(o);
}
