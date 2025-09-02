#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

Y_DECLARE_OUT_SPEC(, NYdb::NScheme::TVirtualTimestamp, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NScheme::TSchemeEntry, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NScheme::TDescribePathResult, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NScheme::TListDirectoryResult, o, x) {
    return x.Out(o);
}
