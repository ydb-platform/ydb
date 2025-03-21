#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

Y_DECLARE_OUT_SPEC(, NYdb::TType, o, x) {
    return x.Out(o);
}
