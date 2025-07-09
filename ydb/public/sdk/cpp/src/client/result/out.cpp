#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

Y_DECLARE_OUT_SPEC(, NYdb::TColumn, o, x) {
    return x.Out(o);
}
