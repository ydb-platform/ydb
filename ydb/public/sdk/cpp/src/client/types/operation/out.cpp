#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>

Y_DECLARE_OUT_SPEC(, NYdb::TOperation, o, x) {
    return x.Out(o);
}
