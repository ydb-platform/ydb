#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>

Y_DECLARE_OUT_SPEC(, NYdb::NImport::TImportDataResult, o, x) {
    return x.Out(o);
}
