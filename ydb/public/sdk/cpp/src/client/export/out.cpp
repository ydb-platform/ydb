#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>

Y_DECLARE_OUT_SPEC(, NYdb::NExport::TExportToYtResponse, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NExport::TExportToS3Response, o, x) {
    return x.Out(o);
}
