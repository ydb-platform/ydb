#include "export.h"

Y_DECLARE_OUT_SPEC(, NYdb::NExport::TExportToYtResponse, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NExport::TExportToS3Response, o, x) {
    return x.Out(o);
}
