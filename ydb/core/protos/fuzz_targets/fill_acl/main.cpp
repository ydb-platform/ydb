#include <ydb/core/ydb_convert/ydb_convert.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) {
        return 0;
    }

    Ydb::Scheme::ModifyPermissionsRequest request;
    if (!request.ParseFromArray(data, size)) {
        return 0;
    }

    TMaybeFail<Ydb::Scheme::ModifyPermissionsRequest> input;
    input.ConstructInPlace(request);

    NKikimrSchemeOp::TModifyScheme output;
    TString error;
    try {
        NKikimr::FillACL(output, input, error);
    } catch (...) {
    }

    return 0;
}
