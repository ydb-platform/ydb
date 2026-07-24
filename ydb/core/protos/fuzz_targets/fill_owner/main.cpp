#include <ydb/core/ydb_convert/ydb_convert.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) {
        return 0;
    }

    Ydb::Scheme::ModifyPermissionsRequest request;
    if (!request.ParseFromArray(data, size)) {
        return 0;
    }

    NKikimrScheme::TEvModifySchemeTransaction output;
    TMaybeFail<Ydb::Scheme::ModifyPermissionsRequest> input;
    input.ConstructInPlace(request);

    try {
        NKikimr::FillOwner(output, input);
    } catch (...) {
    }

    return 0;
}
