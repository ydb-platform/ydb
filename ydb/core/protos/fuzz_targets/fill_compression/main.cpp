#include <ydb/core/ydb_convert/compression.h>

#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    TString input(reinterpret_cast<const char*>(data), size);
    if (input.empty()) {
        input = "zstd";
    }

    NKikimrSchemeOp::TBackupTask::TCompressionOptions output;
    Ydb::StatusIds::StatusCode status;
    TString error;
    try {
        if (NKikimr::CheckCompression(input, status, error)) {
            NKikimr::FillCompression(output, input);
        }
    } catch (...) {
    }

    return 0;
}
