#include "compression.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr {

bool FillCompression(NKikimrSchemeOp::TBackupTask::TCompressionOptions& out, const TString& in,
        Ydb::StatusIds::StatusCode& status, TString& error)
{
    auto returnError = [&](Ydb::StatusIds::StatusCode code, const TString& msg) {
        status = code;
        error = msg;
        return false;
    };

    TStringBuf inBuf = in;

    if (inBuf.StartsWith("zstd")) {
        Y_ABORT_UNLESS(inBuf.SkipPrefix("zstd"));

        if (inBuf) {
            if (inBuf.front() != '-') {
                return returnError(Ydb::StatusIds::UNSUPPORTED,
                    TStringBuilder() << "Unsupported compression codec: " << in);
            }

            int level;
            if (!TryFromString(inBuf.Tail(1), level)) {
                return returnError(Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "Invalid compression level: " << inBuf.Tail(1));
            }

            out.SetLevel(level);
        }

        out.SetCodec("zstd");
    } else {
        return returnError(Ydb::StatusIds::UNSUPPORTED,
            TStringBuilder() << "Unsupported compression codec: " << in);
    }

    status = Ydb::StatusIds::SUCCESS;
    return true;
}

bool CheckCompression(const TString& in, Ydb::StatusIds::StatusCode& status, TString& error) {
    NKikimrSchemeOp::TBackupTask::TCompressionOptions unused;
    return FillCompression(unused, in, status, error);
}

bool FillCompression(NKikimrSchemeOp::TBackupTask::TCompressionOptions& out, const TString& in) {
    Ydb::StatusIds::StatusCode status;
    TString error;

    const auto ret = FillCompression(out, in, status, error);
    Y_VERIFY_S(ret, "Cannot parse compression: " << in);

    return ret;
}

} // NKikimr
