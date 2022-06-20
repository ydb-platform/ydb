#include <ydb/core/protos/long_tx_service.pb.h>

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, NKikimrLongTxService::TEvSubscribeLockResult::EResult, stream, value) {
    stream << NKikimrLongTxService::TEvSubscribeLockResult::EResult_Name(value);
}
