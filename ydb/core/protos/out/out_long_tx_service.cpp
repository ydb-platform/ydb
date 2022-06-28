#include <ydb/core/protos/long_tx_service.pb.h>

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, NKikimrLongTxService::TEvLockStatus::EStatus, stream, value) {
    stream << NKikimrLongTxService::TEvLockStatus::EStatus_Name(value);
}
