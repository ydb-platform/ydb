#include <ydb/core/protos/data_events.pb.h>

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, NKikimrDataEvents::ELockMode, stream, value) {
    stream << NKikimrDataEvents::ELockMode_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrDataEvents::EDataFormat, stream, value) {
    stream << NKikimrDataEvents::EDataFormat_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrDataEvents::TEvWriteResult::EStatus, stream, value) {
    stream << NKikimrDataEvents::TEvWriteResult::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrDataEvents::TEvWrite::TOperation::EOperationType, stream, value) {
    stream << NKikimrDataEvents::TEvWrite::TOperation::EOperationType_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrDataEvents::TEvWrite::ETxMode, stream, value) {
    stream << NKikimrDataEvents::TEvWrite::ETxMode_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrDataEvents::TEvLockRowsResult::EStatus, stream, value) {
    stream << NKikimrDataEvents::TEvLockRowsResult::EStatus_Name(value);
}
