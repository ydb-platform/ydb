#include <ydb/core/protos/tx_sequenceshard.pb.h>

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, NKikimrTxSequenceShard::TEvCreateSequenceResult::EStatus, stream, value) {
    stream << NKikimrTxSequenceShard::TEvCreateSequenceResult::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxSequenceShard::TEvAllocateSequenceResult::EStatus, stream, value) {
    stream << NKikimrTxSequenceShard::TEvAllocateSequenceResult::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxSequenceShard::TEvDropSequenceResult::EStatus, stream, value) {
    stream << NKikimrTxSequenceShard::TEvDropSequenceResult::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxSequenceShard::TEvUpdateSequenceResult::EStatus, stream, value) {
    stream << NKikimrTxSequenceShard::TEvUpdateSequenceResult::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxSequenceShard::TEvFreezeSequenceResult::EStatus, stream, value) {
    stream << NKikimrTxSequenceShard::TEvFreezeSequenceResult::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxSequenceShard::TEvRestoreSequenceResult::EStatus, stream, value) {
    stream << NKikimrTxSequenceShard::TEvRestoreSequenceResult::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxSequenceShard::TEvRedirectSequenceResult::EStatus, stream, value) {
    stream << NKikimrTxSequenceShard::TEvRedirectSequenceResult::EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxSequenceShard::TEvGetSequenceResult::EStatus, stream, value) {
    stream << NKikimrTxSequenceShard::TEvGetSequenceResult::EStatus_Name(value);
}
