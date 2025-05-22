#include <ydb/core/protos/cms.pb.h>

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, NKikimrCms::EAvailabilityMode, stream, value) {
    stream << NKikimrCms::EAvailabilityMode_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrCms::TStatus::ECode, stream, value) {
    stream << NKikimrCms::TStatus::ECode_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrCms::EState, stream, value) {
    stream << NKikimrCms::EState_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrCms::EMarker, stream, value) {
    stream << NKikimrCms::EMarker_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrCms::ETextFormat, stream, value) {
    stream << NKikimrCms::ETextFormat_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrCms::TLogRecordData::EType, stream, value) {
    stream << NKikimrCms::TLogRecordData::EType_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrCms::TAction::EType, stream, value) {
    stream << NKikimrCms::TAction::EType_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrCms::TAction::TIssue::EType, stream, value) {
    stream << NKikimrCms::TAction::TIssue::EType_Name(value);
}
