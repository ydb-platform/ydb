#pragma once

#include "status.h"

#include <ydb/core/protos/s3_settings.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NActorsProto {
class TRemoteHttpInfo;
}

namespace NKikimr::NColumnShard::NLocalDbBackup {

enum class EMessageSeverity {
    Info,
    Error,
};

TString RenderJson(const TStatus& status, TInstant now);

TString RenderPage(ui64 tabletId, const TStatus& status, const TString& message, EMessageSeverity severity,
    const NKikimrSchemeOp::TS3Settings& prefill, TInstant now);

// Reads S3 connection parameters from the POST form of the page. Returns an error message, empty on success.
TString ParseS3Form(const NActorsProto::TRemoteHttpInfo& query, NKikimrSchemeOp::TS3Settings& s3);

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
