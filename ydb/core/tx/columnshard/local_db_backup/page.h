#pragma once

#include "status.h"

#include <ydb/core/protos/s3_settings.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NActorsProto {
class TRemoteHttpInfo;
}

namespace NKikimr::NColumnShard::NLocalDbBackup {

TString RenderPage(
    ui64 tabletId, const TStatus& status, const TString& message, bool isError, const NKikimrSchemeOp::TS3Settings& prefill, TInstant now);

TString RenderJson(const TStatus& status, TInstant now);

TString ParseS3Form(const NActorsProto::TRemoteHttpInfo& query, NKikimrSchemeOp::TS3Settings& s3);

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
