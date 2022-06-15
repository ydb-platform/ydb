#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr {

bool CheckCompression(const TString& in, Ydb::StatusIds::StatusCode& status, TString& error);
bool FillCompression(NKikimrSchemeOp::TBackupTask::TCompressionOptions& out, const TString& in);

} // NKikimr
