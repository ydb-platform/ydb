#pragma once

#include <ydb/library/conclusion/result.h>

namespace NKikimrSchemeOp {
class TBackupTask;
}

namespace NKikimr::NDataShard {

TConclusion<TString> GenCreateTableQuery(const NKikimrSchemeOp::TBackupTask& task);

} // namespace NKikimr::NDataShard
