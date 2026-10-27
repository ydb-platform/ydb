#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "defs.h"

namespace NKikimrSchemeOp {
    class TRestoreTask;
}

namespace NKikimr {
namespace NDataShard {

class TTableInfo;

IActor* CreateS3Downloader(const TActorId& dataShard, ui64 tabletId, ui64 txId, const NKikimrSchemeOp::TRestoreTask& task, const TTableInfo& info, bool directPartImportAllowed);

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
