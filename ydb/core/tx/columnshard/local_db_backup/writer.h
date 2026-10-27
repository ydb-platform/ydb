#pragma once

#include "status.h"

#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <memory>

namespace NKikimr::NColumnShard::NLocalDbBackup {

struct TS3SnapshotWriterParams {
    NKikimrSchemeOp::TS3Settings S3Settings;
    // Optional, updated by the writer as the data is uploaded; read by the monitoring page.
    TProgress::TPtr Progress;
    // Size of a multipart upload part. S3 requires >= 5 MiB for all parts except the last one.
    ui64 UploadPartBytes = 8ull << 20;
    // How long to wait for TEvBackupStarted from the tablet before giving up.
    TDuration StartTimeout = TDuration::Seconds(30);
};

// Builds "<ObjectKeyPattern>/column_shard/<tabletId>/backup_<YYYYMMDDhhmmssZ>_g<gen>_s<step>",
TString MakeBackupPath(const TString& objectKeyPattern, ui64 tabletId, ui32 generation, ui32 step, TInstant now);

std::unique_ptr<NActors::IActor> CreateS3SnapshotWriter(const NActors::TActorId& owner, ui64 tabletId, TS3SnapshotWriterParams&& params);

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
