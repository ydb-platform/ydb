#pragma once

#include "chunk.h"
#include "log.h"

#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_glue.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_rec.h>
#include <ydb/core/blobstorage/vdisk/common/disk_part.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>
#include <ydb/core/base/blobstorage_grouptype.h>

namespace NKikimr::NPDiskTool {

struct TBlobIndexEntry {
    TLogoBlobID Id;
    TMemRecLogoBlob MemRec;
    TVector<TDiskPart> Outbound; // only used when ManyHugeBlobs; otherwise empty
    TString InlineData; // LogoBlobOpt payload kept from the log
};

struct TBlockIndexEntry {
    TKeyBlock Key;
    TMemRecBlock MemRec;
};

struct TBarrierIndexEntry {
    TKeyBarrier Key;
    TMemRecBarrier MemRec;
};

struct THullSnapshot {
    TVector<TBlobIndexEntry> Blobs;
    TVector<TBlockIndexEntry> Blocks;
    TVector<TBarrierIndexEntry> Barriers;
    ui64 LogoBlobsCompactedLsn = 0;
    ui64 BlocksCompactedLsn = 0;
    ui64 BarriersCompactedLsn = 0;
    TMaybe<TErasureType::EErasureSpecies> Erasure;
};

THullSnapshot ReconstructHull(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    const TLogScanResult& log,
    TOwner owner,
    TMaybe<TErasureType::EErasureSpecies> erasure,
    TIssueLog& issues);

} // namespace NKikimr::NPDiskTool
