#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idxsnap_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/barriers/barriers_public.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // Snapshots for DS (data structures) types for LogoBlobs, Blocks, Barriers
    ////////////////////////////////////////////////////////////////////////////
    using TLogoBlobsSnapshot = TLevelIndexSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
    using TBlocksSnapshot = TLevelIndexSnapshot<TKeyBlock, TMemRecBlock>;
    using TBarriersSnapshot = NBarriers::TBarriersDsSnapshot;

    ////////////////////////////////////////////////////////////////////////////
    // THullDsSnap
    // Union of all data structure snapshots
    ////////////////////////////////////////////////////////////////////////////
    struct THullDsSnap {
        TIntrusivePtr<THullCtx> HullCtx;
        TLogoBlobsSnapshot LogoBlobsSnap;
        TBlocksSnapshot BlocksSnap;
        TBarriersSnapshot BarriersSnap;
    };

} // NKikimr

