#pragma once

#include "defs.h"
#include "flat_util_misc.h"
#include "flat_page_iface.h"
#include "flat_page_conf.h"
#include "flat_row_versions.h"
#include "flat_table_committed.h"
#include "flat_writer_conf.h"
#include "flat_comp.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TBarrier : public TSimpleRefCount<TBarrier> {
        TBarrier(ui32 step)
            : Step(step)
        { }

        const ui32 Step = Max<ui32>();
    };

    struct TCompactCfg final : public IDestructable {
        using TConf = NTable::NPage::TConf;

        explicit TCompactCfg(THolder<NTable::TCompactionParams> params)
            : Params(std::move(params))
        {
        }

        NTable::TEpoch Epoch = NTable::TEpoch::Zero();   /* Result epoch of compacted part */
        NTable::NPage::TConf Layout;
        NWriter::TConf Writer;
        THolder<NTable::TCompactionParams> Params;
        NTable::TRowVersionRanges::TSnapshot RemovedRowVersions;

        // Fulltext compact compaction support
        bool IsFulltextCompact = false;
        bool FulltextWithRelevance = false;
        bool FulltextKeySigned = false;
        ui32 FulltextAddedTag = Max<ui32>();
        ui32 FulltextSegmentTag = Max<ui32>();
        ui32 FulltextMaxSegment = 10000;
        // Key column positions are always: [0]=token, [1]=max_id, [2]=generation

        // Non-empty when compaction also needs to produce a tx status table part
        TVector<TIntrusiveConstPtr<NTable::TMemTable>> Frozen;
        TVector<TIntrusiveConstPtr<NTable::TTxStatusPart>> TxStatus;
        // Non-empty for transactions that no longer need their status maintained
        NTable::TTransactionSet GarbageTransactions;
    };

}
}
