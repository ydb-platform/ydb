#pragma once

#include "defs.h"
#include "flat_util_misc.h"
#include "flat_page_iface.h"
#include "flat_page_conf.h"
#include "flat_row_versions.h"
#include "flat_table_committed.h"
#include "flat_writer_conf.h"
#include "flat_comp.h"

#include <ydb/core/protos/blobstorage_base.pb.h>

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

        // Configuration for a single compaction output
        struct TOutputConf {
            TOutputConf()
                : TOutputConf(NTable::TScheme::TFamily{})
            {
            }

            explicit TOutputConf(const NTable::TScheme::TFamily& leaderFamily)
                : MaterializeOuterEdge(Max<ui32>())
                , MaterializeExtEdge(leaderFamily.Large)
            {
            }

            NTable::NPage::TConf Layout;
            NWriter::TConf Writer;
            // Physical blob byte thresholds, including wrappers: materialize
            // values strictly below the edge. Layout serialization edges apply
            // to payload bytes and do not force rewriting existing references.
            // Reserved for future outer materialization control; currently ignored.
            // Outer values are always materialized regardless of this threshold.
            ui32 MaterializeOuterEdge;
            ui32 MaterializeExtEdge;
            ui32 OutputId = 0;
        };

        explicit TCompactCfg(THolder<NTable::TCompactionParams> params)
            : Params(std::move(params))
        {
        }

        NTable::TEpoch Epoch = NTable::TEpoch::Zero();   /* Result epoch of compacted part */

        // Non-empty; the router returns an index into this vector.
        TVector<TOutputConf> Outputs;
        NKikimrBlobStorage::TDataKind::E DataKind = NKikimrBlobStorage::TDataKind::USER;
        THolder<NTable::TCompactionParams> Params;
        NTable::TRowVersionRanges::TSnapshot RemovedRowVersions;

        // Fulltext compact compaction support
        bool IsFulltextCompact = false;
        bool FulltextWithRelevance = false;
        bool FulltextKeySigned = false;
        ui32 FulltextKeySize = 8;
        ui32 FulltextAddedTag = Max<ui32>();
        ui32 FulltextSegmentTag = Max<ui32>();
        ui32 FulltextMaxSegment = 10000;
        // Key column positions are always: [prefix+0]=token, [prefix+1]=generation, [prefix+2]=max_id

        // Non-empty when compaction also needs to produce a tx status table part
        TVector<TIntrusiveConstPtr<NTable::TMemTable>> Frozen;
        TVector<TIntrusiveConstPtr<NTable::TTxStatusPart>> TxStatus;
        // Non-empty for transactions that no longer need their status maintained
        NTable::TTransactionSet GarbageTransactions;
    };

}
}
