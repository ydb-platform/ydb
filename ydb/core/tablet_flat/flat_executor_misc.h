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

        // Non-empty when compaction also needs to write a tx status table part
        NTable::TTransactionMap CommittedTransactions;
        NTable::TTransactionSet RemovedTransactions;
        // The above may contain extra keys, these allow them to be narrowed
        TVector<TIntrusiveConstPtr<NTable::TMemTable>> Frozen;
        TVector<TIntrusiveConstPtr<NTable::TTxStatusPart>> TxStatus;
    };

}
}
