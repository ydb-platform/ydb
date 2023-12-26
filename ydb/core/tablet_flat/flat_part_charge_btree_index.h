#pragma once

#include "flat_table_part.h"
#include "flat_part_iface.h"
#include "flat_part_charge_iface.h"

namespace NKikimr::NTable {

class TChargeBTreeIndex : public ICharge {
    using TBtreeIndexNode = NPage::TBtreeIndexNode;
    using TBtreeIndexMeta = NPage::TBtreeIndexMeta;

public:
    TChargeBTreeIndex(IPages *env, const TPart &part, TTagsRef tags, bool includeHistory = false)
        : Part(&part)
        , Env(env) {
        Y_UNUSED(env);
        Y_UNUSED(part);
        Y_UNUSED(tags);
        Y_UNUSED(includeHistory);
    }

public:
    TResult Do(const TCells key1, const TCells key2, const TRowId row1,
            const TRowId row2, const TKeyCellDefaults &keyDefaults, ui64 itemsLimit,
            ui64 bytesLimit) const noexcept override {
        Y_UNUSED(key1);
        Y_UNUSED(key2);
        Y_UNUSED(row1);
        Y_UNUSED(row2);
        Y_UNUSED(keyDefaults);
        Y_UNUSED(itemsLimit);
        Y_UNUSED(bytesLimit);

        auto& meta = Part->IndexPages.BTreeGroups[0];

        TVector<TBtreeIndexNode> level, nextLevel(Reserve(2));
        for (ui32 high : xrange(meta.LevelsCount)) {
            if (high == 0) {
                if (!TryLoad(meta.PageId, nextLevel)) {
                    return {false, false};
                }
            } else {
                Y_ABORT_UNLESS(level);
                

            }
        }

        return {true, false};
    }

    TResult DoReverse(const TCells key1, const TCells key2, const TRowId row1,
            const TRowId row2, const TKeyCellDefaults &keyDefaults, ui64 itemsLimit,
            ui64 bytesLimit) const noexcept override {
        // TODO: implement
        Y_UNUSED(key1);
        Y_UNUSED(key2);
        Y_UNUSED(row1);
        Y_UNUSED(row2);
        Y_UNUSED(keyDefaults);
        Y_UNUSED(itemsLimit);
        Y_UNUSED(bytesLimit);
        return {true, false};
    }

private:
    bool TryLoad(TPageId pageId, TVector<TBtreeIndexNode>& level) const noexcept {
        auto page = Env->TryGetPage(Part, pageId);
        if (page) {
            level.emplace_back(*page);
            return true;
        }
        return false;
    }

private:
    const TPart* const Part;
    IPages* const Env;
};

}
