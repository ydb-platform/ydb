#pragma once

#include "flat_table_part.h"
#include "flat_part_iface.h"
#include "flat_part_charge_iface.h"

namespace NKikimr::NTable {

    class TChargeBTreeIndex : public ICharge {
    public:
        TChargeBTreeIndex(IPages *env, const TPart &part, TTagsRef tags, bool includeHistory = false) {
            Y_UNUSED(env);
            Y_UNUSED(part);
            Y_UNUSED(tags);
            Y_UNUSED(includeHistory);
        }

        TResult Do(const TCells key1, const TCells key2, const TRowId row1,
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

};

}
