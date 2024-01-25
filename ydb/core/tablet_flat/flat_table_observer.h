#pragma once
#include "defs.h"
#include "flat_row_eggs.h"
#include "flat_update_op.h"

#include <util/generic/ptr.h>

namespace NKikimr::NTable {

    class ITableObserver : public TThrRefBase {
    public:
        /**
         * Called when a new update is applied to the table
         */
        virtual void OnUpdate(
            ERowOp rop,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const TUpdateOp> ops,
            TRowVersion rowVersion) = 0;

        /**
         * Called when an uncommitted update is applied to the table
         */
        virtual void OnUpdateTx(
            ERowOp rop,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const TUpdateOp> ops,
            ui64 txId) = 0;
    };

} // namespace NKikimr::NTable
