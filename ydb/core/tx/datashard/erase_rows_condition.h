#pragma once

#include "defs.h"

#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/flat_row_state.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NDataShard {

class IEraseRowsCondition {
public:
    virtual ~IEraseRowsCondition() = default;
    virtual void AddToRequest(NKikimrTxDataShard::TEvEraseRowsRequest& request) const = 0;
    virtual void Prepare(TIntrusiveConstPtr<NTable::TRowScheme> scheme, TMaybe<NTable::TPos> remapPos = Nothing()) = 0;
    virtual bool Check(const NTable::TRowState& row) const = 0;
    virtual TVector<NTable::TTag> Tags() const = 0;

}; // IEraseRowsCondition

IEraseRowsCondition* CreateEraseRowsCondition(const NKikimrTxDataShard::TEvEraseRowsRequest& request);
IEraseRowsCondition* CreateEraseRowsCondition(const NKikimrTxDataShard::TEvConditionalEraseRowsRequest& request);

} // NDataShard
} // NKikimr
