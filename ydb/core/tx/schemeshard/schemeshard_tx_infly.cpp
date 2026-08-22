#include <ydb/core/protos/tx_datashard.pb.h>  // for NKikimrTxDataShard::TSplitMergeDescription

#include "schemeshard_subop_state_types.h"  // for ETxType
#include "schemeshard_tx_infly.h"

namespace NKikimr::NSchemeShard {

bool TTxState::IsItActuallyMerge() const {
    return (TxType == ETxType::TxSplitTablePartition
        && SplitDescription
        && SplitDescription->SourceRangesSize() > 1
    );
}

}  // namespace NKikimr::NSchemeShard
