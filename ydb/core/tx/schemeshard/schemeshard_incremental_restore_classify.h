#pragma once

#include <ydb/core/protos/tx_datashard.pb.h>

namespace NKikimr::NSchemeShard {

// Single source of truth for "did this scan termination warrant a retry?"
// DS reports the cause (TShardOpResult::EOpEndStatus); SS owns the policy.
inline bool ShouldRetryIncrementalRestore(NKikimrTxDataShard::TShardOpResult::EOpEndStatus s) {
    using NKikimrTxDataShard::TShardOpResult;
    switch (s) {
        case TShardOpResult::END_SUCCESS:           return false;
        case TShardOpResult::END_TRANSIENT_FAILURE: return true;
        case TShardOpResult::END_FATAL_FAILURE:     return false;
        case TShardOpResult::END_UNSPECIFIED:       return false;
    }
    return false;
}

}
