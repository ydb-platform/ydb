#include "columnshard.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TCSCounters::TCSCounters()
    : TBase("CS")
{
    StartBackgroundCount = TBase::GetDeriviative("StartBackground/Count");
    TooEarlyBackgroundCount = TBase::GetDeriviative("TooEarlyBackground/Count");
    SetupCompactionCount = TBase::GetDeriviative("SetupCompaction/Count");
    SetupIndexationCount = TBase::GetDeriviative("SetupIndexation/Count");
    SetupTtlCount = TBase::GetDeriviative("SetupTtl/Count");
    SetupCleanupCount = TBase::GetDeriviative("SetupCleanup/Count");

    SkipIndexationInputDueToGranuleOverloadBytes = TBase::GetDeriviative("SkipIndexationInput/GranuleOverload/Bytes");
    SkipIndexationInputDueToGranuleOverloadCount = TBase::GetDeriviative("SkipIndexationInput/GranuleOverload/Count");
    SkipIndexationInputDueToSplitCompactionBytes = TBase::GetDeriviative("SkipIndexationInput/SplitCompaction/Bytes");
    SkipIndexationInputDueToSplitCompactionCount = TBase::GetDeriviative("SkipIndexationInput/SplitCompaction/Count");

    FutureIndexationInputBytes = TBase::GetDeriviative("FutureIndexationInput/Bytes");
    IndexationInputBytes = TBase::GetDeriviative("IndexationInput/Bytes");

    OverloadInsertTableBytes = TBase::GetDeriviative("OverloadInsertTable/Bytes");
    OverloadInsertTableCount = TBase::GetDeriviative("OverloadInsertTable/Count");
    OverloadGranuleBytes = TBase::GetDeriviative("OverloadGranule/Bytes");
    OverloadGranuleCount = TBase::GetDeriviative("OverloadGranule/Count");
    OverloadShardBytes = TBase::GetDeriviative("OverloadShard/Bytes");
    OverloadShardCount = TBase::GetDeriviative("OverloadShard/Count");

    InternalCompactionGranuleBytes = TBase::GetValueAutoAggregationsClient("InternalCompaction/Bytes");
    InternalCompactionGranulePortionsCount = TBase::GetValueAutoAggregationsClient("InternalCompaction/PortionsCount");

    SplitCompactionGranuleBytes = TBase::GetValueAutoAggregationsClient("SplitCompaction/Bytes");
    SplitCompactionGranulePortionsCount = TBase::GetValueAutoAggregationsClient("SplitCompaction/PortionsCount");
}

}
