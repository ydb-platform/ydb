#ifndef RSEQ_SENSOR_IMPL_INL_H_
#error "Direct inclusion of this file is not allowed, include rseq_sensor_impl.h"
// For the sake of sane code completion.
#include "rseq_sensor_impl.h"
#endif

#include <library/cpp/yt/rseq/per_cpu.h>

#include <new>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

template <class TDerived, class TShard>
TIntrusivePtr<TDerived> TRseqCounterBase<TDerived, TShard>::Create()
{
    int shardCount = NRseq::GetCpuCount();
    return NewWithExtraSpace<TDerived>(shardCount * sizeof(TShard), shardCount);
}

template <class TDerived, class TShard>
TRseqCounterBase<TDerived, TShard>::TRseqCounterBase(int shardCount)
    : ShardCount_(shardCount)
    , Shards_(static_cast<TShard*>(this->GetExtraSpacePtr()))
{
    for (int index = 0; index < ShardCount_; ++index) {
        new (&Shards_[index]) TShard{};
    }
}

template <class TDerived, class TShard>
TRseqCounterBase<TDerived, TShard>::~TRseqCounterBase()
{
    for (int index = 0; index < ShardCount_; ++index) {
        Shards_[index].~TShard();
    }
}

template <class TDerived, class TShard>
TMutableRange<TShard> TRseqCounterBase<TDerived, TShard>::GetShards()
{
    return TMutableRange(Shards_, ShardCount_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
