#pragma once

#include "defs.h"

#include <util/generic/ptr.h>
#include <util/generic/buffer.h>
#include <util/memory/pool.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <ydb/core/blobstorage/vdisk/common/memusage.h>
#include <ydb/core/blobstorage/base/ptr.h>

namespace NKikimr {

    /////////////////////////////////////////////////////////////////////////////////////////
    // TTransparentMemoryPool
    /////////////////////////////////////////////////////////////////////////////////////////
    class TTransparentMemoryPool {
    public:
        TTransparentMemoryPool(TMemoryConsumer&& consumer, size_t initial, TMemoryPool::IGrowPolicy* growPolicy)
            : Consumer(std::move(consumer))
            , MemoryPool(initial, growPolicy)
            , TotalAllocated(0)
        {}

        ~TTransparentMemoryPool() {
            TrackingFree();
        }

        void ClearKeepFirstChunk() noexcept {
            TrackingFree();
            MemoryPool.ClearKeepFirstChunk();
        }

        void* Allocate(size_t len) noexcept {
            i64 l = static_cast<i64>(len);
            Consumer.Add(l);
            TotalAllocated += l;
            return MemoryPool.Allocate(len);
        }

    protected:
        TMemoryConsumer Consumer;
        TMemoryPool MemoryPool;
        i64 TotalAllocated;

        void TrackingFree() {
            Consumer.Subtract(TotalAllocated);
            TotalAllocated = 0;
        }
    };

} // NKikimr
