#pragma once

#include "defs.h"

#include <ydb/library/actors/util/rope.h>

#include <util/thread/lfstack.h>
#include <util/system/filemap.h>

namespace NKikimr {

    class TRopeArenaBackend : public IContiguousChunk {
        static constexpr size_t Capacity = 2 * 1024 * 1024 - 4096 /* lfalloc overhead */ - sizeof(IContiguousChunk);
        char Data[Capacity];

    public:
        TContiguousSpan GetData() const override {
            return {Data, Capacity};
        }

        size_t GetOccupiedMemorySize() const override {
            return Capacity;
        }

        TMutableContiguousSpan GetDataMut() override {
            return {Data, Capacity};
        }

        static TIntrusivePtr<IContiguousChunk> Allocate() {
            return MakeIntrusive<TRopeArenaBackend>();
        }
    };

}
