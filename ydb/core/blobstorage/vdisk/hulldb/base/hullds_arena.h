#pragma once

#include "defs.h"

#include <library/cpp/actors/util/rope.h>

#include <util/thread/lfstack.h>
#include <util/system/filemap.h>

namespace NKikimr {

    class TRopeArenaBackend : public IRopeChunkBackend {
        static constexpr size_t Capacity = 2 * 1024 * 1024 - 4096 /* lfalloc overhead */ - sizeof(IRopeChunkBackend);
        char Data[Capacity];

    public:
        TData GetData() const override {
            return {Data, Capacity};
        }

        size_t GetCapacity() const override {
            return Capacity;
        }

        TMutData GetDataMut() override {
            return {Data, Capacity};
        }

        static TIntrusivePtr<IRopeChunkBackend> Allocate() {
            return MakeIntrusive<TRopeArenaBackend>();
        }
    };

}
