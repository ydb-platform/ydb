#pragma once

#include <util/system/yassert.h>
#include <util/generic/ylimits.h>

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TSpawned {
    public:
        explicit TSpawned(bool spawned)
            : Spawned(spawned)
        { }

        explicit operator bool() const noexcept
        {
            return Spawned;
        }

    private:
        const bool Spawned;
    };

    class TLeft {
    public:
        TLeft() = default;

        explicit operator bool() const noexcept
        {
            return Value > 0;
        }

        ui64 Get() const noexcept
        {
            return Value;
        }

        TLeft& operator +=(const TSpawned& spawned) noexcept
        {
            if (spawned) {
                *this += size_t(1);
            }

            return *this;
        }

        TLeft& operator +=(size_t inc) noexcept
        {
            if (Value > Max<decltype(Value)>() - inc) {

                Y_ABORT("TLeft counter is overflowed");
            }

            Value += inc;

            return *this;
        }

        TLeft& operator -=(size_t dec) noexcept
        {
            Y_ABORT_UNLESS(Value >= dec, "TLeft counter is underflowed");

            Value -= dec;

            return *this;
        }

    private:
        ui64 Value = 0;
    };
}
}
}
