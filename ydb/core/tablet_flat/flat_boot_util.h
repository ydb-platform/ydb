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

    class THistoryCutter {
    public:
        explicit THistoryCutter(const TIntrusiveConstPtr<TTabletStorageInfo> info) : Info(info)
                                                                                   , EarliestGenerationForChannel(info->Channels.size(), std::numeric_limits<ui32>::max())
        {}

        THistoryCutter(THistoryCutter&&) = default;

        void SeenBlob(const TLogoBlobID& blob) {
            ui32 channel = blob.Channel();
            Y_ABORT_UNLESS(channel < EarliestGenerationForChannel.size());
            ui32& earliestGen = EarliestGenerationForChannel[channel];
            earliestGen = std::min(earliestGen, blob.Generation());
        }

        TArrayRef<const TTabletChannelInfo::THistoryEntry> GetHistoryToCut(ui32 channel) const {
            const auto& history = Info->Channels[channel].History;
            ui32 cutoffGen = EarliestGenerationForChannel[channel];
            auto cutoffIt = std::lower_bound(history.begin(), history.end(), cutoffGen, TTabletChannelInfo::THistoryEntry::TCmp());
            if (cutoffIt == history.end()) {
                // Happens only if the log is empty
                // We must not cut the current entry under any circumstances
                --cutoffIt;
            }
            return {history.data(), &(*cutoffIt)};
        }
    private:
        const TIntrusiveConstPtr<TTabletStorageInfo> Info;
        TVector<ui32> EarliestGenerationForChannel;
    };
}
}
}
