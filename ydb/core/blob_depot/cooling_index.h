#pragma once

#include "data.h"

#include <ydb/core/util/backoff.h>
#include <util/generic/function_ref.h>

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TCoolingIndex {
        using TKey = TData::TKey;
        using TSchedule = TFunctionRef<void(TMonotonic)>;

    public:
        struct TPackItem {
            TKey Key;
            ui32 ValueVersion;
            TValueChain SourceChain;
            ui32 Begin;
            ui32 End;
        };

        struct TPack {
            std::vector<TPackItem> Items;
            ui32 Size = 0;
            ui64 HotBytes = 0;
            bool Full = false;
            bool SelectionLimited = false;
        };

        TCoolingIndex(const TData& data, ui64 tabletId, ui64 hotTargetBytes, ui32 targetObjectBytes);

        void RemoveHotKey(const TKey& key, ui32 generation, ui64 sequence);
        void AddHotKey(const TKey& key, const TData::TValue& value);
        void UpdateSourceRetry(const TKey& key, const TData::TValue* value);
        bool HasWork();
        TPack SelectPack(TMonotonic now, TSchedule schedule);
        void FinishPack(const TPack& pack, std::optional<size_t> failedSource, TMonotonic now);

    private:
        const TData& Data;
        const ui64 TabletId;
        const ui64 HotTargetBytes;
        const ui32 TargetObjectBytes;
        static constexpr size_t MaxKeysPerCoolingPack = 1024;
        static constexpr size_t MaxKeysVisitedPerSelection = 2 * MaxKeysPerCoolingPack;

        using THotKey = std::tuple<ui32, ui64, TKey>;
        std::set<THotKey> HotKeys;
        std::optional<THotKey> HotSelectionAfter;
        bool HotSelectionNeedsRestart = false;

        THashSet<TKey> CoolingKeysInFlight;
        struct TSourceRetry {
            TValueChain SourceChain;
            TBackoff Backoff{TDuration::Seconds(5), TDuration::Seconds(60)};
            TMonotonic RetryAt;
        };
        THashMap<TKey, TSourceRetry> CoolingSourceRetries;
        ui64 HotBytesInFlight = 0;

        struct TPendingHotPack {
            TMonotonic FlushAt;
            ui64 SelectionBytes;
            size_t SelectionKeys;
        };
        std::optional<TPendingHotPack> PendingHotPack;

        TPack SelectHotPack(ui64 needed, TMonotonic now, TSchedule schedule);
        TPack SelectCoolingPack(ui64 hotBytesNeeded, TMonotonic now, TSchedule schedule);
    };

} // NKikimr::NBlobDepot
