#include "cooling_index.h"

namespace NKikimr::NBlobDepot {

    using TCoolingIndex = TBlobDepot::TCoolingIndex;

    TCoolingIndex::TCoolingIndex(const TData& data, ui64 tabletId, ui64 hotTargetBytes, ui32 targetObjectBytes)
        : Data(data)
        , TabletId(tabletId)
        , HotTargetBytes(hotTargetBytes)
        , TargetObjectBytes(targetObjectBytes)
    {}

    void TCoolingIndex::RemoveHotKey(const TData::TKey& key, ui32 generation, ui64 sequence) {
        HotKeys.erase(std::make_tuple(generation, sequence, key));
    }

    void TCoolingIndex::AddHotKey(const TData::TKey& key, const TData::TValue& value) {
        if (CoolingKeysInFlight.contains(key)) {
            return;
        }
        if (std::ranges::any_of(value.ValueChain, [](const auto& link) { return link.HasBlobLocator(); })) {
            const auto [it, inserted] = HotKeys.emplace(value.HotWriteGeneration, value.HotWriteSequence, key);
            HotSelectionNeedsRestart |= inserted && HotSelectionAfter && *it <= *HotSelectionAfter;
        }
    }

    void TCoolingIndex::UpdateSourceRetry(const TKey& key, const TData::TValue* value) {
        const auto it = CoolingSourceRetries.find(key);
        if (it != CoolingSourceRetries.end() && (!value || !IsSameValueChain(it->second.SourceChain, value->ValueChain))) {
            CoolingSourceRetries.erase(it);
        }
    }

    bool TCoolingIndex::HasWork() {
        const bool hotWork = !HotKeys.empty() && Data.GetTotalStoredDataSize() > HotTargetBytes;
        if (!hotWork) {
            PendingHotPack.reset();
            HotSelectionAfter.reset();
            HotSelectionNeedsRestart = false;
        }
        return hotWork;
    }

    TCoolingIndex::TPack TCoolingIndex::SelectCoolingPack(ui64 hotBytesNeeded, TMonotonic now, TSchedule schedule) {
        TPack pack;

        // Return false only when the key must be retried in the next pack.
        auto appendKey = [&](const TKey& key) {
            if (CoolingKeysInFlight.contains(key)) {
                return true;
            }
            const auto *value = Data.FindKey(key);
            if (!value || value->GoingToAssimilate || value->IsWrittenUncertainly()) {
                return true;
            }
            if (const auto it = CoolingSourceRetries.find(key); it != CoolingSourceRetries.end() && now < it->second.RetryAt) {
                schedule(it->second.RetryAt);
                return true;
            }

            ui64 size = 0;
            ui64 hotBytes = 0;
            const bool valid = EnumerateValueChainPayload(value->ValueChain, TabletId, TOverloaded{
                [&](TLogoBlobID, ui32 begin, ui32 end) {
                    size += end - begin;
                    hotBytes += end - begin;
                },
                [&](TS3Locator, ui32 begin, ui32 end) {
                    size += end - begin;
                },
            });
            if (!valid || !size || size > Max<ui32>() || !hotBytes) {
                return true;
            }
            if (pack.Size && pack.Size + size > TargetObjectBytes) {
                pack.Full = true;
                return false;
            }

            pack.Items.push_back({key, value->ValueVersion, {}, pack.Size, ui32(pack.Size + size)});
            pack.Size += size;
            pack.HotBytes += hotBytes;
            pack.Full = pack.Size >= TargetObjectBytes || pack.Items.size() == MaxKeysPerCoolingPack;
            return true;
        };

        auto it = HotSelectionAfter ? HotKeys.upper_bound(*HotSelectionAfter) : HotKeys.begin();
        for (size_t visited = 0; it != HotKeys.end(); ++it) {
            if (visited++ == MaxKeysVisitedPerSelection) {
                HotSelectionAfter = *std::prev(it);
                pack.SelectionLimited = true;
                return pack;
            }
            if (!appendKey(std::get<2>(*it)) || pack.Full || pack.HotBytes >= hotBytesNeeded) {
                break;
            }
        }
        HotSelectionAfter.reset();
        if (std::exchange(HotSelectionNeedsRestart, false)) {
            schedule(now);
        }
        return pack;
    }

    TCoolingIndex::TPack TCoolingIndex::SelectHotPack(ui64 needed, TMonotonic now, TSchedule schedule) {
        TPack pack;
        if (needed > HotBytesInFlight) {
            const ui64 bytesToCool = needed - HotBytesInFlight;
            if (!PendingHotPack) {
                PendingHotPack = TPendingHotPack{now + TDuration::Seconds(1), TargetObjectBytes, MaxKeysPerCoolingPack};
            }
            auto& pending = *PendingHotPack;
            const bool flush = now >= pending.FlushAt;
            if (flush || HotSelectionAfter || bytesToCool >= pending.SelectionBytes || HotKeys.size() >= pending.SelectionKeys) {
                pack = SelectCoolingPack(bytesToCool, now, schedule);
                if (pack.SelectionLimited) {
                    schedule(now);
                } else if (!flush && !pack.Full) {
                    // The index also contains keys retained below the hot target. After an
                    // incomplete selection, wait for enough additional work or the flush timer.
                    const ui64 missingBytes = TargetObjectBytes - pack.Size;
                    const size_t missingKeys = MaxKeysPerCoolingPack - pack.Items.size();
                    if (pack.HotBytes > bytesToCool) {
                        // More keys do not help until the excess reaches beyond the last selected blob.
                        pending.SelectionBytes = pack.HotBytes + Min<ui64>(missingBytes, missingKeys);
                        pending.SelectionKeys = Max<size_t>();
                    } else {
                        pending.SelectionBytes = bytesToCool + Min(missingBytes, Max<ui64>() - bytesToCool);
                        pending.SelectionKeys = HotKeys.size() + missingKeys;
                    }
                    pack = {};
                }
            }
            if (flush || pack.Full || pack.SelectionLimited) {
                PendingHotPack.reset();
            } else {
                schedule(pending.FlushAt);
            }
        } else if (!needed) {
            PendingHotPack.reset();
        }
        return pack;
    }

    TCoolingIndex::TPack TCoolingIndex::SelectPack(TMonotonic now, TSchedule schedule) {
        const ui64 hotBytes = Data.GetTotalStoredDataSize();
        const ui64 targetExcess = hotBytes - Min(hotBytes, HotTargetBytes);
        TPack pack = SelectHotPack(targetExcess, now, schedule);
        if (pack.Items.empty()) {
            return {};
        }

        // The selected mappings cannot change during this actor turn.
        for (auto& item : pack.Items) {
            const auto& value = *Data.FindKey(item.Key);
            item.SourceChain = value.ValueChain;
            RemoveHotKey(item.Key, value.HotWriteGeneration, value.HotWriteSequence);
            const bool inserted = CoolingKeysInFlight.insert(item.Key).second;
            Y_ABORT_UNLESS(inserted);
        }
        HotBytesInFlight += pack.HotBytes;
        return pack;
    }

    void TCoolingIndex::FinishPack(const TPack& pack, std::optional<size_t> failedSource, TMonotonic now) {
        for (const auto& item : pack.Items) {
            CoolingKeysInFlight.erase(item.Key);
            if (const auto* value = Data.FindKey(item.Key)) {
                AddHotKey(item.Key, *value);
            }
        }
        HotBytesInFlight -= pack.HotBytes;
        if (failedSource) {
            const auto& item = pack.Items.at(*failedSource);
            if (const auto *value = Data.FindKey(item.Key);
                    value && IsSameValueChain(value->ValueChain, item.SourceChain)) {
                const auto [it, inserted] = CoolingSourceRetries.try_emplace(item.Key);
                if (inserted) {
                    it->second.SourceChain = item.SourceChain;
                }
                it->second.RetryAt = now + it->second.Backoff.Next();
            }
        }
    }

} // NKikimr::NBlobDepot
