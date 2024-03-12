#pragma once
#include "counters.h"

#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/address.h>
#include <ydb/core/tx/columnshard/engines/scheme/tiering/tier_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>

#include <ydb/core/base/appdata.h>

#include <ydb/library/accessor/accessor.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <map>

namespace NKikimr::NOlap {
class TVersionedIndex;
class TPortionInfo;
}

namespace NKikimr::NOlap::NActualizer {

class TTieringProcessContext;

class TGranuleActualizationIndex {
private:
    TCounters Counters;

    class TPortionIndexInfo {
    public:
        class TEviction {
        private:
            TRWAddress RWAddress;
            YDB_READONLY_DEF(TDuration, WaitDuration);
        public:
            const TRWAddress& GetRWAddress() const {
                return RWAddress;
            }

            TEviction(TRWAddress&& rwAddress, const TDuration waitDuration)
                : RWAddress(std::move(rwAddress))
                , WaitDuration(waitDuration)
            {

            }
        };

        class TScheme {
        private:
            TRWAddress RWAddress;
        public:
            const TRWAddress& GetRWAddress() const {
                return RWAddress;
            }

            TScheme(TRWAddress&& rwAddress)
                : RWAddress(std::move(rwAddress))
            {

            }
        };
    private:
        YDB_READONLY_DEF(std::optional<TEviction>, Eviction);
        YDB_READONLY_DEF(std::optional<TScheme>, Scheme);
    public:
        TPortionIndexInfo() = default;

        bool IsEmpty() const {
            return !Eviction && !Scheme;
        }

        void SetEviction(TRWAddress&& address, const TDuration waitDuration) {
            AFL_VERIFY(!Eviction);
            Eviction = TEviction(std::move(address), waitDuration);
        }

        void SetScheme(TRWAddress&& address) {
            AFL_VERIFY(!Scheme);
            Scheme = TScheme(std::move(address));
        }

        void AddCounters(TCounters& counters, const std::shared_ptr<TPortionInfo>& info) const {
            counters.PortionsToSyncSchema->AddPortion(info);
        }
        void RemoveCounters(TCounters& counters, const std::shared_ptr<TPortionInfo>& info) const {
            counters.PortionsToSyncSchema->RemovePortion(info);
        }
    };

    THashMap<TRWAddress, std::map<TDuration, THashSet<ui64>>> PortionIdByWaitDuration;
    THashMap<ui64, TPortionIndexInfo> PortionsInfo;
    THashMap<TRWAddress, THashSet<ui64>> PortionsToActualizeScheme;

    const ui64 PathId;
    const TVersionedIndex& VersionedIndex;

    ISnapshotSchema::TPtr ActualCriticalScheme;
    std::optional<TTiering> Tiering;
    std::optional<ui32> TieringColumnId;
    TInstant StartInstant = TInstant::Zero();
    bool Started = false;
public:
    void Start() {
        AFL_VERIFY(!Started);
        Started = true;
    }

    class TActualizationInfo {
    public:
        class TEvictionInfo {
        private:
            TRWAddress Address;
            YDB_ACCESSOR_DEF(TString, TargetTierName);
            i64 WaitDurationValue;
        public:
            TString DebugString() const {
                return TStringBuilder() << "{address=" << Address.DebugString() << ";target_tier=" << TargetTierName << ";wait_duration=" << TDuration::FromValue(WaitDurationValue) << "}";
            }

            const TRWAddress& GetAddress() const {
                return Address;
            }

            TEvictionInfo(TRWAddress&& address, const TString& targetTierName, const i64 waitDurationValue)
                : Address(std::move(address))
                , TargetTierName(targetTierName)
                , WaitDurationValue(waitDurationValue)
            {

            }

            TDuration GetWaitDuration() const {
                if (WaitDurationValue >= 0) {
                    return TDuration::FromValue(WaitDurationValue);
                } else {
                    return TDuration::Zero();
                }
            }

            TDuration GetLateness() const {
                if (WaitDurationValue >= 0) {
                    return TDuration::Zero();
                } else {
                    return TDuration::FromValue(-WaitDurationValue);
                }
            }
        };

        class TSchemeInfo {
        private:
            TRWAddress Address;
            YDB_ACCESSOR_DEF(std::shared_ptr<ISnapshotSchema>, TargetScheme);
        public:
            TString DebugString() const {
                return TStringBuilder() << "{address=" << Address.DebugString() << ";target_scheme=" << TargetScheme->DebugString() << "}";
            }

            const TRWAddress& GetAddress() const {
                return Address;
            }

            TSchemeInfo(TRWAddress&& address, const std::shared_ptr<ISnapshotSchema>& targetScheme)
                : Address(std::move(address))
                , TargetScheme(targetScheme) {

            }
        };
    private:
        YDB_READONLY_DEF(std::optional<TEvictionInfo>, Eviction);
        YDB_READONLY_DEF(std::optional<TSchemeInfo>, Scheme);
    public:
        TActualizationInfo() = default;

        TString DebugString() const {
            TStringBuilder sb;
            if (Eviction) {
                sb << "EVICTION=" << Eviction->DebugString() << ";";
            }
            if (Scheme) {
                sb << "SCHEME=" << Scheme->DebugString() << ";";
            }
            return sb;
        }

        void SetEviction(TRWAddress&& address, const TString& targetTierName, const i64 waitDurationValue) {
            AFL_VERIFY(!Eviction);
            Eviction = TEvictionInfo(std::move(address), targetTierName, waitDurationValue);
        }

        void SetScheme(TRWAddress&& address, const std::shared_ptr<ISnapshotSchema>& targetScheme) {
            AFL_VERIFY(!Scheme);
            Scheme = TSchemeInfo(std::move(address), targetScheme);
        }
    };

    TGranuleActualizationIndex(const ui64 pathId, const TVersionedIndex& versionedIndex);

    void BuildActualizationTasks(TTieringProcessContext& context, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portionInfos) const;

    [[nodiscard]] bool RefreshTiering(const std::optional<TTiering>& info);
    [[nodiscard]] bool RefreshScheme();

    void Rebuild(const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions);

    TActualizationInfo BuildActualizationInfo(const std::shared_ptr<TPortionInfo>& portion, const TInstant now) const;

    void AddPortion(const std::shared_ptr<TPortionInfo>& portion, const TInstant now);

    void RemovePortion(const std::shared_ptr<TPortionInfo>& portion);
};

}