#pragma once

#include "defs.h"

namespace NKikimr::NBsController {

    class TStoragePoolStat {
        struct TStoragePoolCounters {
            TString Id;
            ::NMonitoring::TDynamicCounterPtr Root;
            TString Name;
            ::NMonitoring::TDynamicCounterPtr Subgroup;
            ::NMonitoring::TDynamicCounterPtr FlagsSubgroup;
            ::NMonitoring::TDynamicCounters::TCounterPtr NumUnknown;
            ::NMonitoring::TDynamicCounters::TCounterPtr NumGreen;
            ::NMonitoring::TDynamicCounters::TCounterPtr NumCyan;
            ::NMonitoring::TDynamicCounters::TCounterPtr NumLightYellow;
            ::NMonitoring::TDynamicCounters::TCounterPtr NumYellow;
            ::NMonitoring::TDynamicCounters::TCounterPtr NumLightOrange;
            ::NMonitoring::TDynamicCounters::TCounterPtr NumOrange;
            ::NMonitoring::TDynamicCounters::TCounterPtr NumRed;
            ::NMonitoring::TDynamicCounters::TCounterPtr NumBlack;
            ::NMonitoring::TDynamicCounters::TCounterPtr AllocatedSize;

            TStoragePoolCounters(::NMonitoring::TDynamicCounterPtr counters, TString id, TString name, ui64 allocatedSize)
                : Id(std::move(id))
                , Root(std::move(counters))
                , Name(std::move(name))
                , Subgroup(Root->GetSubgroup("storagePool", Name))
                , FlagsSubgroup(Subgroup->GetSubgroup("subsystem", "flags"))
                , NumUnknown(FlagsSubgroup->GetCounter("unknown"))
                , NumGreen(FlagsSubgroup->GetCounter("green"))
                , NumCyan(FlagsSubgroup->GetCounter("cyan"))
                , NumLightYellow(FlagsSubgroup->GetCounter("lightYellow"))
                , NumYellow(FlagsSubgroup->GetCounter("yellow"))
                , NumLightOrange(FlagsSubgroup->GetCounter("lightOrange"))
                , NumOrange(FlagsSubgroup->GetCounter("orange"))
                , NumRed(FlagsSubgroup->GetCounter("red"))
                , NumBlack(FlagsSubgroup->GetCounter("black"))
                , AllocatedSize(Subgroup->GetCounter("allocatedSize"))
            {
                *AllocatedSize = allocatedSize;
            }

            void Rename(const TString& name) {
                Root->RemoveSubgroup("storagePool", std::exchange(Name, name));
                Root->RegisterSubgroup("storagePool", Name, Subgroup);
            }

            void Update(std::optional<TStorageStatusFlags> previous, std::optional<TStorageStatusFlags> current) {
                if (previous != current) {
                    if (previous) {
                        GetCounter(*previous)->Dec();
                    }
                    if (current) {
                        GetCounter(*current)->Inc();
                    }
                }
            }

            void UpdateAllocatedSize(i64 allocatedSizeIncrement) {
                *AllocatedSize += allocatedSizeIncrement;
            }

            const ::NMonitoring::TDynamicCounters::TCounterPtr& GetCounter(TStorageStatusFlags flags) const {
                if (!flags.Check(NKikimrBlobStorage::StatusIsValid)) {
                    return NumUnknown;
                } else if (flags.Check(NKikimrBlobStorage::StatusDiskSpaceBlack)) {
                    return NumBlack;
                } else if (flags.Check(NKikimrBlobStorage::StatusDiskSpaceRed)) {
                    return NumRed;
                } else if (flags.Check(NKikimrBlobStorage::StatusDiskSpaceOrange)) {
                    return NumOrange;
                } else if (flags.Check(NKikimrBlobStorage::StatusDiskSpaceLightOrange)) {
                    return NumLightOrange;
                } else if (flags.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
                    return NumYellow;
                } else if (flags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
                    return NumLightYellow;
                } else if (flags.Check(NKikimrBlobStorage::StatusDiskSpaceCyan)) {
                    return NumCyan;
                } else {
                    return NumGreen;
                }
            }
        };

        ::NMonitoring::TDynamicCounterPtr Counters;
        std::unordered_map<TString, TStoragePoolCounters> Map;

    public:
        TStoragePoolStat(::NMonitoring::TDynamicCounterPtr counters)
            : Counters(std::move(counters))
        {}

        ~TStoragePoolStat() {
            for (const auto& [name, value] : Map) {
                Counters->RemoveSubgroup("storagePoolId", value.Id);
            }
        }

        void AddStoragePool(const TString& id, const TString& name, ui64 allocatedSize) {
            const bool inserted = Map.try_emplace(id, Counters->GetSubgroup("storagePoolId", id), id, name,
                allocatedSize).second;
            Y_ABORT_UNLESS(inserted);
        }

        void DeleteStoragePool(const TString& id) {
            Counters->RemoveSubgroup("storagePool", id);
            const size_t erased = Map.erase(id);
            Y_ABORT_UNLESS(erased);
        }

        void RenameStoragePool(const TString& id, const TString& name) {
            const auto it = Map.find(id);
            Y_ABORT_UNLESS(it != Map.end());
            it->second.Rename(name);
        }

        void Update(const TString& id, std::optional<TStorageStatusFlags> previous, std::optional<TStorageStatusFlags> current) {
            const auto it = Map.find(id);
            Y_ABORT_UNLESS(it != Map.end());
            it->second.Update(previous, current);
        }

        void UpdateAllocatedSize(const TString& id, i64 allocatedSizeIncrement) {
            const auto it = Map.find(id);
            Y_ABORT_UNLESS(it != Map.end());
            it->second.UpdateAllocatedSize(allocatedSizeIncrement);
        }

        static TString ConvertId(const std::tuple<ui64, ui64>& id) {
            return TStringBuilder() << std::get<0>(id) << ":" << std::get<1>(id);
        }
    };

} // NKikimr::NBsController
