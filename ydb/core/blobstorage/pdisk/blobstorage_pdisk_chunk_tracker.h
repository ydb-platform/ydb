#pragma once
#include "defs.h"

#include "blobstorage_pdisk_color_limits.h"
#include "blobstorage_pdisk_data.h"
#include "blobstorage_pdisk_defs.h"
#include "blobstorage_pdisk_keeper_params.h"
#include "blobstorage_pdisk_quota_record.h"
#include "blobstorage_pdisk_util_space_color.h"

#include <util/generic/algorithm.h>
#include <util/generic/queue.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk quota tracker.
// Part of the in-memory state.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TPerOwnerQuotaTracker {

    TColorLimits ColorLimits;
    i64 Total;
    size_t ExpectedOwnerCount; // 0 means 'add and remove owners as you go'

    TStackVec<TOwner, 256> ActiveOwnerIds; // Can be accessed only from the main thread (changes only when owner is
                                        // added or removed).
    std::array<TQuotaRecord, 256> QuotaForOwner; // Always allocated, can be read from anywhere
    static_assert(sizeof(TOwner) == 1, "Make sure to use large enough QuotaForOwner buffer");

public:
    TPerOwnerQuotaTracker() {
        TColorLimits limits;
        Reset(0, limits);
    }

    void Reset(i64 total, const TColorLimits &limits) {
        ColorLimits = limits;
        Total = total;
        ExpectedOwnerCount = 0;
        ActiveOwnerIds.clear();
        QuotaForOwner.fill(TQuotaRecord{});
    }

    // The following code is expected to behave OK only when you reduce expected owner count.
    // Increasing expected owner count is fundamentally unfair and may cause instant jumps right into 0 free,
    // overusers will keep their unfair share as a result.
    void SetExpectedOwnerCount(size_t newOwnerCount) {
        if (newOwnerCount != ExpectedOwnerCount) {
            ExpectedOwnerCount = newOwnerCount;
            RedistributeQuotas();
        }
    }

    i64 ForceHardLimit(TOwner ownerId, i64 limit) {
        Y_ABORT_UNLESS(limit >= 0);
        return QuotaForOwner[ownerId].ForceHardLimit(limit, ColorLimits);
    }

    void RedistributeQuotas() {
        size_t parts = Max(ExpectedOwnerCount, ActiveOwnerIds.size());
        if (parts) {
            i64 limit = Total / parts;

            // Divide into equal parts and that's it.
            for (TOwner id : ActiveOwnerIds) {
                ForceHardLimit(id, limit);
            }
        }
    }

    void AddOwner(TOwner id, TVDiskID vdiskId) {
        TQuotaRecord &record = QuotaForOwner[id];
        Y_ABORT_UNLESS(record.GetHardLimit() == 0);
        Y_ABORT_UNLESS(record.GetFree() == 0);
        record.SetName(TStringBuilder() << "Owner# " << id);
        record.SetVDiskId(vdiskId);

        ActiveOwnerIds.push_back(id);
        if (ActiveOwnerIds.size() <= ExpectedOwnerCount || ExpectedOwnerCount == 0) {
            RedistributeQuotas();
        }
    }

    void RemoveOwner(TOwner id) {
        bool isFound = false;
        for (ui64 idx = 0; idx < ActiveOwnerIds.size(); ++idx) {
            if (ActiveOwnerIds[idx] == id) {
                ActiveOwnerIds[idx] = ActiveOwnerIds.back();
                ActiveOwnerIds.pop_back();
                isFound = true;
                break;
            }
        }
        Y_ABORT_UNLESS(isFound);
        ForceHardLimit(id, 0);
    }

    i64 AddSystemOwner(TOwner id, i64 quota, TString name) {
        TQuotaRecord &record = QuotaForOwner[id];
        Y_ABORT_UNLESS(record.GetHardLimit() == 0);
        Y_ABORT_UNLESS(record.GetFree() == 0);
        record.SetName(name);
        i64 inc = ForceHardLimit(id, quota);
        ActiveOwnerIds.push_back(id);
        return inc;
    }

    i64 GetHardLimit(TOwner id) const {
        return QuotaForOwner[id].GetHardLimit();
    }

    i64 GetFree(TOwner id) const {
        return QuotaForOwner[id].GetFree();
    }

    i64 GetUsed(TOwner id) const {
        return QuotaForOwner[id].GetUsed();
    }

    // Tread-safe status flag getter
    NKikimrBlobStorage::TPDiskSpaceColor::E EstimateSpaceColor(TOwner id, i64 allocationSize, double *occupancy) const {
        return QuotaForOwner[id].EstimateSpaceColor(allocationSize, occupancy);
    }

    bool TryAllocate(TOwner id, i64 count, TString &outErrorReason) {
        return QuotaForOwner[id].TryAllocate(count, outErrorReason);
    }

    bool ForceAllocate(TOwner id, i64 count) {
        return QuotaForOwner[id].ForceAllocate(count);
    }

    bool InitialAllocate(TOwner id, i64 count) {
        Y_ABORT_UNLESS(count >= 0);
        return QuotaForOwner[id].ForceAllocate(count);
    }

    void Release(TOwner id, i64 count) {
        QuotaForOwner[id].Release(count);
    }

    void PrintQuotaRow(IOutputStream &str, const TQuotaRecord& q) {
        str << "<tr>";
        str << "<td>" << q.Name << "</td>";
        str << "<td>" << (q.VDiskId ? q.VDiskId->ToStringWOGeneration() : "") << "</td>";
        str << "<td>" << q.GetHardLimit() << "</td>";
        str << "<td>" << q.GetFree() << "</td>";
        str << "<td>" << q.GetUsed() << "</td>";
        double occupancy;
        str << "<td>" << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(q.EstimateSpaceColor(0, &occupancy)) << "</td>";
        str << "<td>" << occupancy << "</td>";
        str << "<td>" << q.Cyan << "</td>";
        str << "<td>" << q.LightYellow << "</td>";
        str << "<td>" << q.Yellow << "</td>";
        str << "<td>" << q.LightOrange << "</td>";
        str << "<td>" << q.PreOrange << "</td>";
        str << "<td>" << q.Orange << "</td>";
        str << "<td>" << q.Red << "</td>";
        str << "<td>" << q.Black << "</td>";
        str << "</tr>";
    }

    void PrintHTML(IOutputStream &str, TQuotaRecord *sharedQuota, NKikimrBlobStorage::TPDiskSpaceColor::E *colorBorder) {
        str << "<pre>";
        str << "ColorLimits#\n";
        ColorLimits.Print(str);
        str << "\nTotal# " << Total;
        str << "\nExpectedOwnerCount# " << ExpectedOwnerCount;
        str << "\nActiveOwners# " << ActiveOwnerIds.size();
        if (colorBorder) {
            str << "\nColorBorder# " << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(*colorBorder) << "\n";
        }
        str << "</pre>";
        str << "<table class='table table-sortable tablesorter tablesorter-bootstrap table-bordered'>";
        str << R"_(<tr>
                <th>Name</th>
                <th>VDiskId</th>
                <th>HardLimit</th>
                <th>Free</th>
                <th>Used</th>
                <th>Color</th>
                <th>Occupancy</th>

                <th>Cyan</th>
                <th>LightYellow</th>
                <th>Yellow</th>
                <th>LightOrange</th>
                <th>PreOrange</th>
                <th>Orange</th>
                <th>Red</th>
                <th>Black</th>
            </tr>
        )_";
        if (sharedQuota) {
            PrintQuotaRow(str, *sharedQuota);
        }
        for (TOwner id : ActiveOwnerIds) {
            PrintQuotaRow(str, QuotaForOwner[id]);
        }
        str << "</table>";
    }

    ui32 ColorFlagLimit(TOwner id, NKikimrBlobStorage::TPDiskSpaceColor::E color) {
        return QuotaForOwner[id].ColorFlagLimit(color);
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk tracker.
// Part of the in-memory state.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TChunkTracker {

using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

    THolder<TPerOwnerQuotaTracker> GlobalQuota;
    THolder<TQuotaRecord> SharedQuota;
    THolder<TPerOwnerQuotaTracker> OwnerQuota;
    TKeeperParams Params;

    TColor::E ColorBorder = NKikimrBlobStorage::TPDiskSpaceColor::GREEN;
    double ColorBorderOccupancy = 0;

public:

    // OwnerSystem - common log quota
    // OwnerSystemLog - syslog quota
    // OwnerSystemReserve - system reseve quota
    // OwnerCommonStaticLog - common static log bonus
    //
    // OwnerBeginUser - per-VDisk qouta

    const i64 SysReserveSize = 5;
    const i64 CommonStaticLogSize = 70;
    i64 MaxCommonLogChunks = 200;

    TChunkTracker()
        : GlobalQuota(new TPerOwnerQuotaTracker())
        , SharedQuota(new TQuotaRecord())
        , OwnerQuota(new TPerOwnerQuotaTracker())
    {}

    bool Reset(const TKeeperParams &params, const TColorLimits &limits, TString &outErrorReason) {
        Params = params;

        GlobalQuota->Reset(params.TotalChunks, limits);
        i64 unappropriated = params.TotalChunks;

        unappropriated += GlobalQuota->AddSystemOwner(OwnerSystemLog, params.SysLogSize, "SysLog");
        if (unappropriated < 0) {
            outErrorReason = (TStringBuilder() << "Error adding OwnerSystemLog quota, size# " << params.SysLogSize
                    << " TotalChunks# " << params.TotalChunks);
            return false;
        }

        unappropriated += GlobalQuota->AddSystemOwner(OwnerSystemReserve, SysReserveSize, "System Reserve");
        if (unappropriated < 0) {
            outErrorReason = (TStringBuilder() << "Error adding OwnerSystemReserve quota, size# " << SysReserveSize
                    << " TotalChunks# " << params.TotalChunks);
            return false;
        }

        i64 staticLog = params.HasStaticGroups ? CommonStaticLogSize : 0;
        unappropriated += GlobalQuota->AddSystemOwner(OwnerCommonStaticLog, staticLog, "Common Log Static Group Bonus");
        if (unappropriated < 0) {
            outErrorReason = (TStringBuilder() << "Error adding OwnerCommonStaticLog quota, size# " << staticLog
                    << " TotalChunks# " << params.TotalChunks);
            return false;
        }

        MaxCommonLogChunks = params.MaxCommonLogChunks;
        if (params.SeparateCommonLog) {
            i64 commonLog = MaxCommonLogChunks;
            if (commonLog + staticLog < params.CommonLogSize) {
                commonLog = params.CommonLogSize - staticLog;
            }
            unappropriated += GlobalQuota->AddSystemOwner(OwnerSystem, commonLog, "Common Log");
            if (unappropriated < 0) {
                outErrorReason = (TStringBuilder() << "Error adding OwnerSystem (common log) quota, size# " << commonLog
                        << " TotalChunks# " << params.TotalChunks);
                return false;
            }
        }

        i64 chunksOwned = 0;
        for (auto& [ownerId, ownerInfo] : params.OwnersInfo) {
            chunksOwned += ownerInfo.ChunksOwned;
        }
        if (chunksOwned > unappropriated) {
            outErrorReason = (TStringBuilder() << "Error adding OwnerBeginUser quota, chunksOwned#" << chunksOwned
                    << " unappropriated# " << unappropriated << " TotalChunks# " << params.TotalChunks);
            return false;
        }
        unappropriated += GlobalQuota->AddSystemOwner(OwnerBeginUser, unappropriated, "Per Owner Chunk Pool");
        if (unappropriated < 0) {
            outErrorReason = (TStringBuilder() << "Error adding OwnerBeginUser quota, size# " << unappropriated
                    << " TotalChunks# " << params.TotalChunks);
            return false;
        }

        SharedQuota->SetName("SharedQuota");
        TColorLimits chunkLimits = TColorLimits::MakeChunkLimits();
        SharedQuota->ForceHardLimit(GlobalQuota->GetHardLimit(OwnerBeginUser), chunkLimits);
        OwnerQuota->Reset(GlobalQuota->GetHardLimit(OwnerBeginUser), chunkLimits);
        OwnerQuota->SetExpectedOwnerCount(params.ExpectedOwnerCount);

        for (auto& [ownerId, ownerInfo] : params.OwnersInfo) {
            i64 chunks = ownerInfo.ChunksOwned;
            AddOwner(ownerId, ownerInfo.VDiskId);
            if (chunks) {
                OwnerQuota->InitialAllocate(ownerId, chunks);
                bool isOk = SharedQuota->InitialAllocate(chunks);
                if (!isOk) {
                    outErrorReason = (TStringBuilder() << "Error adding OwnerQuota, ownerId# " << ownerId << " chunks# " << chunks);
                    return false;
                }
            }
        }

        if (params.CommonLogSize) {
            if (params.SeparateCommonLog) {
                if (!GlobalQuota->InitialAllocate(OwnerSystem, params.CommonLogSize)) {
                    outErrorReason = (TStringBuilder() << "Error InitialAllocate with SeparateCommonLog, size# " << params.CommonLogSize);
                    return false;
                }
            } else {
                if (!SharedQuota->InitialAllocate(params.CommonLogSize)) {
                    outErrorReason = (TStringBuilder() << "Error InitialAllocate, size# " << params.CommonLogSize);
                    return false;
                }
            }
        }

        ColorBorder = params.SpaceColorBorder;
        ColorBorderOccupancy = chunkLimits.GetOccupancyForColor(ColorBorder, GlobalQuota->GetHardLimit(OwnerBeginUser));
        return true;
    }

    void AddOwner(TOwner owner, TVDiskID vdiskId) {
        Y_ABORT_UNLESS(IsOwnerUser(owner));
        OwnerQuota->AddOwner(owner, vdiskId);
    }

    void RemoveOwner(TOwner owner) {
        Y_ABORT_UNLESS(IsOwnerUser(owner));
        OwnerQuota->RemoveOwner(owner);
    }

    i64 GetOwnerHardLimit(TOwner owner) const {
        if (IsOwnerUser(owner)) {
            return OwnerQuota->GetHardLimit(owner);
        } else {
            switch (owner) {
                case OwnerCommonStaticLog:
                    if (Params.SeparateCommonLog) {
                        return GlobalQuota->GetHardLimit(OwnerCommonStaticLog) + GlobalQuota->GetHardLimit(OwnerSystem);
                    } else {
                        return SharedQuota->GetHardLimit() + GlobalQuota->GetHardLimit(OwnerCommonStaticLog);
                    }
                    break;
                case OwnerSystem:
                    if (Params.SeparateCommonLog) {
                        return GlobalQuota->GetHardLimit(OwnerSystem);
                    } else {
                        return SharedQuota->GetHardLimit();
                    }
                    break;
                default:
                    return GlobalQuota->GetHardLimit(owner);
                    break;
            }
        }
    }

    i64 GetOwnerUsed(TOwner owner) const {
        return OwnerQuota->GetUsed(owner);
    }

    /////////////////////////////////////////////////////
    // for used space monitoring
    i64 GetTotalUsed() const {
        return SharedQuota->GetUsed();
    }

    i64 GetTotalHardLimit() const {
        return SharedQuota->GetHardLimit();
    }
    /////////////////////////////////////////////////////

    i64 GetOwnerFree(TOwner owner) const {
        if (IsOwnerUser(owner)) {
            // fix for CLOUDINC-1822: remove OwnerQuota->GetFree(owner) since it broke group balancing in Hive
            return SharedQuota->GetFree();
        } else {
            switch (owner) {
                case OwnerCommonStaticLog:
                    if (Params.SeparateCommonLog) {
                        return GlobalQuota->GetFree(OwnerCommonStaticLog) + GlobalQuota->GetFree(OwnerSystem);
                    } else {
                        return SharedQuota->GetFree() + GlobalQuota->GetFree(OwnerCommonStaticLog);
                    }
                    break;
                case OwnerSystem:
                    if (Params.SeparateCommonLog) {
                        return GlobalQuota->GetFree(OwnerSystem);
                    } else {
                        return SharedQuota->GetFree();
                    }
                    break;
                default:
                    return GlobalQuota->GetFree(owner);
                    break;
            }
        }
    }

    TStatusFlags GetSpaceStatusFlags(TOwner owner, double *occupancy) const {
        return SpaceColorToStatusFlag(GetSpaceColor(owner, occupancy));
    }

    TColor::E GetSpaceColor(TOwner owner, double *occupancy) const {
        return EstimateSpaceColor(owner, 0, occupancy);
    }

    // Estimate status flags after allocation of allocatinoSize
    TColor::E EstimateSpaceColor(TOwner owner, i64 allocationSize, double *occupancy) const {
        if (IsOwnerUser(owner)) {
            double ownerOccupancy, sharedOccupancy;
            TColor::E ret = Min(ColorBorder, OwnerQuota->EstimateSpaceColor(owner, allocationSize, &ownerOccupancy));
            ret = Max(ret, SharedQuota->EstimateSpaceColor(allocationSize, &sharedOccupancy));
            *occupancy = Max(
                Min(ColorBorderOccupancy, ownerOccupancy), // owner occupancy can't exceed its color border top value
                sharedOccupancy
            );
            return ret;
        } else {
            switch (owner) {
                case OwnerCommonStaticLog:
                    if (Params.SeparateCommonLog) {
                        if (GlobalQuota->GetHardLimit(OwnerCommonStaticLog) == 0) {
                            // No static group bonus, use common quota for the request
                            return GlobalQuota->EstimateSpaceColor(OwnerSystem, allocationSize, occupancy);
                        } else {
                            return GlobalQuota->EstimateSpaceColor(OwnerCommonStaticLog, allocationSize, occupancy);
                        }
                    } else {
                        if (GlobalQuota->GetHardLimit(OwnerCommonStaticLog) == 0) {
                            // No static group bonus, use common quota for the request
                            return SharedQuota->EstimateSpaceColor(allocationSize, occupancy);
                        } else {
                            return GlobalQuota->EstimateSpaceColor(OwnerCommonStaticLog, allocationSize, occupancy);
                        }
                    }
                case OwnerSystem:
                    if (Params.SeparateCommonLog) {
                        return GlobalQuota->EstimateSpaceColor(OwnerSystem, allocationSize, occupancy);
                    } else {
                        return SharedQuota->EstimateSpaceColor(allocationSize, occupancy);
                    }
                default:
                    return GlobalQuota->EstimateSpaceColor(owner, allocationSize, occupancy);
            }
        }
    }

    bool TryAllocate(TOwner owner, i64 count, TString &outErrorReason) {
        if (IsOwnerUser(owner)) {
            OwnerQuota->ForceAllocate(owner, count);
            return SharedQuota->TryAllocate(count, outErrorReason);
        } else {
            switch (owner) {
                case OwnerCommonStaticLog:
                    if (Params.SeparateCommonLog) {
                        if (GlobalQuota->TryAllocate(OwnerSystem, count, outErrorReason)) {
                            return true;
                        }
                    } else {
                        if (SharedQuota->TryAllocate(count, outErrorReason)) {
                            return true;
                        }
                    }
                    // Try bonus pool
                    return GlobalQuota->TryAllocate(OwnerCommonStaticLog, count, outErrorReason);
                    break;
                case OwnerSystem:
                    if (Params.SeparateCommonLog) {
                        return GlobalQuota->TryAllocate(owner, count, outErrorReason);
                    } else {
                        return SharedQuota->TryAllocate(count, outErrorReason);
                    }
                    break;
                default:
                    return GlobalQuota->TryAllocate(owner, count, outErrorReason);
                    break;
            }
        }
    }

    void Release(TOwner owner, i64 count) {
        if (IsOwnerUser(owner)) {
            OwnerQuota->Release(owner, count);
            SharedQuota->Release(count);
        } else {
            switch (owner) {
                case OwnerCommonStaticLog:
                case OwnerSystem:
                {
                    // Chunk release for common log (fill bonus pool first, then fill the common pool)
                    i64 usedBonus = GlobalQuota->GetUsed(OwnerCommonStaticLog);
                    i64 releaseBonus = Min(usedBonus, count);
                    if (releaseBonus) {
                        GlobalQuota->Release(OwnerCommonStaticLog, releaseBonus);
                    }
                    i64 releaseCommon = count - releaseBonus;
                    if (releaseCommon) {
                        if (Params.SeparateCommonLog) {
                            GlobalQuota->Release(OwnerSystem, releaseCommon);
                        } else {
                            SharedQuota->Release(releaseCommon);
                        }
                    }
                    break;
                }
                default:
                    // Chunk release for any other owner
                    GlobalQuota->Release(owner, count);
                    break;
            }
        }
    }

    void PrintHTML(IOutputStream &str) {
        str << "<h4>GlobalQuota</h4>";
        GlobalQuota->PrintHTML(str, nullptr, nullptr);
        str << "<h4>OwnerQuota</h4>";
        OwnerQuota->PrintHTML(str, SharedQuota.Get(), &ColorBorder);
    }

    ui32 ColorFlagLimit(TOwner owner, NKikimrBlobStorage::TPDiskSpaceColor::E color) {
        if (IsOwnerUser(owner)) {
            return OwnerQuota->ColorFlagLimit(owner, color);
        } else {
            switch (owner) {
                case OwnerSystem:
                    if (Params.SeparateCommonLog) {
                        return GlobalQuota->ColorFlagLimit(OwnerSystem, color);
                    } else {
                        return SharedQuota->ColorFlagLimit(color);
                    }
                case OwnerCommonStaticLog:
                default:
                    // Chunk release for any other owner
                    return GlobalQuota->ColorFlagLimit(owner, color);
                    break;
            }
        }
    }
};

} // NPDisk
} // NKikimr
