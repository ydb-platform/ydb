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
    i64 ExpectedOwnerSize; // 0 means 'derive owner quota from expected/active owner count'

    TStackVec<TOwner, 256> ActiveOwnerIds; // Can be accessed only from the main thread (changes only when owner is
                                        // added or removed).
    std::array<TQuotaRecord, 256> QuotaForOwner; // Always allocated, can be read from anywhere
    static_assert(sizeof(TOwner) == 1, "Make sure to use large enough QuotaForOwner buffer");

    ui32 NormalizeOwnerWeight(ui32 weight) const {
        return ExpectedOwnerSize ? 1 : weight;
    }

public:
    TPerOwnerQuotaTracker() {
        TColorLimits limits;
        Reset(0, limits);
    }

    void Reset(i64 total, const TColorLimits &limits) {
        ColorLimits = limits;
        Total = total;
        ExpectedOwnerCount = 0;
        ExpectedOwnerSize = 0;
        ActiveOwnerIds.clear();
        QuotaForOwner.fill(TQuotaRecord{});
    }

    // The following code is expected to behave OK only when you reduce expected owner count.
    // Increasing expected owner count is fundamentally unfair and may cause instant jumps right into 0 free,
    // overusers will keep their unfair share as a result.
    void SetExpectedOwnerCount(size_t newOwnerCount) {
        SetExpectedOwnerSettings(newOwnerCount, ExpectedOwnerSize);
    }

    void SetExpectedOwnerSize(i64 newOwnerSize) {
        SetExpectedOwnerSettings(ExpectedOwnerCount, newOwnerSize);
    }

    void SetExpectedOwnerSettings(size_t newOwnerCount, i64 newOwnerSize) {
        Y_VERIFY(newOwnerSize >= 0);
        ExpectedOwnerCount = newOwnerCount;
        ExpectedOwnerSize = newOwnerSize;
        if (ExpectedOwnerSize) {
            for (TOwner id : ActiveOwnerIds) {
                QuotaForOwner[id].SetWeight(1);
            }
        }
        RedistributeQuotas();
    }

    size_t GetNumActiveSlots() {
        size_t sum = 0;
        for (TOwner id: ActiveOwnerIds) {
            sum += QuotaForOwner[id].GetWeight();
        }
        return sum;
    }

    i64 ForceHardLimit(TOwner ownerId, i64 limit) {
        Y_VERIFY(limit >= 0);
        return QuotaForOwner[ownerId].ForceHardLimit(limit, ColorLimits);
    }

    void RedistributeQuotas() {
        if (ExpectedOwnerSize) {
            for (TOwner id : ActiveOwnerIds) {
                ForceHardLimit(id, ExpectedOwnerSize);
            }
        } else {
            size_t parts = Max(ExpectedOwnerCount, GetNumActiveSlots());
            if (parts) {
                i64 limit = Total / parts;

                // Divide into equal parts and that's it.
                for (TOwner id : ActiveOwnerIds) {
                    auto weight = QuotaForOwner[id].GetWeight();
                    ForceHardLimit(id, limit * weight);
                }
            }
        }
    }

    void AddOwner(TOwner id, TVDiskID vdiskId, ui32 weight) {
        TQuotaRecord &record = QuotaForOwner[id];
        Y_VERIFY(record.GetHardLimit() == 0);
        Y_VERIFY(record.GetFree() == 0);
        record.SetName(TStringBuilder() << "Owner# " << id);
        record.SetVDiskId(vdiskId);
        record.SetWeight(NormalizeOwnerWeight(weight));

        ActiveOwnerIds.push_back(id);
        RedistributeQuotas();
    }

    void SetOwnerWeight(TOwner id, ui32 weight) {
        auto it = std::find(ActiveOwnerIds.begin(), ActiveOwnerIds.end(), id);
        Y_VERIFY(it != ActiveOwnerIds.end());

        TQuotaRecord &record = QuotaForOwner[id];
        record.SetWeight(NormalizeOwnerWeight(weight));
        RedistributeQuotas();
    }

    ui32 GetOwnerWeight(TOwner id) {
        return QuotaForOwner[id].GetWeight();
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
        Y_VERIFY(isFound);
        ForceHardLimit(id, 0);
        RedistributeQuotas();
    }

    i64 AddSystemOwner(TOwner id, i64 quota, TString name) {
        TQuotaRecord &record = QuotaForOwner[id];
        Y_VERIFY(record.GetHardLimit() == 0);
        Y_VERIFY(record.GetFree() == 0);
        record.SetName(name);
        i64 inc = ForceHardLimit(id, quota);
        ActiveOwnerIds.push_back(id);
        return inc;
    }

    // Registers an owner with an externally managed quota, i.e. without redistributing Total between the owners.
    void AddReserveOwner(TOwner id, TVDiskID vdiskId, TString name) {
        TQuotaRecord &record = QuotaForOwner[id];
        Y_VERIFY(record.GetHardLimit() == 0);
        Y_VERIFY(record.GetFree() == 0);
        record.SetName(name);
        record.SetVDiskId(vdiskId);
        ActiveOwnerIds.push_back(id);
    }

    void RemoveReserveOwner(TOwner id) {
        for (ui64 idx = 0; idx < ActiveOwnerIds.size(); ++idx) {
            if (ActiveOwnerIds[idx] == id) {
                ActiveOwnerIds[idx] = ActiveOwnerIds.back();
                ActiveOwnerIds.pop_back();
                break;
            }
        }
        QuotaForOwner[id] = TQuotaRecord{};
    }

    i64 GetHardLimit(TOwner id) const {
        return QuotaForOwner[id].GetHardLimit();
    }

    i64 GetFree(TOwner id) const {
        return QuotaForOwner[id].GetFree();
    }

    i64 GetAllocatableFree(TOwner id) const {
        return QuotaForOwner[id].GetAllocatableFree();
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
        Y_VERIFY(count >= 0);
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
        str << "<td>" << q.GetWeight() << "</td>";
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

    void PrintHTML(IOutputStream &str, TQuotaRecord *sharedQuota, NKikimrBlobStorage::TPDiskSpaceColor::E *colorBorder, double *borderOccupancy) {
        str << "<pre>";
        str << "ColorLimits#\n";
        ColorLimits.Print(str);
        str << "\nTotal# " << Total;
        str << "\nExpectedOwnerCount# " << ExpectedOwnerCount;
        str << "\nExpectedOwnerSize# " << ExpectedOwnerSize;
        str << "\nActiveOwners# " << ActiveOwnerIds.size();
        str << "\nNumActiveSlots# " << GetNumActiveSlots();
        if (colorBorder) {
            str << "\nColorBorder# " << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(*colorBorder);
        }
        if (borderOccupancy) {
            str << "\nColorBorderOccupancy# " << *borderOccupancy;
        }
        str << "\n";
        str << "</pre>";
        str << "<table class='table table-sortable tablesorter tablesorter-bootstrap table-bordered'>";
        str << R"_(<tr>
                <th>Name</th>
                <th>VDiskId</th>
                <th>HardLimit</th>
                <th>Free</th>
                <th>Used</th>
                <th>Weight</th>
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
            str << "\n    ";
            PrintQuotaRow(str, *sharedQuota);
        }
        for (TOwner id : ActiveOwnerIds) {
            str << "\n    ";
            PrintQuotaRow(str, QuotaForOwner[id]);
        }
        str << "\n</table>";
    }

    ui32 ColorFlagLimit(TOwner id, NKikimrBlobStorage::TPDiskSpaceColor::E color) const {
        return QuotaForOwner[id].ColorFlagLimit(color);
    }

    double GetOccupancyForColor(NKikimrBlobStorage::TPDiskSpaceColor::E color) const {
        return ColorLimits.GetOccupancyForColor(color, Total);
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk tracker.
// Part of the in-memory state.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TChunkTracker {

using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

    struct TStaticOwnerInfo {
        TOwner OwnerId;
        // Becomes false when the owner is removed while it still holds chunks of its reserve. Such a reserve is
        // returned to the shared quota as the chunks are released.
        bool IsActive;
    };

    THolder<TPerOwnerQuotaTracker> GlobalQuota;
    THolder<TQuotaRecord> SharedQuota;
    THolder<TPerOwnerQuotaTracker> OwnerQuota;
    // Private chunk reserve of each static group owner, carved out of SharedQuota
    THolder<TPerOwnerQuotaTracker> StaticReserve;
    TStackVec<TStaticOwnerInfo, 8> StaticOwners;
    TKeeperParams Params;
    TColorLimits ColorLimits;
    TColorLimits ChunkLimits;

    // Set while the reserve of some owner differs from the desired one, i.e. while there is a reason to recalculate
    // the reserve as soon as chunks are released
    bool IsStaticReserveDirty = false;
    // Set while Reset is replaying the initial allocations, those must be done against the whole chunk pool
    bool IsStaticReserveSuppressed = false;

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

    TChunkTracker()
        : GlobalQuota(new TPerOwnerQuotaTracker())
        , SharedQuota(new TQuotaRecord())
        , OwnerQuota(new TPerOwnerQuotaTracker())
        , StaticReserve(new TPerOwnerQuotaTracker())
    {}

    bool Reset(const TKeeperParams &params, const TColorLimits &limits, TString &outErrorReason) {
        Params = params;
        ColorLimits = limits;

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

        i64 staticLog = params.HasStaticGroups ? params.CommonStaticLogChunks : 0;
        unappropriated += GlobalQuota->AddSystemOwner(OwnerCommonStaticLog, staticLog, "Common Log Static Group Bonus");
        if (unappropriated < 0) {
            outErrorReason = (TStringBuilder() << "Error adding OwnerCommonStaticLog quota, size# " << staticLog
                    << " TotalChunks# " << params.TotalChunks);
            return false;
        }

        if (params.SeparateCommonLog) {
            i64 commonLog = params.MaxCommonLogChunks;
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
        TColorLimits chunkLimits = TColorLimits::MakeChunkLimits(params.ChunkBaseLimit);
        ChunkLimits = chunkLimits;
        SharedQuota->ForceHardLimit(GlobalQuota->GetHardLimit(OwnerBeginUser), chunkLimits);
        OwnerQuota->Reset(GlobalQuota->GetHardLimit(OwnerBeginUser), chunkLimits);
        OwnerQuota->SetExpectedOwnerSettings(params.ExpectedOwnerCount, params.ExpectedOwnerSize);

        // The initial allocations below must be replayed against the whole chunk pool, the reserve of the static
        // group owners is carved out of the free space that is left after that.
        StaticReserve->Reset(0, chunkLimits);
        StaticOwners.clear();
        IsStaticReserveDirty = false;
        IsStaticReserveSuppressed = true;

        for (auto& [ownerId, ownerInfo] : params.OwnersInfo) {
            i64 chunks = ownerInfo.ChunksOwned;
            AddOwner(ownerId, ownerInfo.VDiskId, ownerInfo.Weight);
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
        ColorBorderOccupancy = OwnerQuota->GetOccupancyForColor(ColorBorder);

        IsStaticReserveSuppressed = false;
        RecomputeStaticReserve();
        return true;
    }

    void AddOwner(TOwner owner, TVDiskID vdiskId, ui32 weight = 1) {
        Y_VERIFY(IsOwnerUser(owner));
        OwnerQuota->AddOwner(owner, vdiskId, weight);
        if (IsStaticGroupVDisk(vdiskId)) {
            StaticReserve->AddReserveOwner(owner, vdiskId, TStringBuilder() << "StaticReserve Owner# " << owner);
            StaticOwners.push_back({.OwnerId = owner, .IsActive = true});
        }
        RecomputeStaticReserve();
    }

    void SetOwnerWeight(TOwner owner, ui32 weight) {
        Y_VERIFY(IsOwnerUser(owner));
        OwnerQuota->SetOwnerWeight(owner, weight);
        RecomputeStaticReserve();
    }

    void RemoveOwner(TOwner owner) {
        Y_VERIFY(IsOwnerUser(owner));
        for (TStaticOwnerInfo &info : StaticOwners) {
            if (info.OwnerId == owner) {
                info.IsActive = false;
            }
        }
        OwnerQuota->RemoveOwner(owner);
        RecomputeStaticReserve();
    }

    ui32 GetOwnerWeight(TOwner owner) {
        Y_VERIFY(IsOwnerUser(owner));
        return OwnerQuota->GetOwnerWeight(owner);
    }

    ui32 GetNumActiveSlots() const {
        return OwnerQuota->GetNumActiveSlots();
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

    // Private chunk reserve of a static group owner, 0 for all the other owners
    i64 GetOwnerStaticReserve(TOwner owner) const {
        return StaticReserve->GetHardLimit(owner);
    }

    i64 GetOwnerStaticReserveUsed(TOwner owner) const {
        return StaticReserve->GetUsed(owner);
    }

    i64 GetLogChunkCount() const {
        return GlobalQuota->GetUsed(OwnerSystem);
    }

    /////////////////////////////////////////////////////
    // for used space monitoring
    // The reserve of the static group owners is a part of the user chunk pool, so it is included into the disk-wide
    // numbers below
    i64 GetTotalUsed() const {
        return SharedQuota->GetUsed() + GetStaticReserveUsed();
    }

    i64 GetTotalHardLimit() const {
        return SharedQuota->GetHardLimit() + GetStaticReserveHardLimit();
    }

    TColor::E GetPDiskCapacityAlert() const {
        double occupancy;
        TColor::E sharedColor = NPDisk::EstimateSpaceColor(ChunkLimits, SharedQuota->GetFree() + GetStaticReserveFree(),
                GetTotalHardLimit(), &occupancy);
        if (Params.SeparateCommonLog) {
            TColor::E commonLogColor = GlobalQuota->EstimateSpaceColor(OwnerSystem, 0, &occupancy);
            return Max(sharedColor, commonLogColor);
        } else {
            return sharedColor;
        }
    }
    /////////////////////////////////////////////////////

    i64 GetOwnerFree(TOwner owner, bool personal) const {
        if (IsOwnerUser(owner)) {
            // See CLOUDINC-1822: OwnerQuota->GetFree(owner) broke group balancing in Hive and was replaced by SharedQuota
            // A static group owner can also allocate from its private reserve
            return personal ? OwnerQuota->GetFree(owner) : SharedQuota->GetFree() + StaticReserve->GetFree(owner);
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
            double ownerOccupancy, poolOccupancy;
            TColor::E poolColor;
            if (HasStaticReserve(owner)) {
                // A static group owner allocates from the shared quota and from its private reserve, both of them
                // make up the pool it sees
                poolColor = NPDisk::EstimateSpaceColor(ChunkLimits,
                        SharedQuota->GetFree() + StaticReserve->GetFree(owner) - allocationSize,
                        SharedQuota->GetHardLimit() + StaticReserve->GetHardLimit(owner), &poolOccupancy);
            } else {
                poolColor = SharedQuota->EstimateSpaceColor(allocationSize, &poolOccupancy);
            }
            TColor::E ret = Min(ColorBorder, OwnerQuota->EstimateSpaceColor(owner, allocationSize, &ownerOccupancy));
            ret = Max(ret, poolColor);
            *occupancy = Max(
                Min(ColorBorderOccupancy, ownerOccupancy), // owner occupancy can't exceed its color border top value
                poolOccupancy
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
            if (SharedQuota->TryAllocate(count, outErrorReason)) {
                return true;
            }
            // A static group owner takes what is left in the shared quota and the rest from its private reserve
            if (HasStaticReserve(owner)) {
                i64 fromShared = SharedQuota->GetAllocatableFree();
                if (StaticReserve->TryAllocate(owner, count - fromShared, outErrorReason)) {
                    if (fromShared == 0 || SharedQuota->TryAllocate(fromShared, outErrorReason)) {
                        return true;
                    }
                    StaticReserve->Release(owner, count - fromShared);
                }
            }
            OwnerQuota->Release(owner, count);
            return false;
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
            // Refill the private reserve of the static group owner first to restore its protection
            i64 usedReserve = StaticReserve->GetUsed(owner);
            i64 releaseReserve = Min(usedReserve, count);
            if (releaseReserve) {
                StaticReserve->Release(owner, releaseReserve);
            }
            i64 releaseShared = count - releaseReserve;
            if (releaseShared) {
                SharedQuota->Release(releaseShared);
            }
            if (IsStaticReserveDirty) {
                RecomputeStaticReserve();
            }
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
        GlobalQuota->PrintHTML(str, nullptr, nullptr, nullptr);
        str << "<h4>OwnerQuota</h4>";
        OwnerQuota->PrintHTML(str, SharedQuota.Get(), &ColorBorder, &ColorBorderOccupancy);
        if (!StaticOwners.empty()) {
            str << "<h4>StaticReserve</h4>";
            StaticReserve->PrintHTML(str, nullptr, nullptr, nullptr);
        }
    }

    ui32 ColorFlagLimit(TOwner owner, NKikimrBlobStorage::TPDiskSpaceColor::E color) const {
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

    void SetExpectedOwnerCount(size_t newOwnerCount) {
        Params.ExpectedOwnerCount = newOwnerCount;
        OwnerQuota->SetExpectedOwnerCount(newOwnerCount);
        RecomputeStaticReserve();
    }

    void SetExpectedOwnerSize(i64 newOwnerSize) {
        Params.ExpectedOwnerSize = newOwnerSize;
        OwnerQuota->SetExpectedOwnerSize(newOwnerSize);
        RecomputeStaticReserve();
    }

    void SetExpectedOwnerSettings(size_t newOwnerCount, i64 newOwnerSize) {
        Params.ExpectedOwnerCount = newOwnerCount;
        Params.ExpectedOwnerSize = newOwnerSize;
        OwnerQuota->SetExpectedOwnerSettings(newOwnerCount, newOwnerSize);
        RecomputeStaticReserve();
    }

    void SetColorBorder(NKikimrBlobStorage::TPDiskSpaceColor::E colorBorder) {
        ColorBorder = colorBorder;
        ColorBorderOccupancy = OwnerQuota->GetOccupancyForColor(ColorBorder);
    }

    void SetStaticGroupChunkReservePerMille(ui32 perMille) {
        Params.StaticGroupChunkReservePerMille = perMille;
        RecomputeStaticReserve();
    }

private:
    // A static group owner must keep working even when its neighbours from dynamic groups have eaten the whole
    // shared quota, otherwise the tablets living in the static group take the whole cluster down. Each of them gets
    // a private chunk reserve of its personal quota size, carved out of the shared quota.
    void RecomputeStaticReserve() {
        if (IsStaticReserveSuppressed || StaticOwners.empty()) {
            return;
        }

        const i64 pool = GlobalQuota->GetHardLimit(OwnerBeginUser);
        // On a disk where the common log shares the chunk pool with the owners a reserve would be taken away from
        // the log, which is a way more dangerous kind of starvation
        const i64 maxTotal = Params.SeparateCommonLog ? pool * (i64)Params.StaticGroupChunkReservePerMille / 1000 : 0;

        TStackVec<i64, 8> desired(StaticOwners.size());
        i64 desiredTotal = 0;
        for (size_t idx = 0; idx < StaticOwners.size(); ++idx) {
            desired[idx] = GetDesiredStaticReserve(StaticOwners[idx]);
            desiredTotal += desired[idx];
        }
        if (desiredTotal > maxTotal) {
            for (i64 &value : desired) {
                value = value * maxTotal / desiredTotal;
            }
        }

        // Give back everything that is not needed anymore before taking anything. Otherwise a reserve that has to
        // grow is limited by the free space of the shared quota while the space it needs is still held by another
        // owner of the very same list, and the surplus released later in the pass is left for the dynamic owners
        // to grab.
        for (size_t idx = 0; idx < StaticOwners.size(); ++idx) {
            ShrinkStaticReserve(StaticOwners[idx].OwnerId, desired[idx]);
        }

        i64 deficitTotal = 0;
        for (size_t idx = 0; idx < StaticOwners.size(); ++idx) {
            deficitTotal += GetStaticReserveDeficit(StaticOwners[idx].OwnerId, desired[idx]);
        }
        if (deficitTotal) {
            const i64 available = Max<i64>(SharedQuota->GetFree(), 0);
            if (deficitTotal > available) {
                // There is not enough space for everybody, split it proportionally to what each owner is missing,
                // so that the first owner of the list can not take it all
                for (size_t idx = 0; idx < StaticOwners.size(); ++idx) {
                    GrowStaticReserve(StaticOwners[idx].OwnerId,
                            GetStaticReserveDeficit(StaticOwners[idx].OwnerId, desired[idx]) * available / deficitTotal);
                }
            }
            // Either every deficit fits into the shared quota, or these are the few chunks lost to the rounding above
            for (size_t idx = 0; idx < StaticOwners.size(); ++idx) {
                GrowStaticReserve(StaticOwners[idx].OwnerId,
                        GetStaticReserveDeficit(StaticOwners[idx].OwnerId, desired[idx]));
            }
        }

        IsStaticReserveDirty = false;
        for (size_t idx = 0; idx < StaticOwners.size();) {
            const TStaticOwnerInfo &info = StaticOwners[idx];
            const i64 current = StaticReserve->GetHardLimit(info.OwnerId);
            if (current != desired[idx]) {
                // There was not enough free space in the shared quota for the whole reserve, try again as soon as
                // some chunks are released
                IsStaticReserveDirty = true;
            }
            if (!info.IsActive && current == 0) {
                StaticReserve->RemoveReserveOwner(info.OwnerId);
                StaticOwners[idx] = StaticOwners.back();
                StaticOwners.pop_back();
                desired[idx] = desired.back();
                desired.pop_back();
            } else {
                ++idx;
            }
        }
    }

    i64 GetDesiredStaticReserve(const TStaticOwnerInfo &info) const {
        return info.IsActive ? OwnerQuota->GetHardLimit(info.OwnerId) : 0;
    }

    i64 GetStaticReserveDeficit(TOwner owner, i64 desired) const {
        return Max<i64>(desired - StaticReserve->GetHardLimit(owner), 0);
    }

    // Returns the unused part of a reserve to the shared quota. The reserve is never left with negative free space,
    // so a reserve the owner still holds chunks of shrinks only as those chunks are released.
    void ShrinkStaticReserve(TOwner owner, i64 desired) {
        const i64 current = StaticReserve->GetHardLimit(owner);
        const i64 decrement = Min(current - desired, Max<i64>(StaticReserve->GetFree(owner), 0));
        if (decrement > 0) {
            StaticReserve->ForceHardLimit(owner, current - decrement);
            SharedQuota->ForceHardLimit(SharedQuota->GetHardLimit() + decrement, ChunkLimits);
        }
    }

    // Takes free space of the shared quota into a reserve. The shared quota is never left with negative free space,
    // so an overused disk just gets a smaller reserve, and the reserve grows as soon as the chunks are released.
    void GrowStaticReserve(TOwner owner, i64 increment) {
        increment = Min(increment, Max<i64>(SharedQuota->GetFree(), 0));
        if (increment > 0) {
            SharedQuota->ForceHardLimit(SharedQuota->GetHardLimit() - increment, ChunkLimits);
            StaticReserve->ForceHardLimit(owner, StaticReserve->GetHardLimit(owner) + increment);
        }
    }

    bool HasStaticReserve(TOwner owner) const {
        return StaticReserve->GetHardLimit(owner) > 0;
    }

    i64 GetStaticReserveHardLimit() const {
        i64 total = 0;
        for (const TStaticOwnerInfo &info : StaticOwners) {
            total += StaticReserve->GetHardLimit(info.OwnerId);
        }
        return total;
    }

    i64 GetStaticReserveUsed() const {
        i64 total = 0;
        for (const TStaticOwnerInfo &info : StaticOwners) {
            total += StaticReserve->GetUsed(info.OwnerId);
        }
        return total;
    }

    i64 GetStaticReserveFree() const {
        i64 total = 0;
        for (const TStaticOwnerInfo &info : StaticOwners) {
            total += StaticReserve->GetFree(info.OwnerId);
        }
        return total;
    }
};

} // NPDisk
} // NKikimr
