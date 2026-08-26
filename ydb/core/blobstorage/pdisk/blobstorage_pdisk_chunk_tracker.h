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

    TString GetVDiskIdString(TOwner id) const {
        const std::optional<TVDiskID> &vdiskId = QuotaForOwner[id].VDiskId;
        return vdiskId ? vdiskId->ToStringWOGeneration() : TString();
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

    THolder<TPerOwnerQuotaTracker> GlobalQuota;
    THolder<TQuotaRecord> SharedQuota;
    THolder<TPerOwnerQuotaTracker> OwnerQuota;
    TKeeperParams Params;
    TColorLimits ColorLimits;

    // Chunk reserve of the static group owners. Nothing is taken out of the shared quota for it: the reserve is the
    // number of chunks of the shared quota an owner is guaranteed to be able to allocate, and the part of it the owner
    // does not use yet is simply not given to anybody else.
    std::array<TAtomic, 256> StaticReserve = {}; // Always allocated, can be read from anywhere
    static_assert(sizeof(TOwner) == 1, "Make sure to use large enough StaticReserve buffer");
    // Sum of the unused reserves, i.e. the free space of the shared quota that is held back from the other owners
    TAtomic StaticReserveFreeTotal = 0;
    TStackVec<TOwner, 8> StaticOwners; // Can be accessed only from the main thread

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
        SharedQuota->ForceHardLimit(GlobalQuota->GetHardLimit(OwnerBeginUser), chunkLimits);
        OwnerQuota->Reset(GlobalQuota->GetHardLimit(OwnerBeginUser), chunkLimits);
        OwnerQuota->SetExpectedOwnerSettings(params.ExpectedOwnerCount, params.ExpectedOwnerSize);

        for (TAtomic &reserve : StaticReserve) {
            AtomicSet(reserve, 0);
        }
        AtomicSet(StaticReserveFreeTotal, 0);
        StaticOwners.clear();

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

        RecomputeStaticReserve();
        return true;
    }

    void AddOwner(TOwner owner, TVDiskID vdiskId, ui32 weight = 1) {
        Y_VERIFY(IsOwnerUser(owner));
        OwnerQuota->AddOwner(owner, vdiskId, weight);
        if (IsStaticGroupVDisk(vdiskId)) {
            StaticOwners.push_back(owner);
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
        for (ui64 idx = 0; idx < StaticOwners.size(); ++idx) {
            if (StaticOwners[idx] == owner) {
                StaticOwners[idx] = StaticOwners.back();
                StaticOwners.pop_back();
                AtomicSet(StaticReserve[owner], 0);
                break;
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

    // Number of chunks of the shared quota a static group owner is guaranteed to be able to allocate, 0 for all the
    // other owners
    i64 GetOwnerStaticReserve(TOwner owner) const {
        return AtomicGet(StaticReserve[owner]);
    }

    // The part of the reserve the owner does not use yet, i.e. the space held back from the other owners for it
    i64 GetOwnerStaticReserveFree(TOwner owner) const {
        return Max<i64>(GetOwnerStaticReserve(owner) - OwnerQuota->GetUsed(owner), 0);
    }

    i64 GetLogChunkCount() const {
        return GlobalQuota->GetUsed(OwnerSystem);
    }

    /////////////////////////////////////////////////////
    // for used space monitoring
    i64 GetTotalUsed() const {
        return SharedQuota->GetUsed();
    }

    i64 GetTotalHardLimit() const {
        return SharedQuota->GetHardLimit();
    }

    TColor::E GetPDiskCapacityAlert() const {
        double occupancy;
        TColor::E sharedColor = SharedQuota->EstimateSpaceColor(0, &occupancy);
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
            // The reserves of the other static group owners are not available to this one
            return personal ? OwnerQuota->GetFree(owner)
                    : Max<i64>(SharedQuota->GetFree() - GetStaticReserveFloor(owner), 0);
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
            // The reserves of the other static group owners are as good as occupied for this one
            const i64 unavailable = allocationSize + GetStaticReserveFloor(owner);
            TColor::E ret = Min(ColorBorder, OwnerQuota->EstimateSpaceColor(owner, allocationSize, &ownerOccupancy));
            ret = Max(ret, SharedQuota->EstimateSpaceColor(unavailable, &sharedOccupancy));
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
            // The reserves of the static group owners are the last chunks of the shared quota, this owner may take
            // them only as far as the reserve is its own
            const i64 floor = GetStaticReserveFloor(owner);
            if (floor && SharedQuota->GetAllocatableFree() - floor < count) {
                outErrorReason = (TStringBuilder() << "Allocation of count# " << count
                        << " chunks does not fit into the shared quota, free# " << SharedQuota->GetFree()
                        << " reserved for the static group owners# " << floor
                        << " Marker# BPQ11");
                return false;
            }
            OwnerQuota->ForceAllocate(owner, count);
            if (SharedQuota->TryAllocate(count, outErrorReason)) {
                RecomputeStaticReserveFree();
                return true;
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
            SharedQuota->Release(count);
            // A static group owner that releases chunks gets the released part of its reserve held back again
            RecomputeStaticReserveFree();
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
            str << "<pre>";
            str << "StaticGroupChunkReservePerMille# " << Params.StaticGroupChunkReservePerMille << "\n";
            str << "MaxTotal# " << GetStaticReserveMaxTotal() << "\n";
            str << "Total# " << GetStaticReserveTotal() << "\n";
            str << "HeldBack# " << AtomicGet(StaticReserveFreeTotal) << "\n";
            str << "</pre>";
            str << "<table class='table table-sortable tablesorter tablesorter-bootstrap table-bordered'>";
            str << R"_(<tr>
                <th>Name</th>
                <th>VDiskId</th>
                <th>Reserve</th>
                <th>HeldBack</th>
                <th>Used</th>
            </tr>
        )_";
            for (TOwner owner : StaticOwners) {
                str << "\n    <tr>";
                str << "<td>Owner# " << (ui32)owner << "</td>";
                str << "<td>" << OwnerQuota->GetVDiskIdString(owner) << "</td>";
                str << "<td>" << GetOwnerStaticReserve(owner) << "</td>";
                str << "<td>" << GetOwnerStaticReserveFree(owner) << "</td>";
                str << "<td>" << OwnerQuota->GetUsed(owner) << "</td>";
                str << "</tr>";
            }
            str << "\n</table>";
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
    // A static group owner must keep working even when its neighbours from dynamic groups have eaten the whole shared
    // quota, otherwise the tablets living in the static group take the whole cluster down. Each of them is guaranteed
    // its personal quota worth of chunks: the part of that guarantee the owner does not use yet is held back from the
    // other owners instead of being taken out of the shared quota, so nothing of the disk is left idle.
    void RecomputeStaticReserve() {
        if (StaticOwners.empty()) {
            AtomicSet(StaticReserveFreeTotal, 0);
            return;
        }

        const i64 maxTotal = GetStaticReserveMaxTotal();
        i64 desiredTotal = 0;
        for (TOwner owner : StaticOwners) {
            desiredTotal += OwnerQuota->GetHardLimit(owner);
        }

        for (TOwner owner : StaticOwners) {
            i64 reserve = OwnerQuota->GetHardLimit(owner);
            if (desiredTotal > maxTotal) {
                // The guarantees do not fit into the budget, scale all of them down proportionally
                reserve = reserve * maxTotal / desiredTotal;
            }
            AtomicSet(StaticReserve[owner], reserve);
        }
        RecomputeStaticReserveFree();
    }

    // The reserve of an owner shrinks as it allocates chunks and grows back as it releases them, without any effect
    // on the reserves of the other owners
    void RecomputeStaticReserveFree() {
        i64 total = 0;
        for (TOwner owner : StaticOwners) {
            total += GetOwnerStaticReserveFree(owner);
        }
        AtomicSet(StaticReserveFreeTotal, total);
    }

    // Upper bound for the total size of the reserves, keeps the reserves of a disk with few owners from swallowing
    // the whole chunk pool
    i64 GetStaticReserveMaxTotal() const {
        return GlobalQuota->GetHardLimit(OwnerBeginUser) * (i64)Params.StaticGroupChunkReservePerMille / 1000;
    }

    i64 GetStaticReserveTotal() const {
        i64 total = 0;
        for (TOwner owner : StaticOwners) {
            total += GetOwnerStaticReserve(owner);
        }
        return total;
    }

    // Free space of the shared quota that is reserved for the static group owners other than this one, i.e. the part
    // of it this owner is not allowed to touch
    i64 GetStaticReserveFloor(TOwner owner) const {
        return Max<i64>(AtomicGet(StaticReserveFreeTotal) - GetOwnerStaticReserveFree(owner), 0);
    }
};

} // NPDisk
} // NKikimr
