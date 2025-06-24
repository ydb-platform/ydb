#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy.h>

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

class TVDiskMock {
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    const TVDiskID VDiskId;
    TMap<TLogoBlobID, TString> Blobs;
    TMap<TLogoBlobID, TString> NotYetBlobs;
    bool IsError;
    NKikimrProto::EReplyStatus Status;
    TMap<TLogoBlobID, NKikimrProto::EReplyStatus> SpecialStatuses;
public:
    TIntrusivePtr<NBackpressure::TFlowRecord> FlowRecord;

    TVDiskMock(TVDiskID vDiskId)
        : VDiskId(vDiskId)
        , IsError(false)
        , Status(NKikimrProto::OK)
        , FlowRecord(new NBackpressure::TFlowRecord())
    {
    }

    void SetInfo(TIntrusivePtr<TBlobStorageGroupInfo> info) {
        Info = info;
    }

    TActorId GetActorId() {
        return Info->GetActorId(Info->GetOrderNumber(VDiskId));
    }

    TVDiskID GetVDiskId() const {
        return VDiskId;
    }

    NKikimrProto::EReplyStatus Put(const TLogoBlobID id, const TString &data) {
        if (IsError) {
            return Status;
        }
        auto it = SpecialStatuses.find(id);
        if (it != SpecialStatuses.end()) {
            return it->second;
        }
        if (Blobs.count(id)) {
            return NKikimrProto::ALREADY;
        }
        Blobs[id] = data;
        return NKikimrProto::OK;
    }

    void Wipe() {
        Blobs.clear();
        FlowRecord->SetPredictedDelayNs(0);
    }

    void SetError(NKikimrProto::EReplyStatus status) {
        if (status == NKikimrProto::NOT_YET) {
            for (auto it = Blobs.begin(); it != Blobs.end(); ++it) {
                NotYetBlobs[it->first] = it->second;
            }
        } else {
            IsError = true;
            Status = status;
        }
    }

    void SetNotYet(const TLogoBlobID blobID) {
        auto it = Blobs.find(blobID);
        if (it != Blobs.end()) {
            NotYetBlobs[blobID] = it->second;
        }
    }

    void SetPredictedDelayNs(ui64 predictDelaysNs) {
        FlowRecord->SetPredictedDelayNs(predictDelaysNs);
    }

    void UnsetError() {
        NotYetBlobs.clear();
        IsError = false;
        Status = NKikimrProto::OK;
    }

    void SetSpecialStatus(const TLogoBlobID blobID, NKikimrProto::EReplyStatus status) {
        SpecialStatuses[blobID] = status;
    }

    void OnVGet(const TEvBlobStorage::TEvVGet &vGet, TEvBlobStorage::TEvVGetResult &outVGetResult) {
        auto &request = vGet.Record;
        if (IsError) {
            outVGetResult.MakeError(Status, TString(), request);
            return;
        }

        outVGetResult.Record.SetStatus(NKikimrProto::OK);

        Y_ABORT_UNLESS(request.HasVDiskID());
        TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
        VDiskIDFromVDiskID(vDiskId, outVGetResult.Record.MutableVDiskID());
        if (request.HasCookie()) {
            outVGetResult.Record.SetCookie(request.GetCookie());
        }

        if (request.HasRangeQuery()) {
            ProcessRangeIndexQuery(vGet, outVGetResult);
            return;
        }

        // TODO: Check for overlapping / out of order queries
        size_t size = request.ExtremeQueriesSize();
        for (unsigned i = 0; i < size; i++) {
            const NKikimrBlobStorage::TExtremeQuery &query = request.GetExtremeQueries(i);
            TLogoBlobID id = LogoBlobIDFromLogoBlobID(query.GetId());
            ui64 partId = id.PartId();
            ui64 partBegin = partId ? partId : 1;
            ui64 partEnd = partId ? partId : 7;

            ui64 shift = (query.HasShift() ? query.GetShift() : 0);
            ui64 *cookie = nullptr;
            ui64 cookieValue = 0;
            if (query.HasCookie()) {
                cookieValue = query.GetCookie();
                cookie = &cookieValue;
            }

            ui64 partSize = Info->Type.PartSize(id);

            ui64 rShift = (shift > partSize ? partSize : shift);
            ui64 resultSize = query.HasSize() ? query.GetSize() : 0;
            if (!resultSize) {
                resultSize = partSize;
            }
            resultSize = Min(partSize - rShift, resultSize);

            bool isNoData = true;
            TLogoBlobID idFirst(id, partBegin);
            TLogoBlobID idLast(id, partEnd);
            for (auto it = Blobs.lower_bound(idFirst); it != Blobs.end() && it->first <= idLast; it++) {
                isNoData = false;
                if (NotYetBlobs.find(it->first) != NotYetBlobs.end()) {
                    outVGetResult.AddResult(NKikimrProto::NOT_YET, it->first, shift, static_cast<ui32>(resultSize), cookie);
                } else {
                    auto buffer = TRcBuf::Copy(it->second.data() + rShift, resultSize);
                    outVGetResult.AddResult(NKikimrProto::OK, it->first, shift, TRope(std::move(buffer)), cookie);
                }
            }
            if (isNoData) {
                CTEST << "VDisk# " << VDiskId.ToString() << " blob# " << id.ToString() << " NODATA" << Endl;
                outVGetResult.AddResult(NKikimrProto::NODATA, id, cookie);
            }
        }
    }

    void ProcessRangeIndexQuery(const TEvBlobStorage::TEvVGet& vGet, TEvBlobStorage::TEvVGetResult& outVGetResult) {
        const auto& record = vGet.Record;
        auto vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        const NKikimrBlobStorage::TRangeQuery& query = record.GetRangeQuery();

        auto first = LogoBlobIDFromLogoBlobID(query.GetFrom());
        auto last = LogoBlobIDFromLogoBlobID(query.GetTo());

        ui64 *cookie = nullptr;
        ui64 cookieValue = 0;
        if (query.HasCookie()) {
            cookieValue = query.GetCookie();
            cookie = &cookieValue;
        }

        TMap<TLogoBlobID, TIngress> resultBlobs;

        if (first <= last) {
            auto from = Blobs.lower_bound(first);
            auto to = Blobs.upper_bound(last);
            for (auto it = from; it != to; ++it) {
                auto& id = it->first;
                auto ingress = TIngress::CreateIngressWithLocal(&Info->GetTopology(), vDiskId, id);
                resultBlobs[id.FullID()].Merge(*ingress);
            }
            for (auto it = resultBlobs.begin(); it != resultBlobs.end(); ++it) {
                ui64 ingr = it->second.Raw();
                ui64 *pingr = (record.GetShowInternals() ? &ingr : nullptr);
                const NMatrix::TVectorType local = it->second.LocalParts(Info->GetTopology().GType);
                outVGetResult.AddResult(NKikimrProto::OK, it->first, cookie, pingr, &local, true, false);
            }
        } else {
            auto from = Blobs.upper_bound(first);
            auto to = Blobs.lower_bound(last);
            for (auto it = std::reverse_iterator(from); it != std::reverse_iterator(to); ++it) {
                auto& id = it->first;
                auto ingress = TIngress::CreateIngressWithLocal(&Info->GetTopology(), vDiskId, id);
                resultBlobs[id.FullID()].Merge(*ingress);
            }
            for (auto it = resultBlobs.rbegin(); it != resultBlobs.rend(); ++it) {
                ui64 ingr = it->second.Raw();
                ui64 *pingr = (record.GetShowInternals() ? &ingr : nullptr);
                const NMatrix::TVectorType local = it->second.LocalParts(Info->GetTopology().GType);
                outVGetResult.AddResult(NKikimrProto::OK, it->first, cookie, pingr, &local, true, false);
            }
        }

    }
};

class TBlobTestSet {
public:
    struct TBlob {
        TLogoBlobID Id;
        TString Data;

        TBlob(TLogoBlobID id, TString data)
            : Id(id)
            , Data(data)
        {}
    };

private:
    TVector<TBlob> Blobs;
    TMap<TLogoBlobID, TString> DataById;

    TString AlphaData(ui32 size) {
        TString data = TString::Uninitialized(size);
        char *p = data.Detach();
        for (ui32 offset = 0; offset < size; ++offset) {
            p[offset] = (ui8)offset;
        }
        return data;
    }
public:
    TBlobTestSet()
    {}

    template <typename TIter>
    void AddBlobs(TIter begin, TIter end) {
        for (auto it = begin; it != end; ++it) {
            Blobs.emplace_back(*it);
            DataById[it->Id] = it->Data;
        }
    }

    template <typename TCont>
    void AddBlobs(const TCont& blobs) {
        AddBlobs(blobs.begin(), blobs.end());
    }

    void GenerateSet(ui32 setIdx, ui32 count, ui32 forceSize = 0) {
        switch (setIdx) {
            case 0:
                for (ui64 i = 0; i < count; ++i) {
                    ui32 step = i + 1;
                    ui32 size = forceSize ? forceSize : 750 + i * 31;
                    TLogoBlobID id(1, 2, step, 0, size, 0);
                    TString data = AlphaData(size);
                    Blobs.emplace_back(id, data);
                    DataById[id] = data;
                }
                break;
            case 1:
                for (ui64 i = 0; i < count; ++i) {
                    ui32 step = i + 1;
                    ui32 size = forceSize ? forceSize : 92 + i * 31;
                    TLogoBlobID id(1, 2, step, 0, size, 0);
                    TString data = AlphaData(size);
                    Blobs.emplace_back(id, data);
                    DataById[id] = data;
                }
                break;
            case 2:
                for (ui64 i = 0; i < count; ++i) {
                    ui32 step = i + 1;
                    ui32 size = forceSize ? forceSize : 92 + i * 31;
                    TLogoBlobID id(1, 2, step, 0, size, 0, 0, TErasureType::CrcModeWholePart);
                    TString data = AlphaData(size);
                    Blobs.emplace_back(id, data);
                    DataById[id] = data;
                }
                break;
            default:
                Y_ABORT_UNLESS(false, "Unexpected setIdx# %" PRIu32, setIdx);
                break;
        }
    }

    ui64 Size() const {
        return Blobs.size();
    }

    const TBlob& Get(ui32 idx) const {
        Y_ABORT_UNLESS(idx < Blobs.size());
        return Blobs[idx];
    }

    TString Get(TLogoBlobID id, ui32 shift, ui32 size) const {
        auto it = DataById.find(id);
        Y_ABORT_UNLESS(it != DataById.end());
        TString data = it->second;
        shift = Min(shift, id.BlobSize());
        ui32 rSize = size ? size : id.BlobSize();
        rSize = Min(rSize, id.BlobSize() - shift);
        TString result = TString::Uninitialized(rSize);
        memcpy(result.Detach(), data.data() + shift, rSize);
        return result;
    }

    void Check(ui32 idx, TLogoBlobID id, ui32 shift, ui32 size, TString buffer) const {
        const TBlob& blob = Get(idx);
        Y_ABORT_UNLESS(id == blob.Id);
        if (size != buffer.size()) {
            UNIT_ASSERT_VALUES_EQUAL(size, buffer.size());
        }
        const char *a = blob.Data.data();
        const char *b = buffer.data();
        UNIT_ASSERT(shift < blob.Data.size());
        UNIT_ASSERT(shift + size <= blob.Data.size());
        UNIT_ASSERT(size <= buffer.size());
        if (memcmp(a + shift, b, size) != 0) {
            for (ui32 offset = 0; offset < size; ++offset) {
                UNIT_ASSERT_VALUES_EQUAL_C((ui8)a[shift + offset], (ui8)b[offset],
                        "Id# " << id.ToString() << " offset# " << offset << " shift# " << shift << " size# " << size);
            }
        }
    }
};

struct TPartLocation {
    TLogoBlobID BlobId;
    TVDiskID VDiskId;

    static TString ToString(const TPartLocation &self) {
        return TStringBuilder()
            << "{BlobId# " << self.BlobId.ToString()
            << " VDiskId# " << self.VDiskId.ToString()
            << "}";
    }

    TString ToString() const {
        return ToString(*this);
    }

    bool operator<(const TPartLocation &other) const {
        return std::tie(BlobId, VDiskId) < std::tie(other.BlobId, other.VDiskId);
    }
};

class TGroupMock {
    const ui32 GroupId;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    const ui32 FailRealms;
    const ui32 FailDomains;
    const ui32 DisksPerFailDomain;

    TVector<TVDiskMock> VDisks;
    THashMap<TVDiskID, ui32> VDisksIdxMap;

    TVDiskMock& GetVDisk(ui32 diskIdx) {
        Y_ABORT_UNLESS(diskIdx < VDisks.size(), "i# %" PRIu32 " size# %" PRIu32, (ui32)diskIdx, (ui32)VDisks.size());
        return VDisks[diskIdx];
    }

    TVDiskMock& GetVDisk(ui32 failDomainIdx, ui32 driveIdx) {
        ui32 i = failDomainIdx * DisksPerFailDomain + driveIdx;
        Y_ABORT_UNLESS(i < VDisks.size(), "i# %" PRIu32 " size# %" PRIu32, (ui32)i, (ui32)VDisks.size());
        return VDisks[i];
    }

    TVDiskMock& GetVDisk(TVDiskID vdiskId) {
        return VDisks[VDisksIdxMap[vdiskId]];
    }

    void InitBsInfo() {
        for (auto& mock : VDisks) {
            mock.SetInfo(Info);
        }
    }

    void InitVDisks() {
        for (ui64 realmIdx = 0; realmIdx < FailRealms; ++realmIdx) {
        for (ui64 domainIdx = 0; domainIdx < FailDomains; ++domainIdx) {
        for (ui64 driveIdx = 0; driveIdx < DisksPerFailDomain; ++driveIdx) {
            TVDiskID vDiskId(GroupId, 1, realmIdx, domainIdx, driveIdx);
            VDisksIdxMap[vDiskId] = VDisks.size();
            VDisks.emplace_back(vDiskId);
        }
        }
        }
    }

public:
    TGroupMock(ui32 groupId, TIntrusivePtr<TBlobStorageGroupInfo> info)
        : GroupId(groupId)
        , Info(info)
        , FailRealms(Info->GetTopology().GetTotalFailRealmsNum())
        , FailDomains(Info->GetTopology().GetNumFailDomainsPerFailRealm())
        , DisksPerFailDomain(Info->GetTopology().GetNumVDisksPerFailDomain())
    {
        InitVDisks();
        InitBsInfo();
    }

    TGroupMock(ui32 groupId, TErasureType::EErasureSpecies erasureSpecies, ui32 failDomains, ui32 failRealms, ui32 drivesPerFailDomain)
        : TGroupMock(groupId, new TBlobStorageGroupInfo(erasureSpecies, drivesPerFailDomain, failDomains, failRealms))
    {}

    ui32 VDiskIdx(const TVDiskID &id) {
        return (ui32)id.FailRealm * FailDomains * DisksPerFailDomain + (ui32)id.FailDomain * DisksPerFailDomain + (ui32)id.VDisk;
    }

    TIntrusivePtr<TBlobStorageGroupInfo> GetInfo() {
        return Info;
    }

    void OnVGet(const TEvBlobStorage::TEvVGet &vGet, TEvBlobStorage::TEvVGetResult &outVGetResult) {
        Y_ABORT_UNLESS(vGet.Record.HasVDiskID());
        TVDiskID vDiskId = VDiskIDFromVDiskID(vGet.Record.GetVDiskID());
        GetVDisk(vDiskId).OnVGet(vGet, outVGetResult);
    }

    NKikimrProto::EReplyStatus OnVPut(TEvBlobStorage::TEvVPut &vPut) {
        const NKikimrBlobStorage::TEvVPut &record = vPut.Record;
        Y_ABORT_UNLESS(record.HasVDiskID());
        TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        ui32 idx = VDiskIdx(vDiskId);
        TVDiskMock &disk = VDisks[idx];
        if (disk.GetVDiskId() != vDiskId) {
            return NKikimrProto::RACE;
        }
        const TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(record.GetBlobID());
        TString buffer = vPut.GetBuffer().ConvertToString();
        NKikimrProto::EReplyStatus status = disk.Put(blobId, buffer);
        return status;
    }

    template <typename TEvent>
    TVDiskID GetVDiskID(TEvent &vPut) {
        const auto &record = vPut.Record;
        TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        ui32 idx = VDiskIdx(vDiskId);
        TVDiskMock &disk = VDisks[idx];
        return disk.GetVDiskId();
    }

    TVector<NKikimrProto::EReplyStatus> OnVMultiPut(TEvBlobStorage::TEvVMultiPut &vMultiPut) {
        const NKikimrBlobStorage::TEvVMultiPut &record = vMultiPut.Record;
        Y_ABORT_UNLESS(record.HasVDiskID());
        TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        ui32 idx = VDiskIdx(vDiskId);
        TVDiskMock &disk = VDisks[idx];
        if (disk.GetVDiskId() != vDiskId) {
            return TVector<NKikimrProto::EReplyStatus>(record.ItemsSize(), NKikimrProto::RACE);
        }
        Y_ABORT_UNLESS(disk.GetVDiskId() == vDiskId);
        TVector<NKikimrProto::EReplyStatus> statuses;
        for (ui64 itemIdx = 0; itemIdx < record.ItemsSize(); ++itemIdx) {
            auto &item = record.GetItems(itemIdx);
            const TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
            TString buffer = vMultiPut.GetItemBuffer(itemIdx).ConvertToString();
            statuses.push_back(disk.Put(blobId, buffer));
        }
        return statuses;
    }

    void Wipe(ui32 domainIdx) {
        for (ui64 driveIdx = 0; driveIdx < DisksPerFailDomain; ++driveIdx) {
            GetVDisk(domainIdx, driveIdx).Wipe();
        }
    }

    void Wipe() {
        for (ui32 idx = 0; idx < VDisks.size(); ++idx) {
            VDisks[idx].Wipe();
        }
    }

    void SetError(ui32 domainIdx, NKikimrProto::EReplyStatus status) {
        for (ui64 driveIdx = 0; driveIdx < DisksPerFailDomain; ++driveIdx) {
            GetVDisk(domainIdx, driveIdx).SetError(status);
        }
    }

    void SetError(TVDiskID vdiskId, NKikimrProto::EReplyStatus status) {
        GetVDisk(vdiskId).SetError(status);
    }

    void SetPredictedDelayNs(ui32 domainIdx, ui64 predictedDelayNs) {
        for (ui64 driveIdx = 0; driveIdx < DisksPerFailDomain; ++driveIdx) {
            GetVDisk(domainIdx, driveIdx).SetPredictedDelayNs(predictedDelayNs);
        }
    }

    void UnsetError(ui32 domainIdx) {
        for (ui64 driveIdx = 0; driveIdx < DisksPerFailDomain; ++driveIdx) {
            GetVDisk(domainIdx, driveIdx).UnsetError();
        }
    }

    void SetSpecialStatus(ui32 domainIdx, TLogoBlobID blobID, NKikimrProto::EReplyStatus status) {
        for (ui64 driveIdx = 0; driveIdx < DisksPerFailDomain; ++driveIdx) {
            GetVDisk(domainIdx, driveIdx).SetSpecialStatus(blobID, status);
        }
    }

    void SetSpecialStatuses(const TMap<TPartLocation, NKikimrProto::EReplyStatus> &statuses) {
        for (auto &[partLocation, status] : statuses) {
            ui64 vDiskIdx = VDiskIdx(partLocation.VDiskId);
            VDisks[vDiskIdx].SetSpecialStatus(partLocation.BlobId, status);
        }
    }

    void SetNotYetBlob(ui32 domainIdx, const TLogoBlobID blobID) {
        for (ui64 driveIdx = 0; driveIdx < DisksPerFailDomain; ++driveIdx) {
            GetVDisk(domainIdx, driveIdx).SetNotYet(blobID);
        }
    }

    void Put(const TLogoBlobID id, const TString &data, ui32 handoffsToUse = 0,
        const THashSet<ui32>* selectedParts = nullptr)
    {
        const ui32 hash = id.Hash();
        const ui32 totalvd = Info->Type.BlobSubgroupSize();
        const ui32 totalParts = Info->Type.TotalPartCount();
        Y_ABORT_UNLESS(id.BlobSize() == data.size());
        Y_ABORT_UNLESS(totalvd >= totalParts);
        TBlobStorageGroupInfo::TServiceIds vDisksSvc;
        TBlobStorageGroupInfo::TVDiskIds vDisksId;
        Info->PickSubgroup(hash, &vDisksId, &vDisksSvc);

        TString encryptedData = data;
        char *dataBytes = encryptedData.Detach();
        Encrypt(dataBytes, dataBytes, 0, encryptedData.size(), id, *Info);

        TDataPartSet partSet;
        partSet.Parts.resize(totalParts);
        Info->Type.SplitData((TErasureType::ECrcMode)id.CrcMode(), encryptedData, partSet);

        if (Info->Type.GetErasure() == TErasureType::ErasureMirror3of4) {
            auto part1 = partSet.Parts[0].OwnedString.ConvertToString();
            auto part2 = partSet.Parts[1].OwnedString.ConvertToString();
            GetVDisk(vDisksId[0]).Put(TLogoBlobID(id, 1), part1);
            GetVDisk(vDisksId[1]).Put(TLogoBlobID(id, 2), part2);
            GetVDisk(vDisksId[vDisksId.size() - 1]).Put(TLogoBlobID(id, 2), partSet.Parts[1].OwnedString.ConvertToString());
            for (ui32 i = 2; i < vDisksId.size() - 1; ++i) {
                GetVDisk(vDisksId[i]).Put(TLogoBlobID(id, 3), TString());
            }
        } else {
            for (ui32 i = 0; i < totalParts; ++i) {
                if (selectedParts && !selectedParts->contains(i + 1)) {
                    continue;
                }
                TLogoBlobID pId(id, i + 1);
                TRope pData = partSet.Parts[i].OwnedString;
                if (i < handoffsToUse) {
                    Y_ABORT_UNLESS(totalParts + i < totalvd);
                    GetVDisk(vDisksId[totalParts + i]).Put(pId, pData.ConvertToString());
                } else {
                    GetVDisk(vDisksId[i]).Put(pId, pData.ConvertToString());
                }
            }
        }
    }

    i32 DomainIdxForBlobSubgroupIdx(const TLogoBlobID id, i32 subgroupIdx) {
        const ui32 hash = id.Hash();
        const ui32 totalvd = Info->Type.BlobSubgroupSize();
        const ui32 totalParts = Info->Type.TotalPartCount();
        Y_ABORT_UNLESS(totalvd >= totalParts);
        TBlobStorageGroupInfo::TServiceIds vDisksSvc;
        TBlobStorageGroupInfo::TVDiskIds vDisksId;
        Info->PickSubgroup(hash, &vDisksId, &vDisksSvc);

        return vDisksId[subgroupIdx].FailDomain;
    }

    void PutBlobSet(const TBlobTestSet &blobSet, ui32 handoffsToUse = 0) {
        for (ui64 i = 0; i < blobSet.Size(); ++i) {
            const auto &blob = blobSet.Get(i);
            Put(blob.Id, blob.Data, handoffsToUse);
        }
    }

    TIntrusivePtr<TGroupQueues> MakeGroupQueues() {
        TIntrusivePtr<TGroupQueues> groupQueues(new TGroupQueues(Info->GetTopology()));
        ui32 idx = 0;
        for (auto& domain : groupQueues->FailDomains) {
            for (auto& vDisk : domain.VDisks) {
                vDisk.Queues.FlowRecordForQueueId(NKikimrBlobStorage::EVDiskQueueId::PutTabletLog).Reset(
                        VDisks[idx].FlowRecord);
                vDisk.Queues.FlowRecordForQueueId(NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob).Reset(
                        VDisks[idx].FlowRecord);
                vDisk.Queues.FlowRecordForQueueId(NKikimrBlobStorage::EVDiskQueueId::PutUserData).Reset(
                        VDisks[idx].FlowRecord);
                vDisk.Queues.FlowRecordForQueueId(NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead).Reset(
                        VDisks[idx].FlowRecord);
                vDisk.Queues.FlowRecordForQueueId(NKikimrBlobStorage::EVDiskQueueId::GetFastRead).Reset(
                        VDisks[idx].FlowRecord);
                vDisk.Queues.FlowRecordForQueueId(NKikimrBlobStorage::EVDiskQueueId::GetDiscover).Reset(
                        VDisks[idx].FlowRecord);
                ++idx;
            }
        }
        return groupQueues;
    }
};

} // namespace NKikimr
