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

    void SetCorrupted() {
        for (auto it = Blobs.begin(); it != Blobs.end(); ++it) {
            TString &str = it->second;
            if (!str.empty()) {
                str[0] = (str[0] == '#') ? '*' : '#';
            }
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
        //ui64 messageCookie = request->Record.GetCookie();

        outVGetResult.Record.SetStatus(NKikimrProto::OK);
        // Ignore RangeQuery (pretend there are no results)

        // TODO: Check for overlapping / out of order queries
        size_t size = request.ExtremeQueriesSize();
        for (unsigned i = 0; i < size; i++) {
            const NKikimrBlobStorage::TExtremeQuery &query = request.GetExtremeQueries(i);
            Y_ABORT_UNLESS(request.HasVDiskID());
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
        Y_ABORT_UNLESS(request.HasVDiskID());
        TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
        VDiskIDFromVDiskID(vDiskId, outVGetResult.Record.MutableVDiskID());
        if (request.HasCookie()) {
            outVGetResult.Record.SetCookie(request.GetCookie());
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
    const TErasureType::EErasureSpecies ErasureSpecies;
    const ui32 FailDomains;
    const ui32 DrivesPerFailDomain;

    TVector<TVDiskMock> VDisks;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;

    TVDiskMock& GetVDisk(ui32 failDomainIdx, ui32 driveIdx) {
        ui32 i = failDomainIdx * DrivesPerFailDomain + driveIdx;
        Y_ABORT_UNLESS(i < VDisks.size(), "i# %" PRIu32 " size# %" PRIu32, (ui32)i, (ui32)VDisks.size());
        return VDisks[i];
    }

    void InitBsInfo() {
        for (auto& mock : VDisks) {
            mock.SetInfo(Info);
        }
    }

    void InitVDisks() {
        for (ui64 idx = 0; idx < FailDomains; ++idx) {
            ui64 realmIdx = (ErasureSpecies == TErasureType::ErasureMirror3dc) ? idx / 3 : 0;
            ui64 domainIdx = (ErasureSpecies == TErasureType::ErasureMirror3dc) ? idx % 3 : idx;
            for (ui64 driveIdx = 0; driveIdx < DrivesPerFailDomain; ++driveIdx) {
                TVDiskID vDiskId(GroupId, 1, realmIdx, domainIdx, driveIdx);
                VDisks.emplace_back(vDiskId);
            }
        }
    }

public:
    TGroupMock(ui32 groupId, TErasureType::EErasureSpecies erasureSpecies, ui32 failDomains, ui32 drivesPerFailDomain,
            TIntrusivePtr<TBlobStorageGroupInfo> info)
        : GroupId(groupId)
        , ErasureSpecies(erasureSpecies)
        , FailDomains(failDomains)
        , DrivesPerFailDomain(drivesPerFailDomain)
        , Info(info)
    {
        Y_UNUSED(ErasureSpecies);
        InitVDisks();
        InitBsInfo();
    }

    TGroupMock(ui32 groupId, TErasureType::EErasureSpecies erasureSpecies, ui32 failDomains, ui32 drivesPerFailDomain)
        : TGroupMock(groupId, erasureSpecies, failDomains, drivesPerFailDomain,
                new TBlobStorageGroupInfo(
                    erasureSpecies,
                    drivesPerFailDomain,
                    (erasureSpecies == TErasureType::ErasureMirror3dc ? failDomains / 3 : failDomains) ,
                    (erasureSpecies == TErasureType::ErasureMirror3dc ? 3 : 1)))
    {
    }

    ui32 VDiskIdx(const TVDiskID &id) {
        ui32 idx = (ui32)(id.FailDomain + 3 * id.FailRealm) * DrivesPerFailDomain + (ui32)id.VDisk;
        return idx;
    }

    TIntrusivePtr<TBlobStorageGroupInfo> GetInfo() {
        return Info;
    }

    void OnVGet(const TEvBlobStorage::TEvVGet &vGet, TEvBlobStorage::TEvVGetResult &outVGetResult) {
        Y_ABORT_UNLESS(vGet.Record.HasVDiskID());
        TVDiskID vDiskId = VDiskIDFromVDiskID(vGet.Record.GetVDiskID());
        GetVDisk(vDiskId.FailDomain + 3 * vDiskId.FailRealm, vDiskId.VDisk).OnVGet(vGet, outVGetResult);
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
        for (ui64 driveIdx = 0; driveIdx < DrivesPerFailDomain; ++driveIdx) {
            GetVDisk(domainIdx, driveIdx).Wipe();
        }
    }

    void Wipe() {
        for (ui32 idx = 0; idx < VDisks.size(); ++idx) {
            VDisks[idx].Wipe();
        }
    }

    void SetError(ui32 domainIdx, NKikimrProto::EReplyStatus status) {
        for (ui64 driveIdx = 0; driveIdx < DrivesPerFailDomain; ++driveIdx) {
            GetVDisk(domainIdx, driveIdx).SetError(status);
        }
    }

    void SetCorrupted(ui32 domainIdx) {
        for (ui64 driveIdx = 0; driveIdx < DrivesPerFailDomain; ++driveIdx) {
            GetVDisk(domainIdx, driveIdx).SetCorrupted();
        }
    }

    void SetPredictedDelayNs(ui32 domainIdx, ui64 predictedDelayNs) {
        for (ui64 driveIdx = 0; driveIdx < DrivesPerFailDomain; ++driveIdx) {
            GetVDisk(domainIdx, driveIdx).SetPredictedDelayNs(predictedDelayNs);
        }
    }

    void UnsetError(ui32 domainIdx) {
        for (ui64 driveIdx = 0; driveIdx < DrivesPerFailDomain; ++driveIdx) {
            GetVDisk(domainIdx, driveIdx).UnsetError();
        }
    }

    void SetSpecialStatus(ui32 domainIdx, TLogoBlobID blobID, NKikimrProto::EReplyStatus status) {
        for (ui64 driveIdx = 0; driveIdx < DrivesPerFailDomain; ++driveIdx) {
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
        for (ui64 driveIdx = 0; driveIdx < DrivesPerFailDomain; ++driveIdx) {
            GetVDisk(domainIdx, driveIdx).SetNotYet(blobID);
        }
    }

    void Put(const TLogoBlobID id, const TString &data, ui32 handoffsToUse = 0) {
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

        if (ErasureSpecies == TErasureType::Erasure4Plus2Block) {
            TDataPartSet partSet;
            partSet.Parts.resize(totalParts);
            Info->Type.SplitData((TErasureType::ECrcMode)id.CrcMode(), encryptedData, partSet);
            for (ui32 i = 0; i < totalParts; ++i) {
                TLogoBlobID pId(id, i + 1);
                TRope pData = partSet.Parts[i].OwnedString;
                if (i < handoffsToUse) {
                    Y_ABORT_UNLESS(totalParts + i < totalvd);
                    GetVDisk(vDisksId[totalParts + i].FailDomain, vDisksId[totalParts + i].VDisk).Put(pId, pData.ConvertToString());
                } else {
                    GetVDisk(vDisksId[i].FailDomain, vDisksId[i].VDisk).Put(pId, pData.ConvertToString());
                }
            }
        } else if (ErasureSpecies == TErasureType::ErasureMirror3of4) {
            TLogoBlobID pId1(id, 1);
            TLogoBlobID pId2(id, 2);
            TLogoBlobID pId3(id, 3);
            GetVDisk(vDisksId[0].FailDomain, vDisksId[0].VDisk).Put(pId1, encryptedData);
            GetVDisk(vDisksId[2].FailDomain, vDisksId[2].VDisk).Put(pId1, encryptedData);
            GetVDisk(vDisksId[1].FailDomain, vDisksId[1].VDisk).Put(pId2, encryptedData);
            GetVDisk(vDisksId[3].FailDomain, vDisksId[3].VDisk).Put(pId2, encryptedData);
            GetVDisk(vDisksId[7].FailDomain, vDisksId[7].VDisk).Put(pId3, TString());
        } else {
            for (ui32 i = 0; i < totalParts; ++i) {
                TLogoBlobID pId(id, i + 1);
                GetVDisk(vDisksId[i].FailDomain + 3 * vDisksId[i].FailRealm, vDisksId[i].VDisk).Put(pId, encryptedData);
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
