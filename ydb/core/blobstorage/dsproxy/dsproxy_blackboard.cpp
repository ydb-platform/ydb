#include "dsproxy_blackboard.h"

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBlobState
//

void TBlobState::TState::AddResponseData(ui32 fullSize, ui32 shift, TRope&& data) {
    const ui32 size = data.size();
    // Add the data to the Data buffer
    Y_ABORT_UNLESS(size);
    Y_ABORT_UNLESS(shift < fullSize && size <= fullSize - shift);
    Data.Write(shift, std::move(data));
}

void TBlobState::TState::AddPartToPut(TRope&& partData) {
    Y_ABORT_UNLESS(partData);
    Data.SetMonolith(std::move(partData));
}


void TBlobState::Init(const TLogoBlobID &id, const TBlobStorageGroupInfo &info) {
    Id = id;
    Parts.resize(info.Type.TotalPartCount());
    ui32 blobSubgroupSize = info.Type.BlobSubgroupSize();
    Disks.resize(blobSubgroupSize);
    TBlobStorageGroupInfo::TOrderNums nums;
    info.GetTopology().PickSubgroup(Id.Hash(), nums);
    Y_DEBUG_ABORT_UNLESS(nums.size() == blobSubgroupSize);
    for (ui32 i = 0; i < blobSubgroupSize; ++i) {
        Disks[i].OrderNumber = nums[i];
        Disks[i].DiskParts.resize(info.Type.TotalPartCount());
    }
    IsChanged = true;
}

void TBlobState::AddNeeded(ui64 begin, ui64 size) {
    Y_ABORT_UNLESS(bool(Id));
    Whole.Needed.Add(begin, begin + size);
    IsChanged = true;
}

void TBlobState::AddPartToPut(ui32 partIdx, TRope&& partData) {
    Y_ABORT_UNLESS(bool(Id));
    Y_ABORT_UNLESS(partIdx < Parts.size());
    Parts[partIdx].AddPartToPut(std::move(partData));
    IsChanged = true;
}

bool TBlobState::Restore(const TBlobStorageGroupInfo &info) {
    const TIntervalVec<i32> fullBlobInterval(0, Id.BlobSize());
    const TIntervalSet<i32> here = Whole.Here();
    Y_DEBUG_ABORT_UNLESS((here - fullBlobInterval).IsEmpty()); // ensure no excessive data outsize blob's boundaries
    if (fullBlobInterval.IsSubsetOf(here)) { // we already have 'whole' part, no need for restoration
        return true;
    }

    TStackVec<TRope, TypicalPartsInBlob> parts(info.Type.TotalPartCount());
    ui32 partsPresent = 0;
    for (ui32 i = 0; i < parts.size(); ++i) {
        if (const ui32 partSize = info.Type.PartSize(TLogoBlobID(Id, i + 1))) {
            const TIntervalVec<i32> fullPartInterval(0, partSize);
            const TIntervalSet<i32> partHere = Parts[i].Here();
            Y_DEBUG_ABORT_UNLESS((partHere - fullPartInterval).IsEmpty()); // ensure no excessive part data outside boundaries
            if (fullPartInterval.IsSubsetOf(partHere)) {
                parts[i] = Parts[i].Data.Read(0, partSize);
                if (++partsPresent >= info.Type.MinimalRestorablePartCount()) {
                    TRope whole;
                    ErasureRestore((TErasureType::ECrcMode)Id.CrcMode(), info.Type, Id.BlobSize(), &whole, parts, 0, 0, false);
                    Y_ABORT_UNLESS(whole.size() == Id.BlobSize());
                    Whole.Data.SetMonolith(std::move(whole));
                    Y_DEBUG_ABORT_UNLESS(Whole.Here() == fullBlobInterval);
                    return true;
                }
            }
        }
    }

    return false;
}

void TBlobState::AddResponseData(const TBlobStorageGroupInfo &info, const TLogoBlobID &id, ui32 orderNumber,
        ui32 shift, TRope&& data) {
    // Add actual data to Parts
    Y_ABORT_UNLESS(id.PartId() != 0);
    const ui32 partIdx = id.PartId() - 1;
    Y_ABORT_UNLESS(partIdx < Parts.size());
    const ui32 partSize = info.Type.PartSize(id);
    const ui32 dataSize = data.size();
    if (partSize) {
        Parts[partIdx].AddResponseData(partSize, shift, std::move(data));
    }
    IsChanged = true;

    const ui32 diskIdx = info.GetIdxInSubgroup(info.GetVDiskId(orderNumber), id.Hash());
    Y_ABORT_UNLESS(diskIdx != info.Type.BlobSubgroupSize());
    TDisk& disk = Disks[diskIdx];
    Y_ABORT_UNLESS(disk.OrderNumber == orderNumber);

    // Mark part as present for the disk
    Y_ABORT_UNLESS(partIdx < disk.DiskParts.size());
    TDiskPart &diskPart = disk.DiskParts[partIdx];
    diskPart.Situation = ESituation::Present;
    if (partSize) {
        TIntervalVec<i32> responseInterval(shift, shift + dataSize);
        diskPart.Requested.Subtract(responseInterval);
    }
}

void TBlobState::AddNoDataResponse(const TBlobStorageGroupInfo &info, const TLogoBlobID &id, ui32 orderNumber) {
    Y_ABORT_UNLESS(id.PartId() != 0);
    const ui32 partIdx = id.PartId() - 1;
    IsChanged = true;

    const ui32 diskIdx = info.GetIdxInSubgroup(info.GetVDiskId(orderNumber), id.Hash());
    Y_ABORT_UNLESS(diskIdx != info.Type.BlobSubgroupSize());
    TDisk& disk = Disks[diskIdx];
    Y_ABORT_UNLESS(disk.OrderNumber == orderNumber);

    Y_ABORT_UNLESS(partIdx < disk.DiskParts.size());
    TDiskPart &diskPart = disk.DiskParts[partIdx];
    diskPart.Situation = ESituation::Absent;
    diskPart.Requested.Clear();
}

void TBlobState::AddPutOkResponse(const TBlobStorageGroupInfo &info, const TLogoBlobID &id, ui32 orderNumber) {
    Y_ABORT_UNLESS(id.PartId() != 0);
    const ui32 partIdx = id.PartId() - 1;
    IsChanged = true;

    const ui32 diskIdx = info.GetIdxInSubgroup(info.GetVDiskId(orderNumber), id.Hash());
    Y_ABORT_UNLESS(diskIdx != info.Type.BlobSubgroupSize());
    TDisk& disk = Disks[diskIdx];
    Y_ABORT_UNLESS(disk.OrderNumber == orderNumber);

    Y_ABORT_UNLESS(partIdx < disk.DiskParts.size());
    TDiskPart& diskPart = disk.DiskParts[partIdx];
    diskPart.Situation = ESituation::Present;
}

void TBlobState::AddErrorResponse(const TBlobStorageGroupInfo &info, const TLogoBlobID &id, ui32 orderNumber) {
    Y_ABORT_UNLESS(id.PartId() != 0);
    ui32 partIdx = id.PartId() - 1;
    IsChanged = true;

    const ui32 diskIdx = info.GetIdxInSubgroup(info.GetVDiskId(orderNumber), id.Hash());
    Y_ABORT_UNLESS(diskIdx != info.Type.BlobSubgroupSize());
    TDisk& disk = Disks[diskIdx];
    Y_ABORT_UNLESS(disk.OrderNumber == orderNumber);

    Y_ABORT_UNLESS(partIdx < disk.DiskParts.size());
    TDiskPart &diskPart = disk.DiskParts[partIdx];
    diskPart.Situation = ESituation::Error;
    diskPart.Requested.Clear();
}

void TBlobState::AddNotYetResponse(const TBlobStorageGroupInfo &info, const TLogoBlobID &id, ui32 orderNumber) {
    Y_ABORT_UNLESS(id.PartId() != 0);
    const ui32 partIdx = id.PartId() - 1;
    IsChanged = true;

    const ui32 diskIdx = info.GetIdxInSubgroup(info.GetVDiskId(orderNumber), id.Hash());
    Y_ABORT_UNLESS(diskIdx != info.Type.BlobSubgroupSize());
    TDisk& disk = Disks[diskIdx];
    Y_ABORT_UNLESS(disk.OrderNumber == orderNumber);

    Y_ABORT_UNLESS(partIdx < disk.DiskParts.size());
    TDiskPart &diskPart = disk.DiskParts[partIdx];
    diskPart.Situation = ESituation::Lost;
    diskPart.Requested.Clear();
}

ui64 TBlobState::GetPredictedDelayNs(const TBlobStorageGroupInfo &info, TGroupQueues &groupQueues,
        ui32 diskIdxInSubring, NKikimrBlobStorage::EVDiskQueueId queueId) const {
    Y_UNUSED(info);
    return groupQueues.GetPredictedDelayNsByOrderNumber(Disks[diskIdxInSubring].OrderNumber, queueId);
}

void TBlobState::GetWorstPredictedDelaysNs(const TBlobStorageGroupInfo &info, TGroupQueues &groupQueues,
        NKikimrBlobStorage::EVDiskQueueId queueId, ui32 nWorst, TDiskDelayPredictions *outNWorst) const {
    outNWorst->resize(Disks.size());
    for (ui32 diskIdx = 0; diskIdx < Disks.size(); ++diskIdx) {
        (*outNWorst)[diskIdx] = { GetPredictedDelayNs(info, groupQueues, diskIdx, queueId), diskIdx };
    }
    std::partial_sort(outNWorst->begin(), outNWorst->begin() + std::min(nWorst, (ui32)Disks.size()), outNWorst->end());
}

bool TBlobState::HasWrittenQuorum(const TBlobStorageGroupInfo& info, const TBlobStorageGroupInfo::TGroupVDisks& expired) const {
    TSubgroupPartLayout layout;
    for (ui32 diskIdx = 0, numDisks = Disks.size(); diskIdx < numDisks; ++diskIdx) {
        const TDisk& disk = Disks[diskIdx];
        for (ui32 partIdx = 0, numParts = disk.DiskParts.size(); partIdx < numParts; ++partIdx) {
            const TDiskPart& part = disk.DiskParts[partIdx];
            if (part.Situation == ESituation::Present && !expired[disk.OrderNumber]) {
                layout.AddItem(diskIdx, partIdx, info.Type);
            }
        }
    }
    return info.GetQuorumChecker().GetBlobState(layout, {&info.GetTopology()}) == TBlobStorageGroupInfo::EBS_FULL;
}

TString TBlobState::ToString() const {
    TStringStream str;
    str << "{Id# " << Id.ToString() << Endl;
    str << " IsChanged# " << IsChanged << Endl;
    str << " Whole# " << Whole.ToString() << Endl;
    str << " WholeSituation# " << SituationToString(WholeSituation) << Endl;
    for (ui32 i = 0; i < Parts.size(); ++i) {
        str << Endl << " Parts[" << i << "]# " << Parts[i].ToString() << Endl;
    }
    for (ui32 i = 0; i < Disks.size(); ++i) {
        str << Endl << " Disks[" << i << "]# " << Disks[i].ToString() << Endl;
    }
    str << " BlobIdx# " << BlobIdx << Endl;
    str << "}";
    return str.Str();
}

TString TBlobState::SituationToString(ESituation situation) {
    switch (situation) {
        case ESituation::Unknown:
            return "ESituation::Unknown";
        case ESituation::Error:
            return "ESituation::Error";
        case ESituation::Absent:
            return "ESituation::Absent";
        case ESituation::Lost:
            return "ESituation::Lost";
        case ESituation::Present:
            return "ESituation::Present";
        case ESituation::Sent:
            return "ESituation::Sent";
    }
    Y_ABORT_UNLESS(false, "Unexpected situation# %" PRIu64, ui64(situation));
    return "";
}

TString TBlobState::TDisk::ToString() const {
    TStringStream str;
    str << "{OrderNumber# " << OrderNumber;
    str << " IsSlow# " << IsSlow;
    for (ui32 i = 0; i < DiskParts.size(); ++i) {
    str << Endl;
        str << " DiskParts[" << i << "]# " << DiskParts[i].ToString();
    }
    str << "}";
    return str.Str();
}

TString TBlobState::TDiskPart::ToString() const {
    TStringStream str;
    str << "{Requested# " << Requested.ToString();
    str << " Situation# " << SituationToString(Situation);
    str << "}";
    return str.Str();
}

TString TBlobState::TState::ToString() const {
    TStringStream str;
    str << "{Data# " << Data.Print();
    str << " Here# " << Here().ToString();
    str << "}";
    return str.Str();
}

TString TBlobState::TWholeState::ToString() const {
    TStringStream str;
    str << "{Data# " << Data.Print();
    str << " Here# " << Here().ToString();
    str << " Needed# " << Needed.ToString();
    str << " NotHere# " << NotHere().ToString();
    str << "}";
    return str.Str();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TGroupDiskRequests
//

void TGroupDiskRequests::AddGet(ui32 diskOrderNumber, const TLogoBlobID &id, const TIntervalSet<i32> &intervalSet) {
    for (const auto& pair : intervalSet) {
        GetsPending.emplace_back(diskOrderNumber, id, pair.first, pair.second - pair.first);
    }
}

void TGroupDiskRequests::AddGet(ui32 diskOrderNumber, const TLogoBlobID &id, ui32 shift, ui32 size) {
    GetsPending.emplace_back(diskOrderNumber, id, shift, size);
}

void TGroupDiskRequests::AddPut(ui32 diskOrderNumber, const TLogoBlobID &id, TRope buffer,
        TDiskPutRequest::EPutReason putReason, bool isHandoff, size_t blobIdx) {
    PutsPending.emplace_back(diskOrderNumber, id, buffer, putReason, isHandoff, blobIdx);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBlackboard
//

void TBlackboard::AddNeeded(const TLogoBlobID &id, ui32 inShift, ui32 inSize) {
    Y_ABORT_UNLESS(bool(id));
    Y_ABORT_UNLESS(id.PartId() == 0);
    Y_ABORT_UNLESS(id.BlobSize() != 0);
    ui64 fullSize = id.BlobSize();
    ui64 shift = Min(ui64(inShift), fullSize);
    ui64 maxSize = fullSize - shift;
    ui64 size = (inSize ? Min(ui64(inSize), maxSize) : maxSize);
    //Cerr << "size " << size << " shift " << shift << Endl;
    if (size > 0) {
        (*this)[id].AddNeeded(shift, size);
    } else {
        TStringStream str;
        str << "It is impossible to read 0 bytes! Do not send such requests.";
        str << " Id# " << id.ToString() << " inShift# " << inShift << " inSize# " << inSize;
        Y_ABORT_UNLESS(false, "%s", str.Str().c_str());
    }
}

void TBlackboard::AddPartToPut(const TLogoBlobID &id, ui32 partIdx, TRope&& partData) {
    Y_ABORT_UNLESS(bool(id));
    Y_ABORT_UNLESS(id.PartId() == 0);
    Y_ABORT_UNLESS(id.BlobSize() != 0);
    Y_ABORT_UNLESS(partData.size() == Info->Type.PartSize(TLogoBlobID(id, partIdx + 1)),
        "partData# %zu partSize# %" PRIu64, partData.size(), Info->Type.PartSize(TLogoBlobID(id, partIdx + 1)));
    (*this)[id].AddPartToPut(partIdx, std::move(partData));
}

void TBlackboard::AddPutOkResponse(const TLogoBlobID &id, ui32 orderNumber) {
    Y_ABORT_UNLESS(bool(id));
    Y_ABORT_UNLESS(id.PartId() != 0);
    TBlobState &state = GetState(id);
    state.AddPutOkResponse(*Info, id, orderNumber);
}

void TBlackboard::AddResponseData(const TLogoBlobID &id, ui32 orderNumber, ui32 shift, TRope&& data) {
    Y_ABORT_UNLESS(bool(id));
    Y_ABORT_UNLESS(id.PartId() != 0);
    TBlobState &state = GetState(id);
    state.AddResponseData(*Info, id, orderNumber, shift, std::move(data));
}

void TBlackboard::AddNoDataResponse(const TLogoBlobID &id, ui32 orderNumber) {
    Y_ABORT_UNLESS(bool(id));
    Y_ABORT_UNLESS(id.PartId() != 0);
    TBlobState &state = GetState(id);
    state.AddNoDataResponse(*Info, id, orderNumber);
}

void TBlackboard::AddNotYetResponse(const TLogoBlobID &id, ui32 orderNumber) {
    Y_ABORT_UNLESS(bool(id));
    Y_ABORT_UNLESS(id.PartId() != 0);
    TBlobState &state = GetState(id);
    state.AddNotYetResponse(*Info, id, orderNumber);
}

void TBlackboard::AddErrorResponse(const TLogoBlobID &id, ui32 orderNumber) {
    Y_ABORT_UNLESS(bool(id));
    Y_ABORT_UNLESS(id.PartId() != 0);
    TBlobState &state = GetState(id);
    state.AddErrorResponse(*Info, id, orderNumber);
}

EStrategyOutcome TBlackboard::RunStrategies(TLogContext &logCtx, const TStackVec<IStrategy*, 1>& s,
        float slowDiskThreshold, TBatchedVec<TFinishedBlob> *finished,
        const TBlobStorageGroupInfo::TGroupVDisks *expired) {
    for (auto it = BlobStates.begin(); it != BlobStates.end(); ) {
        auto& blob = it->second;
        if (!std::exchange(blob.IsChanged, false)) {
            ++it;
            continue;
        }

        // recalculate blob outcome if it is not yet determined
        NKikimrProto::EReplyStatus status = NKikimrProto::OK;
        TString errorReason;
        for (IStrategy *strategy : s) {
            switch (auto res = strategy->Process(logCtx, blob, *Info, *this, GroupDiskRequests, slowDiskThreshold)) {
                case EStrategyOutcome::IN_PROGRESS:
                    status = NKikimrProto::UNKNOWN;
                    break;

                case EStrategyOutcome::ERROR:
                    if (!finished) {
                        return res;
                    }
                    status = NKikimrProto::ERROR;
                    errorReason = std::move(res.ErrorReason);
                    break;

                case EStrategyOutcome::DONE:
                    break;
            }
            if (status != NKikimrProto::OK) {
                break;
            }
        }
        if (status == NKikimrProto::OK && expired && !blob.HasWrittenQuorum(*Info, *expired)) {
            status = NKikimrProto::UNKNOWN;
        }
        if (status != NKikimrProto::UNKNOWN) {
            if (finished) { // we are operating on independent blobs
                finished->push_back(TFinishedBlob{
                    blob.BlobIdx,
                    status,
                    std::move(errorReason),
                });
            }
            const auto [doneIt, inserted, node] = DoneBlobStates.insert(BlobStates.extract(it++));
            Y_ABORT_UNLESS(inserted);
        } else {
            ++it;
        }
    }

    return BlobStates.empty() ? EStrategyOutcome::DONE : EStrategyOutcome::IN_PROGRESS;
}

EStrategyOutcome TBlackboard::RunStrategy(TLogContext &logCtx, const IStrategy& s, float slowDiskThreshold,
        TBatchedVec<TFinishedBlob> *finished, const TBlobStorageGroupInfo::TGroupVDisks *expired) {
    return RunStrategies(logCtx, {const_cast<IStrategy*>(&s)}, slowDiskThreshold, finished, expired);
}

TBlobState& TBlackboard::GetState(const TLogoBlobID &id) {
    Y_ABORT_UNLESS(bool(id));
    TLogoBlobID fullId = id.FullID();
    auto it = BlobStates.find(fullId);
    if (it == BlobStates.end()) {
        it = DoneBlobStates.find(fullId);
        Y_VERIFY_S(it != DoneBlobStates.end(), "The blob was not found in BlobStates and DoneBlobStates"
                << " blobId# " << fullId
                << " BlackBoard# " << ToString());
    }
    return it->second;
}

ssize_t TBlackboard::AddPartMap(const TLogoBlobID &id, ui32 diskOrderNumber, ui32 requestIndex) {
    Y_ABORT_UNLESS(id);
    TBlobState &state = GetState(id);
    ssize_t ret = state.PartMap.size();
    state.PartMap.emplace_back(TEvBlobStorage::TEvGetResult::TPartMapItem{
            diskOrderNumber,
            id.PartId(),
            requestIndex,
            Max<ui32>(),
            {},
        });
    return ret;
}

void TBlackboard::ReportPartMapStatus(const TLogoBlobID &id, ssize_t partMapIndex, ui32 responseIndex, NKikimrProto::EReplyStatus status) {
    Y_ABORT_UNLESS(id);
    Y_ABORT_UNLESS(partMapIndex >= 0);
    TBlobState &state = GetState(id);
    Y_ABORT_UNLESS(static_cast<size_t>(partMapIndex) < state.PartMap.size());
    TEvBlobStorage::TEvGetResult::TPartMapItem &item = state.PartMap[partMapIndex];
    Y_ABORT_UNLESS(item.ResponseIndex == responseIndex || item.ResponseIndex == Max<ui32>());
    item.ResponseIndex = responseIndex;
    item.Status.emplace_back(id.PartId(), status);
}

void TBlackboard::GetWorstPredictedDelaysNs(const TBlobStorageGroupInfo &info, TGroupQueues &groupQueues,
        NKikimrBlobStorage::EVDiskQueueId queueId, ui32 nWorst, TDiskDelayPredictions *outNWorst) const {
    ui32 totalVDisks = info.GetTotalVDisksNum();
    outNWorst->resize(totalVDisks);
    for (ui32 orderNumber = 0; orderNumber < totalVDisks; ++orderNumber) {
        (*outNWorst)[orderNumber] = { groupQueues.GetPredictedDelayNsByOrderNumber(orderNumber, queueId), orderNumber };
    }
    std::partial_sort(outNWorst->begin(), outNWorst->begin() + std::min(nWorst, totalVDisks), outNWorst->end());
}

void TBlackboard::RegisterBlobForPut(const TLogoBlobID& id, size_t blobIdx) {
    const auto [it, inserted] = BlobStates.try_emplace(id);
    Y_ABORT_UNLESS(inserted);
    TBlobState& state = it->second;
    state.Init(id, *Info);
    state.BlobIdx = blobIdx;
}

TBlobState& TBlackboard::operator [](const TLogoBlobID& id) {
    const auto [it, inserted] = BlobStates.try_emplace(id);
    TBlobState& state = it->second;
    if (inserted) {
        state.Init(id, *Info);
    }
    return state;
}

TString TBlackboard::ToString() const {
    TStringStream str;
    str << "{BlobStates size# " << BlobStates.size();
    str << Endl;
    str << " Data# {";
    str << Endl;
    for (auto it = BlobStates.begin(); it != BlobStates.end(); ++it) {
        str << "{id# " << it->first.ToString() << " state# {" << it->second.ToString() << "}}";
        str << Endl;
    }
    str << "}";
    str << Endl;
    str << " DoneBlobStates size # " << DoneBlobStates.size();
    str << Endl;
    str << " DoneData# {";
    for (auto &kv : DoneBlobStates) {
        str << "{id# " << kv.first.ToString() << " state# {" << kv.second.ToString() << "}}";
        str << Endl;
    }
    str << "}";
    str << Endl;
    // ...
    str << " PutHandleClass# " << EPutHandleClass_Name(PutHandleClass);
    str << Endl;
    str << " GetHandleClass# " << EGetHandleClass_Name(GetHandleClass);
    str << Endl;
    str << "}";
    return str.Str();
}

void TBlackboard::InvalidatePartStates(ui32 orderNumber) {
    const TVDiskID vdiskId = Info->GetVDiskId(orderNumber);
    for (auto& [id, state] : BlobStates) {
        if (const ui32 diskIdx = Info->GetIdxInSubgroup(vdiskId, id.Hash()); diskIdx != Info->Type.BlobSubgroupSize()) {
            for (TBlobState::TDiskPart& part : state.Disks[diskIdx].DiskParts) {
                if (part.Situation == TBlobState::ESituation::Present) {
                    part.Situation = TBlobState::ESituation::Unknown;
                    if (state.WholeSituation == TBlobState::ESituation::Present) {
                        state.WholeSituation = TBlobState::ESituation::Unknown;
                    }
                    state.IsChanged = true;
                }
            }
        }
    }
}

}//NKikimr
