#include "dsproxy_blackboard.h"

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBlobState
//

void TBlobState::TState::AddResponseData(ui32 fullSize, ui32 shift, TString &data) {
    // Add the data to the Data buffer
    Y_VERIFY(data.size());
    Y_VERIFY(shift + data.size() <= fullSize);
    Data.Write(shift, data.data(), data.size());
    // Mark the interval as present in the Data buffer
    Here.Add(shift, shift + data.size());
}

void TBlobState::TState::AddPartToPut(TRope &data) {
    Y_VERIFY(data.size());
    Data.SetMonolith(data);
    Here.Assign(0, data.size());
}


void TBlobState::Init(const TLogoBlobID &id, const TBlobStorageGroupInfo &info) {
    Id = id;
    Parts.resize(info.Type.TotalPartCount());
    ui32 blobSubgroupSize = info.Type.BlobSubgroupSize();
    Disks.resize(blobSubgroupSize);
    TBlobStorageGroupInfo::TServiceIds vdisksSvc;
    TBlobStorageGroupInfo::TVDiskIds vdisksId;
    const ui32 hash = Id.Hash();
    info.PickSubgroup(hash, &vdisksId, &vdisksSvc);
    for (ui32 i = 0; i < blobSubgroupSize; ++i) {
        Disks[i].OrderNumber = info.GetOrderNumber(vdisksId[i]);
        Disks[i].DiskParts.resize(info.Type.TotalPartCount());
    }
    IsChanged = true;
}

void TBlobState::AddNeeded(ui64 begin, ui64 size) {
    Y_VERIFY(bool(Id));
    Whole.Needed.Add(begin, begin + size);
    Whole.NotHere.Add(begin, begin + size);
    IsChanged = true;
}

void TBlobState::AddPartToPut(ui32 partIdx, TRope &partData) {
    Y_VERIFY(bool(Id));
    Y_VERIFY(partIdx < Parts.size());
    Parts[partIdx].AddPartToPut(partData);
    IsChanged = true;
}

void TBlobState::MarkBlobReadyToPut(ui8 blobIdx) {
    Y_VERIFY(WholeSituation == ESituation::Unknown || WholeSituation == ESituation::Present);
    WholeSituation = ESituation::Present;
    BlobIdx = blobIdx;
    IsChanged = true;
}

bool TBlobState::Restore(const TBlobStorageGroupInfo &info) {
    TIntervalVec<i32> fullBlobInterval(0, Id.BlobSize());
    if (fullBlobInterval.IsSubsetOf(Whole.Here)) {
        return true;
    }

    const ui32 parts = info.Type.TotalPartCount();
    ui32 partsPresent = 0;
    for (ui32 i = 0; i < parts; ++i) {
        if (const ui32 partSize = info.Type.PartSize(TLogoBlobID(Id, i + 1))) {
            if (TIntervalVec<i32>(0, partSize).IsSubsetOf(Parts[i].Here)) {
                ++partsPresent;
            }
        }
    }
    if (partsPresent < info.Type.MinimalRestorablePartCount()) {
        return false;
    }

    TDataPartSet partSet;
    partSet.Parts.resize(parts);
    for (ui32 i = 0; i < parts; ++i) {
        if (const ui32 partSize = info.Type.PartSize(TLogoBlobID(Id, i + 1))) {
            if (TIntervalVec<i32>(0, partSize).IsSubsetOf(Parts[i].Here)) {
                partSet.PartsMask |= (1 << i);
                TRope data(MakeIntrusive<TRopeSharedDataBackend>(TSharedData::Uninitialized(partSize)));
                Parts[i].Data.Read(0, data.UnsafeGetContiguousSpanMut().data(), partSize);
                partSet.Parts[i].ReferenceTo(data);
            }
        }
    }
    partSet.FullDataSize = Id.BlobSize();

    TRope whole;
    info.Type.RestoreData((TErasureType::ECrcMode)Id.CrcMode(), partSet, whole, false, true, false);
    Whole.Data.Write(0, whole.GetContiguousSpan().data(), Id.BlobSize());
    Whole.Here.Add(fullBlobInterval);
    Whole.NotHere.Subtract(fullBlobInterval);
    return true;
}

void TBlobState::AddResponseData(const TBlobStorageGroupInfo &info, const TLogoBlobID &id, ui32 orderNumber,
        ui32 shift, TString &data, bool keep, bool doNotKeep) {
    // Add actual data to Parts
    Y_VERIFY(id.PartId() != 0);
    ui32 partIdx = id.PartId() - 1;
    Y_VERIFY(partIdx < Parts.size());
    const ui32 partSize = info.Type.PartSize(id);
    if (partSize) {
        Parts[partIdx].AddResponseData(partSize, shift, data);
    }
    IsChanged = true;
    // Mark part as present for the disk
    bool isFound = false;
    for (ui32 diskIdx = 0; diskIdx < Disks.size(); ++diskIdx) {
        TDisk &disk = Disks[diskIdx];
        if (disk.OrderNumber == orderNumber) {
            isFound = true;
            Y_VERIFY(partIdx < disk.DiskParts.size());
            TDiskPart &diskPart = disk.DiskParts[partIdx];
            //Cerr << Endl << "present diskIdx# " << diskIdx << " partIdx# " << partIdx << Endl << Endl;
            diskPart.Situation = ESituation::Present;
            if (partSize) {
                TIntervalVec<i32> responseInterval(shift, shift + data.size());
                diskPart.Requested.Subtract(responseInterval);
            }
            break;
        }
    }
    Y_VERIFY(isFound);
    Keep |= keep;
    DoNotKeep |= doNotKeep;
}

void TBlobState::AddNoDataResponse(const TBlobStorageGroupInfo &info, const TLogoBlobID &id, ui32 orderNumber) {
    Y_UNUSED(info);
    Y_VERIFY(id.PartId() != 0);
    ui32 partIdx = id.PartId() - 1;
    IsChanged = true;
    // Mark part as absent for the disk
    bool isFound = false;
    for (ui32 diskIdx = 0; diskIdx < Disks.size(); ++diskIdx) {
        TDisk &disk = Disks[diskIdx];
        if (disk.OrderNumber == orderNumber) {
            isFound = true;
            Y_VERIFY(partIdx < disk.DiskParts.size());
            TDiskPart &diskPart = disk.DiskParts[partIdx];
            //Cerr << Endl << "absent diskIdx# " << diskIdx << " partIdx# " << partIdx << Endl << Endl;
            diskPart.Situation = ESituation::Absent;
            diskPart.Requested.Clear();
            break;
        }
    }
    Y_VERIFY(isFound);
}

void TBlobState::AddPutOkResponse(const TBlobStorageGroupInfo &info, const TLogoBlobID &id, ui32 orderNumber) {
    Y_UNUSED(info);
    Y_VERIFY(id.PartId() != 0);
    ui32 partIdx = id.PartId() - 1;
    IsChanged = true;
    // Mark part as put ok for the disk
    bool isFound = false;
    for (ui32 diskIdx = 0; diskIdx < Disks.size(); ++diskIdx) {
        TDisk &disk = Disks[diskIdx];
        if (disk.OrderNumber == orderNumber) {
            isFound = true;
            Y_VERIFY(partIdx < disk.DiskParts.size());
            TDiskPart &diskPart = disk.DiskParts[partIdx];
            //Cerr << Endl << "put ok diskIdx# " << diskIdx << " partIdx# " << partIdx << Endl << Endl;
            diskPart.Situation = ESituation::Present;
            break;
        }
    }
    Y_VERIFY(isFound);
}

void TBlobState::AddErrorResponse(const TBlobStorageGroupInfo &info, const TLogoBlobID &id, ui32 orderNumber) {
    Y_UNUSED(info);
    Y_VERIFY(id.PartId() != 0);
    ui32 partIdx = id.PartId() - 1;
    IsChanged = true;
    // Mark part as error for the disk
    bool isFound = false;
    for (ui32 diskIdx = 0; diskIdx < Disks.size(); ++diskIdx) {
        TDisk &disk = Disks[diskIdx];
        if (disk.OrderNumber == orderNumber) {
            isFound = true;
            Y_VERIFY(partIdx < disk.DiskParts.size());
            TDiskPart &diskPart = disk.DiskParts[partIdx];
            //Cerr << Endl << "error diskIdx# " << diskIdx << " partIdx# " << partIdx << Endl << Endl;
            diskPart.Situation = ESituation::Error;
            diskPart.Requested.Clear();
            break;
        }
    }
    Y_VERIFY(isFound);
}

void TBlobState::AddNotYetResponse(const TBlobStorageGroupInfo &info, const TLogoBlobID &id, ui32 orderNumber,
        bool keep, bool doNotKeep) {
    Y_UNUSED(info);
    Y_VERIFY(id.PartId() != 0);
    ui32 partIdx = id.PartId() - 1;
    IsChanged = true;
    // Mark part as error for the disk
    bool isFound = false;
    for (ui32 diskIdx = 0; diskIdx < Disks.size(); ++diskIdx) {
        TDisk &disk = Disks[diskIdx];
        if (disk.OrderNumber == orderNumber) {
            isFound = true;
            Y_VERIFY(partIdx < disk.DiskParts.size());
            TDiskPart &diskPart = disk.DiskParts[partIdx];
            //Cerr << Endl << "error diskIdx# " << diskIdx << " partIdx# " << partIdx << Endl << Endl;
            diskPart.Situation = ESituation::Lost;
            diskPart.Requested.Clear();
            break;
        }
    }
    Y_VERIFY(isFound);
    Keep |= keep;
    DoNotKeep |= doNotKeep;
}

ui64 TBlobState::GetPredictedDelayNs(const TBlobStorageGroupInfo &info, TGroupQueues &groupQueues,
        ui32 diskIdxInSubring, NKikimrBlobStorage::EVDiskQueueId queueId) const {
    Y_UNUSED(info);
    return groupQueues.GetPredictedDelayNsByOrderNumber(Disks[diskIdxInSubring].OrderNumber, queueId);
}

void TBlobState::GetWorstPredictedDelaysNs(const TBlobStorageGroupInfo &info, TGroupQueues &groupQueues,
        NKikimrBlobStorage::EVDiskQueueId queueId,
        ui64 *outWorstNs, ui64 *outNextToWorstNs, i32 *outWorstSubgroupIdx) const {
    *outWorstSubgroupIdx = -1;
    *outWorstNs = 0;
    *outNextToWorstNs = 0;
    for (ui32 diskIdx = 0; diskIdx < Disks.size(); ++diskIdx) {
        ui64 predictedNs = GetPredictedDelayNs(info, groupQueues, diskIdx, queueId);
        if (predictedNs > *outWorstNs) {
            *outNextToWorstNs = *outWorstNs;
            *outWorstNs = predictedNs;
            *outWorstSubgroupIdx = diskIdx;
        } else if (predictedNs > *outNextToWorstNs) {
            *outNextToWorstNs = predictedNs;
        }
    }
}

TString TBlobState::ToString() const {
    TStringStream str;
    str << "{Id# " << Id.ToString();
    str << Endl;
    str << " Whole# " << Whole.ToString();
    str << Endl;
    str << " WholeSituation# " << SituationToString(WholeSituation);
    str << Endl;
    for (ui32 i = 0; i < Parts.size(); ++i) {
    str << Endl;
        str << " Parts[" << i << "]# " << Parts[i].ToString();
    str << Endl;
    }
    for (ui32 i = 0; i < Disks.size(); ++i) {
    str << Endl;
        str << " Disks[" << i << "]# " << Disks[i].ToString();
    str << Endl;
    }
    str << " BlobIdx# " << (ui32)BlobIdx;
    str << Endl;
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
    Y_VERIFY(false, "Unexpected situation# %" PRIu64, ui64(situation));
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
    str << " Here# " << Here.ToString();
    str << "}";
    return str.Str();
}

TString TBlobState::TWholeState::ToString() const {
    TStringStream str;
    str << "{Data# " << Data.Print();
    str << " Here# " << Here.ToString();
    str << " Needed# " << Needed.ToString();
    str << " NotHere# " << NotHere.ToString();
    str << "}";
    return str.Str();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TGroupDiskRequests
//

TGroupDiskRequests::TGroupDiskRequests(ui32 disks) {
    DiskRequestsForOrderNumber.resize(disks);
}

void TGroupDiskRequests::AddGet(const ui32 diskOrderNumber, const TLogoBlobID &id, const TIntervalSet<i32> &intervalSet) {
    Y_VERIFY(diskOrderNumber < DiskRequestsForOrderNumber.size());
    auto &requestsToSend = DiskRequestsForOrderNumber[diskOrderNumber].GetsToSend;
    for (auto pair: intervalSet) {
        requestsToSend.emplace_back(id, pair.first, pair.second - pair.first);
    }
}

void TGroupDiskRequests::AddGet(const ui32 diskOrderNumber, const TLogoBlobID &id, const ui32 shift,
        const ui32 size) {
    Y_VERIFY(diskOrderNumber < DiskRequestsForOrderNumber.size());
    DiskRequestsForOrderNumber[diskOrderNumber].GetsToSend.emplace_back(id, shift, size);
}

void TGroupDiskRequests::AddPut(const ui32 diskOrderNumber, const TLogoBlobID &id, TRope buffer,
        TDiskPutRequest::EPutReason putReason, bool isHandoff, std::vector<std::pair<ui64, ui32>> *extraBlockChecks,
        NWilson::TSpan *span, ui8 blobIdx) {
    Y_VERIFY(diskOrderNumber < DiskRequestsForOrderNumber.size());
    DiskRequestsForOrderNumber[diskOrderNumber].PutsToSend.emplace_back(id, buffer, putReason, isHandoff,
        extraBlockChecks, span, blobIdx);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBlackboard
//

void TBlackboard::AddNeeded(const TLogoBlobID &id, ui32 inShift, ui32 inSize) {
    Y_VERIFY(bool(id));
    Y_VERIFY(id.PartId() == 0);
    Y_VERIFY(id.BlobSize() != 0);
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
        Y_VERIFY(false, "%s", str.Str().c_str());
    }
}

void TBlackboard::AddPartToPut(const TLogoBlobID &id, ui32 partIdx, TRope &partData) {
    Y_VERIFY(bool(id));
    Y_VERIFY(id.PartId() == 0);
    Y_VERIFY(id.BlobSize() != 0);
    (*this)[id].AddPartToPut(partIdx, partData);
}

void TBlackboard::MarkBlobReadyToPut(const TLogoBlobID &id, ui8 blobIdx) {
    Y_VERIFY(bool(id));
    Y_VERIFY(id.PartId() == 0);
    Y_VERIFY(id.BlobSize() != 0);
    (*this)[id].MarkBlobReadyToPut(blobIdx);
}

void TBlackboard::MoveBlobStateToDone(const TLogoBlobID &id) {
    Y_VERIFY(bool(id));
    Y_VERIFY(id.PartId() == 0);
    Y_VERIFY(id.BlobSize() != 0);
    auto it = BlobStates.find(id);
    if (it == BlobStates.end()) {
        auto doneIt = DoneBlobStates.find(id);
        const char *errorMsg = doneIt == DoneBlobStates.end() ?
                "This blobId is not in BlobStates or in DoneBlobStates" :
                "This blobId is already in DoneBlobStates";
        Y_VERIFY_S(false, errorMsg << " BlobId# " << id << " Blackboard# " << ToString());
    } else {
        if (!it->second.IsDone) {
            DoneCount++;
            it->second.IsDone = true;
        }
        auto node = BlobStates.extract(it);
        DoneBlobStates.insert(std::move(node));
    }
}

void TBlackboard::AddPutOkResponse(const TLogoBlobID &id, ui32 orderNumber) {
    Y_VERIFY(bool(id));
    Y_VERIFY(id.PartId() != 0);
    TBlobState &state = GetState(id);
    state.AddPutOkResponse(*Info, id, orderNumber);
}

void TBlackboard::AddResponseData(const TLogoBlobID &id, ui32 orderNumber, ui32 shift, TString &data, bool keep, bool doNotKeep) {
    Y_VERIFY(bool(id));
    Y_VERIFY(id.PartId() != 0);
    TBlobState &state = GetState(id);
    state.AddResponseData(*Info, id, orderNumber, shift, data, keep, doNotKeep);
}

void TBlackboard::AddNoDataResponse(const TLogoBlobID &id, ui32 orderNumber) {
    Y_VERIFY(bool(id));
    Y_VERIFY(id.PartId() != 0);
    TBlobState &state = GetState(id);
    state.AddNoDataResponse(*Info, id, orderNumber);
}

void TBlackboard::AddNotYetResponse(const TLogoBlobID &id, ui32 orderNumber, bool keep, bool doNotKeep) {
    Y_VERIFY(bool(id));
    Y_VERIFY(id.PartId() != 0);
    TBlobState &state = GetState(id);
    state.AddNotYetResponse(*Info, id, orderNumber, keep, doNotKeep);
}

void TBlackboard::AddErrorResponse(const TLogoBlobID &id, ui32 orderNumber) {
    Y_VERIFY(bool(id));
    Y_VERIFY(id.PartId() != 0);
    TBlobState &state = GetState(id);
    state.AddErrorResponse(*Info, id, orderNumber);
}

EStrategyOutcome TBlackboard::RunStrategy(TLogContext &logCtx, const IStrategy& s, TBatchedVec<TBlobStates::value_type*> *finished) {
    IStrategy& temp = const_cast<IStrategy&>(s); // better UX
    Y_VERIFY(BlobStates.size());
    TString errorReason;
    for (auto it = BlobStates.begin(); it != BlobStates.end(); ++it) {
        auto& blob = it->second;
        if (!blob.IsChanged) {
            continue;
        }
        blob.IsChanged = false;
        // recalculate blob outcome if it is not yet determined
        switch (auto res = temp.Process(logCtx, blob, *Info, *this, GroupDiskRequests)) {
            case EStrategyOutcome::IN_PROGRESS:
                if (blob.IsDone) {
                    DoneCount--;
                    blob.IsDone = false;
                }
                break;

            case EStrategyOutcome::ERROR:
                if (IsAllRequestsTogether) {
                    return res;
                } else {
                    blob.Status = NKikimrProto::ERROR;
                    if (finished) {
                        finished->push_back(&*it);
                    }
                    if (errorReason) {
                        errorReason += " && ";
                        errorReason += res.ErrorReason;
                    } else {
                        errorReason = res.ErrorReason;
                    }
                }
                if (!blob.IsDone) {
                    DoneCount++;
                    blob.IsDone = true;
                }
                break;

            case EStrategyOutcome::DONE:
                if (!IsAllRequestsTogether) {
                    blob.Status = NKikimrProto::OK;
                    if (finished) {
                        finished->push_back(&*it);
                    }
                }
                if (!blob.IsDone) {
                    DoneCount++;
                    blob.IsDone = true;
                }
                break;
        }
    }

    const bool isDone = (DoneCount == (BlobStates.size() + DoneBlobStates.size()));
    EStrategyOutcome outcome(isDone ? EStrategyOutcome::DONE : EStrategyOutcome::IN_PROGRESS);
    outcome.ErrorReason = std::move(errorReason);
    return outcome;
}

TBlobState& TBlackboard::GetState(const TLogoBlobID &id) {
    Y_VERIFY(bool(id));
    TLogoBlobID fullId = id.FullID();
    auto it = BlobStates.find(fullId);
    if (it == BlobStates.end()) {
        it = DoneBlobStates.find(fullId);
        Y_VERIFY_S(it != DoneBlobStates.end(), "The blob was not found in BlobStates and DoneBlobStates"
                << " blobId# " << fullId
                << " BlackBoard# " << ToString());
    }
    TBlobState &state = it->second;
    return state;
}

ssize_t TBlackboard::AddPartMap(const TLogoBlobID &id, ui32 diskOrderNumber, ui32 requestIndex) {
    Y_VERIFY(id);
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
    Y_VERIFY(id);
    Y_VERIFY(partMapIndex >= 0);
    TBlobState &state = GetState(id);
    Y_VERIFY(static_cast<size_t>(partMapIndex) < state.PartMap.size());
    TEvBlobStorage::TEvGetResult::TPartMapItem &item = state.PartMap[partMapIndex];
    Y_VERIFY(item.ResponseIndex == responseIndex || item.ResponseIndex == Max<ui32>());
    item.ResponseIndex = responseIndex;
    item.Status.emplace_back(id.PartId(), status);
}

void TBlackboard::GetWorstPredictedDelaysNs(const TBlobStorageGroupInfo &info, TGroupQueues &groupQueues,
        NKikimrBlobStorage::EVDiskQueueId queueId,
        ui64 *outWorstNs, ui64 *outNextToWorstNs, i32 *outWorstOrderNumber) const {
    *outWorstOrderNumber = -1;
    *outWorstNs = 0;
    *outNextToWorstNs = 0;
    ui32 totalVDisks = info.GetTotalVDisksNum();
    for (ui32 orderNumber = 0; orderNumber < totalVDisks; ++orderNumber) {
        ui64 predictedNs = groupQueues.GetPredictedDelayNsByOrderNumber(orderNumber, queueId);
        if (predictedNs > *outWorstNs) {
            *outNextToWorstNs = *outWorstNs;
            *outWorstNs = predictedNs;
            *outWorstOrderNumber = orderNumber;
        } else if (predictedNs > *outNextToWorstNs) {
            *outNextToWorstNs = predictedNs;
        }
    }
}

void TBlackboard::RegisterBlobForPut(const TLogoBlobID& id, std::vector<std::pair<ui64, ui32>> *extraBlockChecks,
        NWilson::TSpan *span) {
    TBlobState& state = (*this)[id];
    if (!state.ExtraBlockChecks) {
        state.ExtraBlockChecks = extraBlockChecks;
    } else {
        Y_VERIFY(state.ExtraBlockChecks == extraBlockChecks);
    }
    if (!state.Span) {
        state.Span = span;
    } else {
        Y_VERIFY(state.Span == span);
    }
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


}//NKikimr
