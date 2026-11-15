#include "vdisk_events.h"
#include <library/cpp/string_utils/base64/base64.h>
#include <util/generic/overloaded.h>
#include <ydb/core/blobstorage/base/blobstorage_checksum.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>

namespace NKikimr {

    TEvGetVDiskSpaceReportRequest::TEvGetVDiskSpaceReportRequest() = default;

    TEvGetVDiskSpaceReportResponse::TEvGetVDiskSpaceReportResponse() = default;

    TEvGetVDiskSpaceReportResponse::TEvGetVDiskSpaceReportResponse(
            NKikimrProto::EReplyStatus status, const TString& errorReason,
            const TInstant& now, const ::NMonitoring::TDynamicCounters::TCounterPtr& counterPtr,
            const NVDiskMon::TLtcHistoPtr& histoPtr)
        : TEvVResultBasePB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG)
    {
        Record.SetStatus(NKikimrProto::EReplyStatus_Name(status));
        if (errorReason) {
            Record.SetErrorReason(errorReason);
        }
    }

    TEvBlobStorage::TEvVPutResult::TEvVPutResult() = default;

    TEvBlobStorage::TEvVPutResult::TEvVPutResult(const NKikimrProto::EReplyStatus status,
            const TLogoBlobID &logoBlobId, const TVDiskID &vdisk, const ui64 *cookie, TOutOfSpaceStatus oosStatus,
            const TInstant &now, ui32 recByteSize, NKikimrBlobStorage::TEvVPut *record,
            const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
            const NVDiskMon::TLtcHistoPtr &histoPtr, const ui64 bufferSizeBytes,
            ui64 incarnationGuid, const TString& errorReason)
        : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
                recByteSize, record, skeletonFrontIDPtr)
    {
        IncrementSize(bufferSizeBytes);
        Record.SetStatus(status);
        LogoBlobIDFromLogoBlobID(logoBlobId, Record.MutableBlobID());
        VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        if (cookie) {
            Record.SetCookie(*cookie);
        }
        Record.SetStatusFlags(oosStatus.Flags);
        Record.SetApproximateFreeSpaceShare(oosStatus.ApproximateFreeSpaceShare);
        if (record && record->HasTimestamps()) {
            Record.MutableTimestamps()->CopyFrom(record->GetTimestamps());
        }
        if (status == NKikimrProto::OK) {
            Record.SetIncarnationGuid(incarnationGuid);
        }
        if (errorReason && status != NKikimrProto::OK) {
            Record.SetErrorReason(errorReason);
        }
    }

    void TEvBlobStorage::TEvVPut::StorePayload(TRope&& buffer, bool checksumming) {
        if (checksumming) {
            Record.SetChecksum(CalculateXxh3Hash(buffer.Begin(), buffer.GetSize()).second);
            Record.SetChecksumType(NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob);
        }
        AddPayload(std::move(buffer));
    }

    void TEvBlobStorage::TEvVMultiPut::StorePayload(const TRcBuf& buffer, NKikimrBlobStorage::TVMultiPutItem *item,
            bool checksumming) {
        TRope rope(buffer);
        if (checksumming) {
            item->SetChecksum(CalculateXxh3Hash(rope.Begin(), rope.GetSize()).second);
            item->SetChecksumType(NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob);
        }
        AddPayload(std::move(rope));
        Y_DEBUG_ABORT_UNLESS(Record.ItemsSize() == GetPayloadCount());
    }

    TRope TEvBlobStorage::TEvVMultiPut::GetItemBuffer(ui64 itemIdx) const {
        Y_ABORT_UNLESS(itemIdx < GetPayloadCount());
        return GetPayload(itemIdx);
    }

    TEvBlobStorage::TEvVGetBarrier::TEvVGetBarrier(const TVDiskID &vdisk, const TKeyBarrier &from, const TKeyBarrier &to, ui32 *maxResults,
            bool showInternals)
    {
        VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        from.Serialize(*Record.MutableFrom());
        to.Serialize(*Record.MutableTo());
        if (maxResults)
            Record.SetMaxResults(*maxResults);
        if (showInternals)
            Record.SetShowInternals(true);
        Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::GetFastRead);
    }

    TString TEvBlobStorage::TEvVGetBarrier::ToString() const {
        TStringStream str;
        str << "{From# " << TKeyBarrier(Record.GetFrom()).ToString()
            << " To# " << TKeyBarrier(Record.GetTo()).ToString();
        if (Record.HasMsgQoS()) {
            TEvBlobStorage::TEvVPut::OutMsgQos(Record.GetMsgQoS(), str);
        }
        if (Record.HasShowInternals() && Record.GetShowInternals())
            str << " ShowInternals";
        str << "}";
        return str.Str();
    }

    void TEvBlobStorage::TEvVGetBarrierResult::AddResult(const TKeyBarrier &key, const TMemRecBarrier &memRec, bool showInternals) {
        auto k = Record.AddKeys();
        key.Serialize(*k);
        auto v = Record.AddValues();
        memRec.Serialize(*v, showInternals);
    }

    TEvBlobStorage::TEvVSyncFull::TEvVSyncFull(const TSyncState &syncState, const TVDiskID &sourceVDisk, const TVDiskID &targetVDisk,
            ui64 cookie, NKikimrBlobStorage::ESyncFullStage stage, const TLogoBlobID &logoBlobFrom,
            ui64 blockTabletFrom, const TKeyBarrier &barrierFrom, NKikimrBlobStorage::EFullSyncProtocol protocol)
    {
        SyncStateFromSyncState(syncState, Record.MutableSyncState());
        VDiskIDFromVDiskID(sourceVDisk, Record.MutableSourceVDiskID());
        VDiskIDFromVDiskID(targetVDisk, Record.MutableTargetVDiskID());
        Record.SetCookie(cookie);
        Record.SetStage(stage);
        LogoBlobIDFromLogoBlobID(logoBlobFrom, Record.MutableLogoBlobFrom());
        Record.SetBlockTabletFrom(blockTabletFrom);
        barrierFrom.Serialize(*Record.MutableBarrierFrom());
        Record.SetProtocol(protocol);
    }

    TEvGetSkeletonStateResult::TEvGetSkeletonStateResult(TActorId chunkKeeperActorId)
        : ChunkKeeperActorId(chunkKeeperActorId)
    {}
} // NKikimr

namespace NKikimr::NBackpressure {

const char *EQueueClientType2String(EQueueClientType t) {
    switch (t) {
        case EQueueClientType::None:        return "None";
        case EQueueClientType::ReplJob:     return "ReplJob";
        case EQueueClientType::DSProxy:     return "DSProxy";
        case EQueueClientType::VDiskLoad:   return "VDiskLoad";
        case EQueueClientType::VPatch:      return "VPatch";
        case EQueueClientType::Balancing:   return "Balancing";
    }

    Y_ABORT("unexpected EQueueClientType");
}

TQueueClientId::TQueueClientId(const NKikimrBlobStorage::TMsgQoS& msgQoS)
{
    switch (msgQoS.ClientId_case()) {
        case NKikimrBlobStorage::TMsgQoS::ClientIdCase::kProxyNodeId:
            Type = EQueueClientType::DSProxy;
            Identifier = msgQoS.GetProxyNodeId();
            break;

        case NKikimrBlobStorage::TMsgQoS::ClientIdCase::kReplVDiskId:
            Type = EQueueClientType::ReplJob;
            Identifier = msgQoS.GetReplVDiskId();
            break;

        case NKikimrBlobStorage::TMsgQoS::ClientIdCase::kVDiskLoadId:
            Type = EQueueClientType::VDiskLoad;
            Identifier = msgQoS.GetVDiskLoadId();
            break;

        case NKikimrBlobStorage::TMsgQoS::ClientIdCase::kVPatchVDiskId:
            Type = EQueueClientType::VPatch;
            Identifier = msgQoS.GetVPatchVDiskId();
            break;

        case NKikimrBlobStorage::TMsgQoS::ClientIdCase::CLIENTID_NOT_SET:
            Type = EQueueClientType::None;
            Identifier = 0;
            break;

        case NKikimrBlobStorage::TMsgQoS::ClientIdCase::kBalancingVDiskId:
            Type = EQueueClientType::Balancing;
            Identifier = msgQoS.GetBalancingVDiskId();
            break;

        default:
            Y_ABORT("unexpected case");
    }
}

void TQueueClientId::Serialize(NKikimrBlobStorage::TMsgQoS *msgQoS) const {
    switch (Type) {
        case EQueueClientType::None:
            break;

        case EQueueClientType::DSProxy:
            msgQoS->SetProxyNodeId(Identifier);
            break;

        case EQueueClientType::ReplJob:
            msgQoS->SetReplVDiskId(Identifier);
            break;

        case EQueueClientType::VDiskLoad:
            msgQoS->SetVDiskLoadId(Identifier);
            break;

        case EQueueClientType::VPatch:
            msgQoS->SetVPatchVDiskId(Identifier);
            break;

        case EQueueClientType::Balancing:
            msgQoS->SetBalancingVDiskId(Identifier);
            break;
    }
}

void TQueueClientId::Output(IOutputStream &str) const {
    str << "{Type# " << EQueueClientType2String(Type) << " Identifier# " << Identifier << "}";
}

TString TQueueClientId::ToString() const {
    TStringStream str;
    Output(str);
    return str.Str();
}

bool Good(EStatus s) {
    return s == NKikimrBlobStorage::TWindowFeedback::Success ||
        s == NKikimrBlobStorage::TWindowFeedback::WindowUpdate ||
        s == NKikimrBlobStorage::TWindowFeedback::Processed;
}

void TMessageId::Serialize(NKikimrBlobStorage::TMessageId &to) const {
    to.SetSequenceId(SequenceId);
    to.SetMsgId(MsgId);
}

TString TMessageId::ToString() const {
    TStringStream str;
    Out(str);
    return str.Str();
}

} // namespace NKikimr::NBackpressure

namespace NKikimr {

void TVDiskSkeletonTrace::AddMark(const char * const mark) {
    if (MarkCount < BufferSize) {
        Marks[MarkCount++] = mark;
    }
}

TString TVDiskSkeletonTrace::ToString() const {
    TStringBuilder msg;
    msg << "[";
    for (ui32 idx = 0; idx < MarkCount; ++idx) {
        if (idx) {
            msg << ',';
        }
        msg << '"' << Marks[idx] << '"';
    }
    msg << "]";
    if (AdditionalTrace) {
        msg << '+' << AdditionalTrace->ToString();
    }
    return msg;
}

void TVMsgContext::Output(IOutputStream &str) const {
    str << "{ClientId# ";
    ClientId.Output(str);
    str << " RecByteSize# " << RecByteSize
        << " MsgId# ";
    MsgId.Out(str);
    str << " Cost# " << Cost
        << " ExtQueueId# " << ExtQueueId
        << " IntQueueId# " << IntQueueId
        << " InternalMessageId# " << InternalMessageId
        << "}";
}

TString TVMsgContext::ToString() const {
    TStringStream str;
    Output(str);
    return str.Str();
}

ui32 TVDiskNonlocalResultBase::GetChannelToSend() {
    Y_ABORT_UNLESS(!ResultSent);
    ResultSent = true;
    return Channel;
}

void TEvVResultBase::FinalizeAndSend(const TActorContext& /*ctx*/, std::unique_ptr<IEventHandle> ev) {
    Y_ABORT_UNLESS(!Finalized); // call Finalize only once
    Finalized = true;

    if (CounterPtr) {
        CounterPtr->Inc();
    }

    if (HistoPtr) {
        HistoPtr->Collect(TAppData::TimeProvider->Now() - Start, Size);
    }

    TActivationContext::Send(ev.release());
}

void TEvBlobStorage::TEvVPut::InitWithoutBuffer(const TLogoBlobID &logoBlobId, const TVDiskID &vdisk, const bool ignoreBlock,
        const ui64 *cookie, TInstant deadline, NKikimrBlobStorage::EPutHandleClass cls,
        TWriteSource writeSource,
        NKikimrBlobStorage::TDataKind::E dataKind)
{
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&logoBlobId, sizeof(logoBlobId));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&vdisk, sizeof(vdisk));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&ignoreBlock, sizeof(ignoreBlock));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&cookie, sizeof(cookie));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&deadline, sizeof(deadline));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&cls, sizeof(cls));

    LogoBlobIDFromLogoBlobID(logoBlobId, Record.MutableBlobID());
    Record.SetFullDataSize(logoBlobId.BlobSize());
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    if (ignoreBlock) {
        Record.SetIgnoreBlock(ignoreBlock);
    }
    if (cookie) {
        Record.SetCookie(*cookie);
    }
    if (deadline != TInstant::Max()) {
        Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
    }
    Record.SetHandleClass(cls);
    Record.MutableMsgQoS()->SetExtQueueId(HandleClassToQueueId(cls));
    if (writeSource != TWriteSource::Unknown) {
        Record.SetWriteSourceOp(WriteSourceToProto(writeSource));
    }
    if (dataKind != NKikimrBlobStorage::TDataKind::USER) {
        Record.SetDataKind(dataKind);
    }
}

bool TEvBlobStorage::TEvVPut::Validate(TString& errorReason) {
    if (!Record.HasBlobID()) {
        errorReason = "TEvVPut rejected by VDisk. It has no query";
    } else if (!Record.HasVDiskID()) {
        errorReason = "TEvVPut rejected by VDisk. It has no VDiskID";
    } else if (!Record.MutableVDiskID()->HasGroupID()) {
        errorReason = "TEvVPut rejected by VDisk. It has no VDiskID::GroupID";
    } else if (Record.MutableMsgQoS() == nullptr) {
        errorReason = "TEvVPut rejected by VDisk. MsgQoS is undefined";
    } else if (!Record.MutableMsgQoS()->HasExtQueueId()) {
        errorReason = "TEvVPut rejected by VDisk. ExtQueueId is undefined";
    } else if (GetPayloadCount() == 0) {
        errorReason = "TEvVPut rejected by VDisk. Payload empty and no buffer provided";
    } else {
        return true;
    }

    return false;
}

void TEvBlobStorage::TEvVPut::OutMsgQos(const NKikimrBlobStorage::TMsgQoS &qos, TStringStream &str) {
    str << "{MsgQoS";
    if (qos.HasDeadlineSeconds()) {
        str << " DeadlineSeconds# " << qos.GetDeadlineSeconds();
    }
    if (qos.HasMsgId()) {
        str << " MsgId# " << qos.GetMsgId();
    }
    if (qos.HasCost()) {
        str << " Cost# " << qos.GetCost();
    }
    if (qos.HasExtQueueId()) {
        str << " ExtQueueId# " << NKikimrBlobStorage::EVDiskQueueId_Name(qos.GetExtQueueId()).data();
    }
    if (qos.HasIntQueueId()) {
        str << " IntQueueId# " << NKikimrBlobStorage::EVDiskInternalQueueId_Name(qos.GetIntQueueId()).data();
    }
    if (qos.HasCostSettings()) {
        str << " CostSettings# {";
        const NKikimrBlobStorage::TVDiskCostSettings &costSettings = qos.GetCostSettings();
        if (costSettings.HasSeekTimeUs()) {
            str << " SeekTimeUs# " << costSettings.GetSeekTimeUs();
        }
        if (costSettings.HasReadSpeedBps()) {
            str << " ReadSpeedBps# " << costSettings.GetReadSpeedBps();
        }
        if (costSettings.HasWriteSpeedBps()) {
            str << " WriteSpeedBps# " << costSettings.GetWriteSpeedBps();
        }
        if (costSettings.HasReadBlockSize()) {
            str << " ReadBlockSize# " << costSettings.GetReadBlockSize();
        }
        if (costSettings.HasWriteBlockSize()) {
            str << " WriteBlockSize# " << costSettings.GetWriteBlockSize();
        }
        if (costSettings.HasMinHugeBlobInBytes()) {
            str << " MinHugeBlobInBytes# " << costSettings.GetMinHugeBlobInBytes();
        }
        str << "}";
    }

    if (qos.HasSendMeCostSettings()) {
        str << " SendMeCostSettings# " << qos.GetSendMeCostSettings();
    }
    if (qos.HasWindow()) {
        str << " Window# {";
        const NKikimrBlobStorage::TWindowFeedback &window = qos.GetWindow();
        if (window.HasStatus()) {
            str << " Status# " << NKikimrBlobStorage::TWindowFeedback::EStatus_Name(window.GetStatus()).data();
        }
        if (window.HasActualWindowSize()) {
            str << " ActualWindowSize# " << window.GetActualWindowSize();
        }
        if (window.HasMaxWindowSize()) {
            str << " MaxWindowSize# " << window.GetMaxWindowSize();
        }
        if (window.HasExpectedMsgId()) {
            str << " ExpectedMsgId# " << window.GetExpectedMsgId();
        }
        if (window.HasFailedMsgId()) {
            str << " FailedMsgId# " << window.GetFailedMsgId();
        }
        str << "}";
    }
    str << "}";
}

TString TEvBlobStorage::TEvVPut::ToString(const TEvBlobStorage::TEvVPut& ev) {
    const auto& record = ev.Record;
    TStringStream str;
    str << "{ID# ";
    if (record.HasBlobID()) {
        str << LogoBlobIDFromLogoBlobID(record.GetBlobID()).ToString();
    } else {
        str << "<missing>";
    }
    str << " FDS# " << record.GetFullDataSize();
    if (record.GetIgnoreBlock()) {
        str << " IgnoreBlock";
    }
    if (record.HasHandleClass()) {
        str << " HandleClass# " << record.GetHandleClass();
    }
    if (record.HasMsgQoS()) {
        str << " ";
        TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
    }
    const size_t size = ev.GetBufferBytes();
    str << " DataSize# " << size << " Data# ";
    if (ev.GetPayloadCount() != 1) {
        str << "<invalid payload count# " << ev.GetPayloadCount() << ">";
    } else if (size > 16) {
        str << "<too_large>";
    } else {
        TString encoded;
        Base64Encode(ev.GetPayload(0).ConvertToString(), encoded);
        str << encoded;
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvVPutResult::ToString(const NKikimrBlobStorage::TEvVPutResult &record) {
    TStringStream str;
    TLogoBlobID id = LogoBlobIDFromLogoBlobID(record.GetBlobID());
    str << "{EvVPutResult Status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus()).data();
    if (record.HasErrorReason()) {
        str << " ErrorReason# " << '"' << EscapeC(record.GetErrorReason()) << '"';
    }
    str << " ID# " << id.ToString();
    if (record.HasCookie()) {
        str << " Cookie# " << record.GetCookie();
    }
    if (record.HasMsgQoS()) {
        str << " ";
        TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
    }
    str << "}";

    return str.Str();
}

void TEvBlobStorage::TEvVPutResult::MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
        const NKikimrBlobStorage::TEvVPut &request) {
    Record.SetStatus(status);
    if (status != NKikimrProto::OK && errorReason) {
        Record.SetErrorReason(errorReason);
    }
    if (request.HasBlobID()) {
        Record.MutableBlobID()->CopyFrom(request.GetBlobID());
    }

    if (request.HasVDiskID()) {
        Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
    }
    if (request.HasCookie()) {
        Record.SetCookie(request.GetCookie());
    }
    if (request.HasTimestamps()) {
        Record.MutableTimestamps()->CopyFrom(request.GetTimestamps());
    }
}

TEvBlobStorage::TEvVMultiPut::TEvVMultiPut(const TVDiskID &vdisk, TInstant deadline, NKikimrBlobStorage::EPutHandleClass cls,
             bool ignoreBlock, const ui64 *cookie)
{
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&vdisk, sizeof(vdisk));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&ignoreBlock, sizeof(ignoreBlock));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&cookie, sizeof(cookie));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&deadline, sizeof(deadline));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&cls, sizeof(cls));

    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    Record.SetIgnoreBlock(ignoreBlock);
    if (cookie) {
        Record.SetCookie(*cookie);
    }
    if (deadline != TInstant::Max()) {
        Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
    }
    Record.SetHandleClass(cls);
    Record.MutableMsgQoS()->SetExtQueueId(HandleClassToQueueId(cls));
}

void TEvBlobStorage::TEvVMultiPut::AddVPut(const TLogoBlobID &logoBlobId, const TRcBuf &buffer, ui64 *cookie, bool issueKeepFlag, bool ignoreBlock,
        bool isZeroEntry, std::vector<std::pair<ui64, ui32>> *extraBlockChecks, NWilson::TTraceId traceId,
        bool checksumming,
        TWriteSource writeSource,
        NKikimrBlobStorage::TDataKind::E dataKind) {
    NKikimrBlobStorage::TVMultiPutItem *item = Record.AddItems();
    LogoBlobIDFromLogoBlobID(logoBlobId, item->MutableBlobID());
    item->SetFullDataSize(logoBlobId.BlobSize());
    StorePayload(buffer, item, checksumming);
    item->SetFullDataSize(logoBlobId.BlobSize());
    if (cookie) {
        item->SetCookie(*cookie);
    }
    if (issueKeepFlag) {
        item->SetIssueKeepFlag(true);
    }
    if (ignoreBlock) {
        item->SetIgnoreBlock(true);
    }
    if (isZeroEntry) {
        item->SetIsZeroEntry(true);
    }
    if (extraBlockChecks) {
        for (const auto& [tabletId, generation] : *extraBlockChecks) {
            auto *p = item->AddExtraBlockChecks();
            p->SetTabletId(tabletId);
            p->SetGeneration(generation);
        }
    }
    if (traceId) {
        traceId.Serialize(item->MutableTraceId());
    }
    if (writeSource != TWriteSource::Unknown) {
        item->SetWriteSourceOp(WriteSourceToProto(writeSource));
    }
    if (dataKind != NKikimrBlobStorage::TDataKind::USER) {
        item->SetDataKind(dataKind);
    }
}

bool TEvBlobStorage::TEvVMultiPut::Validate(TString& errorReason) {
    if (Record.ItemsSize() == 0) {
        errorReason =  "TEvVMultiPut rejected by VDisk. It has 0 blobs to put";
    } else if (!Record.HasVDiskID()) {
        errorReason = "TEvVMultiPut rejected by VDisk. VDiskID is undefined";
    } else if (!Record.MutableVDiskID()->HasGroupID()) {
        errorReason = "TEvVMultiPut rejected by VDisk. It has no VDiskID::GroupID";
    } else if (Record.MutableMsgQoS() == nullptr) {
        errorReason = "TEvVMultiPut rejected by VDisk. MsgQoS is undefined";
    } else if (!Record.MutableMsgQoS()->HasExtQueueId()) {
        errorReason =  "TEvVMultiPut rejected by VDisk. ExtQueueId is undefined";
    } else {
        return true;
    }

    return false;
}

TString TEvBlobStorage::TEvVMultiPut::ToString() const {
    TStringStream str;
    str << "{EvVMultiPut";
    ui64 size = Record.ItemsSize();
    for (ui64 itemIdx = 0; itemIdx < size; ++itemIdx) {
        const NKikimrBlobStorage::TVMultiPutItem &item = Record.GetItems(itemIdx);
        str << " Item# {VMultiPutItem";
        str << " ID# ";
        if (item.HasBlobID()) {
            str << LogoBlobIDFromLogoBlobID(item.GetBlobID()).ToString();
        } else {
            str << "<missing>";
        }
        str << " FullDataSize# " << item.GetFullDataSize();
        if (itemIdx < GetPayloadCount()) {
            const size_t size = GetPayload(itemIdx).GetSize();
            str << " DataSize# " << size << " Data# ";
            if (size > 16) {
                str << "<too_large>";
            } else {
                TString encoded;
                Base64Encode(GetPayload(itemIdx).ConvertToString(), encoded);
                str << encoded;
            }
        } else {
            str << " Data# <missing payload>";
        }
        if (item.HasCookie()) {
            str << " Cookie# " << item.GetCookie();
        }
        str << "}";
    }
    if (GetPayloadCount() > size) {
        str << " ExtraPayloadCount# " << GetPayloadCount() - size;
    }

    str << " VDiskId# ";
    if (Record.HasVDiskID()) {
        str << VDiskIDFromVDiskID(Record.GetVDiskID());
    } else {
        str << "<missing>";
    }

    if (Record.GetIgnoreBlock()) {
        str << " IgnoreBlock";
    }
    if (Record.HasHandleClass()) {
        str << " HandleClass# " << Record.GetHandleClass();
    }
    if (Record.HasMsgQoS()) {
        str << " ";
        TEvBlobStorage::TEvVPut::OutMsgQos(Record.GetMsgQoS(), str);
    }
    if (Record.HasCookie()) {
        str << " Cookie# " << Record.GetCookie();
    }
    str << "}";
    return str.Str();
}

TEvBlobStorage::TEvVMultiPutResult::TEvVMultiPutResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const ui64 *cookie,
        const TInstant &now, ui32 recByteSize, NKikimrBlobStorage::TEvVMultiPut *record,
        const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
        const NVDiskMon::TLtcHistoPtr &histoPtr, const ui64 bufferSizeBytes, ui64 incarnationGuid,
        const TString& errorReason)
    : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
            recByteSize, record, skeletonFrontIDPtr)
{
    IncrementSize(bufferSizeBytes);
    Record.SetStatus(status);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    if (cookie) {
        Record.SetCookie(*cookie);
    }
    if (record && record->HasTimestamps()) {
        Record.MutableTimestamps()->CopyFrom(record->GetTimestamps());
    }
    if (status == NKikimrProto::OK) {
        Record.SetIncarnationGuid(incarnationGuid);
    }
    if (errorReason && status != NKikimrProto::OK) {
        Record.SetErrorReason(errorReason);
    }
}

void TEvBlobStorage::TEvVMultiPutResult::AddVPutResult(NKikimrProto::EReplyStatus status, const TString& errorReason, const TLogoBlobID &logoBlobId,
        ui64 *cookie, ui32 statusFlags, bool writtenBeyondBarrier)
{
    NKikimrBlobStorage::TVMultiPutResultItem *item = Record.AddItems();
    item->SetStatus(status);
    if (errorReason && status != NKikimrProto::OK) {
        item->SetErrorReason(errorReason);
    }
    LogoBlobIDFromLogoBlobID(logoBlobId, item->MutableBlobID());
    if (cookie) {
        item->SetCookie(*cookie);
    }
    item->SetStatusFlags(statusFlags);
    if (writtenBeyondBarrier) {
        item->SetWrittenBeyondBarrier(true);
    }
}

void TEvBlobStorage::TEvVMultiPutResult::MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
        const NKikimrBlobStorage::TEvVMultiPut &request) {
    Record.SetStatus(status);
    if (errorReason && status != NKikimrProto::OK) {
        Record.SetErrorReason(errorReason);
    }
    if (status == NKikimrProto::NOTREADY) {
        status = NKikimrProto::ERROR; // treat BS_QUEUE internally generated NOTREADY as ERROR
    }

    for (auto &item : request.GetItems()) {
        Y_ABORT_UNLESS(item.HasBlobID());
        TLogoBlobID logoBlobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
        ui64 cookieValue = 0;
        ui64 *cookie = nullptr;
        if (item.HasCookie()) {
            cookieValue = item.GetCookie();
            cookie = &cookieValue;
        }
        AddVPutResult(status, errorReason, logoBlobId, cookie);
    }

    if (request.HasVDiskID()) {
        Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
    }
    if (request.HasCookie()) {
        Record.SetCookie(request.GetCookie());
    }
    if (request.HasTimestamps()) {
        Record.MutableTimestamps()->CopyFrom(request.GetTimestamps());
    }
}

TString TEvBlobStorage::TEvVMultiPutResult::ToString(const NKikimrBlobStorage::TEvVMultiPutResult &record) {
    TStringStream str;
    str << "{EvVMultiPutResult Status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus()).data();
    if (record.HasErrorReason()) {
        str << " ErrorReason# " << '"' << EscapeC(record.GetErrorReason()) << '"';
    }
    ui64 size = record.ItemsSize();
    for (ui64 itemIdx = 0; itemIdx < size; ++itemIdx) {
        const NKikimrBlobStorage::TVMultiPutResultItem &item = record.GetItems(itemIdx);
        str << " Item# {VMultiPutResultItem";
        str << " Status# " << NKikimrProto::EReplyStatus_Name(item.GetStatus()).data();
        if (item.HasErrorReason()) {
            str << " ErrorReason# " << '"' << EscapeC(item.GetErrorReason()) << '"';
        }
        TLogoBlobID id = LogoBlobIDFromLogoBlobID(item.GetBlobID());
        str << " ID# " << id.ToString();
        if (item.HasCookie()) {
            str << " Cookie# " << item.GetCookie();
        }
        str << "}";
    }
    if (record.HasMsgQoS()) {
        str << " ";
        TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
    }
    if (record.HasCookie()) {
        str << " Cookie# " << record.GetCookie();
    }
    str << "}";
    return str.Str();
}

bool TEvBlobStorage::TEvVGet::Validate(TString& errorReason) {
    if (!Record.HasRangeQuery() && Record.ExtremeQueriesSize() == 0) {
        errorReason = "TEvVGet rejected by VDisk. It has no query";
    } else if (!Record.HasVDiskID()) {
        errorReason = "TEvVGet rejected by VDisk. It has no VDiskID";
    } else if (!Record.MutableVDiskID()->HasGroupID()) {
        errorReason = "TEvVGet rejected by VDisk. It has no VDiskID::GroupID";
    } else if (Record.MutableMsgQoS() == nullptr) {
        errorReason = "TEvVGet rejected by VDisk. MsgQoS is undefined";
    } else if (! Record.MutableMsgQoS()->HasExtQueueId()) {
        errorReason = "TEvVGet rejected by VDisk. ExtQueueId is undefined";
    } else {
        return true;
    }
    return false;
}

TString TEvBlobStorage::TEvVGet::ToString(const NKikimrBlobStorage::TEvVGet &record) {
    TStringStream str;
    if (record.HasRangeQuery()) {
        const NKikimrBlobStorage::TRangeQuery &query = record.GetRangeQuery();
        str << "{RangeQuery# ";
        if (query.HasFrom()) {
            str << LogoBlobIDFromLogoBlobID(query.GetFrom()).ToString();
        } else {
            str << "<missing>";
        }
        str << " ";
        if (query.HasTo()) {
            str << LogoBlobIDFromLogoBlobID(query.GetTo()).ToString();
        } else {
            str << "<missing>";
        }
        if (query.HasMaxResults()) {
            str << " MaxResults# " << query.GetMaxResults();
        }
        if (query.HasCookie()) {
            str << " c# " << query.GetCookie();
        }
        str << "}";
    }
    size_t size = record.ExtremeQueriesSize();
    for (unsigned i = 0; i < size; i++) {
        const NKikimrBlobStorage::TExtremeQuery &query = record.GetExtremeQueries(i);
        str << "{ExtrQuery# ";
        if (query.HasId()) {
            str << LogoBlobIDFromLogoBlobID(query.GetId()).ToString();
        } else {
            str << "<missing>";
        }
        str << " sh# " << query.GetShift() << " sz# " << query.GetSize();
        if (query.HasCookie()) {
            str << " c# " << query.GetCookie();
        }
        str << "}";
    }
    if (record.GetIndexOnly())
        str << " IndexOnly";
    if (record.HasMsgQoS()) {
        str << ' ';
        TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
    }
    str << " Notify# " << record.GetNotifyIfNotReady()
        << " Internals# " << record.GetShowInternals()
        << " TabletId# " << record.GetTabletId()
        << " AcquireBlockedGeneration# " << record.GetAcquireBlockedGeneration()
        << " ForceBlockedGeneration# " << record.GetForceBlockedGeneration()
        << "}";
    return str.Str();
}

TEvBlobStorage::TEvVGet::TEvVGet(const TVDiskID &vdisk,
        TInstant deadline,
        NKikimrBlobStorage::EGetHandleClass cls,
        bool notifyIfNotReady,
        bool showInternals,
        TMaybe<ui64> requestCookie,
        bool indexOnly,
        bool extreme,
        std::optional<TForceBlockTabletData> forceBlockTabletData)
    : Extreme(extreme)
{
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    Record.SetHandleClass(cls);
    if (notifyIfNotReady) {
        Record.SetNotifyIfNotReady(true);
    }
    if (showInternals) {
        Record.SetShowInternals(true);
    }
    if (requestCookie) {
        Record.SetCookie(*requestCookie);
    }
    if (indexOnly) {
        Record.SetIndexOnly(true);
    }
    if (deadline != TInstant::Max()) {
        Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
    }
    if (forceBlockTabletData) {
        Record.MutableForceBlockTabletData()->SetId(forceBlockTabletData->Id);
        Record.MutableForceBlockTabletData()->SetGeneration(forceBlockTabletData->Generation);
    }
    Record.MutableMsgQoS()->SetExtQueueId(HandleClassToQueueId(cls));
    Record.SetEnablePayload(true);
}

TEvBlobStorage::TEvVGetResult::TEvVGetResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TInstant &now,
        ui32 recByteSize, NKikimrBlobStorage::TEvVGet *queryRecord, const TActorIDPtr &skeletonFrontIDPtr,
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr,
        TMaybe<ui64> cookie, ui32 channel, ui64 incarnationGuid)
    : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, channel, recByteSize, queryRecord, skeletonFrontIDPtr)
    , EnablePayload(queryRecord && queryRecord->GetEnablePayload())
{
    Record.SetStatus(status);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    if (queryRecord && queryRecord->HasTimestamps()) {
        Record.MutableTimestamps()->CopyFrom(queryRecord->GetTimestamps());
    }

    // copy cookie if it was set in initial query
    if (cookie)
        Record.SetCookie(*cookie);

    if (status == NKikimrProto::OK) {
        Record.SetIncarnationGuid(incarnationGuid);
    }
}

void TEvBlobStorage::TEvVGetResult::AddResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &logoBlobId, ui64 sh,
               std::variant<TRope, ui32> dataOrSize, const ui64 *cookie,
               const ui64 *ingress, bool keep, bool doNotKeep,
               const ui64 *checksum,
               NKikimrBlobStorage::TChecksumType checksumType) {
    TRope *data = nullptr;
    ui32 size = 0;

    std::visit(TOverloaded{
        [&](TRope& x) { data = &x; size = x.size(); },
        [&](ui32& x) { size = x; }
    }, dataOrSize);

    IncrementSize(size);
    NKikimrBlobStorage::TQueryResult *r = Record.AddResult();
    r->SetStatus(status);
    LogoBlobIDFromLogoBlobID(logoBlobId, r->MutableBlobID());
    if (sh != 0) {
        r->SetShift(sh);
    }
    if (data) {
        SetBlobData(*r, std::move(*data));
    }
    r->SetSize(size);
    r->SetFullDataSize(logoBlobId.BlobSize());
    if (cookie) {
        r->SetCookie(*cookie);
    }
    if (ingress)
        r->SetIngress(*ingress);
    if (keep) {
        r->SetKeep(true);
    }
    if (doNotKeep) {
        r->SetDoNotKeep(true);
    }
    if (checksum) {
        r->SetChecksum(*checksum);
        r->SetChecksumType(checksumType);
    }
    Y_DEBUG_ABORT_UNLESS(keep + doNotKeep <= 1);
}

void TEvBlobStorage::TEvVGetResult::AddResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &logoBlobId, const ui64 *cookie,
               const ui64 *ingress, const NMatrix::TVectorType *local, bool keep,
               bool doNotKeep) {
    NKikimrBlobStorage::TQueryResult *r = Record.AddResult();
    r->SetStatus(status);
    LogoBlobIDFromLogoBlobID(logoBlobId, r->MutableBlobID());
    if (cookie)
        r->SetCookie(*cookie);
    if (ingress)
        r->SetIngress(*ingress);
    if (local) {
        for (ui8 i = local->FirstPosition(); i != local->GetSize(); i = local->NextPosition(i)) {
            r->AddParts(i + 1);
        }
    }
    if (keep) {
        r->SetKeep(true);
    }
    if (doNotKeep) {
        r->SetDoNotKeep(true);
    }
    Y_DEBUG_ABORT_UNLESS(keep + doNotKeep <= 1);
}

TString TEvBlobStorage::TEvVGetResult::ToString() const {
    TStringStream str;
    str << "{EvVGetResult QueryResult Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
    if (Record.HasErrorReason()) {
        str << " ErrorReason# '" << EscapeC(Record.GetErrorReason()) << '\'';
    }
    for (const auto& result : Record.GetResult()) {
        str << " {";
        str << (result.HasBlobID() ? LogoBlobIDFromLogoBlobID(result.GetBlobID()).ToString() : "?")
            << " " << (result.HasStatus() ? NKikimrProto::EReplyStatus_Name(result.GetStatus()) : "?");
        if (result.HasShift()) {
            str << " Shift# " << result.GetShift();
        }
        if (result.HasSize()) {
            str << " Size# " << result.GetSize();
        }
        if (result.HasFullDataSize()) {
            str << " FullDataSize# " << result.GetFullDataSize();
        }
        if (result.HasBufferData()) {
            const TString& data = result.GetBufferData();
            str << " BufferData# "
                << (data.size() <= 16 ? Base64Encode(data) : (TStringBuilder() << "<too_large>" << data.size() << 'b'));
        }
        if (result.HasPayloadId()) {
            const TRope& data = GetPayload(result.GetPayloadId());
            str << " PayloadId# " << result.GetPayloadId()
                << " Data# " << (data.size() <= 16 ? Base64Encode(data.ConvertToString()) : (TStringBuilder() << "<too_large>" << data.size() << 'b'));
        }
        if (result.HasCookie()) {
            str << " Cookie# " << result.GetCookie();
        }
        if (result.HasIngress()) {
            ui64 ingress = result.GetIngress();
            str << " Ingress# " << ingress;
        }
        if (const auto& v = result.GetParts(); !v.empty()) {
            str << " Parts# " << FormatList(v);
        }
        if (result.HasKeep() || result.HasDoNotKeep()) {
            str << " Keep# " << result.GetKeep() << " DoNotKeep# " << result.GetDoNotKeep();
        }
        str << "}";
    }
    str << " BlockedGeneration# " << Record.GetBlockedGeneration();
    str << "}";
    return str.Str();
}

void TEvBlobStorage::TEvVGetResult::MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
        const NKikimrBlobStorage::TEvVGet &request) {
    NKikimrProto::EReplyStatus messageStatus = status;
    NKikimrProto::EReplyStatus queryStatus = status != NKikimrProto::NOTREADY ? status : NKikimrProto::ERROR;

    Record.SetStatus(messageStatus);
    // Ignore RangeQuery (pretend there are no results)

    size_t size = request.ExtremeQueriesSize();
    for (unsigned i = 0; i < size; i++) {
        const NKikimrBlobStorage::TExtremeQuery &query = request.GetExtremeQueries(i);
        Y_ABORT_UNLESS(request.HasVDiskID());
        TLogoBlobID id = LogoBlobIDFromLogoBlobID(query.GetId());
        ui64 shift = (query.HasShift() ? query.GetShift() : 0);
        ui64 *cookie = nullptr;
        ui64 cookieValue = 0;
        if (query.HasCookie()) {
            cookieValue = query.GetCookie();
            cookie = &cookieValue;
        }
        AddResult(queryStatus, id, shift, 0u, cookie);
    }

    if (request.HasVDiskID()) {
        TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
        VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
    }
    if (request.HasCookie()) {
        Record.SetCookie(request.GetCookie());
    }
    if (request.HasTimestamps()) {
        Record.MutableTimestamps()->CopyFrom(request.GetTimestamps());
    }
    if (errorReason) {
        Record.SetErrorReason(errorReason);
    }
}

TEvBlobStorage::TEvVBlock::TEvVBlock(ui64 tabletId, ui32 generation, const TVDiskID &vdisk, TInstant deadline,
        TWriteSource writeSource, ui64 issuerGuid,
        std::optional<ui32> version)
{
    Record.SetTabletId(tabletId);
    Record.SetGeneration(generation);
    if (issuerGuid) {
        Record.SetIssuerGuid(issuerGuid);
    }
    if (version) {
        Record.SetVersion(*version);
    }
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    if (deadline != TInstant::Max()) {
        Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
    }
    Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::PutTabletLog);
    if (writeSource != TWriteSource::Unknown) {
        Record.SetWriteSourceOp(WriteSourceToProto(writeSource));
    }
}

TEvBlobStorage::TEvVBlockResult::TEvVBlockResult(NKikimrProto::EReplyStatus status, const TTabletActGen *actual, const TVDiskID &vdisk,
        const TInstant &now, ui32 recByteSize, NKikimrBlobStorage::TEvVBlock *queryRecord,
        const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
        const NVDiskMon::TLtcHistoPtr &histoPtr, ui64 incarnationGuid)
    : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
            recByteSize, queryRecord, skeletonFrontIDPtr)
{
    Record.SetStatus(status);
    if (actual) {
        Record.SetTabletId(actual->TabletId);
        Record.SetGeneration(actual->ActualGen);
    }
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    if (status == NKikimrProto::OK || status == NKikimrProto::ALREADY) {
        Record.SetIncarnationGuid(incarnationGuid);
    }
}

TString TEvBlobStorage::TEvVBlockResult::ToString() const {
    TStringStream str;
    str << "{EvVBlockResult Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
    if (Record.HasTabletId()) {
        str << "TabletId# " << Record.GetTabletId();
    }
    if (Record.HasGeneration()) {
        str << "Generation# " << Record.GetGeneration();
    }
    if (Record.HasIsTabletStorageInfoVersionObsolete()) {
        str << " IsTabletStorageInfoVersionObsolete# "
            << Record.GetIsTabletStorageInfoVersionObsolete();
    }
    if (Record.HasVDiskID()) {
        str << "VDisk# " << VDiskIDFromVDiskID(Record.GetVDiskID()).ToString().c_str();
    }
    str << "}";
    return str.Str();
}

void TEvBlobStorage::TEvVBlockResult::MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
        const NKikimrBlobStorage::TEvVBlock &request) {
    Record.SetStatus(status);
    Y_ABORT_UNLESS(request.HasTabletId());
    Record.SetTabletId(request.GetTabletId());
    Y_ABORT_UNLESS(request.HasGeneration());
    Record.SetGeneration(request.GetGeneration());
    Y_ABORT_UNLESS(request.HasVDiskID());
    TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
    VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
}

TEvBlobStorage::TEvVPatchStart::TEvVPatchStart(const TLogoBlobID &originalBlobId, const TLogoBlobID &patchedBlobId, const TVDiskID &vDiskId,
        TInstant deadline, TMaybe<ui64> cookie, bool notifyIfNotReady)
{
    LogoBlobIDFromLogoBlobID(originalBlobId, Record.MutableOriginalBlobId());
    LogoBlobIDFromLogoBlobID(patchedBlobId, Record.MutablePatchedBlobId());
    VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
    if (cookie) {
        Record.SetCookie(*cookie);
    }
    if (deadline != TInstant::Max()) {
        Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
    }
    if (notifyIfNotReady) {
        Record.SetNotifyIfNotReady(true);
    }
    Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::GetFastRead);
}

TString TEvBlobStorage::TEvVPatchStart::ToString(const NKikimrBlobStorage::TEvVPatchStart &record) {
    TStringStream str;
    TLogoBlobID originalId = LogoBlobIDFromLogoBlobID(record.GetOriginalBlobId());
    TLogoBlobID patchedId = LogoBlobIDFromLogoBlobID(record.GetPatchedBlobId());
    str << "{TEvVPatchStart";
    str << " OriginalBlobId# " << originalId.ToString();
    str << " PatchedBlobId# " << patchedId.ToString();
    if (record.HasMsgQoS()) {
        str << " ";
        TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
    }
    str << "}";
    return str.Str();
}

TEvBlobStorage::TEvVPatchFoundParts::TEvVPatchFoundParts(NKikimrProto::EReplyStatus status, const TLogoBlobID &originalBlobId,
        const TLogoBlobID &patchedBlobId, const TVDiskID &vDiskId, TMaybe<ui64> cookie, const TInstant &now,
        const TString &errorReason, NKikimrBlobStorage::TEvVPatchStart *record,
        const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
        const NVDiskMon::TLtcHistoPtr &histoPtr, ui64 incarnationGuid)
    : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr,
            TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG, record->GetCachedSize(),
            record, skeletonFrontIDPtr)
{
    Record.SetStatus(status);
    LogoBlobIDFromLogoBlobID(originalBlobId, Record.MutableOriginalBlobId());
    LogoBlobIDFromLogoBlobID(patchedBlobId, Record.MutablePatchedBlobId());
    VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());

    if (cookie) {
        Record.SetCookie(*cookie);
    }
    if (record && record->HasTimestamps()) {
        Record.MutableTimestamps()->CopyFrom(record->GetTimestamps());
    }
    if (status == NKikimrProto::OK) {
        Record.SetIncarnationGuid(incarnationGuid);
    }
    Record.SetErrorReason(errorReason);
}

TString TEvBlobStorage::TEvVPatchFoundParts::ToString(const NKikimrBlobStorage::TEvVPatchFoundParts &record) {
    TStringStream str;
    TLogoBlobID originalId = LogoBlobIDFromLogoBlobID(record.GetOriginalBlobId());
    TLogoBlobID patchedId = LogoBlobIDFromLogoBlobID(record.GetPatchedBlobId());
    str << "{TEvVPatchFoundParts";
    str << " OriginalBlobId# " << originalId.ToString();
    str << " PatchedBlobId# " << patchedId.ToString();
    if (record.HasMsgQoS()) {
        str << " ";
        TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
    }
    str << "}";
    return str.Str();
}

void TEvBlobStorage::TEvVPatchFoundParts::MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
        const NKikimrBlobStorage::TEvVPatchStart &request) {
    Record.SetErrorReason(errorReason);
    Record.SetStatus(status);
    Y_ABORT_UNLESS(request.HasOriginalBlobId());
    TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(request.GetOriginalBlobId());
    LogoBlobIDFromLogoBlobID(blobId, Record.MutableOriginalBlobId());
    Y_ABORT_UNLESS(request.HasVDiskID());
    TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
    VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
    Y_ABORT_UNLESS(request.HasCookie());
    Record.SetCookie(request.GetCookie());
}

TEvBlobStorage::TEvVPatchDiff::TEvVPatchDiff(const TLogoBlobID &originalPartBlobId, const TLogoBlobID &patchedPartBlobId, const TVDiskID &vDiskId,
        ui32 expectedXorDiffs, TInstant deadline, TMaybe<ui64> cookie)
{
    LogoBlobIDFromLogoBlobID(originalPartBlobId, Record.MutableOriginalPartBlobId());
    LogoBlobIDFromLogoBlobID(patchedPartBlobId, Record.MutablePatchedPartBlobId());
    VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
    if (expectedXorDiffs) {
        Record.SetExpectedXorDiffs(expectedXorDiffs);
    }
    if (cookie) {
        Record.SetCookie(*cookie);
    }
    if (deadline != TInstant::Max()) {
        Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
    }
    Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob);
}

void TEvBlobStorage::TEvVPatchDiff::AddDiff(ui64 startIdx, const TString &buffer) {
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(buffer.data(), buffer.size());

    NKikimrBlobStorage::TDiffBlock *r = Record.AddDiffs();
    r->SetOffset(startIdx);
    r->SetBuffer(buffer.data(), buffer.size());
}

void TEvBlobStorage::TEvVPatchDiff::AddDiff(ui64 startIdx, const TRcBuf &buffer) {
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(buffer.data(), buffer.size());

    NKikimrBlobStorage::TDiffBlock *r = Record.AddDiffs();
    r->SetOffset(startIdx);
    r->SetBuffer(buffer.data(), buffer.size());
}

void TEvBlobStorage::TEvVPatchDiff::AddXorReceiver(const TVDiskID &vDiskId, ui8 partId) {
    NKikimrBlobStorage::TXorDiffReceiver *r = Record.AddXorReceivers();
    VDiskIDFromVDiskID(vDiskId, r->MutableVDiskID());
    r->SetPartId(partId);
}

TString TEvBlobStorage::TEvVPatchDiff::ToString(const NKikimrBlobStorage::TEvVPatchDiff &record) {
    TStringStream str;
    TLogoBlobID originalId = LogoBlobIDFromLogoBlobID(record.GetOriginalPartBlobId());
    TLogoBlobID patchedId = LogoBlobIDFromLogoBlobID(record.GetPatchedPartBlobId());
    str << "{TEvVPatchDiff";
    str << " OriginalBlobId# " << originalId.ToString();
    str << " PatchedBlobId# " << patchedId.ToString();
    if (record.HasMsgQoS()) {
        str << " ";
        TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
    }
    str << "}";
    return str.Str();
}

TEvBlobStorage::TEvVPatchXorDiff::TEvVPatchXorDiff(const TLogoBlobID &originalPartBlobId, const TLogoBlobID &patchedPartBlobId, const TVDiskID &vDiskId,
        ui8 partId, TInstant deadline, TMaybe<ui64> cookie)
{
    LogoBlobIDFromLogoBlobID(originalPartBlobId, Record.MutableOriginalPartBlobId());
    LogoBlobIDFromLogoBlobID(patchedPartBlobId, Record.MutablePatchedPartBlobId());
    VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
    Record.SetFromPartId(partId);
    if (cookie) {
        Record.SetCookie(*cookie);
    }
    if (deadline != TInstant::Max()) {
        Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
    }
    Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob);
}

void TEvBlobStorage::TEvVPatchXorDiff::AddDiff(ui64 startIdx, const TRcBuf &buffer) {
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(buffer.data(), buffer.size());

    NKikimrBlobStorage::TDiffBlock *r = Record.AddDiffs();
    r->SetOffset(startIdx);
    r->SetBuffer(buffer.data(), buffer.size());
}

TString TEvBlobStorage::TEvVPatchXorDiff::ToString(const NKikimrBlobStorage::TEvVPatchXorDiff &record) {
    TStringStream str;
    TLogoBlobID originalId = LogoBlobIDFromLogoBlobID(record.GetOriginalPartBlobId());
    TLogoBlobID patchedId = LogoBlobIDFromLogoBlobID(record.GetPatchedPartBlobId());
    str << "{TEvVPatchXorDiff";
    str << " OriginalBlobId# " << originalId.ToString();
    str << " PatchedBlobId# " << patchedId.ToString();
    if (record.HasMsgQoS()) {
        str << " ";
        TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
    }
    str << "}";
    return str.Str();
}

TEvBlobStorage::TEvVPatchXorDiffResult::TEvVPatchXorDiffResult(NKikimrProto::EReplyStatus status, TInstant now,
        NKikimrBlobStorage::TEvVPatchXorDiff *record, const TActorIDPtr &skeletonFrontIDPtr,
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
        const NVDiskMon::TLtcHistoPtr &histoPtr)
    : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr,
            TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG, record->GetCachedSize(),
            record, skeletonFrontIDPtr)
{
    Record.SetStatus(status);
    if (record && record->HasTimestamps()) {
        Record.MutableTimestamps()->CopyFrom(record->GetTimestamps());
    }
}

void TEvBlobStorage::TEvVPatchXorDiffResult::MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
        const NKikimrBlobStorage::TEvVPatchXorDiff &request)
{
    Record.SetStatus(status);
    if (request.HasTimestamps()) {
        Record.MutableTimestamps()->CopyFrom(request.GetTimestamps());
    }
}

TEvBlobStorage::TEvVPatchResult::TEvVPatchResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &originalPartBlobId,
        const TLogoBlobID &patchedPartBlobId, const TVDiskID &vDiskId, TMaybe<ui64> cookie, TInstant now,
        NKikimrBlobStorage::TEvVPatchDiff *record, const TActorIDPtr &skeletonFrontIDPtr,
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr,
        ui64 incarnationGuid)
    : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr,
            TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG, record->GetCachedSize(),
            record, skeletonFrontIDPtr)
{
    Record.SetStatus(status);
    LogoBlobIDFromLogoBlobID(originalPartBlobId, Record.MutableOriginalPartBlobId());
    LogoBlobIDFromLogoBlobID(patchedPartBlobId, Record.MutablePatchedPartBlobId());
    VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());

    if (cookie) {
        Record.SetCookie(*cookie);
    }
    if (record && record->HasTimestamps()) {
        Record.MutableTimestamps()->CopyFrom(record->GetTimestamps());
    }
    if (status == NKikimrProto::OK) {
        Record.SetIncarnationGuid(incarnationGuid);
    }
}

void TEvBlobStorage::TEvVPatchResult::MakeError(NKikimrProto::EReplyStatus status, const TString &errorReason,
        const NKikimrBlobStorage::TEvVPatchDiff &request)
{
    Record.SetErrorReason(errorReason);
    Record.SetStatus(status);

    Y_ABORT_UNLESS(request.HasOriginalPartBlobId());
    TLogoBlobID originalPartBlobId = LogoBlobIDFromLogoBlobID(request.GetOriginalPartBlobId());
    LogoBlobIDFromLogoBlobID(originalPartBlobId, Record.MutableOriginalPartBlobId());

    Y_ABORT_UNLESS(request.HasPatchedPartBlobId());
    TLogoBlobID patchedPartBlobId = LogoBlobIDFromLogoBlobID(request.GetPatchedPartBlobId());
    LogoBlobIDFromLogoBlobID(patchedPartBlobId, Record.MutablePatchedPartBlobId());

    Y_ABORT_UNLESS(request.HasVDiskID());
    Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
    Y_ABORT_UNLESS(request.HasCookie());
    Record.SetCookie(request.GetCookie());
}

TEvBlobStorage::TEvVGetBlock::TEvVGetBlock(ui64 tabletId, const TVDiskID &vdisk, TInstant deadline)
{
    Record.SetTabletId(tabletId);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    if (deadline != TInstant::Max()) {
        Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
    }
    Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::GetFastRead);
}

TEvBlobStorage::TEvVGetBlockResult::TEvVGetBlockResult(NKikimrProto::EReplyStatus status, ui64 tabletId, const TVDiskID &vdisk,
        const TInstant &now, ui32 recByteSize, NKikimrBlobStorage::TEvVGetBlock *queryRecord,
        const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
        const NVDiskMon::TLtcHistoPtr &histoPtr)
    : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
            recByteSize, queryRecord, skeletonFrontIDPtr)
{
    Record.SetStatus(status);
    Record.SetTabletId(tabletId);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
}

TEvBlobStorage::TEvVGetBlockResult::TEvVGetBlockResult(NKikimrProto::EReplyStatus status, ui64 tabletId, ui32 generation, const TVDiskID &vdisk,
        const TInstant &now, ui32 recByteSize, NKikimrBlobStorage::TEvVGetBlock *queryRecord,
        const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
        const NVDiskMon::TLtcHistoPtr &histoPtr)
    : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
            recByteSize, queryRecord, skeletonFrontIDPtr)
{
    Record.SetStatus(status);
    Record.SetTabletId(tabletId);
    Record.SetGeneration(generation);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
}

TString TEvBlobStorage::TEvVGetBlockResult::ToString() const {
    TStringStream str;
    str << "{EvVGetBlockResult Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
    if (Record.HasTabletId()) {
        str << "TabletId# " << Record.GetTabletId();
    }
    if (Record.HasGeneration()) {
        str << "Generation# " << Record.GetGeneration();
    }
    if (Record.HasVDiskID()) {
        str << "VDisk# " << VDiskIDFromVDiskID(Record.GetVDiskID()).ToString().c_str();
    }
    str << "}";
    return str.Str();
}

void TEvBlobStorage::TEvVGetBlockResult::MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
        const NKikimrBlobStorage::TEvVGetBlock &request) {
    Record.SetStatus(status);
    Y_ABORT_UNLESS(request.HasTabletId());
    Record.SetTabletId(request.GetTabletId());
    // WARNING: Generation is not set!
    Y_ABORT_UNLESS(request.HasVDiskID());
    TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
    VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
}


TEvBlobStorage::TEvVCollectGarbage::TEvVCollectGarbage(ui64 tabletId, ui32 recordGeneration, ui32 perGenerationCounter, ui32 channel, bool collect,
    ui32 collectGeneration, ui32 collectStep, bool hard,
    const TVector<TLogoBlobID> *keep, const TVector<TLogoBlobID> *doNotKeep,
    const TVDiskID &vdisk, TInstant deadline,
    TWriteSource writeSource)
{
    Record.SetTabletId(tabletId);
    Record.SetRecordGeneration(recordGeneration);
    Record.SetPerGenerationCounter(perGenerationCounter);
    Record.SetChannel(channel);
    if (collect) {
        Record.SetCollectGeneration(collectGeneration);
        Record.SetCollectStep(collectStep);
        Record.SetHard(hard);
    }
    if (keep) {
        for (ui64 i = 0; i < keep->size(); ++i) {
            LogoBlobIDFromLogoBlobID(keep->at(i), Record.AddKeep());
        }
    }
    if (doNotKeep) {
        for (ui64 i = 0; i < doNotKeep->size(); ++i) {
            LogoBlobIDFromLogoBlobID(doNotKeep->at(i), Record.AddDoNotKeep());
        }
    }
    if (deadline != TInstant::Max()) {
        Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
    }
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::PutTabletLog);
    if (writeSource != TWriteSource::Unknown) {
        Record.SetWriteSourceOp(WriteSourceToProto(writeSource));
    }
}

TString TEvBlobStorage::TEvVCollectGarbage::ToString() const {
    TStringStream str;
    str << "{TEvVCollectGarbage for [tablet:gen:cnt:channel]=[" << Record.GetTabletId() << ":"
        << Record.GetRecordGeneration() << ":" << Record.GetPerGenerationCounter() << ":"
        << Record.GetChannel() << "]";
    if (Record.HasCollectGeneration() || Record.HasCollectStep()) {
        str << " collect=[" << Record.GetCollectGeneration() << ":" << Record.GetCollectStep() << "]";
    }

    if (Record.KeepSize() > 0) {
        str << " Keep:";
        for (const auto &item : Record.GetKeep())
            str << " " << LogoBlobIDFromLogoBlobID(item);
    }

    if (Record.DoNotKeepSize() > 0) {
        str << " DoNotKeep:";
        for (const auto &item : Record.GetDoNotKeep())
            str << LogoBlobIDFromLogoBlobID(item);
    }
    return str.Str();
}

TEvBlobStorage::TEvVCollectGarbageResult::TEvVCollectGarbageResult(NKikimrProto::EReplyStatus status, ui64 tabletId, ui32 recordGeneration, ui32 channel,
        const TVDiskID &vdisk, const TInstant &now, ui32 recByteSize,
        NKikimrBlobStorage::TEvVCollectGarbage *queryRecord, const TActorIDPtr &skeletonFrontIDPtr,
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr,
        ui64 incarnationGuid)
    : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
            recByteSize, queryRecord, skeletonFrontIDPtr)
{
    Record.SetStatus(status);
    Record.SetTabletId(tabletId);
    Record.SetRecordGeneration(recordGeneration);
    Record.SetChannel(channel);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    if (status == NKikimrProto::OK) {
        Record.SetIncarnationGuid(incarnationGuid);
    }
}

TString TEvBlobStorage::TEvVCollectGarbageResult::ToString() const {
    TStringStream str;
    str << "{EvVCollectGarbageResult Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
    if (Record.HasTabletId()) {
        str << " TabletId# " << Record.GetTabletId();
    }
    if (Record.HasRecordGeneration()) {
        str << " RecordGeneration# " << Record.GetRecordGeneration();
    }
    if (Record.HasChannel()) {
        str << " Channel# " << Record.GetChannel();
    }
    if (Record.HasVDiskID()) {
        str << " VDisk# " << VDiskIDFromVDiskID(Record.GetVDiskID()).ToString().c_str();
    }
    str << "}";
    return str.Str();
}

void TEvBlobStorage::TEvVCollectGarbageResult::MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
        const NKikimrBlobStorage::TEvVCollectGarbage &request) {
    Record.SetStatus(status);
    Y_ABORT_UNLESS(request.HasTabletId());
    Record.SetTabletId(request.GetTabletId());
    Y_ABORT_UNLESS(request.HasRecordGeneration());
    Record.SetRecordGeneration(request.GetRecordGeneration());
    Y_ABORT_UNLESS(request.HasChannel());
    Record.SetChannel(request.GetChannel());
    Y_ABORT_UNLESS(request.HasVDiskID());
    TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
    VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
}

TEvBlobStorage::TEvVGetBarrierResult::TEvVGetBarrierResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TInstant &now,
        ui32 recByteSize, NKikimrBlobStorage::TEvVGetBarrier *queryRecord,
        const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
        const NVDiskMon::TLtcHistoPtr &histoPtr)
    : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
            recByteSize, queryRecord, skeletonFrontIDPtr)
{
    Record.SetStatus(status);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
}

void TEvBlobStorage::TEvVGetBarrierResult::MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
        const NKikimrBlobStorage::TEvVGetBarrier &request) {
    Record.SetStatus(status);
    Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
}

TEvBlobStorage::TEvVCheckReadinessResult::TEvVCheckReadinessResult(NKikimrProto::EReplyStatus status, bool checksumming) {
    Record.SetStatus(status);
    Record.SetExtraBlockChecksSupport(true);
    Record.SetChecksumming(checksumming);
}

TEvBlobStorage::TEvVCompact::TEvVCompact(const TVDiskID &vdisk, NKikimrBlobStorage::TEvVCompact::EOpType opType) {
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    Record.SetOpType(opType);
}

TEvBlobStorage::TEvVCompactResult::TEvVCompactResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk) {
    Record.SetStatus(status);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
}

TEvBlobStorage::TEvVCompactResult::TEvVCompactResult(NKikimrProto::EReplyStatus status, const NKikimrBlobStorage::TVDiskID &vdisk) {
    Record.SetStatus(status);
    Record.MutableVDiskID()->CopyFrom(vdisk);
}

TEvBlobStorage::TEvVDefrag::TEvVDefrag(const TVDiskID &vdisk, bool full) {
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    Record.SetFull(full);
}

TEvBlobStorage::TEvVDefragResult::TEvVDefragResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk) {
    Record.SetStatus(status);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
}

TEvBlobStorage::TEvVDefragResult::TEvVDefragResult(NKikimrProto::EReplyStatus status, const NKikimrBlobStorage::TVDiskID &vdisk) {
    Record.SetStatus(status);
    Record.MutableVDiskID()->CopyFrom(vdisk);
}

TEvBlobStorage::TEvVBaldSyncLog::TEvVBaldSyncLog(const TVDiskID &vdisk, bool dropChunksExplicitly) {
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    Record.SetDropChunksExplicitly(dropChunksExplicitly);
}

TEvBlobStorage::TEvVBaldSyncLogResult::TEvVBaldSyncLogResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk) {
    Record.SetStatus(status);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
}

TEvBlobStorage::TEvVBaldSyncLogResult::TEvVBaldSyncLogResult(NKikimrProto::EReplyStatus status, const NKikimrBlobStorage::TVDiskID &vdisk) {
    Record.SetStatus(status);
    Record.MutableVDiskID()->CopyFrom(vdisk);
}

TEvBlobStorage::TEvVDbStat::TEvVDbStat(const TVDiskID &vdisk, NKikimrBlobStorage::EDbStatAction action,
           NKikimrBlobStorage::EDbStatType dbStatType, bool pretty) {
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    Record.SetAction(action);
    Record.SetType(dbStatType);
    Record.SetPrettyPrint(pretty);
}

TEvBlobStorage::TEvVDbStat::TEvVDbStat(const TVDiskID &vdisk, ui64 tabletId, bool pretty) {
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    Record.SetAction(NKikimrBlobStorage::StatTabletAction);
    Record.SetType(NKikimrBlobStorage::StatTabletType);
    Record.SetTabletId(tabletId);
    Record.SetPrettyPrint(pretty);
}

TEvBlobStorage::TEvVDbStatResult::TEvVDbStatResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TInstant &now,
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr)
    : TEvVResultBasePB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG)
{
    Record.SetStatus(status);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
}

TEvBlobStorage::TEvVAssimilate::TEvVAssimilate(const TVDiskID& vdiskId, std::optional<ui64> skipBlocksUpTo,
        std::optional<std::tuple<ui64, ui8>> skipBarriersUpTo, std::optional<TLogoBlobID> skipBlobsUpTo,
        bool ignoreDecommitState, bool reverse) {
    VDiskIDFromVDiskID(vdiskId, Record.MutableVDiskID());
    if (skipBlocksUpTo) {
        Record.SetSkipBlocksUpTo(*skipBlocksUpTo);
    }
    if (skipBarriersUpTo) {
        auto *barrier = Record.MutableSkipBarriersUpTo();
        barrier->SetTabletId(std::get<0>(*skipBarriersUpTo));
        barrier->SetChannel(std::get<1>(*skipBarriersUpTo));
    }
    if (skipBlobsUpTo) {
        LogoBlobIDFromLogoBlobID(*skipBlobsUpTo, Record.MutableSkipBlobsUpTo());
    }
    if (ignoreDecommitState) {
        Record.SetIgnoreDecommitState(ignoreDecommitState);
    }
    if (reverse) {
        Record.SetReverse(reverse);
    }
}

TEvBlobStorage::TEvVAssimilateResult::TEvVAssimilateResult(NKikimrProto::EReplyStatus status, TString errorReason, TVDiskID vdiskId) {
    Record.SetStatus(status);
    if (status != NKikimrProto::OK) {
        Record.SetErrorReason(errorReason);
    }
    VDiskIDFromVDiskID(vdiskId, Record.MutableVDiskID());
}

TEvBlobStorage::TEvVSyncGuid::TEvVSyncGuid(const TVDiskID &sourceVDisk, const TVDiskID &targetVDisk) {
    VDiskIDFromVDiskID(sourceVDisk, Record.MutableSourceVDiskID());
    VDiskIDFromVDiskID(targetVDisk, Record.MutableTargetVDiskID());
}

TEvBlobStorage::TEvVSyncGuid::TEvVSyncGuid(const TVDiskID &sourceVDisk,
             const TVDiskID &targetVDisk,
             ui64 guid,
             NKikimrBlobStorage::TSyncGuidInfo::EState state) {
    VDiskIDFromVDiskID(sourceVDisk, Record.MutableSourceVDiskID());
    VDiskIDFromVDiskID(targetVDisk, Record.MutableTargetVDiskID());
    auto w = Record.MutableInfo();
    w->SetGuid(guid);
    w->SetState(state);
}

TEvBlobStorage::TEvVSyncGuidResult::TEvVSyncGuidResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TInstant &now,
        ui64 guid, EState state, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
        const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel)
    : TEvVResultBasePB(now, counterPtr, histoPtr, channel)
{
    Record.SetStatus(status);
    auto guidInfo = Record.MutableReadInfo();
    guidInfo->SetGuid(guid);
    guidInfo->SetState(state);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
}

TEvBlobStorage::TEvVSyncGuidResult::TEvVSyncGuidResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TInstant &now,
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
        const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel)
    : TEvVResultBasePB(now, counterPtr, histoPtr, channel)
{
    Record.SetStatus(status);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
}

void TEvBlobStorage::TEvVSyncGuidResult::Output(IOutputStream &str) const {
    auto vd = VDiskIDFromVDiskID(Record.GetVDiskID());
    str << "{TEvVSyncGuidResult: Status# " << Record.GetStatus();
    str << " VDiskId# " << vd.ToString() << "}";
}

TString TEvBlobStorage::TEvVSyncGuidResult::ToString() const {
    TStringStream str;
    Output(str);
    return str.Str();
}

TEvBlobStorage::TEvVSync::TEvVSync(const TSyncState &syncState, const TVDiskID &sourceVDisk, const TVDiskID &targetVDisk) {
    SyncStateFromSyncState(syncState, Record.MutableSyncState());
    VDiskIDFromVDiskID(sourceVDisk, Record.MutableSourceVDiskID());
    VDiskIDFromVDiskID(targetVDisk, Record.MutableTargetVDiskID());
}

TEvBlobStorage::TEvVSyncResult::TEvVSyncResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk,
               const TSyncState &newSyncState, bool finished, NPDisk::TStatusFlags flags, const TInstant &now,
               const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
               const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel)
    : TEvVResultBasePB(now, counterPtr, histoPtr, channel)
{
    Record.SetStatus(status);
    SyncStateFromSyncState(newSyncState, Record.MutableNewSyncState());
    Record.SetFinished(finished);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    Record.SetStatusFlags(flags);
}

TEvBlobStorage::TEvVSyncResult::TEvVSyncResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, NPDisk::TStatusFlags flags,
        const TInstant &now, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
        const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel)
    : TEvVResultBasePB(now, counterPtr, histoPtr, channel)
{
    Record.SetStatus(status);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    Record.SetStatusFlags(flags);
}

TString TEvBlobStorage::TEvVSyncResult::ToString() const {
    TStringStream str;
    str << "{Status:" << Record.GetStatus()
        << " NewSyncState# " << SyncStateFromSyncState(Record.GetNewSyncState())
        << " Finished# " << Record.GetFinished()
        << " VDiskID# " << VDiskIDFromVDiskID(Record.GetVDiskID())
        << "}";
    return str.Str();
}

NKikimrBlobStorage::EFullSyncProtocol TEvBlobStorage::TEvVSyncFull::GetProtocol() {
    return Record.HasProtocol()
        ? Record.GetProtocol()
        : NKikimrBlobStorage::EFullSyncProtocol::Legacy;
}

TEvBlobStorage::TEvVSyncFullResult::TEvVSyncFullResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TSyncState &syncState,
        ui64 cookie, const TInstant &now, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
        const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel, NKikimrBlobStorage::EFullSyncProtocol protocol)
    : TEvVResultBasePB(now, counterPtr, histoPtr, channel)
{
    Record.SetStatus(status);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    Record.SetCookie(cookie);
    SyncStateFromSyncState(syncState, Record.MutableSyncState());
    Record.SetProtocol(protocol);
}

TEvBlobStorage::TEvVSyncFullResult::TEvVSyncFullResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, ui64 cookie,
        const TInstant &now, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
        const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel, NKikimrBlobStorage::EFullSyncProtocol protocol)
    : TEvVResultBasePB(now, counterPtr, histoPtr, channel)
{
    Record.SetStatus(status);
    VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
    Record.SetCookie(cookie);
    Record.SetProtocol(protocol);
}

TString TEvBlobStorage::TEvVSyncFullResult::ToString() const {
    TStringStream str;
    str << "{Status:" << Record.GetStatus()
        << " SyncState# " << SyncStateFromSyncState(Record.GetSyncState())
        << " Finished# " << Record.GetFinished()
        << " VDiskID# " << VDiskIDFromVDiskID(Record.GetVDiskID())
        << " Cookie# " << Record.GetCookie()
        << " Stage# " << Record.GetStage()
        << "}";
    return str.Str();
}

TEvBlobStorage::TEvVWindowChange::TEvVWindowChange(NKikimrBlobStorage::EVDiskQueueId queueId, TDropConnection) {
    Record.SetFrontQueueId(queueId);
    Record.SetDropConnection(true);
}

TString TEvBlobStorage::TEvCaptureVDiskLayoutResult::TLayoutRecord::ToString() const {
    return TStringBuilder() << "{Location# " << Location.ToString()
        << " Database# " << (int)Database
        << " RecordType# " << (int)RecordType
        << " BlobId# " << BlobId.ToString()
        << " SstId# " << SstId
        << " Level# " << Level << '}';
}

TEvBlobStorage::TEvVTakeSnapshot::TEvVTakeSnapshot(TVDiskID vdiskId, const TString& snapshotId, ui32 timeToLiveSec) {
    VDiskIDFromVDiskID(vdiskId, Record.MutableVDiskID());
    Record.SetSnapshotId(snapshotId);
    Record.SetTimeToLiveSec(timeToLiveSec);
}

TEvBlobStorage::TEvVTakeSnapshotResult::TEvVTakeSnapshotResult(NKikimrProto::EReplyStatus status, const TString& errorReason, TVDiskID vdiskId) {
    Record.SetStatus(status);
    if (errorReason) {
        Record.SetErrorReason(errorReason);
    }
    VDiskIDFromVDiskID(vdiskId, Record.MutableVDiskID());
}

TEvBlobStorage::TEvVReleaseSnapshot::TEvVReleaseSnapshot(TVDiskID vdiskId, const TString& snapshotId) {
    VDiskIDFromVDiskID(vdiskId, Record.MutableVDiskID());
    Record.SetSnapshotId(snapshotId);
}

TEvBlobStorage::TEvVReleaseSnapshotResult::TEvVReleaseSnapshotResult(NKikimrProto::EReplyStatus status, const TString& errorReason, TVDiskID vdiskId) {
    Record.SetStatus(status);
    if (errorReason) {
        Record.SetErrorReason(errorReason);
    }
    VDiskIDFromVDiskID(vdiskId, Record.MutableVDiskID());
}

TEvGetLogoBlobIndexStatResponseAck::TEvGetLogoBlobIndexStatResponseAck(ui64 sequenceId, bool cancel) {
    Record.set_sequence_id(sequenceId);
    if (cancel) {
        Record.set_cancel(true);
    }
}

} // namespace NKikimr
