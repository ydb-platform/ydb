#include "dsproxy.h"
#include "dsproxy_mon.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>

namespace NKikimr {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DISCOVER request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TEntryInfo {
    TIngress Present;
    TIngress Seen;

    TEntryInfo(const TEntryInfo &x) noexcept
    {
        Y_ABORT_UNLESS(x.Present.Raw() == 0 && x.Seen.Raw() == 0);
    }

    TEntryInfo()
    {}

    void RegisterBlob(const TIngress &ingress, const TBlobStorageGroupInfo *info, const TVDiskID &vdisk,
            const TLogoBlobID &fullid) {
        AddIngress(ingress);
        NMatrix::TVectorType localParts = ingress.LocalParts(info->Type);
        if (!localParts.Empty()) {  // VDisk has some local parts of the blob
            const ui32 totalPartCount = info->Type.TotalPartCount();
            for (ui32 partIdx = 0; partIdx < totalPartCount; ++partIdx) {
                if (localParts.Get(partIdx)) {  // VDisk has local part# partIdx
                    TLogoBlobID blobPartId(fullid, partIdx + 1);
                    AddPart(info, vdisk, blobPartId);
                }
            }
        }
    }

    void AddIngress(const TIngress &ingress) {
        Seen.Merge(ingress);
    }

    void AddPart(const TBlobStorageGroupInfo *info, const TVDiskID &vdisk, const TLogoBlobID &id) {
        ui32 partIdx = id.PartId() - 1;
        Y_ABORT_UNLESS(partIdx < info->Type.TotalPartCount());
        TIngress partIngress = *TIngress::CreateIngressWithLocal(&info->GetTopology(), vdisk, id);
        Present.Merge(partIngress);
    }
};

typedef TMap<TLogoBlobID, TEntryInfo, TGreater<TLogoBlobID> > TEntryInfos;

class TTrackerFailDomain {
    ui32 SentMask;
    ui32 ReplyMask;
    ui32 SuccessMask;

    ui32 RequestsSent;
    ui32 Replies;
    ui32 Success;
    bool IsDomainSuccess;
    bool IsDomainReply;

    bool UpdateDomain() {
        Y_ABORT_UNLESS(!IsDomainReply);
        Y_ABORT_UNLESS(!IsDomainSuccess);
        if (RequestsSent == Replies) {
            IsDomainReply = true;
            if (RequestsSent == Success) {
                IsDomainSuccess = true;
            }
            return true;
        }
        return false;
    }
public:
    TTrackerFailDomain()
        : SentMask(0)
        , ReplyMask(0)
        , SuccessMask(0)
        , RequestsSent(0)
        , Replies(0)
        , Success(0)
        , IsDomainSuccess(false)
        , IsDomainReply(false)
    {}

    ui32 GetRequestsSent() { return RequestsSent; };
    ui32 GetReplies() { return Replies; };
    ui32 GetSuccess() { return Success; };
    bool GetIsDomainSuccess() { return IsDomainSuccess; };
    bool GetIsDomainReply() { return IsDomainReply; };

    void SentTo(ui32 vDisk) {
        Y_ABORT_UNLESS(vDisk < MaskSizeBits);
        ui32 bit = (1 << vDisk);
        Y_ABORT_UNLESS(!(SentMask & bit));
        Y_ABORT_UNLESS(!(ReplyMask & bit));
        Y_ABORT_UNLESS(!(SuccessMask & bit));
        SentMask |= bit;
        RequestsSent++;
    }

    void AnotherRequest(ui32 vDisk) {
        Y_ABORT_UNLESS(vDisk < MaskSizeBits);
        ui32 bit = (1 << vDisk);
        Y_ABORT_UNLESS(SentMask & bit);
        Y_ABORT_UNLESS(ReplyMask & bit);
        Y_ABORT_UNLESS(SuccessMask & bit);
        ReplyMask &= ~bit;
        SuccessMask &= ~bit;
        Y_ABORT_UNLESS(Replies);
        Y_ABORT_UNLESS(Success);
        Replies--;
        Success--;
        Y_ABORT_UNLESS(Replies < RequestsSent);
        Y_ABORT_UNLESS(Success < RequestsSent);
        IsDomainReply = false;
        IsDomainSuccess = false;
    }

    bool GotSuccessReply(ui32 vDisk) {
        Y_ABORT_UNLESS(vDisk < MaskSizeBits);
        ui32 bit = (1 << vDisk);
        Y_ABORT_UNLESS(SentMask & bit);
        Y_ABORT_UNLESS(!(ReplyMask & bit));
        Y_ABORT_UNLESS(!(SuccessMask & bit));
        Y_ABORT_UNLESS(Replies < RequestsSent);
        Y_ABORT_UNLESS(Success < RequestsSent);
        ReplyMask |= bit;
        SuccessMask |= bit;
        Replies++;
        Success++;
        return UpdateDomain();
    }

    bool GotErrorReply(ui32 vDisk) {
        Y_ABORT_UNLESS(vDisk < MaskSizeBits);
        ui32 bit = (1 << vDisk);
        Y_ABORT_UNLESS(SentMask & bit);
        Y_ABORT_UNLESS(!(ReplyMask & bit));
        Y_ABORT_UNLESS(!(SuccessMask & bit));
        Y_ABORT_UNLESS(Replies < RequestsSent);
        Y_ABORT_UNLESS(Success < RequestsSent);
        ReplyMask |= bit;
        Replies++;
        return UpdateDomain();
    }
};


struct TGroupResponseTracker {
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TStackVec<TTrackerFailDomain, TypicalFailDomainsInGroup> Domains;
    ui32 DomainRequestsSent;
    ui32 DomainReplies;
    ui32 DomainSuccess;

    TEntryInfos EntryInfo;
    ui32 CurrentRequestSize;

    TGroupResponseTracker(TIntrusivePtr<TBlobStorageGroupInfo> info)
        : Info(std::move(info))
        , Domains(Info->GetTotalFailDomainsNum())
        , DomainRequestsSent(0)
        , DomainReplies(0)
        , DomainSuccess(0)
        , CurrentRequestSize(BeginRequestSize)
    {
        for (const auto& vdisk : Info->GetVDisks()) {
            auto vd = Info->GetVDiskId(vdisk.OrderNumber);
            SentTo(vd);
        }
        DomainRequestsSent = Domains.size();
    }

    bool IsUnreadableDisintegrated() {
        return (DomainReplies - DomainSuccess > Info->Type.ParityParts());
    }

    void OnReply(const TVDiskID& vdisk, NKikimrProto::EReplyStatus status) {
        const ui32 failDomainOrderNumber = Info->GetFailDomainOrderNumber(vdisk);
        TTrackerFailDomain& domain = Domains[failDomainOrderNumber];
        const ui32 vDiskIdx = vdisk.VDisk;

        Y_ABORT_UNLESS(domain.GetReplies() < domain.GetRequestsSent(),
            "Replies# %" PRIu32 " RequestsSent# %" PRIu32,
            (ui32)domain.GetReplies(), (ui32)domain.GetRequestsSent());
        Y_ABORT_UNLESS(domain.GetSuccess() < domain.GetRequestsSent(),
            "Success# %" PRIu32 " RequestsSent# %" PRIu32,
            (ui32)domain.GetSuccess(), (ui32)domain.GetRequestsSent());
        bool isDomain = false;
        if (status != NKikimrProto::ERROR && status != NKikimrProto::VDISK_ERROR_STATE) {
            Y_ABORT_UNLESS(status == NKikimrProto::OK || status == NKikimrProto::NOT_YET);
            isDomain = domain.GotSuccessReply(vDiskIdx);
        } else {
            isDomain = domain.GotErrorReply(vDiskIdx);
        }

        if (isDomain) {
            if (domain.GetIsDomainReply()) {
                DomainReplies++;
            }
            if (domain.GetIsDomainSuccess()) {
                DomainSuccess++;
            }
        }
    }

    void AnotherRequest(const TVDiskID& vdisk) {
        const ui32 failDomainOrderNumber = Info->GetFailDomainOrderNumber(vdisk);
        TTrackerFailDomain& domain = Domains[failDomainOrderNumber];
        const ui32 vDiskIdx = vdisk.VDisk;

        if (domain.GetIsDomainReply()) {
            DomainReplies--;
        }
        if (domain.GetIsDomainSuccess()) {
            DomainSuccess--;
        }
        domain.AnotherRequest(vDiskIdx);
    }

    void SentTo(const TVDiskID& vdisk) {
        const ui32 failDomainOrderNumber = Info->GetFailDomainOrderNumber(vdisk);
        TTrackerFailDomain& domain = Domains[failDomainOrderNumber];
        const ui32 vDiskIdx = vdisk.VDisk;
        domain.SentTo(vDiskIdx);
    }
};


class TBlobStorageGroupDiscoverRequest : public TBlobStorageGroupRequestActor<TBlobStorageGroupDiscoverRequest>{

    struct TBlobInfo {
        TLogoBlobID Id;
        TIngress Ingress;

        TBlobInfo(const TLogoBlobID &id, const TIngress ingress)
            : Id(id)
            , Ingress(ingress)
        {}
    };

    struct TVDiskInfo {
        TVector<TBlobInfo> Blobs;
        TLogoBlobID nextLogoBlobId;
        ui32 LastRequestSize;
        bool IsMoreRequested;
        bool IsAllRead;
        bool IsError;
        bool IsResponsive;

        TVDiskInfo()
            : LastRequestSize(BeginRequestSize)
            , IsMoreRequested(false)
            , IsAllRead(false)
            , IsError(false)
            , IsResponsive(false)
        {}
    };

    typedef TMap<ui64, TVDiskInfo> TVDiskInfoContainer;

    TVDiskInfoContainer VDiskInfo;

    const ui64 TabletId;
    const ui32 MinGeneration;
    const bool ReadBody;
    const bool DiscoverBlockedGeneration;
    const TInstant Deadline;
    const TInstant StartTime;

    TGroupResponseTracker GroupResponseTracker;
    std::unique_ptr<TEvBlobStorage::TEvDiscoverResult> PendingResult;

    ui32 GetBlockReplies = 0;
    ui32 GetBlockErrors = 0;

    ui32 BlockedGen = 0;
    ui32 VGetBlockedGen = 0;
    bool IsGetBlockDone;
    bool IsIterativeDone = false;
    bool IsGetDataDone = false;

    ui64 TotalSent = 0;
    ui64 TotalRecieved = 0;

    const ui32 ForceBlockedGeneration;
    const bool FromLeader;

    template<typename TPtr>
    void SendResult(TPtr& result) {
        Y_ABORT_UNLESS(result);
        const TDuration duration = TActivationContext::Now() - StartTime;
        Mon->CountDiscoverResponseTime(duration);
        const bool success = result->Status == NKikimrProto::OK;
        LWPROBE(DSProxyRequestDuration, TEvBlobStorage::EvDiscover, 0, duration.SecondsFloat() * 1000.0,
                TabletId, Info->GroupID.GetRawId(), TLogoBlobID::MaxChannel, "", success);
        SendResponseAndDie(std::move(result));
    }

    friend class TBlobStorageGroupRequestActor<TBlobStorageGroupDiscoverRequest>;
    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        std::unique_ptr<TEvBlobStorage::TEvDiscoverResult> result(new TEvBlobStorage::TEvDiscoverResult(status, MinGeneration,
                    BlockedGen));
        result->ErrorReason = ErrorReason;
        A_LOG_LOG_S(true, PriorityForStatusOutbound(status), "BSD01", "Result# " << result->Print(false));
        SendResult(result);
    }

    void Handle(TEvBlobStorage::TEvVGetBlockResult::TPtr &ev) {
        ProcessReplyFromQueue(ev);

        TotalRecieved++;
        NKikimrBlobStorage::TEvVGetBlockResult &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasStatus());
        const NKikimrProto::EReplyStatus status = record.GetStatus();

        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());

        Y_ABORT_UNLESS(status == NKikimrProto::OK || status == NKikimrProto::NODATA || status == NKikimrProto::ERROR
            || status == NKikimrProto::VDISK_ERROR_STATE, "status# %" PRIu32, ui32(status));

        A_LOG_LOG_S(false, PriorityForStatusInbound(status), "BSD03",
            "Status# " << NKikimrProto::EReplyStatus_Name(status)
            << " vdisk# " << vdisk.ToString()
            << " NodeId# " << Info->GetActorId(vdisk).NodeId());

        ++GetBlockReplies;
        if (status == NKikimrProto::NODATA) {
            // nothing
        } else if (status == NKikimrProto::OK) {
            Y_ABORT_UNLESS(record.HasGeneration());
            BlockedGen = Max(BlockedGen, record.GetGeneration());
        } else if (status == NKikimrProto::ERROR || status == NKikimrProto::VDISK_ERROR_STATE) {
            ++GetBlockErrors;
        } else {
            Y_ABORT("status: %s" , NKikimrProto::EReplyStatus_Name(status).data());
        }

        // Not Minimal Restorable, but minimal needed for write to succseed
        if (!IsGetBlockDone && GetBlockReplies == Info->Type.TotalPartCount()) {
            IsGetBlockDone = true;
            if (IsIterativeDone && (IsGetDataDone || !ReadBody)) {
                A_LOG_LOG_S(true, PriorityForStatusOutbound(PendingResult->Status), "BSD05",
                    "Die. Result# "<< PendingResult->Print(false));
                SendResult(PendingResult);
            }
        }
    }

    void HandleIgnore(TEvBlobStorage::TEvVGetResult::TPtr &ev) {
        ProcessReplyFromQueue(ev);
        CountEvent(*ev->Get());

        TotalRecieved++;
        NKikimrBlobStorage::TEvVGetResult &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasStatus());
        const NKikimrProto::EReplyStatus status = record.GetStatus();

        R_LOG_DEBUG_S("BSD29", "Handle TEvVGetResult Ignore"
                << " status# " << NKikimrProto::EReplyStatus_Name(status)
                << " ev# " << ev->Get()->ToString());

        Y_ABORT_UNLESS(TotalRecieved < TotalSent);
        return;
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev) {
        ProcessReplyFromQueue(ev);
        CountEvent(*ev->Get());

        TotalRecieved++;
        NKikimrBlobStorage::TEvVGetResult &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasStatus());
        const NKikimrProto::EReplyStatus status = record.GetStatus();

        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());

        Y_ABORT_UNLESS(status == NKikimrProto::OK || status == NKikimrProto::ERROR || status == NKikimrProto::VDISK_ERROR_STATE);
        if (IsIterativeDone) {
            Y_ABORT_UNLESS(TotalRecieved < TotalSent);
            return;
        }

        // todo: continuous requests
        // todo: reconfigurable channels
        VGetBlockedGen = Max(VGetBlockedGen, record.GetBlockedGeneration());

        TVDiskInfo &vDiskData = VDiskInfo.at(TVDiskIdShort(vdisk).GetRaw());

        A_LOG_LOG_S(false, PriorityForStatusInbound(status), "BSD07", "Handle TEvVGetResult"
            << " Status# " << NKikimrProto::EReplyStatus_Name(status)
            << " vdisk# " << vdisk.ToString()
            << " NodeId# " << Info->GetActorId(vdisk).NodeId()
            << " ev# " << ev->Get()->ToString());

        vDiskData.IsResponsive = true;

        NKikimrProto::EReplyStatus replyStatus = status;
        if (status == NKikimrProto::OK) {
            if (record.GetIsRangeOverflow() && !record.ResultSize()) {
                A_LOG_CRIT_S("BSD40", "Handle TEvVGetResult inconsistent IsRangeOverflow set with ResultSize# 0");
                replyStatus = NKikimrProto::ERROR;
                vDiskData.IsError = true;
            }
        } else {
            replyStatus = NKikimrProto::ERROR;
            vDiskData.IsError = true;
        }
        for (ui32 i = 0, e = (ui32)record.ResultSize(); i != e; ++i) {
            const NKikimrBlobStorage::TQueryResult &result = record.GetResult(i);
            Y_ABORT_UNLESS(result.HasStatus());
            NKikimrProto::EReplyStatus recordStatus = result.GetStatus();
            if (recordStatus == NKikimrProto::ERROR || recordStatus == NKikimrProto::VDISK_ERROR_STATE) {
                replyStatus = NKikimrProto::ERROR;
                vDiskData.IsError = true;
                break;
            } else if (recordStatus == NKikimrProto::NOT_YET || recordStatus == NKikimrProto::OK) {
                Y_ABORT_UNLESS(result.HasBlobID());
                TLogoBlobID id = LogoBlobIDFromLogoBlobID(result.GetBlobID());
                Y_ABORT_UNLESS(result.HasIngress());
                TIngress ingress(result.GetIngress());
                vDiskData.Blobs.emplace_back(id, ingress);
            } else if (recordStatus == NKikimrProto::NODATA) {
                vDiskData.IsAllRead = true;
            } else {
                Y_ABORT("status: %s" , NKikimrProto::EReplyStatus_Name(recordStatus).data());
            }
        }
        if (record.ResultSize() < vDiskData.LastRequestSize && !vDiskData.IsError && !record.GetIsRangeOverflow()) {
            vDiskData.IsAllRead = true;
        }

        vDiskData.IsMoreRequested = false;
        if (vDiskData.Blobs.size() > 0) {
            TLogoBlobID &lastBlobId = vDiskData.Blobs.back().Id;

            ui64 tablet = lastBlobId.TabletID();
            ui32 channel = lastBlobId.Channel();
            ui32 gen = lastBlobId.Generation();
            ui32 step = lastBlobId.Step();
            ui32 cookie = lastBlobId.Cookie();

            if (cookie > 0)
                vDiskData.nextLogoBlobId = TLogoBlobID(tablet, gen, step, channel, TLogoBlobID::MaxBlobSize, cookie - 1);
            else if (step > 0)
                vDiskData.nextLogoBlobId = TLogoBlobID(tablet, gen, step - 1, channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
            else if (gen > 0)
                vDiskData.nextLogoBlobId = TLogoBlobID(tablet, gen - 1, Max<ui32>(), channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
            else
                vDiskData.IsAllRead = true;
        }

        GroupResponseTracker.OnReply(vdisk, replyStatus);
        if (!StepDiscovery()) {
            return;
        }
        if (TotalRecieved >= TotalSent) {
            Sleep(TDuration::Seconds(1));
            TStringStream str;
            str << " logacc# ";
            LogCtx.LogAcc.Output(str);
            str << " VERIFY FAILED ";
            str << "Group# " << Info->GroupID << " Discover# " << SelfId().ToString();

            Y_ABORT_UNLESS(false, "%s", str.Str().data());
        }
        Y_ABORT_UNLESS(TotalRecieved < TotalSent);
    }

    bool StepDiscovery() {
        // We intentionally stop working with too many domain failures, but in theory we can continue working until
        // (ring.DomainReplies - ring.DomainSuccess >= Info->Type.MinimalRestorablePartCount())

        // Always reply with ERROR if the ring has disintegrated to an unreadable state
        if (GroupResponseTracker.IsUnreadableDisintegrated()) {
            R_LOG_ERROR_S("BSD08", "StepDiscovery Die. Disintegrated."
                << " DomainRequestsSent# " << (ui32)GroupResponseTracker.DomainRequestsSent
                << " DomainReplies# " << (ui32)GroupResponseTracker.DomainReplies
                << " DomainSuccess# " << (ui32)GroupResponseTracker.DomainSuccess
                << " ParityParts# " << (ui32)Info->Type.ParityParts()
                << " Handoff# " << (ui32)Info->Type.Handoff());
            TStringStream str;
            str << "Group# " << Info->GroupID << " disintegrated, type A.";
            ErrorReason = str.Str();
            ReplyAndDie(NKikimrProto::ERROR);
            return false;
        }

        // todo: handle vdisk stats

        // Feed more data when possible
        bool isAllRead = true;
        bool isAllDisksAbleToStep = true;
        bool isFirst = true;
        TLogoBlobID stepToId;

        for (TVDiskInfoContainer::iterator vDiskIt = VDiskInfo.begin(); vDiskIt != VDiskInfo.end(); ++vDiskIt) {
            TVDiskIdShort vId(vDiskIt->first);
            TVDiskInfo &curVDisk = vDiskIt->second;
            if (!curVDisk.IsError && curVDisk.Blobs.size() > 0) {
                TLogoBlobID &last = curVDisk.Blobs.back().Id;
                if (isFirst || stepToId < last) {
                    stepToId = TLogoBlobID(last, 0);
                    isFirst = false;
                }
            } else {
                if (!curVDisk.IsError && curVDisk.IsMoreRequested) {
                    isAllDisksAbleToStep = false;
                }
            }
            if (!curVDisk.IsError && !curVDisk.IsAllRead) {
                isAllRead = false;
            }
        }
        const ui32 totalPartCount = Info->Type.TotalPartCount();
        if (isAllDisksAbleToStep && !isFirst) {
            for (TVDiskInfoContainer::iterator vDiskIt = VDiskInfo.begin(); vDiskIt != VDiskInfo.end(); ++vDiskIt) {
                TVDiskIdShort vId(vDiskIt->first);
                TVDiskInfo &curVDisk = vDiskIt->second;
                if (!curVDisk.IsError) {
                    ui32 blobIdx;
                    for (blobIdx = 0; blobIdx < curVDisk.Blobs.size(); ++blobIdx) {
                        TLogoBlobID &id = curVDisk.Blobs[blobIdx].Id;
                        if (id >= stepToId || isAllRead) {
                            // Y_ABORT_UNLESS(id.PartId() == 0);
                            const TLogoBlobID fullid = id.FullID();
                            TVDiskID vDiskId(Info->CreateVDiskID(vId));
                            TIngress ingress = curVDisk.Blobs[blobIdx].Ingress;
                            TEntryInfo &entry = GroupResponseTracker.EntryInfo[fullid];
                            entry.RegisterBlob(ingress, Info.Get(), vDiskId, fullid);
                        } else {
                            break;
                        }
                    }
                    curVDisk.Blobs.erase(curVDisk.Blobs.begin(), curVDisk.Blobs.begin() + blobIdx);
                }
            }
        }

        ui32 unknownCount = GroupResponseTracker.DomainRequestsSent - GroupResponseTracker.DomainSuccess;
        ui32 minimalRestorableParts = Info->Type.MinimalRestorablePartCount();

        A_LOG_DEBUG_S("BSD09", "StepDiscovery"
            << " DomainRequestsSent# " << (ui32)GroupResponseTracker.DomainRequestsSent
            << " DomainReplies# " << (ui32)GroupResponseTracker.DomainReplies
            << " DomainSuccess# " << (ui32)GroupResponseTracker.DomainSuccess
            << " ParityParts# " << (ui32)Info->Type.ParityParts()
            << " Handoff# " << (ui32)Info->Type.Handoff()
            << " isAllRead# " << isAllRead
            << " isAllDisksAbleToStep# " << isAllDisksAbleToStep
            << " isFirst# " << isFirst
            << " unknownCount# " << unknownCount
            << " minimalRestorableParts# " << minimalRestorableParts
            << " totalPartCount# " << totalPartCount);
        if (unknownCount <= Info->Type.ParityParts()) {
            const ui32 subgroupSize = Info->Type.BlobSubgroupSize();
            for (TEntryInfos::const_iterator it = GroupResponseTracker.EntryInfo.begin();
                    it != GroupResponseTracker.EntryInfo.end(); ++it) {
                const TEntryInfo &entryInfo = it->second;
                const TLogoBlobID &logoBlobId = it->first;
                ui32 seenPartCount = TSubgroupPartLayout::CountEffectiveReplicas(entryInfo.Seen, Info->Type);
                ui32 partCount = TSubgroupPartLayout::CountEffectiveReplicas(entryInfo.Present, Info->Type);
                A_LOG_DEBUG_S("BSD27", "logoBlobId# " << logoBlobId.ToString()
                        << " partCount# " << partCount
                        << " seenPartCount# " << seenPartCount);

                ui32 errorDomains = 0;
                if (partCount >= minimalRestorableParts) {
                    TBlobStorageGroupInfo::TServiceIds vDisksSvc;
                    TBlobStorageGroupInfo::TVDiskIds vDisksId;
                    Info->PickSubgroup(logoBlobId.Hash(), &vDisksId, &vDisksSvc);
                    for (ui32 idx = 0; idx < subgroupSize; ++idx) {
                        const TVDiskID &id = vDisksId[idx];
                        if (VDiskInfo.at(TVDiskIdShort(id).GetRaw()).IsError) {
                            errorDomains++;
                        }
                    }
                }

                TBlobStorageGroupInfo::EBlobState blobState = Info->BlobState(partCount, errorDomains);

                //if ((partCount >= minimalRestorableParts) && (partCount + errorDomains >= totalPartCount)) {
                if (blobState & (TBlobStorageGroupInfo::EBSF_RECOVERABLE | TBlobStorageGroupInfo::EBSF_FULL)) {
                    // (?) do via proxy?
                    IsIterativeDone = true;
                    if (ReadBody) {
                        std::unique_ptr<TEvBlobStorage::TEvGet> getRequest(new TEvBlobStorage::TEvGet(
                                logoBlobId, 0, 0, Deadline, NKikimrBlobStorage::EGetHandleClass::Discover));
                        getRequest->MustRestoreFirst = true;
                        getRequest->IsVerboseNoDataEnabled = true;
                        getRequest->IsInternal = true;
                        getRequest->TabletId = TabletId;
                        getRequest->AcquireBlockedGeneration = true;
                        SendToProxy(std::move(getRequest), 0, Span.GetTraceId());
                        TotalSent++;

                        A_LOG_DEBUG_S("BSD10", "Sent EvGet logoBlobId# " << logoBlobId.ToString());

                        Become(&TThis::StateWait);
                        return true;
                    } else if (IsGetBlockDone) {
                        std::unique_ptr<TEvBlobStorage::TEvDiscoverResult> result(
                            new TEvBlobStorage::TEvDiscoverResult(logoBlobId, MinGeneration, TString(), BlockedGen));
                        A_LOG_LOG_S(true, PriorityForStatusOutbound(result->Status), "BSD11", "Die. Result# "
                                << result->Print(false));
                        SendResult(result);
                        return false;
                    } else {
                        PendingResult.reset(new TEvBlobStorage::TEvDiscoverResult(
                                    logoBlobId, MinGeneration, TString(), BlockedGen));
                        A_LOG_DEBUG_S("BSD12", "Pending result is set, Result# " << PendingResult->ToString());
                        Become(&TThis::StateWait);
                        return false;
                    }
                } else if (blobState & TBlobStorageGroupInfo::EBSF_DISINTEGRATED) {
                    // Reply with error.
                    R_LOG_ERROR_S("BSD30", "StepDiscovery Die."
                        << " Reply with ERROR! "
                        << " logoBlobId# " << logoBlobId
                        << " seenPartCount# " << seenPartCount
                        << " partCount# " << partCount
                        << " unknownCount# " << unknownCount
                        << " minimalRestorableParts# " << minimalRestorableParts
                        << " errorDomains# " << errorDomains
                        << " totalPartCount# " << totalPartCount);
                    TStringStream str;
                    str << "Group# " << Info->GroupID << " disintegrated, type B.";
                    ErrorReason = str.Str();
                    ReplyAndDie(NKikimrProto::ERROR);
                    return false;
                }
                if (partCount + unknownCount >= totalPartCount) {
                    // Unknown parts may decide the fate of the data.
                    // TODO: consider only the nodes from the logoblobs subgroup here, so that fail domains out of this
                    // subgroup, don't trigger false expectations and we successfully detect fantom blob here.
                    // TIMEOUT also may mean that no definite answer can be obtained
                    return true;
                }
                if (seenPartCount == totalPartCount) {
                    // A valid set of parts was seen, so, the blob must be considered present.
                    // We must restore the blob.
                    // Since we got here, restoring the lob is impossible and we are about to move on to the next one.
                    // Reply with error to celebrate the impossible state we are in.
                    TStringStream str;
                    str << "It's currently impossible to restore the blob, logoBlobId# " << logoBlobId
                        << " seenPartCount# " << seenPartCount
                        << " partCount# " << partCount
                        << " unknownCount# " << unknownCount
                        << " minimalRestorableParts# " << minimalRestorableParts
                        << " errorDomains# " << errorDomains
                        << " totalPartCount# " << totalPartCount;
                    ErrorReason = str.Str();
                    R_LOG_ERROR_S("BSD28", "StepDiscovery Die. Reply with ERROR! " << ErrorReason);
                    ReplyAndDie(NKikimrProto::ERROR);
                    return false;
                }
                // There is not enough data to restore the blob, it must be a fantom
            }

            if (isAllRead) {
                // We are sure that we got nothing to reprot,
                if (IsGetBlockDone) {
                    R_LOG_INFO_S("BSD13", "isAllRead and IsGetBlockDone, but nothing to report, respond with NODATA Die.");
                    ReplyAndDie(NKikimrProto::NODATA);
                    return false;
                }
                IsGetDataDone = true;
                IsIterativeDone = true;
                PendingResult.reset(new TEvBlobStorage::TEvDiscoverResult(NKikimrProto::NODATA, MinGeneration,
                            BlockedGen));
                A_LOG_DEBUG_S("BSD14", "isAllRead, setting pending result, response# " << PendingResult->ToString());
                return true;
            }
            // Nothing found so far. Clear the buffer and continue.
            // TODO: discard any data before the 'clearing' threshold (from slowpoke vdisks)
            // TODO: don't count the data from the 'bad' domains
            GroupResponseTracker.EntryInfo.clear();

            GroupResponseTracker.CurrentRequestSize = Min(MaxRequestSize, GroupResponseTracker.CurrentRequestSize * 2);
        }

        if (isAllDisksAbleToStep) {
            // Request more data
            const TLogoBlobID to = TLogoBlobID(TabletId, MinGeneration, 0, 0, 0, 0, 1);
            for (TVDiskInfoContainer::iterator vDiskIt = VDiskInfo.begin(); vDiskIt != VDiskInfo.end(); ++vDiskIt) {
                TVDiskIdShort vId(vDiskIt->first);
                TVDiskInfo &curVDisk = vDiskIt->second;
                TVDiskID vDiskId = Info->CreateVDiskID(vId);

                if (!curVDisk.IsError && !curVDisk.IsAllRead && !curVDisk.IsMoreRequested) {
                    const TActorId &vdisk = Info->GetActorId(vDiskId);

                    auto msg = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(vDiskId, Deadline,
                            NKikimrBlobStorage::EGetHandleClass::Discover, TEvBlobStorage::TEvVGet::EFlags::ShowInternals,
                            {}, curVDisk.nextLogoBlobId, to, GroupResponseTracker.CurrentRequestSize, nullptr,
                            TEvBlobStorage::TEvVGet::TForceBlockTabletData(TabletId, ForceBlockedGeneration));
                    msg->Record.SetSuppressBarrierCheck(true);
                    const ui64 cookie = TVDiskIdShort(vDiskId).GetRaw();
                    A_LOG_DEBUG_S("BSD15", "Request more data sending TEvVGet Tablet# " << TabletId
                        << " vDiskId# " << vDiskId.ToString()
                        << " node# " << vdisk.NodeId()
                        << " msg# " << msg->ToString()
                        << " cookie# " << cookie);
                    CountEvent(*msg);
                    SendToQueue(std::move(msg), cookie);
                    TotalSent++;

                    curVDisk.IsMoreRequested = true;
                    curVDisk.LastRequestSize = GroupResponseTracker.CurrentRequestSize;

                    // Subtract disk's answer from answer counters/reset ready flags
                    GroupResponseTracker.AnotherRequest(vDiskId);
                } else {
                    A_LOG_DEBUG_S("BSD26", "do not ask more data from vDiskId# " << vDiskId.ToString()
                        << " IsError# " << curVDisk.IsError
                        << " IsAllRead# " << curVDisk.IsAllRead
                        << " IsMoreRequested# " << curVDisk.IsMoreRequested
                        << " BlobsSize# " << curVDisk.Blobs.size());
                }
            }
        }
        return true;
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr &ev) {
        TotalRecieved++;
        TEvBlobStorage::TEvGetResult *msg = ev->Get();
        const NKikimrProto::EReplyStatus status = msg->Status;

        switch (status) {
        case NKikimrProto::OK:
            {
                Y_ABORT_UNLESS(msg->ResponseSz == 1);
                TEvBlobStorage::TEvGetResult::TResponse &response = msg->Responses[0];
                switch (response.Status) {
                case NKikimrProto::OK:
                    IsGetDataDone = true;
                    if (IsGetBlockDone) {
                        std::unique_ptr<TEvBlobStorage::TEvDiscoverResult> result(
                            new TEvBlobStorage::TEvDiscoverResult(
                                response.Id, MinGeneration, response.Buffer.ConvertToString(), BlockedGen));
                        A_LOG_DEBUG_S("BSD16", "Handle TEvGetResult status# OK Die. TEvDiscoverResult# "
                            << result->Print(false));
                        SendResult(result);
                        return;
                    }
                    PendingResult.reset(new TEvBlobStorage::TEvDiscoverResult(response.Id, MinGeneration,
                                response.Buffer.ConvertToString(), BlockedGen));
                    A_LOG_DEBUG_S("BSD17", "Handle TEvGetResult status# OK"
                        << " Setting pending result# " << PendingResult->ToString());
                    Y_ABORT_UNLESS(TotalRecieved < TotalSent);
                    return;
                case NKikimrProto::NODATA: {
                    if ((VGetBlockedGen != msg->BlockedGeneration) || (IsGetBlockDone && BlockedGen &&
                                (BlockedGen != msg->BlockedGeneration || BlockedGen != VGetBlockedGen))) {
                        // Blocked generation has changed in the middle of the discovery process
                        TStringStream str;
                        str << "TEvGetResult status# " << NKikimrProto::EReplyStatus_Name(status)
                            << " Group# " << Info->GroupID
                            << " for tablet# " << TabletId
                            << " BlockedGen# " << BlockedGen
                            << " GetBlockReplies# " << GetBlockReplies
                            << " GetBlockErrors# " << GetBlockErrors
                            << " VGetBlockedGen# " << VGetBlockedGen
                            << " Get.BlockedGeneration# " << msg->BlockedGeneration
                            << " response status# NODATA, Reply with ERROR!"
                            << " There might be another instance of the tablet trying to block the channel right now."
                            << " Can't read the blob id# " << response.Id.ToString();
                        ErrorReason = str.Str();
                        A_LOG_DEBUG_S("BSD32", "Handle " << ErrorReason << " Reply with ERROR");
                        ReplyAndDie(NKikimrProto::ERROR);
                        return;
                    }

                    TStringStream str;
                    str << "Handle TEvGetResult status# " << NKikimrProto::EReplyStatus_Name(status)
                        << " Group# " << Info->GroupID
                        << " for tablet# " << TabletId
                        << " BlockedGen# " << BlockedGen
                        << " GetBlockReplies# " << GetBlockReplies
                        << " GetBlockErrors# " << GetBlockErrors
                        << " VGetBlockedGen# " << VGetBlockedGen
                        << " Get.BlockedGeneration# " << msg->BlockedGeneration
                        << " FromLeader# " << (FromLeader ? "true" : "false")
                        << " response status# NODATA, Reply with ERROR! "
                        << " looks like we have !!! LOST THE BLOB !!! id# " << response.Id.ToString();

                    R_LOG_ALERT_S("BSD18", str.Str());

                    if (FromLeader) {
                        Sleep(TDuration::Seconds(1));

                        str << " logacc# ";
                        LogCtx.LogAcc.Output(str);
                        str << " verboseNoData# ";
                        str << msg->DebugInfo;

                        Y_ABORT_UNLESS(false, "%s", str.Str().data());
                    }

                    IsGetDataDone = true;
                    if (IsGetBlockDone) {
                        R_LOG_ERROR_S("BSD19", "Handle TEvGetResult Die. status# "
                            << NKikimrProto::EReplyStatus_Name(status)
                            << " Group# " << Info->GroupID
                            << " for tablet# " << TabletId << " response status# NODATA, Reply with ERROR!");
                        ReplyAndDie(NKikimrProto::ERROR);
                        return;
                    }
                    PendingResult.reset(new TEvBlobStorage::TEvDiscoverResult(NKikimrProto::ERROR, MinGeneration,
                                BlockedGen));
                    R_LOG_ERROR_S("BSD20", "Handle TEvGetResult status# " << NKikimrProto::EReplyStatus_Name(status)
                        << " for tablet# " << TabletId << " response status# NODATA, set PendingResult to ERROR!");
                    Y_ABORT_UNLESS(TotalRecieved < TotalSent);
                    ReplyAndDie(NKikimrProto::ERROR);
                    return;
                }
                default: {
                        R_LOG_ERROR_S("BSD21", "Handle TEvGetResult Die. status# "
                                << NKikimrProto::EReplyStatus_Name(status)
                                << " for tablet# " << TabletId
                                << " Unexpected response status# "
                                << NKikimrProto::EReplyStatus_Name(response.Status));

                        TStringStream str;
                        str << " Unexpected get response status# "
                            << NKikimrProto::EReplyStatus_Name(response.Status)
                            << " " << msg->ToString();
                        ErrorReason = str.Str();
                        ReplyAndDie(response.Status);
                    }
                    return;
                }
            }
            Y_ABORT_UNLESS(TotalRecieved < TotalSent);
            return;
        default: {
                R_LOG_ERROR_S("BSD23", "Handle TEvGetResult unexpected status# " << NKikimrProto::EReplyStatus_Name(status)
                    << " Die. for tablet# " << TabletId);
                TStringStream str;
                str << "Unexpected EvGetResult status# " << NKikimrProto::EReplyStatus_Name(status)
                    << " " << msg->ToString();
                ErrorReason = str.Str();
                ReplyAndDie(NKikimrProto::ERROR);
            }
            return;
        }
        Y_ABORT_UNLESS(TotalRecieved < TotalSent);
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) {
        ++*Mon->NodeMon->RestartDiscover;
        auto ev = std::make_unique<TEvBlobStorage::TEvDiscover>(TabletId, MinGeneration, ReadBody, DiscoverBlockedGeneration,
            Deadline, ForceBlockedGeneration, FromLeader);
        ev->RestartCounter = counter;
        return ev;
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_GROUP_DISCOVER;
    }

    static const auto& ActiveCounter(const TIntrusivePtr<TBlobStorageGroupProxyMon>& mon) {
        return mon->ActiveDiscover;
    }

    static constexpr ERequestType RequestType() {
        return ERequestType::Discover;
    }

    TBlobStorageGroupDiscoverRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
            const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
            const TIntrusivePtr<TBlobStorageGroupProxyMon> mon, TEvBlobStorage::TEvDiscover *ev,
            ui64 cookie, NWilson::TTraceId traceId, TInstant now,
            TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters)
        : TBlobStorageGroupRequestActor(info, state, mon, source, cookie,
                NKikimrServices::BS_PROXY_DISCOVER, true, {}, now, storagePoolCounters, ev->RestartCounter,
                NWilson::TSpan(TWilson::BlobStorage, std::move(traceId), "DSProxy.Discover"),
                std::move(ev->ExecutionRelay))
        , TabletId(ev->TabletId)
        , MinGeneration(ev->MinGeneration)
        , ReadBody(ev->ReadBody)
        , DiscoverBlockedGeneration(ev->DiscoverBlockedGeneration)
        , Deadline(ev->Deadline)
        , StartTime(now)
        , GroupResponseTracker(Info)
        , IsGetBlockDone(!DiscoverBlockedGeneration)
        , ForceBlockedGeneration(ev->ForceBlockedGeneration)
        , FromLeader(ev->FromLeader)
    {}

    void Bootstrap() {
        A_LOG_INFO_S("BSD31", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " TabletId# " << TabletId
            << " MinGeneration# " << MinGeneration
            << " ReadBody# " << (ReadBody ? "true" : "false")
            << " Deadline# " << Deadline
            << " ForceBlockedGeneration# " << ForceBlockedGeneration
            << " FromLeader# " << (FromLeader ? "true" : "false")
            << " RestartCounter# " << RestartCounter);

        const TLogoBlobID from = TLogoBlobID(TabletId, Max<ui32>(), Max<ui32>(), 0,
            TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxChannel, TLogoBlobID::MaxPartId);
        const TLogoBlobID to = TLogoBlobID(TabletId, MinGeneration, 0, 0, 0, 0, 1);

        for (const auto& vdisk : Info->GetVDisks()) {
            auto vd = Info->GetVDiskId(vdisk.OrderNumber);
            if (!IsGetBlockDone) {
                const ui64 cookie = TVDiskIdShort(vd).GetRaw();
                auto getBlock = std::make_unique<TEvBlobStorage::TEvVGetBlock>(TabletId, vd, Deadline);
                A_LOG_DEBUG_S("BSD24", "Sending TEvVGetBlock Tablet# " << TabletId
                    << " vDiskId# " << vd
                    << " cookie# " << cookie
                    << " node# " << Info->GetActorId(vd).NodeId());
                SendToQueue(std::move(getBlock), cookie);
                TotalSent++;
            }

            auto msg = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(vd, Deadline,
                NKikimrBlobStorage::EGetHandleClass::Discover, TEvBlobStorage::TEvVGet::EFlags::ShowInternals,
                {}, from, to, GroupResponseTracker.CurrentRequestSize, nullptr, TEvBlobStorage::TEvVGet::TForceBlockTabletData(TabletId, ForceBlockedGeneration));
            msg->Record.SetTabletId(TabletId);
            msg->Record.SetAcquireBlockedGeneration(true);
            const ui64 cookie = TVDiskIdShort(vd).GetRaw();
            A_LOG_DEBUG_S("BSD25", "Sending TEvVGet Tablet# " << TabletId
                << " vDiskId# " << vd
                << " node# " << Info->GetActorId(vd).NodeId()
                << " msg# " << msg->ToString()
                << " cookie# " << cookie
                << " ForceBlockedGeneration# " << msg->Record.GetForceBlockedGeneration());
            CountEvent(*msg);
            SendToQueue(std::move(msg), cookie);
            TotalSent++;

            TVDiskInfo &curVDisk = VDiskInfo[TVDiskIdShort(vd).GetRaw()];
            curVDisk.IsMoreRequested = true;
            curVDisk.LastRequestSize = GroupResponseTracker.CurrentRequestSize;
            curVDisk.nextLogoBlobId = from;
        }

        Become(&TThis::StateInit);
    }

    STATEFN(StateInit) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVGetBlockResult, Handle);
            hFunc(TEvBlobStorage::TEvVGetResult, Handle);
        }
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVGetBlockResult, Handle);
            hFunc(TEvBlobStorage::TEvVGetResult, HandleIgnore);
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupDiscoverRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
        const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
        const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvDiscover *ev,
        ui64 cookie, NWilson::TTraceId traceId, TInstant now,
        TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters) {
    return new TBlobStorageGroupDiscoverRequest(info, state, source, mon, ev, cookie, std::move(traceId), now,
            storagePoolCounters);
}

}//NKikimr
