#include "ddisk_actor.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

namespace {

    const TVector<double> NvmeLatencyHistBoundsMs = {
        0.01, 0.02, 0.03, 0.04, 0.05,
        0.1, 0.25, 0.5, 0.75,
        1, 2, 4, 8, 32, 128,
        1'024,
        65'536
    };

    const TVector<double> RequestSizeBoundsKiB = {
        4, 8, 16, 32, 64, 128, 256, 512,
        1024, 2048, 4096,
        1048576,
    };

    constexpr ui32 ZeroBlockSize = 4096;
    static const TString ZeroBlock(ZeroBlockSize, '\0');

    // Returns a rope of `size` zero bytes built from refcounted references to a
    // single shared 4 KiB block. `size` must be a multiple of ZeroBlockSize.
    TRope MakeZeroRope(ui32 size) {
        Y_DEBUG_ABORT_UNLESS(size % ZeroBlockSize == 0,
            "size# %" PRIu32 " must be aligned to %" PRIu32, size, ZeroBlockSize);
        
        TRope result;
        const ui32 blockCount = size / ZeroBlockSize;
        for (ui32 i = 0; i < blockCount; ++i) {
            result.Insert(result.End(), TRope(ZeroBlock));
        }
        return result;
    }

} // anonymous

class TDDiskActorInMem : public TActorBootstrapped<TDDiskActorInMem> {
private:
    struct TInterfaceOpCounters {
        NMonitoring::TDynamicCounters::TCounterPtr Requests;
        NMonitoring::TDynamicCounters::TCounterPtr Bytes;
        NMonitoring::TDynamicCounters::TCounterPtr BytesInFlight;
        NMonitoring::THistogramPtr RequestSizeKiB;
        NMonitoring::THistogramPtr ResponseTime;
        NMonitoring::TDynamicCounters::TCounterPtr ReplyOk;
        NMonitoring::TDynamicCounters::TCounterPtr ReplyErr;

        void Request(ui32 bytes = 0) {
            ++*Requests;
            if (bytes) {
                *Bytes += bytes;
                *BytesInFlight += bytes;
                RequestSizeKiB->Collect(bytes >> 10);
            }
        }

        void Reply(bool ok, ui32 bytes = 0, double durationMs = 0) {
            ++*(ok ? ReplyOk : ReplyErr);
            if (bytes) {
                *BytesInFlight -= bytes;
            }
            if (durationMs != 0) {
                ResponseTime->Collect(durationMs);
            }
        }
    };

    struct TCounters {
        struct {
#define DECLARE_COUNTERS_INTERFACE(NAME) \
            TInterfaceOpCounters NAME;

            LIST_COUNTERS_INTERFACE_OPS(DECLARE_COUNTERS_INTERFACE)

#undef DECLARE_COUNTERS_INTERFACE
        } Interface;
    };

    TVDiskConfig::TBaseInfo BaseInfo;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TPersistentBufferFormat PersistentBufferFormat;
    TDDiskConfig Config;
    TIntrusivePtr<NMonitoring::TDynamicCounters> CountersBase;
    std::vector<std::pair<TString, TString>> CountersChain;
    TCounters Counters;
    bool IsPersistentBufferActor;
    TString DDiskId;
    ui64 DDiskInstanceGuid;

    TActorId PersistentBufferActorId;

    ui64 NextCookie = 1;

    struct TWritePersistentBuffersInflight {
        TActorId Sender;
        ui64 Cookie;
        ui32 Size;
        i64 StartTs;
        ui32 PendingCount;
        std::unique_ptr<TEvWritePersistentBuffersResult> Response;
        // partCookie -> result index in Response
        THashMap<ui64, ui32> PartCookieToIndex;
    };

    struct TSyncWithPersistentBufferInflight {
        TActorId Sender;
        ui64 Cookie;
        i64 StartTs;
        ui32 PendingCount;
        std::unique_ptr<TEvSyncWithPersistentBufferResult> Response;
        // partCookie -> segment index in Response
        THashMap<ui64, ui32> PartCookieToIndex;
    };

    struct TSyncWithDDiskInflight {
        TActorId Sender;
        ui64 Cookie;
        i64 StartTs;
        ui32 PendingCount;
        std::unique_ptr<TEvSyncWithDDiskResult> Response;
        // partCookie -> segment index in Response
        THashMap<ui64, ui32> PartCookieToIndex;
    };

    struct TReadThenWriteInflight {
        TActorId Sender;
        ui64 Cookie;
        i64 StartTs;
        ui64 Lsn;
        ui32 Generation;
        ui32 ReplyTimeoutMicroseconds;
        TQueryCredentials Creds;
        std::vector<NKikimrBlobStorage::NDDisk::TDDiskId> PersistentBufferIds;
    };

    THashMap<ui64, TWritePersistentBuffersInflight> WritePbsInflight;
    THashMap<ui64, ui64> WritePbsPartCookieToOp;

    THashMap<ui64, TSyncWithPersistentBufferInflight> SyncWithPbInflight;
    THashMap<ui64, ui64> SyncPartCookieToOp;

    THashMap<ui64, TSyncWithDDiskInflight> SyncWithDDiskInflightMap;
    THashMap<ui64, ui64> SyncDDiskPartCookieToOp;

    // readCookie -> read-then-write inflight info
    THashMap<ui64, TReadThenWriteInflight> ReadThenWriteInflightMap;

public:
    TDDiskActorInMem(
        TVDiskConfig::TBaseInfo&& baseInfo,
        TIntrusivePtr<TBlobStorageGroupInfo> info,
        TPersistentBufferFormat&& pbFormat,
        TDDiskConfig&& ddiskConfig,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
        bool isPersistentBufferActor)
        : BaseInfo(std::move(baseInfo))
        , Info(std::move(info))
        , PersistentBufferFormat(std::move(pbFormat))
        , Config(std::move(ddiskConfig))
        , CountersBase(GetServiceCounters(counters, "ddisks"))
        , IsPersistentBufferActor(isPersistentBufferActor)
    {
        DDiskId = TStringBuilder() << '[' << BaseInfo.PDiskActorID.NodeId() << ':' << BaseInfo.PDiskId
            << ':' << BaseInfo.VDiskSlotId << ']';
        DDiskInstanceGuid = RandomNumber<ui64>();

        InitCounters();
    }

    void InitCounters() {
        TVector<double> latencyHistBounds = NvmeLatencyHistBoundsMs;

        CountersChain.emplace_back("ddiskPool", BaseInfo.StoragePoolName);
        CountersChain.emplace_back("group", Sprintf("%09" PRIu32, Info->GroupID));
        CountersChain.emplace_back("orderNumber", Sprintf("%02" PRIu32, Info->GetOrderNumber(BaseInfo.VDiskIdShort)));
        CountersChain.emplace_back("pdisk", Sprintf("%09" PRIu32, BaseInfo.PDiskId));
        CountersChain.emplace_back("media", to_lower(NPDisk::DeviceTypeStr(BaseInfo.DeviceType, true)));

        auto counters = CountersBase;
        for (const auto& [name, value] : CountersChain) {
            counters = counters->GetSubgroup(name, value);
        }

        auto cInterface = counters->GetSubgroup("subsystem", "interface");

#define XX(NAME) \
        { \
            auto sg = cInterface->GetSubgroup("operation", #NAME); \
            Counters.Interface.NAME.Requests = sg->GetCounter("Requests", true); \
            Counters.Interface.NAME.ReplyOk = sg->GetCounter("ReplyOk", true); \
            Counters.Interface.NAME.ReplyErr = sg->GetCounter("ReplyErr", true); \
            Counters.Interface.NAME.Bytes = sg->GetCounter("Bytes", true); \
            Counters.Interface.NAME.BytesInFlight = sg->GetCounter("BytesInFlight", false); \
            Counters.Interface.NAME.RequestSizeKiB = sg->GetHistogram("RequestSizeKiB", \
                NMonitoring::ExplicitHistogram(RequestSizeBoundsKiB)); \
            Counters.Interface.NAME.ResponseTime = sg->GetHistogram("ResponseTime", \
                NMonitoring::ExplicitHistogram(latencyHistBounds)); \
        }
        LIST_COUNTERS_INTERFACE_OPS(XX)
#undef XX
    }

    void Bootstrap() {
        STLOG(PRI_DEBUG, BS_DDISK, BSDD09, "TDDiskActorInMem::Bootstrap (in-memory)", (DDiskId, DDiskId), (IsPersistentBufferActor, IsPersistentBufferActor));
        
        if (IsPersistentBufferActor) {
            Become(&TThis::StateFuncPersistentBuffer);
        } else {
            Become(&TThis::StateFuncDDisk);
            CreatePersistentBuffer();
        }
    }

    void CreatePersistentBuffer() {
        auto pbActor = std::make_unique<TDDiskActorInMem>(
            TVDiskConfig::TBaseInfo(BaseInfo),
            Info,
            TPersistentBufferFormat(PersistentBufferFormat),
            TDDiskConfig(Config),
            CountersBase,
            true /*isPersistentBufferActor*/);
        auto *as = TActivationContext::ActorSystem();
        PersistentBufferActorId = as->Register(pbActor.release(), TMailboxType::Revolving, AppData()->SystemPoolId);
        auto pbServiceId = MakeBlobStoragePersistentBufferId(BaseInfo.PDiskActorID.NodeId(), BaseInfo.PDiskId, BaseInfo.VDiskSlotId);
        as->RegisterLocalService(pbServiceId, PersistentBufferActorId);
        STLOG(PRI_DEBUG, BS_DDISK, BSDD03, "TDDiskActorInMem::CreatePersistentBuffer()", 
            (DDiskId, DDiskId), (pbServiceId, pbServiceId), (PersistentBufferActorId, PersistentBufferActorId));
    }

    STFUNC(StateFuncDDisk) {
        STRICT_STFUNC_BODY(
            hFunc(TEvConnect, Handle)
            hFunc(TEvDisconnect, Handle)
            hFunc(TEvWrite, Handle)
            hFunc(TEvRead, Handle)
            hFunc(TEvSyncWithPersistentBuffer, Handle)
            hFunc(TEvSyncWithDDisk, Handle)
            hFunc(TEvReadPersistentBufferResult, Handle)
            hFunc(TEvReadResult, Handle)
            hFunc(TEvents::TEvUndelivered, Handle)

            IgnoreFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate)
            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    }

    STFUNC(StateFuncPersistentBuffer) {
        STRICT_STFUNC_BODY(
            hFunc(TEvConnect, Handle)
            hFunc(TEvDisconnect, Handle)
            hFunc(TEvWritePersistentBuffer, Handle)
            hFunc(TEvWritePersistentBuffers, Handle)
            hFunc(TEvWritePersistentBufferResult, Handle)
            hFunc(TEvReadThenWritePersistentBuffers, Handle)
            hFunc(TEvReadPersistentBuffer, Handle)
            hFunc(TEvReadPersistentBufferResult, Handle)
            hFunc(TEvErasePersistentBuffer, Handle)
            hFunc(TEvBatchErasePersistentBuffer, Handle)
            hFunc(TEvListPersistentBuffer, Handle)
            hFunc(TEvGetPersistentBufferInfo, Handle)
            hFunc(TEvents::TEvUndelivered, Handle)

            IgnoreFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate)
            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    }

    void Handle(TEvConnect::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto& creds = record.GetCredentials();
        
        ui64 tabletId = creds.GetTabletId();
        ui32 generation = creds.GetGeneration();
        
        STLOG(PRI_DEBUG, BS_DDISK, BSDD11, "TDDiskActorInMem::Handle(TEvConnect)",
            (DDiskId, DDiskId),
            (TabletId, tabletId),
            (Generation, generation));
        
        Send(ev->Sender, new TEvConnectResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK,
            std::nullopt,
            DDiskInstanceGuid),
            0, ev->Cookie);
    }

    void Handle(TEvDisconnect::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_DDISK, BSDD12, "TDDiskActorInMem::Handle(TEvDisconnect)", (DDiskId, DDiskId));
        
        Send(ev->Sender, new TEvDisconnectResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK),
            0, ev->Cookie);
    }

    void Handle(TEvWrite::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const TBlockSelector selector(record.GetSelector());

        STLOG(PRI_DEBUG, BS_DDISK, BSDD13, "TDDiskActorInMem::Handle(TEvWrite)",
            (DDiskId, DDiskId), (Size, selector.Size));

        const auto startTs = HPNow();
        Counters.Interface.Write.Request(selector.Size);

        Send(ev->Sender, new TEvWriteResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK),
            0, ev->Cookie);

        Counters.Interface.Write.Reply(true, selector.Size, HPMilliSecondsFloat(HPNow() - startTs));
    }

    void Handle(TEvRead::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto& selector = record.GetSelector();
        ui32 size = selector.GetSize();
        
        STLOG(PRI_DEBUG, BS_DDISK, BSDD14, "TDDiskActorInMem::Handle(TEvRead)", 
            (DDiskId, DDiskId), (Size, size));

        const auto startTs = HPNow();
        Counters.Interface.Read.Request(size);

        Send(ev->Sender, new TEvReadResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK,
            std::nullopt,
            MakeZeroRope(size)),
            0, ev->Cookie);

        Counters.Interface.Read.Reply(true, size, HPMilliSecondsFloat(HPNow() - startTs));
    }

    void Handle(TEvSyncWithPersistentBuffer::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_DDISK, BSDD15, "TDDiskActorInMem::Handle(TEvSyncWithPersistentBuffer)", (DDiskId, DDiskId));

        const auto startTs = HPNow();
        Counters.Interface.SyncWithPersistentBuffer.Request(0);

        const auto& record = ev->Get()->Record;

        if (!record.SegmentsSize()) {
            auto response = std::make_unique<TEvSyncWithPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST, "segments must be non-empty");
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            Counters.Interface.SyncWithPersistentBuffer.Reply(false, 0, HPMilliSecondsFloat(HPNow() - startTs));
            return;
        }

        if (!record.HasDDiskId()) {
            auto response = std::make_unique<TEvSyncWithPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST, "source ddisk id must be set");
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            Counters.Interface.SyncWithPersistentBuffer.Reply(false, 0, HPMilliSecondsFloat(HPNow() - startTs));
            return;
        }

        const TQueryCredentials creds(record.GetCredentials());
        const TQueryCredentials sourceCreds(creds.TabletId, creds.Generation,
            record.GetDDiskInstanceGuid(), true);
        const auto& ddiskId = record.GetDDiskId();
        const TActorId sourcePbId = MakeBlobStoragePersistentBufferId(
            ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId());

        const ui64 opCookie = NextCookie++;
        auto& inflight = SyncWithPbInflight[opCookie];
        inflight.Sender = ev->Sender;
        inflight.Cookie = ev->Cookie;
        inflight.StartTs = startTs;
        inflight.PendingCount = record.SegmentsSize();
        inflight.Response = std::make_unique<TEvSyncWithPersistentBufferResult>(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        for (size_t i = 0; i < record.SegmentsSize(); ++i) {
            inflight.Response->AddSegmentResult(NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN, "");
        }

        for (size_t i = 0; i < record.SegmentsSize(); ++i) {
            const auto& segment = record.GetSegments(i);
            const TBlockSelector selector(segment.GetSelector());
            const ui64 partCookie = NextCookie++;
            inflight.PartCookieToIndex[partCookie] = i;
            SyncPartCookieToOp[partCookie] = opCookie;

            auto readEv = std::make_unique<TEvReadPersistentBuffer>(
                sourceCreds, selector, segment.GetLsn(), segment.GetGeneration(), TReadInstruction(true));
            Send(sourcePbId, readEv.release(), IEventHandle::FlagTrackDelivery, partCookie);
        }
    }

    void Handle(TEvSyncWithDDisk::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_DDISK, BSDD16, "TDDiskActorInMem::Handle(TEvSyncWithDDisk)", (DDiskId, DDiskId));

        const auto startTs = HPNow();
        Counters.Interface.SyncWithDDisk.Request(0);

        const auto& record = ev->Get()->Record;

        if (!record.SegmentsSize()) {
            auto response = std::make_unique<TEvSyncWithDDiskResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST, "segments must be non-empty");
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            Counters.Interface.SyncWithDDisk.Reply(false, 0, HPMilliSecondsFloat(HPNow() - startTs));
            return;
        }

        if (!record.HasDDiskId()) {
            auto response = std::make_unique<TEvSyncWithDDiskResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST, "source ddisk id must be set");
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            Counters.Interface.SyncWithDDisk.Reply(false, 0, HPMilliSecondsFloat(HPNow() - startTs));
            return;
        }

        const TQueryCredentials creds(record.GetCredentials());
        const TQueryCredentials sourceCreds(creds.TabletId, creds.Generation,
            record.GetDDiskInstanceGuid(), true);
        const auto& ddiskId = record.GetDDiskId();
        const TActorId sourceDDiskId = MakeBlobStorageDDiskId(
            ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId());

        const ui64 opCookie = NextCookie++;
        auto& inflight = SyncWithDDiskInflightMap[opCookie];
        inflight.Sender = ev->Sender;
        inflight.Cookie = ev->Cookie;
        inflight.StartTs = startTs;
        inflight.PendingCount = record.SegmentsSize();
        inflight.Response = std::make_unique<TEvSyncWithDDiskResult>(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        for (size_t i = 0; i < record.SegmentsSize(); ++i) {
            inflight.Response->AddSegmentResult(NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN, "");
        }

        for (size_t i = 0; i < record.SegmentsSize(); ++i) {
            const auto& segment = record.GetSegments(i);
            const TBlockSelector selector(segment.GetSelector());
            const ui64 partCookie = NextCookie++;
            inflight.PartCookieToIndex[partCookie] = i;
            SyncDDiskPartCookieToOp[partCookie] = opCookie;

            auto readEv = std::make_unique<TEvRead>(sourceCreds, selector, TReadInstruction(true));
            Send(sourceDDiskId, readEv.release(), IEventHandle::FlagTrackDelivery, partCookie);
        }
    }

    void Handle(TEvReadResult::TPtr& ev) {
        const ui64 partCookie = ev->Cookie;
        auto itPart = SyncDDiskPartCookieToOp.find(partCookie);
        if (itPart == SyncDDiskPartCookieToOp.end()) {
            return;
        }
        const ui64 opCookie = itPart->second;
        SyncDDiskPartCookieToOp.erase(itPart);

        auto itOp = SyncWithDDiskInflightMap.find(opCookie);
        if (itOp == SyncWithDDiskInflightMap.end()) {
            return;
        }
        auto& inflight = itOp->second;

        auto idxIt = inflight.PartCookieToIndex.find(partCookie);
        Y_ABORT_UNLESS(idxIt != inflight.PartCookieToIndex.end());
        const ui32 idx = idxIt->second;
        inflight.PartCookieToIndex.erase(idxIt);

        const auto& record = ev->Get()->Record;
        auto* segmentRes = inflight.Response->Record.MutableSegmentResults(idx);
        segmentRes->SetStatus(record.GetStatus());
        if (record.HasErrorReason()) {
            segmentRes->SetErrorReason(record.GetErrorReason());
        }

        Y_ABORT_UNLESS(inflight.PendingCount > 0);
        if (--inflight.PendingCount == 0) {
            auto response = std::move(inflight.Response);
            const TActorId sender = inflight.Sender;
            const ui64 cookie = inflight.Cookie;
            const i64 startTs = inflight.StartTs;
            SyncWithDDiskInflightMap.erase(itOp);
            Send(sender, response.release(), 0, cookie);
            Counters.Interface.SyncWithDDisk.Reply(true, 0,
                HPMilliSecondsFloat(HPNow() - startTs));
        }
    }

    void Handle(TEvWritePersistentBuffer::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const TBlockSelector selector(record.GetSelector());

        STLOG(PRI_DEBUG, BS_DDISK, BSDD17, "TDDiskActorInMem::Handle(TEvWritePersistentBuffer)",
            (DDiskId, DDiskId), (Size, selector.Size));

        const auto startTs = HPNow();
        Counters.Interface.WritePersistentBuffer.Request(selector.Size);

        Send(ev->Sender, new TEvWritePersistentBufferResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK,
            std::nullopt,
            1.0,
            0.0),
            0, ev->Cookie);

        Counters.Interface.WritePersistentBuffer.Reply(true, selector.Size, HPMilliSecondsFloat(HPNow() - startTs));
    }

    void Handle(TEvWritePersistentBuffers::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_DDISK, BSDD17, "TDDiskActorInMem::Handle(TEvWritePersistentBuffers)", (DDiskId, DDiskId));

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();

        const auto startTs = HPNow();
        Counters.Interface.WritePersistentBuffer.Request(selector.Size);

        const TWriteInstruction instr(record.GetInstruction());
        TRope payload;
        if (instr.PayloadId) {
            payload = ev->Get()->GetPayload(*instr.PayloadId);
        }

        StartWritePersistentBuffers(ev->Sender, ev->Cookie, creds, selector, lsn,
            std::move(payload), record.GetPersistentBufferIds(), startTs);
    }

    template <typename TPbIdRange>
    void StartWritePersistentBuffers(
        TActorId sender, ui64 cookie,
        const TQueryCredentials& creds, const TBlockSelector& selector,
        ui64 lsn, TRope payload,
        const TPbIdRange& pbIds, i64 startTs)
    {
        const ui64 opCookie = NextCookie++;
        auto& inflight = WritePbsInflight[opCookie];
        inflight.Sender = sender;
        inflight.Cookie = cookie;
        inflight.Size = selector.Size;
        inflight.StartTs = startTs;
        inflight.PendingCount = pbIds.size();
        inflight.Response = std::make_unique<TEvWritePersistentBuffersResult>();

        if (inflight.PendingCount == 0) {
            auto response = std::move(inflight.Response);
            Send(sender, response.release(), 0, cookie);
            WritePbsInflight.erase(opCookie);
            Counters.Interface.WritePersistentBuffer.Reply(true, selector.Size,
                HPMilliSecondsFloat(HPNow() - startTs));
            return;
        }

        ui32 idx = 0;
        for (const auto& pbId : pbIds) {
            auto* res = inflight.Response->Record.AddResult();
            *res->MutablePersistentBufferId() = pbId;
            auto* res2 = res->MutableResult();
            res2->SetStatus(NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN);
            res2->SetFreeSpace(-1);
            res2->SetPDiskNormalizedOccupancy(-1);

            const ui64 partCookie = NextCookie++;
            inflight.PartCookieToIndex[partCookie] = idx++;
            WritePbsPartCookieToOp[partCookie] = opCookie;

            auto msg = std::make_unique<TEvWritePersistentBuffer>(
                creds, selector, lsn, TWriteInstruction(0));
            msg->AddPayload(TRope(payload));
            auto pbServiceId = MakeBlobStoragePersistentBufferId(
                pbId.GetNodeId(), pbId.GetPDiskId(), pbId.GetDDiskSlotId());
            Send(pbServiceId, msg.release(), IEventHandle::FlagTrackDelivery, partCookie);
        }
    }

    void Handle(TEvWritePersistentBufferResult::TPtr& ev) {
        const ui64 partCookie = ev->Cookie;
        auto itPart = WritePbsPartCookieToOp.find(partCookie);
        if (itPart == WritePbsPartCookieToOp.end()) {
            return;
        }
        const ui64 opCookie = itPart->second;
        WritePbsPartCookieToOp.erase(itPart);

        auto itOp = WritePbsInflight.find(opCookie);
        if (itOp == WritePbsInflight.end()) {
            return;
        }
        auto& inflight = itOp->second;

        auto idxIt = inflight.PartCookieToIndex.find(partCookie);
        Y_ABORT_UNLESS(idxIt != inflight.PartCookieToIndex.end());
        const ui32 idx = idxIt->second;
        inflight.PartCookieToIndex.erase(idxIt);

        const auto& record = ev->Get()->Record;
        auto* res2 = inflight.Response->Record.MutableResult(idx)->MutableResult();
        res2->SetStatus(record.GetStatus());
        res2->SetErrorReason(record.GetErrorReason());
        res2->SetFreeSpace(record.GetFreeSpace());
        res2->SetPDiskNormalizedOccupancy(record.GetPDiskNormalizedOccupancy());

        Y_ABORT_UNLESS(inflight.PendingCount > 0);
        if (--inflight.PendingCount == 0) {
            auto response = std::move(inflight.Response);
            const TActorId sender = inflight.Sender;
            const ui64 cookie = inflight.Cookie;
            const ui32 size = inflight.Size;
            const i64 startTs = inflight.StartTs;
            WritePbsInflight.erase(itOp);
            Send(sender, response.release(), 0, cookie);
            Counters.Interface.WritePersistentBuffer.Reply(true, size,
                HPMilliSecondsFloat(HPNow() - startTs));
        }
    }

    void Handle(TEvReadPersistentBufferResult::TPtr& ev) {
        const ui64 cookie = ev->Cookie;

        // Dispatch to read-then-write flow if matches.
        if (auto itRtw = ReadThenWriteInflightMap.find(cookie); itRtw != ReadThenWriteInflightMap.end()) {
            HandleReadThenWriteReadResult(ev, itRtw);
            return;
        }

        // Otherwise, treat as sync-with-persistent-buffer read result.
        auto itPart = SyncPartCookieToOp.find(cookie);
        if (itPart == SyncPartCookieToOp.end()) {
            return;
        }
        const ui64 opCookie = itPart->second;
        SyncPartCookieToOp.erase(itPart);

        auto itOp = SyncWithPbInflight.find(opCookie);
        if (itOp == SyncWithPbInflight.end()) {
            return;
        }
        auto& inflight = itOp->second;

        auto idxIt = inflight.PartCookieToIndex.find(cookie);
        Y_ABORT_UNLESS(idxIt != inflight.PartCookieToIndex.end());
        const ui32 idx = idxIt->second;
        inflight.PartCookieToIndex.erase(idxIt);

        const auto& record = ev->Get()->Record;
        auto* segmentRes = inflight.Response->Record.MutableSegmentResults(idx);
        segmentRes->SetStatus(record.GetStatus());
        if (record.HasErrorReason()) {
            segmentRes->SetErrorReason(record.GetErrorReason());
        }

        Y_ABORT_UNLESS(inflight.PendingCount > 0);
        if (--inflight.PendingCount == 0) {
            auto response = std::move(inflight.Response);
            const TActorId sender = inflight.Sender;
            const ui64 senderCookie = inflight.Cookie;
            const i64 startTs = inflight.StartTs;
            SyncWithPbInflight.erase(itOp);
            Send(sender, response.release(), 0, senderCookie);
            Counters.Interface.SyncWithPersistentBuffer.Reply(true, 0,
                HPMilliSecondsFloat(HPNow() - startTs));
        }
    }

    void Handle(TEvReadThenWritePersistentBuffers::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_DDISK, BSDD17, "TDDiskActorInMem::Handle(TEvReadThenWritePersistentBuffers)", (DDiskId, DDiskId));

        const auto& record = ev->Get()->Record;
        TQueryCredentials creds(record.GetCredentials());
        creds.FromPersistentBuffer = true;
        const ui64 lsn = record.GetLsn();
        const ui32 generation = record.GetGeneration();
        const ui32 timeout = record.GetReplyTimeoutMicroseconds();

        const auto startTs = HPNow();
        Counters.Interface.WritePersistentBuffer.Request(0);

        const ui64 readCookie = NextCookie++;
        auto& rInflight = ReadThenWriteInflightMap[readCookie];
        rInflight.Sender = ev->Sender;
        rInflight.Cookie = ev->Cookie;
        rInflight.StartTs = startTs;
        rInflight.Lsn = lsn;
        rInflight.Generation = generation;
        rInflight.ReplyTimeoutMicroseconds = timeout;
        rInflight.Creds = creds;
        rInflight.PersistentBufferIds.reserve(record.PersistentBufferIdsSize());
        for (const auto& pbId : record.GetPersistentBufferIds()) {
            rInflight.PersistentBufferIds.push_back(pbId);
        }

        // Read locally (the parent PB), then forward write to all listed pbs.
        auto readMsg = std::make_unique<TEvReadPersistentBuffer>();
        creds.Serialize(readMsg->Record.MutableCredentials());
        readMsg->Record.SetLsn(lsn);
        readMsg->Record.SetGeneration(generation);
        TReadInstruction(true).Serialize(readMsg->Record.MutableInstruction());
        Send(SelfId(), readMsg.release(), 0, readCookie);
    }

    void HandleReadThenWriteReadResult(TEvReadPersistentBufferResult::TPtr& ev,
            THashMap<ui64, TReadThenWriteInflight>::iterator it)
    {
        const auto& record = ev->Get()->Record;
        // Move state out before erase.
        TReadThenWriteInflight inflight = std::move(it->second);
        ReadThenWriteInflightMap.erase(it);

        if (record.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            auto response = std::make_unique<TEvWritePersistentBuffersResult>();
            for (const auto& pbId : inflight.PersistentBufferIds) {
                auto* res = response->Record.AddResult();
                *res->MutablePersistentBufferId() = pbId;
                auto* res2 = res->MutableResult();
                res2->SetStatus(record.GetStatus());
                if (record.HasErrorReason()) {
                    res2->SetErrorReason(record.GetErrorReason());
                }
                res2->SetFreeSpace(-1);
                res2->SetPDiskNormalizedOccupancy(-1);
            }
            Send(inflight.Sender, response.release(), 0, inflight.Cookie);
            Counters.Interface.WritePersistentBuffer.Reply(false, 0,
                HPMilliSecondsFloat(HPNow() - inflight.StartTs));
            return;
        }

        TRope payload;
        if (record.HasReadResult()) {
            payload = ev->Get()->GetPayload(record.GetReadResult().GetPayloadId());
        }
        const TBlockSelector selector{
            record.GetVChunkIndex(), record.GetOffsetInBytes(), record.GetSizeInBytes()};

        StartWritePersistentBuffers(inflight.Sender, inflight.Cookie, inflight.Creds,
            selector, inflight.Lsn, std::move(payload), inflight.PersistentBufferIds,
            inflight.StartTs);
    }

    void Handle(TEvReadPersistentBuffer::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto& selector = record.GetSelector();
        ui32 size = selector.GetSize();
        ui64 vChunkIndex = selector.GetVChunkIndex();
        ui32 offsetInBytes = selector.GetOffsetInBytes();
        
        STLOG(PRI_DEBUG, BS_DDISK, BSDD18, "TDDiskActorInMem::Handle(TEvReadPersistentBuffer)", 
            (DDiskId, DDiskId), (Size, size));

        const auto startTs = HPNow();
        Counters.Interface.ReadPersistentBuffer.Request(size);

        Send(ev->Sender, new TEvReadPersistentBufferResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK,
            std::nullopt,
            vChunkIndex,
            offsetInBytes,
            size,
            MakeZeroRope(size)),
            0, ev->Cookie);

        Counters.Interface.ReadPersistentBuffer.Reply(true, size, HPMilliSecondsFloat(HPNow() - startTs));
    }

    void Handle(TEvErasePersistentBuffer::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_DDISK, BSDD19, "TDDiskActorInMem::Handle(TEvErasePersistentBuffer)", (DDiskId, DDiskId));

        const auto startTs = HPNow();
        Counters.Interface.ErasePersistentBuffer.Request(0);

        Send(ev->Sender, new TEvErasePersistentBufferResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK,
            std::nullopt,
            1.0,
            0.0),
            0, ev->Cookie);

        Counters.Interface.ErasePersistentBuffer.Reply(true, 0, HPMilliSecondsFloat(HPNow() - startTs));
    }

    void Handle(TEvBatchErasePersistentBuffer::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_DDISK, BSDD20, "TDDiskActorInMem::Handle(TEvBatchErasePersistentBuffer)", (DDiskId, DDiskId));

        const auto startTs = HPNow();
        Counters.Interface.ErasePersistentBuffer.Request(0);

        Send(ev->Sender, new TEvErasePersistentBufferResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK,
            std::nullopt,
            1.0,
            0.0),
            0, ev->Cookie);

        Counters.Interface.ErasePersistentBuffer.Reply(true, 0, HPMilliSecondsFloat(HPNow() - startTs));
    }

    void Handle(TEvListPersistentBuffer::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_DDISK, BSDD21, "TDDiskActorInMem::Handle(TEvListPersistentBuffer)", (DDiskId, DDiskId));

        const auto startTs = HPNow();
        Counters.Interface.ListPersistentBuffer.Request(0);

        Send(ev->Sender, new TEvListPersistentBufferResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK),
            0, ev->Cookie);

        Counters.Interface.ListPersistentBuffer.Reply(true, 0, HPMilliSecondsFloat(HPNow() - startTs));
    }

    void Handle(TEvGetPersistentBufferInfo::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_DDISK, BSDD22, "TDDiskActorInMem::Handle(TEvGetPersistentBufferInfo)", (DDiskId, DDiskId));
        
        auto response = std::make_unique<TEvPersistentBufferInfo>();
        response->StartedAt = TInstant::Now();
        response->AllocatedChunks = 0;
        response->MaxChunks = 0;
        response->SectorSize = 4096;
        response->ChunkSize = 128_MB;
        response->FreeSectors = 0;
        response->InMemoryCacheSize = 0;
        response->InMemoryCacheLimit = 128_MB;
        response->DiskOperationsInflight = 0;
        response->PendingEvents = 0;
        
        Send(ev->Sender, response.release(), 0, ev->Cookie);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_DDISK, BSDD23, "TDDiskActorInMem::Handle(TEvUndelivered)",
            (DDiskId, DDiskId), (SourceType, ev->Get()->SourceType), (Cookie, ev->Cookie));

        const auto sourceType = ev->Get()->SourceType;
        const ui64 partCookie = ev->Cookie;

        if (sourceType == TEvWritePersistentBuffer::EventType) {
            auto fakeReply = std::make_unique<TEvWritePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                "Event undelivered");
            auto handle = std::make_unique<IEventHandle>(SelfId(), ev->Sender, fakeReply.release(),
                0, partCookie);
            TActivationContext::Send(handle.release());
        } else if (sourceType == TEvReadPersistentBuffer::EventType) {
            auto fakeReply = std::make_unique<TEvReadPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                "Event undelivered");
            auto handle = std::make_unique<IEventHandle>(SelfId(), ev->Sender, fakeReply.release(),
                0, partCookie);
            TActivationContext::Send(handle.release());
        } else if (sourceType == TEvRead::EventType) {
            auto fakeReply = std::make_unique<TEvReadResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                "Event undelivered");
            auto handle = std::make_unique<IEventHandle>(SelfId(), ev->Sender, fakeReply.release(),
                0, partCookie);
            TActivationContext::Send(handle.release());
        }
    }

    void PassAway() override {
        if (!IsPersistentBufferActor && PersistentBufferActorId) {
            Send(PersistentBufferActorId, new NActors::TEvents::TEvPoison());
        }
        CountersBase->RemoveSubgroupChain(CountersChain);
        TActorBootstrapped::PassAway();
    }
};

IActor *CreateDDiskActorInMem(
    TVDiskConfig::TBaseInfo&& baseInfo,
    TIntrusivePtr<TBlobStorageGroupInfo> info,
    TPersistentBufferFormat&& pbFormat,
    TDDiskConfig&& ddiskConfig,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
{
    return new TDDiskActorInMem(
        std::move(baseInfo),
        std::move(info),
        std::move(pbFormat),
        std::move(ddiskConfig),
        std::move(counters),
        false /*isPersistentBufferActor*/);
}

} // NKikimr::NDDisk
