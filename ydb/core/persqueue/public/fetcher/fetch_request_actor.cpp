#include "fetch_request_actor.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/pq_rl_helpers.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

#define LOG_PREFIX "[" << NActors::TlsActivationContext->AsActorContext().SelfID << "] "
#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PQ_FETCH_REQUEST, LOG_PREFIX << stream)
#define LOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::PQ_FETCH_REQUEST, LOG_PREFIX << stream)
#define LOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PQ_FETCH_REQUEST, LOG_PREFIX << stream)
#define LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PQ_FETCH_REQUEST, LOG_PREFIX << stream)
#define LOG(level, stream) LOG_LOG_S(*NActors::TlsActivationContext, level, NKikimrServices::PQ_FETCH_REQUEST, LOG_PREFIX << stream)

namespace NKikimr::NPQ {

using namespace NMsgBusProxy;
using namespace NSchemeCache;


namespace {
    static constexpr TDuration MaxTimeout = TDuration::Seconds(30);
    static constexpr ui64 TimeoutWakeupTag = 1000;
}

struct TTabletInfo {
    TActorId PipeClient;
    bool BrokenPipe = false;
    std::vector<size_t> PartitionIndexes;
};

struct TTopicInfo {
    THashMap<ui32, ui64> PartitionToTablet;

    TIntrusiveConstPtr<TSchemeCacheNavigate::TPQGroupInfo> PQInfo;
    TString RealPath;
    THashSet<ui32> PartitionsToRequest;

    //fetchRequest part
    THashMap<ui32, TAutoPtr<TEvPersQueue::TEvHasDataInfo>> HasDataRequests;
};


using namespace NActors;

class TPQFetchRequestActor : public TActorBootstrapped<TPQFetchRequestActor>
                           , private TRlHelpers {
private:
    TFetchRequestSettings Settings;

    size_t FetchRequestCurrentPartitionIndex;
    ui64 FetchRequestCurrentReadTablet;
    ui32 FetchRequestBytesLeft;
    bool ProcessingFinished = false;
    THolder<TEvPQ::TEvFetchResponse> Response;
    const TActorId SchemeCache;

    // TopicPath -> TopicInfo
    THashMap<TString, TTopicInfo> TopicInfo;
    // TabletId -> TabletInfo
    THashMap<ui64, TTabletInfo> TabletInfo;

    TActorId RequesterId;
    ui64 PendingQuotaAmount;

    TActorId YdbProxy;
    TActorId LongTimer;

    std::unordered_map<TString, TString> CdcPathToPrivateTopicPath;

    enum class EPartitionStatus {
        DataRequested,
        DataReceived,
        HasDataRequested,
        HasDataReceived,
        Unprocessed
    };
    std::vector<EPartitionStatus> PartitionStatus;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PQ_FETCH_REQUEST;
    }

    TPQFetchRequestActor(const TFetchRequestSettings& settings, const TActorId& schemeCacheId, const TActorId& requesterId)
        : TRlHelpers({}, settings.RlCtx, 8_KB, false, TDuration::Seconds(1))
        , Settings(settings)
        , FetchRequestCurrentPartitionIndex(0)
        , FetchRequestCurrentReadTablet(0)
        , FetchRequestBytesLeft(Settings.TotalMaxBytes)
        , SchemeCache(schemeCacheId)
        , RequesterId(requesterId)
    {
        ui64 deadline = TAppData::TimeProvider->Now().MilliSeconds() + Min<ui64>(Settings.MaxWaitTimeMs, MaxTimeout.MilliSeconds());

        PartitionStatus.resize(Settings.Partitions.size());

        for (size_t i = 0; i < Settings.Partitions.size(); ++i) {
            const auto& p = Settings.Partitions[i];
            if (p.Topic.empty()) {
                Response = CreateErrorReply(Ydb::StatusIds::BAD_REQUEST, "Empty topic in fetch request");
                return;
            } if (!p.MaxBytes) {
                Response = CreateErrorReply(Ydb::StatusIds::BAD_REQUEST, "No maxBytes for partition in fetch request");
                return;
            }
            auto topicPath = CanonizePath(p.Topic);
            bool res = TopicInfo[topicPath].PartitionsToRequest.insert(p.Partition).second;
            if (!res) {
                Response = CreateErrorReply(Ydb::StatusIds::BAD_REQUEST, "Some partition specified multiple times in fetch request");
                return;
            }
            TAutoPtr<TEvPersQueue::TEvHasDataInfo> fetchInfo(new TEvPersQueue::TEvHasDataInfo());
            fetchInfo->Record.SetPartition(p.Partition);
            fetchInfo->Record.SetOffset(p.Offset);
            fetchInfo->Record.SetDeadline(deadline);
            fetchInfo->Record.SetCookie(i);
            fetchInfo->Record.SetClientId(Settings.Consumer);
            TopicInfo[topicPath].HasDataRequests[p.Partition] = fetchInfo;

            PartitionStatus[i] = EPartitionStatus::Unprocessed;
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Tag == TimeoutWakeupTag) {
            return FinishProcessing(ctx);
        }

        const auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
        OnWakeup(tag);
        switch (tag) {
            case EWakeupTag::RlAllowed:
                ProceedFetchRequest(ctx);
                PendingQuotaAmount = 0;
                break;

            case EWakeupTag::RlNoResource:
                // Re-requesting the quota. We do this until we get a quota.
                RequestDataQuota(PendingQuotaAmount, ctx);
                break;

            default:
                Y_VERIFY_DEBUG_S(false, "Unsupported tag: " << static_cast<ui64>(tag));
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_I("Fetch request actor boostrapped. Request is valid: " << (!Response));

        // handle error from constructor
        if (Response) {
            return SendReplyAndDie(std::move(Response), ctx);
        }

        LongTimer = CreateLongTimer(Min<TDuration>(MaxTimeout, TDuration::MilliSeconds(Settings.MaxWaitTimeMs)),
            new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup(TimeoutWakeupTag)));

        DescribeTopics(ctx);
        Become(&TPQFetchRequestActor::StateDescribe);
    }

    void DescribeTopics(const TActorContext&) {
        LOG_D("DescribeTopics");

        std::unordered_set<TString> topics;
        for (const auto& part : Settings.Partitions) {
            topics.insert(part.Topic);
        }

        NDescriber::TDescribeSettings settings = {
            .UserToken = Settings.UserToken,
            .AccessRights = NACLib::EAccessRights::SelectRow
        };
        RegisterWithSameMailbox(NDescriber::CreateDescriberActor(SelfId(), Settings.Database, std::move(topics), settings));
    }

    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx) {
        LOG_D("Handle NDescriber::TEvDescribeTopicsResponse");

        for (auto& [topicPath, info] : ev->Get()->Topics) {
            switch (info.Status) {
                case NDescriber::EStatus::SUCCESS: {
                    auto& topicInfo = TopicInfo[topicPath];
                    topicInfo.PQInfo = info.Info;
                    topicInfo.RealPath = std::move(info.RealPath);
                    break;
                }
                default:
                    return SendReplyAndDie(
                        CreateErrorReply(
                            info.Status == NDescriber::EStatus::UNAUTHORIZED ? Ydb::StatusIds::UNAUTHORIZED : Ydb::StatusIds::SCHEME_ERROR,
                            NDescriber::Description(topicPath, info.Status)
                        ),
                        ctx
                    );
            }
        }

        OnMetadataReceived(ctx);
    }

    STRICT_STFUNC(StateDescribe,
        HFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        HFunc(TEvents::TEvWakeup, Handle);
        CFunc(NActors::TEvents::TSystem::Poison, Die);
    )


    void OnMetadataReceived(const TActorContext& ctx) {
        for (auto& [name, info]: TopicInfo) {
            ProcessTopicMetadata(name, info, ctx);
        }

        Become(&TPQFetchRequestActor::StateWork);
    }

    void ProcessTopicMetadata(const TString& name, TTopicInfo& topicInfo, const TActorContext& ctx) {
        const auto& pqDescr = topicInfo.PQInfo->Description;

        for (const auto& partition : pqDescr.GetPartitions()) {
            ui32 partitionId = partition.GetPartitionId();
            ui64 tabletId = partition.GetTabletId();
            if (!topicInfo.PartitionsToRequest.contains(partitionId)) {
                continue;
            }

            bool res = topicInfo.PartitionToTablet.insert({partitionId, tabletId}).second;
            AFL_ENSURE(res)("partitionId", partitionId);

            if (TabletInfo.find(tabletId) == TabletInfo.end()) {
                auto& tabletInfo = TabletInfo[tabletId];

                // Tablet node resolution relies on opening a pipe
                tabletInfo.PipeClient = CreatePipe(tabletId, ctx);
            }

            auto& tabletInfo = TabletInfo[tabletId];
            auto& fetchInfo = topicInfo.HasDataRequests[partitionId];
            const auto partitionIndex = fetchInfo->Record.GetCookie();
            tabletInfo.PartitionIndexes.push_back(partitionIndex);
            LOG_D("Sending TEvPersQueue::TEvHasDataInfo " << fetchInfo->Record.ShortDebugString());
            NTabletPipe::SendData(ctx, tabletInfo.PipeClient, fetchInfo.Release());
            PartitionStatus[partitionIndex] = EPartitionStatus::HasDataRequested;
        }

        if (!topicInfo.PartitionsToRequest.empty() && topicInfo.PartitionsToRequest.size() != topicInfo.PartitionToTablet.size()) {
            auto reason = TStringBuilder() << "no one of requested partitions in topic " << name << ", Marker# PQ12";
            return SendReplyAndDie(CreateErrorReply(Ydb::StatusIds::BAD_REQUEST, reason), ctx);
        }

        AFL_ENSURE(!TabletInfo.empty()); // if TabletInfo is empty - topic is empty
    }

    void ProceedFetchRequest(const TActorContext& ctx) {
        if (FetchRequestCurrentReadTablet) { //already got active read request
            LOG_D("Fetch request is pending. TabletId=" << FetchRequestCurrentReadTablet
                << " partitionIndex=" << FetchRequestCurrentPartitionIndex << "/" << Settings.Partitions.size());
            return;
        }

        AFL_ENSURE(FetchRequestCurrentPartitionIndex <= Settings.Partitions.size())
            ("l", FetchRequestCurrentPartitionIndex)
            ("r", Settings.Partitions.size());

        while (true) {
            LOG_D("Processing " << FetchRequestCurrentPartitionIndex << "/" << Settings.Partitions.size());
            if (FetchRequestCurrentPartitionIndex == Settings.Partitions.size()) {
                CreateOkResponse();
                return SendReplyAndDie(std::move(Response), ctx);
            }

            auto& status = PartitionStatus[FetchRequestCurrentPartitionIndex];
            if (status == EPartitionStatus::DataReceived) {
                LOG_D("Skip partition " << FetchRequestCurrentPartitionIndex << " because status is DataReceived");
                ++FetchRequestCurrentPartitionIndex;
                continue;
            }

            if (FetchRequestBytesLeft == 0) {
                LOG_D("Partition " << FetchRequestCurrentPartitionIndex << " status is " << (int)status << " bytesLeft=" << FetchRequestBytesLeft);
                if (status == EPartitionStatus::HasDataReceived) {
                    ++FetchRequestCurrentPartitionIndex;
                    continue;
                }
                return;
            }

            auto& req = Settings.Partitions[FetchRequestCurrentPartitionIndex];
            auto [topic, topicInfo] = GetTopicInfo(req.Topic);

            auto partitionId = req.Partition;

            ui64 tabletId = topicInfo.PartitionToTablet[partitionId];
            AFL_ENSURE(tabletId)
                ("topic", topic)
                ("partition", partitionId)
                ("tabletId", tabletId);

            auto jt = TabletInfo.find(tabletId);
            AFL_ENSURE(jt != TabletInfo.end())
                ("topic", topic)
                ("partition", partitionId)
                ("tabletId", tabletId);
            auto& tabletInfo = jt->second;
            AFL_ENSURE(!tabletInfo.BrokenPipe); // If pipe is broken, than partition status is DataReceived. It is verified early.

            FetchRequestCurrentReadTablet = tabletId;
            PartitionStatus[FetchRequestCurrentPartitionIndex] = EPartitionStatus::DataRequested;

            //Form read request
            auto request = CreateReadRequest(topic, req);
            LOG_D("Sending request: " << request->Record.ShortDebugString());
            NTabletPipe::SendData(ctx, tabletInfo.PipeClient, request.release());

            break;
        }
    }

    void Handle(TEvPersQueue::TEvHasDataInfoResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        auto partitionIndex = record.GetCookie();
        LOG_D("Handle TEvPersQueue::TEvHasDataInfoResponse " << record.ShortDebugString());
        if (partitionIndex >= PartitionStatus.size()) {
            Y_VERIFY_DEBUG(partitionIndex < PartitionStatus.size());
            return;
        }
        auto& status = PartitionStatus[partitionIndex];
        LOG_D("Partition " << partitionIndex << " status is " << (int)status);
        if (status != EPartitionStatus::HasDataRequested) {
            // On timeout we resend send HasData
            return;
        }

        status = EPartitionStatus::HasDataReceived;

        auto& partition = Settings.Partitions[partitionIndex];
        if (record.GetEndOffset() < partition.Offset) {
            AddResult(partitionIndex, EPartitionStatus::DataReceived, NPersQueue::NErrorCode::EErrorCode::READ_ERROR_TOO_BIG_OFFSET, record.GetEndOffset());
        } else if (record.GetEndOffset() == partition.Offset) {
            AddResult(partitionIndex, EPartitionStatus::DataReceived, NPersQueue::NErrorCode::EErrorCode::READ_NOT_DONE, record.GetEndOffset());
        } else {
            AddResult(partitionIndex, EPartitionStatus::HasDataReceived, NPersQueue::NErrorCode::EErrorCode::OK, record.GetEndOffset());
        }

        ProceedFetchRequest(ctx);
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        LOG_D("Handle TEvPersQueue::TEvRespons " << record.ShortDebugString());
        AFL_ENSURE(record.HasPartitionResponse());

        if (record.GetPartitionResponse().GetCookie() != FetchRequestCurrentPartitionIndex || FetchRequestCurrentReadTablet == 0) {
            LOG_W("proxy fetch error: got response from tablet " << record.GetPartitionResponse().GetCookie()
                                << " while waiting from " << FetchRequestCurrentPartitionIndex << " and requested tablet is " << FetchRequestCurrentReadTablet);
            return;
        }

        if (FetchRequestBytesLeft >= (ui32)record.ByteSize()) {
            FetchRequestBytesLeft -= (ui32)record.ByteSize();
        } else {
            FetchRequestBytesLeft = 0;
        }
        FetchRequestCurrentReadTablet = 0;
        EnsureResponse();

        AFL_ENSURE(FetchRequestCurrentPartitionIndex < Settings.Partitions.size());
        PartitionStatus[FetchRequestCurrentPartitionIndex] = EPartitionStatus::DataReceived;

        const auto& req = Settings.Partitions[FetchRequestCurrentPartitionIndex];
        const auto& partitionId = req.Partition;

        auto res = Response->Response.MutablePartResult(FetchRequestCurrentPartitionIndex);
        res->SetTopic(req.Topic);
        res->SetPartition(partitionId);
        auto read = res->MutableReadResult();
        if (record.HasPartitionResponse() && record.GetPartitionResponse().HasCmdReadResult()) {
            read->CopyFrom(record.GetPartitionResponse().GetCmdReadResult());
        }
        if (record.HasErrorCode()) {
            read->SetErrorCode(record.GetErrorCode());
        }
        if (record.HasErrorReason()) {
            read->SetErrorReason(record.GetErrorReason());
        }

        ++FetchRequestCurrentPartitionIndex;

        auto [_, topicInfo] = GetTopicInfo(req.Topic);

        SetMeteringMode(topicInfo.PQInfo->Description.GetPQTabletConfig().GetMeteringMode());

        LOG_D("After processing result FetchRequestBytesLeft=" << FetchRequestBytesLeft);
        if (FetchRequestBytesLeft == 0) {
            FinishProcessing(ctx);
        } else if (IsQuotaRequired()) {
            PendingQuotaAmount = CalcRuConsumption(GetPayloadSize(record)) + (Settings.RuPerRequest ? 1 : 0);
            Settings.RuPerRequest = false;
            RequestDataQuota(PendingQuotaAmount, ctx);
        } else {
            ProceedFetchRequest(ctx);
        }
    }

    TActorId CreatePipe(ui64 tabletId, const TActorContext& ctx) {
        auto retryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        retryPolicy.RetryLimitCount = 5;
        NTabletPipe::TClientConfig clientConfig(retryPolicy);

        return ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
    }

    void AddResult(size_t partitionIndex, EPartitionStatus status, NPersQueue::NErrorCode::EErrorCode errorCode, std::optional<ui64> maxOffset = std::nullopt) {
        EnsureResponse();

        PartitionStatus[partitionIndex] = status;

        const auto& req = Settings.Partitions[partitionIndex];

        auto* res = Response->Response.MutablePartResult(partitionIndex);
        res->SetTopic(req.Topic);
        res->SetPartition(req.Partition);
        auto* read = res->MutableReadResult();
        read->SetErrorCode(errorCode);
        if (maxOffset) {
            read->SetMaxOffset(*maxOffset);
        }
    }

    void HandlePipeError(const ui64 tabletId, const TActorContext& ctx) {
        auto it = TabletInfo.find(tabletId);
        // All pipes openned for tablets from the TabletInfo
        AFL_ENSURE(it != TabletInfo.end())("tabletId", tabletId);
        auto& pipeInfo = it->second;

        pipeInfo.BrokenPipe = true;

        for (const auto partitionIndex : pipeInfo.PartitionIndexes) {
            if (PartitionStatus[partitionIndex] != EPartitionStatus::DataReceived) {
                AddResult(partitionIndex, EPartitionStatus::DataReceived, NPersQueue::NErrorCode::EErrorCode::TABLET_PIPE_DISCONNECTED);
            }
        }

        if (FetchRequestCurrentReadTablet == tabletId) {
            FetchRequestCurrentReadTablet = 0;
        }

        ProceedFetchRequest(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            HandlePipeError(ev->Get()->TabletId, ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        HandlePipeError(ev->Get()->TabletId, ctx);
    }

    void FinishProcessing(const TActorContext& ctx) {
        if (ProcessingFinished) {
            return;
        }
        ProcessingFinished = true;

        for (size_t i = 0; i < PartitionStatus.size(); ++i) {
            if (PartitionStatus[i] == EPartitionStatus::HasDataRequested) { // rerequest HasData without timeout
                auto& p = Settings.Partitions[i];
                auto [topic, topicInfo] = GetTopicInfo(p.Topic);

                auto fetchInfo = std::make_unique<TEvPersQueue::TEvHasDataInfo>();
                fetchInfo->Record.SetPartition(p.Partition);
                fetchInfo->Record.SetOffset(p.Offset);
                fetchInfo->Record.SetCookie(i);
                fetchInfo->Record.SetClientId(Settings.Consumer);
                fetchInfo->Record.SetDeadline(0);

                auto tabletId = topicInfo.PartitionToTablet[p.Partition];
                LOG_D("Sending TEvPersQueue::TEvHasDataInfo " << fetchInfo->Record.ShortDebugString());
                NTabletPipe::SendData(ctx, TabletInfo[tabletId].PipeClient, fetchInfo.release());
            }
        }

        FetchRequestBytesLeft = 0;
        ProceedFetchRequest(ctx);
    }

    std::pair<const TString&, TTopicInfo&> GetTopicInfo(const TString& topicPath) {
        auto it = TopicInfo.find(CanonizePath(topicPath));
        AFL_ENSURE(it != TopicInfo.end())("topic", topicPath);

        return {it->second.RealPath, it->second};
    }

    std::unique_ptr<TEvPersQueue::TEvRequest> CreateReadRequest(const TString& topic, const TPartitionFetchRequest& fetchRequest) {
        auto request = std::make_unique<TEvPersQueue::TEvRequest>();
        request->Record.SetRequestId(TStringBuilder() << "request" << "-id-" << FetchRequestCurrentPartitionIndex << "-" << Settings.Partitions.size());

        auto* partitionRequest = request->Record.MutablePartitionRequest();
        partitionRequest->SetCookie(FetchRequestCurrentPartitionIndex);
        partitionRequest->SetTopic(topic);
        partitionRequest->SetPartition(fetchRequest.Partition);

        auto read = partitionRequest->MutableCmdRead();
        read->SetClientId(Settings.Consumer);
        read->SetOffset(fetchRequest.Offset);
        read->SetCount(1000000);
        read->SetTimeoutMs(0);
        read->SetBytes(Min<ui32>(fetchRequest.MaxBytes, FetchRequestBytesLeft));
        read->SetReadTimestampMs(fetchRequest.ReadTimestampMs);
        read->SetExternalOperation(true);

        return request;
    }

    ui64 GetPayloadSize(const NKikimrClient::TResponse& record) const {
        ui64 readBytesSize = 0;
        const auto& response = record.GetPartitionResponse();
        if (response.HasCmdReadResult()) {
            const auto& results = response.GetCmdReadResult().GetResult();
            for (auto& r : results) {
                auto proto(NKikimr::GetDeserializedData(r.GetData()));
                readBytesSize += proto.GetData().size();
            }
        }
        return readBytesSize;
    }

    void SendReplyAndDie(THolder<TEvPQ::TEvFetchResponse> event, const TActorContext& ctx) {
        LOG_D("Reply to " << RequesterId << ": " << event->Response.ShortDebugString());
        ctx.Send(RequesterId, event.Release());
        Die(ctx);
    }

    THolder<TEvPQ::TEvFetchResponse> CreateErrorReply(Ydb::StatusIds::StatusCode status, const TString& message) {
        auto response = MakeHolder<TEvPQ::TEvFetchResponse>();
        response->Status = status;
        response->Message = message;
        return response;
    }

    void EnsureResponse() {
        if (!Response) {
            Response = MakeHolder<TEvPQ::TEvFetchResponse>();

            for (size_t i = 0; i < PartitionStatus.size(); ++i) {
                Response->Response.AddPartResult();
            }
        }
    }

    void CreateOkResponse() {
        EnsureResponse();
        Response->Status = Ydb::StatusIds::SUCCESS;
    }

    void EnsureYdbProxy() {
        if (!YdbProxy) {
            YdbProxy = RegisterWithSameMailbox(NReplication::CreateLocalYdbProxy(Settings.Database));
        }
    }

    STRICT_STFUNC(StateWork,
        HFunc(TEvPersQueue::TEvHasDataInfoResponse, Handle);
        HFunc(TEvPersQueue::TEvResponse, Handle);

        HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        HFunc(TEvTabletPipe::TEvClientConnected, Handle);

        HFunc(TEvents::TEvWakeup, Handle);
        CFunc(NActors::TEvents::TSystem::PoisonPill, Die);
    )

    void Die(const TActorContext& ctx) override {
        for (auto& [_, tabletInfo]: TabletInfo) {
            if (!tabletInfo.BrokenPipe) {
                NTabletPipe::CloseClient(ctx, tabletInfo.PipeClient);
            }
        }
        if (YdbProxy) {
            Send(YdbProxy, new TEvents::TEvPoison());
        }
        if (LongTimer) {
            Send(LongTimer, new TEvents::TEvPoison());
        }
        TRlHelpers::PassAway(SelfId());
        TActorBootstrapped<TPQFetchRequestActor>::Die(ctx);
    }
};

NActors::IActor* CreatePQFetchRequestActor(
    const TFetchRequestSettings& settings,
    const TActorId& schemeCache,
    const TActorId& requester
) {
    return new TPQFetchRequestActor(settings, schemeCache, requester);
}

} // namespace NKikimr::NPQ
