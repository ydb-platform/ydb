#include "fetch_request_actor.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>

#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/msgbus.pb.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/pq_rl_helpers.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/public/lib/base/msgbus_status.h>

#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PQ_FETCH_REQUEST, stream)
#define LOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::PQ_FETCH_REQUEST, stream)
#define LOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PQ_FETCH_REQUEST, stream)
#define LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PQ_FETCH_REQUEST, stream)

namespace NKikimr::NPQ {

using namespace NMsgBusProxy;
using namespace NSchemeCache;


namespace {
    static constexpr TDuration MaxTimeout = TDuration::MilliSeconds(30000);
    static constexpr ui64 TimeoutWakeupTag = 1000;
}

struct TTabletInfo {
    TActorId PipeClient;
    bool BrokenPipe = false;
};

struct TTopicInfo {
    TVector<ui64> Tablets;
    THashMap<ui32, ui64> PartitionToTablet;

    TIntrusiveConstPtr<TSchemeCacheNavigate::TPQGroupInfo> PQInfo;
    NPersQueue::TDiscoveryConverterPtr Converter;
    ui32 NumParts = 0;
    THashSet<ui32> PartitionsToRequest;

    //fetchRequest part
    THashMap<ui32, TAutoPtr<TEvPersQueue::TEvHasDataInfo>> HasDataRequests;
};


using namespace NActors;

class TPQFetchRequestActor : public TActorBootstrapped<TPQFetchRequestActor>
                           , private TRlHelpers {
private:
    TFetchRequestSettings Settings;

    ui32 FetchRequestReadsDone;
    ui64 FetchRequestCurrentReadTablet;
    ui64 CurrentCookie;
    ui32 FetchRequestBytesLeft;
    THolder<TEvPQ::TEvFetchResponse> Response;
    const TActorId SchemeCache;

    // TopicPath -> TopicInfo
    THashMap<TString, TTopicInfo> TopicInfo;
    // TabletId -> TabletInfo
    THashMap<ui64, TTabletInfo> TabletInfo;
    // PartitionIndex
    std::unordered_set<ui64> HasDataRequestsInFlight;
    // PartitionIndex
    std::deque<ui64> PartitionsWithData;

    ui32 TopicsAnswered;
    THashSet<ui64> TabletsDiscovered;
    THashSet<ui64> TabletsAnswered;
    ui32 PartTabletsRequested;
    TString ErrorReason;
    TActorId RequesterId;
    ui64 PendingQuotaAmount;

    TActorId LongTimer;

    std::unordered_map<TString, TString> PrivateTopicPathToCdcPath;
    std::unordered_map<TString, TString> CdcPathToPrivateTopicPath;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PQ_FETCH_REQUEST;
    }

    TPQFetchRequestActor(const TFetchRequestSettings& settings, const TActorId& schemeCacheId, const TActorId& requesterId)
        : TRlHelpers({}, settings.RlCtx, 8_KB, false, TDuration::Seconds(1))
        , Settings(settings)
        , FetchRequestReadsDone(0)
        , FetchRequestCurrentReadTablet(0)
        , CurrentCookie(1)
        , FetchRequestBytesLeft(0)
        , SchemeCache(schemeCacheId)
        , TopicsAnswered(0)
        , PartTabletsRequested(0)
        , RequesterId(requesterId)
    {
        ui64 deadline = TAppData::TimeProvider->Now().MilliSeconds() + Min<ui64>(Settings.MaxWaitTimeMs, MaxTimeout.MilliSeconds());
        FetchRequestBytesLeft = Settings.TotalMaxBytes;
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
            TopicInfo[topicPath].HasDataRequests[p.Partition] = fetchInfo;

            HasDataRequestsInFlight.insert(i);
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Tag == TimeoutWakeupTag) {
            HandleTimeout(ctx);
            return;
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

        // We add 6 second because message from partition can be in flight and
        // wakeup period in the partition is 5 seconds. timeout must be more
        LongTimer = CreateLongTimer(Min<TDuration>(MaxTimeout, TDuration::MilliSeconds(Settings.MaxWaitTimeMs)),
            new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup(TimeoutWakeupTag)));

        SendSchemeCacheRequest(ctx);
        Become(&TPQFetchRequestActor::StateFunc);
    }

    void SendSchemeCacheRequest(const TActorContext& ctx) {
        LOG_D("SendSchemeCacheRequest");

        auto schemeCacheRequest = std::make_unique<TSchemeCacheNavigate>(1);
        schemeCacheRequest->DatabaseName = Settings.Database;

        THashSet<TString> topicsRequested;
        if (PrivateTopicPathToCdcPath.empty()) {
            for (const auto& part : Settings.Partitions) {
                topicsRequested.insert(part.Topic);
            }
        } else {
            for (const auto& [key, _] : PrivateTopicPathToCdcPath) {
                topicsRequested.insert(key);
            }
        }

        for (const auto& topicName : topicsRequested) {
            auto split = NKikimr::SplitPath(topicName);
            TSchemeCacheNavigate::TEntry entry;
            entry.Path.insert(entry.Path.end(), split.begin(), split.end());

            entry.SyncVersion = true;
            entry.ShowPrivatePath = true;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;

            schemeCacheRequest->ResultSet.emplace_back(std::move(entry));
        }

        ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.release()));
    }

    void HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        LOG_D("Handle SchemeCache response");
        auto& result = ev->Get()->Request;
        bool anyCdcTopicInRequest = false;
        for (const auto& entry : result->ResultSet) {
            auto path = CanonizePath(NKikimr::JoinPath(entry.Path));
            switch (entry.Status) {
                case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                case TSchemeCacheNavigate::EStatus::RootUnknown:
                    return SendReplyAndDie(
                            CreateErrorReply(
                                Ydb::StatusIds::SCHEME_ERROR,
                                TStringBuilder() << "Topic not found: " << path
                            ),
                            ctx
                    );
                case TSchemeCacheNavigate::EStatus::Ok:
                    break;
                default:
                    return SendReplyAndDie(
                            CreateErrorReply(
                                Ydb::StatusIds::SCHEME_ERROR,
                                TStringBuilder() << "Got error: " << ToString(entry.Status) << " trying to find topic: " << path
                            ), ctx
                    );
            }
            if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
                anyCdcTopicInRequest = true;
                AFL_ENSURE(entry.ListNodeEntry->Children.size() == 1);
                auto privateTopicPath = CanonizePath(JoinPath(ChildPath(NKikimr::SplitPath(path), entry.ListNodeEntry->Children.at(0).Name)));
                PrivateTopicPathToCdcPath[privateTopicPath] = path;
                CdcPathToPrivateTopicPath[path] = privateTopicPath;
                TopicInfo[privateTopicPath] = std::move(TopicInfo[path]);
                TopicInfo.erase(path);
                continue;
            }
            if (entry.Kind != TSchemeCacheNavigate::EKind::KindTopic) {
                return SendReplyAndDie(
                        CreateErrorReply(
                            Ydb::StatusIds::SCHEME_ERROR, TStringBuilder() << "No such topic: " << path
                        ), ctx
                );
            }
            if (!entry.PQGroupInfo) {
                return SendReplyAndDie(
                        CreateErrorReply(
                            Ydb::StatusIds::SCHEME_ERROR,
                            TStringBuilder() << "Could not get valid description for topic: " << path
                        ), ctx
                );
            }
            if (!entry.PQGroupInfo->Description.HasBalancerTabletID() || entry.PQGroupInfo->Description.GetBalancerTabletID() == 0) {
                return SendReplyAndDie(
                        CreateErrorReply(
                            Ydb::StatusIds::SCHEME_ERROR,
                            TStringBuilder() << "Topic not created: " << path
                        ), ctx
                );
            }
            if (!CheckAccess(*entry.SecurityObject)) {
                return SendReplyAndDie(
                        CreateErrorReply(
                            Ydb::StatusIds::UNAUTHORIZED,
                            TStringBuilder() << "Access denied for topic: " << path
                        ), ctx
                );;
            }

            auto& topicInfo = TopicInfo[path];
            topicInfo.PQInfo = entry.PQGroupInfo;
        }

        if (anyCdcTopicInRequest) {
            SendSchemeCacheRequest(ctx);
            return;
        }

        for (auto& [name, info]: TopicInfo) {
            ProcessTopicMetadata(name, info, ctx);
        }
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
        return ProcessFetchRequestResult(ev, ctx);
    }

    void Handle(TEvPersQueue::TEvHasDataInfoResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        LOG_D("Handle TEvPersQueue::TEvHasDataInfoResponse " << record.ShortDebugString());
        auto it = HasDataRequestsInFlight.find(record.GetCookie());
        AFL_ENSURE(it != HasDataRequestsInFlight.end())("cookie", record.GetCookie());
        PartitionsWithData.push_back(record.GetCookie());
        HasDataRequestsInFlight.erase(it);

        ProceedFetchRequest(ctx);
    }

    TActorId CreatePipe(ui64 tabletId, const TActorContext& ctx) {
        auto retryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        retryPolicy.RetryLimitCount = 5;
        NTabletPipe::TClientConfig clientConfig(retryPolicy);

        return ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
    }

    void ProcessTopicMetadata(const TString& name, TTopicInfo& topicInfo, const TActorContext& ctx) {
        const auto& pqDescr = topicInfo.PQInfo->Description;

        ++TopicsAnswered;
        topicInfo.NumParts = pqDescr.PartitionsSize();

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
                topicInfo.Tablets.push_back(tabletId);

                // Tablet node resolution relies on opening a pipe
                tabletInfo.PipeClient = CreatePipe(tabletId, ctx);
            }

            const auto& tabletInfo = TabletInfo[tabletId];
            auto& fetchInfo = topicInfo.HasDataRequests[partitionId];
            LOG_D("Sending TEvPersQueue::TEvHasDataInfo " << fetchInfo->Record.ShortDebugString());
            NTabletPipe::SendData(ctx, tabletInfo.PipeClient, fetchInfo.Release());
            ++PartTabletsRequested;
        }

        if (!topicInfo.PartitionsToRequest.empty() && topicInfo.PartitionsToRequest.size() != topicInfo.PartitionToTablet.size()) {
            auto reason = TStringBuilder() << "no one of requested partitions in topic " << name << ", Marker# PQ12";
            return SendReplyAndDie(CreateErrorReply(Ydb::StatusIds::BAD_REQUEST, reason), ctx);
        }

        AFL_ENSURE(!TabletInfo.empty()); // if TabletInfo is empty - topic is empty
    }

    void HandlePipeError(const ui64 tabletId, const TActorContext& ctx) {
        auto it = TabletInfo.find(tabletId);
        // All pipes openned for tablets from the TabletInfo
        AFL_ENSURE(it != TabletInfo.end())("tabletId", tabletId);

        it->second.BrokenPipe = true;
        if (FetchRequestCurrentReadTablet == tabletId) {
            // fail current read
            ctx.Send(ctx.SelfID, FormEmptyCurrentRead(CurrentCookie).Release());
        }
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

    void HandleTimeout(const TActorContext& ctx) {
        if (Response) {
            return SendReplyOnDoneAndDie(ctx);
        }
        TString reason = "Timeout while waiting for response, may be just slow, Marker# PQ11";
        LOG_D(reason);
        return SendReplyAndDie(CreateErrorReply(Ydb::StatusIds::TIMEOUT, reason), ctx);
    }

    void Die(const TActorContext& ctx) override {
        for (auto& [_, tabletInfo]: TabletInfo) {
            if (!tabletInfo.BrokenPipe) {
                NTabletPipe::CloseClient(ctx, tabletInfo.PipeClient);
            }
        }
        if (LongTimer) {
            Send(LongTimer, new TEvents::TEvPoison());
        }
        TRlHelpers::PassAway(SelfId());
        TActorBootstrapped<TPQFetchRequestActor>::Die(ctx);
    }

    TAutoPtr<TEvPersQueue::TEvResponse> FormEmptyCurrentRead(ui64 cookie) {
        TAutoPtr<TEvPersQueue::TEvResponse> req(new TEvPersQueue::TEvResponse);
        auto read = req->Record.MutablePartitionResponse()->MutableCmdReadResult();
        req->Record.MutablePartitionResponse()->SetCookie(cookie);
        read->SetErrorCode(NPersQueue::NErrorCode::READ_NOT_DONE);
        return req.Release();
    }

    std::optional<size_t> NextPartition() {
        if (PartitionsWithData.empty()) {
            return std::nullopt;
        }
        size_t i = RandomNumber<size_t>(PartitionsWithData.size());
        if (i == 0) {
            auto partitionIndex = std::move(PartitionsWithData.front());
            PartitionsWithData.pop_front();
            return partitionIndex;
        }

        ui64 result = PartitionsWithData[i];
        PartitionsWithData[i] = PartitionsWithData.front();
        PartitionsWithData.pop_front();

        return result;
    }

    void ProceedFetchRequest(const TActorContext& ctx) {
        if (FetchRequestCurrentReadTablet) { //already got active read request
            return;
        }

        if (FetchRequestReadsDone == Settings.Partitions.size()) {
            CreateOkResponse();
            return SendReplyAndDie(std::move(Response), ctx);
        }
        AFL_ENSURE(FetchRequestReadsDone < Settings.Partitions.size())
            ("l", FetchRequestReadsDone)
            ("r", Settings.Partitions.size());

        auto partitionIndex = NextPartition();
        if (!partitionIndex) {
            return;
        }

        CurrentCookie = *partitionIndex;

        auto& req = Settings.Partitions[*partitionIndex];

        auto topic = req.Topic;
        auto cdcTopicNameIt = CdcPathToPrivateTopicPath.find(req.Topic);
        if (cdcTopicNameIt != CdcPathToPrivateTopicPath.end()) {
            topic = cdcTopicNameIt->second;
        }

        auto it = TopicInfo.find(CanonizePath(topic));
        AFL_ENSURE(it != TopicInfo.end())("topic", topic);

        auto& topicInfo = it->second;

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

        FetchRequestCurrentReadTablet = tabletId;
        if (FetchRequestBytesLeft == 0) { //answer right now
            ctx.Send(ctx.SelfID, FormEmptyCurrentRead(CurrentCookie).Release());
            return;
        }
        if (tabletInfo.BrokenPipe) { //answer right now
            // TODO leader error
            ctx.Send(ctx.SelfID, FormEmptyCurrentRead(CurrentCookie).Release());
            return;
        }

        //Form read request
        auto request = CreateReadRequest(topic, req);
        LOG_D("Sending request: " << request->Record.ShortDebugString());
        NTabletPipe::SendData(ctx, tabletInfo.PipeClient, request.release());
    }

    std::unique_ptr<TEvPersQueue::TEvRequest> CreateReadRequest(const TString& topic, const TPartitionFetchRequest& fetchRequest) {
        auto request = std::make_unique<TEvPersQueue::TEvRequest>();
        request->Record.SetRequestId(TStringBuilder() << "request" << "-id-" << FetchRequestReadsDone << "-" << Settings.Partitions.size());

        auto* partitionRequest = request->Record.MutablePartitionRequest();
        partitionRequest->SetCookie(CurrentCookie);
        partitionRequest->SetTopic(topic);
        partitionRequest->SetPartition(fetchRequest.Partition);

        auto read = partitionRequest->MutableCmdRead();
        read->SetClientId(fetchRequest.ClientId);
        read->SetOffset(fetchRequest.Offset);
        read->SetCount(1000000);
        read->SetTimeoutMs(0);
        read->SetBytes(Min<ui32>(fetchRequest.MaxBytes, FetchRequestBytesLeft));
        read->SetReadTimestampMs(fetchRequest.ReadTimestampMs);
        read->SetExternalOperation(true);

        return request;
    }

    void ProcessFetchRequestResult(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        AFL_ENSURE(record.HasPartitionResponse());

        if (record.GetPartitionResponse().GetCookie() != CurrentCookie || FetchRequestCurrentReadTablet == 0) {
            LOG_W("proxy fetch error: got response from tablet " << record.GetPartitionResponse().GetCookie()
                                << " while waiting from " << CurrentCookie << " and requested tablet is " << FetchRequestCurrentReadTablet);
            return;
        }

        if (FetchRequestBytesLeft >= (ui32)record.ByteSize()) {
            FetchRequestBytesLeft -= (ui32)record.ByteSize();
        } else {
            FetchRequestBytesLeft = 0;
        }
        FetchRequestCurrentReadTablet = 0;
        if (!Response) {
            Response = MakeHolder<TEvPQ::TEvFetchResponse>();
        }

        AFL_ENSURE(FetchRequestReadsDone < Settings.Partitions.size());
        const auto& req = Settings.Partitions[CurrentCookie];
        const auto& topic = req.Topic;
        const auto& partitionId = req.Partition;

        auto res = Response->Response.AddPartResult();
        auto privateTopicToCdcIt = PrivateTopicPathToCdcPath.find(topic);
        if (privateTopicToCdcIt == PrivateTopicPathToCdcPath.end()) {
            res->SetTopic(topic);
        } else {
            res->SetTopic(PrivateTopicPathToCdcPath[topic]);
        }

        res->SetPartition(partitionId);
        auto read = res->MutableReadResult();
        if (record.HasPartitionResponse() && record.GetPartitionResponse().HasCmdReadResult())
            read->CopyFrom(record.GetPartitionResponse().GetCmdReadResult());
        if (record.HasErrorCode())
            read->SetErrorCode(record.GetErrorCode());
        if (record.HasErrorReason())
            read->SetErrorReason(record.GetErrorReason());

        ++FetchRequestReadsDone;

        auto it = TopicInfo.find(CanonizePath(topic));
        AFL_ENSURE(it != TopicInfo.end());

        SetMeteringMode(it->second.PQInfo->Description.GetPQTabletConfig().GetMeteringMode());

        if (FetchRequestBytesLeft == 0) {
            SendReplyOnDoneAndDie(ctx);
        } else if (IsQuotaRequired()) {
            PendingQuotaAmount = CalcRuConsumption(GetPayloadSize(record)) + (Settings.RuPerRequest ? 1 : 0);
            Settings.RuPerRequest = false;
            RequestDataQuota(PendingQuotaAmount, ctx);
        } else {
            ProceedFetchRequest(ctx);
        }
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

    bool CheckAccess(const TSecurityObject& access) {
        if (Settings.User == nullptr)
            return true;
        return access.CheckAccess(NACLib::EAccessRights::SelectRow, *Settings.User);
    }

    void SendReplyOnDoneAndDie(const TActorContext& ctx) {
        // TODO дополнить недостающие партиции
        SendReplyAndDie(std::move(Response), ctx);
    }

     void SendReplyAndDie(THolder<TEvPQ::TEvFetchResponse> event, const TActorContext& ctx) {
        ctx.Send(RequesterId, event.Release());
        Die(ctx);
    }

    THolder<TEvPQ::TEvFetchResponse> CreateErrorReply(Ydb::StatusIds::StatusCode status, const TString& message) {
        auto response = MakeHolder<TEvPQ::TEvFetchResponse>();
        response->Status = status;
        response->Message = message;
        return response;
    }

    void CreateOkResponse() {
        if (!Response) {
            Response = MakeHolder<TEvPQ::TEvFetchResponse>();
        }
        Response->Status = Ydb::StatusIds::SUCCESS;
    }

    STRICT_STFUNC(StateFunc,
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleSchemeCacheResponse);

            HFunc(TEvPersQueue::TEvHasDataInfoResponse, Handle);
            HFunc(TEvPersQueue::TEvResponse, Handle);

            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);

            HFunc(TEvents::TEvWakeup, Handle);
            CFunc(NActors::TEvents::TSystem::PoisonPill, Die);
    )
};

NActors::IActor* CreatePQFetchRequestActor(
    const TFetchRequestSettings& settings,
    const TActorId& schemeCache,
    const TActorId& requester
) {
    return new TPQFetchRequestActor(settings, schemeCache, requester);
}

} // namespace NKikimr::NPQ
