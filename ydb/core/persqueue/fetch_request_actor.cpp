#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>

#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/msgbus.pb.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/persqueue/pq_rl_helpers.h>
#include <ydb/core/persqueue/user_info.h>
#include <ydb/core/persqueue/write_meta.h>

#include <ydb/public/lib/base/msgbus_status.h>

#include "events/global.h"
#include "events/internal.h"
#include "fetch_request_actor.h"

namespace NKikimr::NPQ {

using namespace NMsgBusProxy;
using namespace NSchemeCache;


namespace {
    static constexpr TDuration DefaultTimeout = TDuration::MilliSeconds(30000);
}

struct TTabletInfo { // ToDo !! remove
    ui32 NodeId = 0;
    TString Topic;
    TActorId PipeClient;
    bool BrokenPipe = false;
    bool IsBalancer = false;
    TVector<NKikimrPQ::TOffsetsResponse::TPartResult> OffsetResponses;
    TVector<NKikimrPQ::TStatusResponse::TPartResult> StatusResponses;
};

struct TTopicInfo {
    TVector<ui64> Tablets;
    THashMap<ui32, ui64> PartitionToTablet;
    ui64 BalancerTabletId = 0;

    NKikimrPQ::TPQTabletConfig Config;
    TIntrusiveConstPtr<TSchemeCacheNavigate::TPQGroupInfo> PQInfo;
    NPersQueue::TDiscoveryConverterPtr Converter;
    ui32 NumParts = 0;
    THashSet<ui32> PartitionsToRequest;

    //fetchRequest part
    THashMap<ui32, TAutoPtr<TEvPersQueue::TEvHasDataInfo>> FetchInfo;
};


using namespace NActors;

class TPQFetchRequestActor : public TActorBootstrapped<TPQFetchRequestActor>
                           , private TRlHelpers {

struct TEvPrivate {
    enum EEv {
        EvTimeout = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvTimeout : NActors::TEventLocal<TEvTimeout, EvTimeout> {
    };

};

private:
    TFetchRequestSettings Settings;

    bool CanProcessFetchRequest; //any partitions answered that it has data or WaitMs timeout occured
    ui32 FetchRequestReadsDone;
    ui64 FetchRequestCurrentReadTablet;
    ui64 CurrentCookie;
    ui32 FetchRequestBytesLeft;
    THolder<TEvPQ::TEvFetchResponse> Response;
    TVector<TActorId> PQClient;
    const TActorId SchemeCache;

    THashMap<TString, TTopicInfo> TopicInfo;
    THashMap<ui64, TTabletInfo> TabletInfo;

    ui32 TopicsAnswered;
    THashSet<ui64> TabletsDiscovered;
    THashSet<ui64> TabletsAnswered;
    ui32 PartTabletsRequested;
    TString ErrorReason;
    TActorId RequesterId;
    ui64 PendingQuotaAmount;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PQ_FETCH_REQUEST;
    }

    TPQFetchRequestActor(const TFetchRequestSettings& settings, const TActorId& schemeCacheId, const TActorId& requesterId)
        : TRlHelpers({}, settings.RlCtx, 8_KB, false, TDuration::Seconds(1))
        , Settings(settings)
        , CanProcessFetchRequest(false)
        , FetchRequestReadsDone(0)
        , FetchRequestCurrentReadTablet(0)
        , CurrentCookie(1)
        , FetchRequestBytesLeft(0)
        , SchemeCache(schemeCacheId)
        , TopicsAnswered(0)
        , PartTabletsRequested(0)
        , RequesterId(requesterId)
    {
        ui64 deadline = TAppData::TimeProvider->Now().MilliSeconds() + Min<ui32>(Settings.MaxWaitTimeMs, 30000);
        FetchRequestBytesLeft = Settings.TotalMaxBytes;
        for (const auto& p : Settings.Partitions) {
            if (p.Topic.empty()) {
                Response = CreateErrorReply(Ydb::StatusIds::BAD_REQUEST, "Empty topic in fetch request");
                return;
            } if (!p.MaxBytes) {
                Response = CreateErrorReply(Ydb::StatusIds::BAD_REQUEST, "No maxBytes for partition in fetch request");
                return;
            }
            auto path = CanonizePath(p.Topic);
            bool res = TopicInfo[path].PartitionsToRequest.insert(p.Partition).second;
            if (!res) {
                Response = CreateErrorReply(Ydb::StatusIds::BAD_REQUEST, "Some partition specified multiple times in fetch request");
                return;
            }
            TAutoPtr<TEvPersQueue::TEvHasDataInfo> fetchInfo(new TEvPersQueue::TEvHasDataInfo());
            fetchInfo->Record.SetPartition(p.Partition);
            fetchInfo->Record.SetOffset(p.Offset);
            fetchInfo->Record.SetDeadline(deadline);
            TopicInfo[path].FetchInfo[p.Partition] = fetchInfo;
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
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
        LOG_INFO_S(ctx, NKikimrServices::PQ_FETCH_REQUEST, "Fetch request actor boostrapped. Request is valid: " << (!Response));

        // handle error from constructor
        if (Response) {
            return SendReplyAndDie(std::move(Response), ctx);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_FETCH_REQUEST, "scheduling HasDataInfoResponse in " << Settings.MaxWaitTimeMs);
        ctx.Schedule(TDuration::MilliSeconds(Min<ui32>(Settings.MaxWaitTimeMs, 30000)), new TEvPersQueue::TEvHasDataInfoResponse);

        SendSchemeCacheRequest(ctx);
        Schedule(DefaultTimeout, new TEvPrivate::TEvTimeout());
        Become(&TPQFetchRequestActor::StateFunc);
    }

    void SendSchemeCacheRequest(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_FETCH_REQUEST, "SendSchemeCacheRequest");

        auto schemeCacheRequest = std::make_unique<TSchemeCacheNavigate>(1);
        schemeCacheRequest->DatabaseName = Settings.Database;

        THashSet<TString> topicsRequested;
        for (const auto& part : Settings.Partitions) {
            auto ins = topicsRequested.insert(part.Topic).second;
            if (!ins)
                continue;
            auto split = NKikimr::SplitPath(part.Topic);
            TSchemeCacheNavigate::TEntry entry;
            entry.Path.insert(entry.Path.end(), split.begin(), split.end());

            entry.SyncVersion = true;
            entry.ShowPrivatePath = false;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;

            schemeCacheRequest->ResultSet.emplace_back(std::move(entry));
        }

        ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.release()));
    }

    void HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_FETCH_REQUEST, "Handle SchemeCache response");
        auto& result = ev->Get()->Request;
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

            auto& description = entry.PQGroupInfo->Description;
            auto& topicInfo = TopicInfo[path];
            topicInfo.BalancerTabletId = description.GetBalancerTabletID();
            topicInfo.PQInfo = entry.PQGroupInfo;
        }
        for (auto& p: TopicInfo) {
            ProcessMetadata(p.first, p.second, ctx);
        }

    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
        return ProcessFetchRequestResult(ev, ctx);
    }

    void Handle(TEvPersQueue::TEvHasDataInfoResponse::TPtr&, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "got HasDatainfoResponse");
        ProceedFetchRequest(ctx);
    }

    void ProcessMetadata(const TString& name, TTopicInfo& info, const TActorContext& ctx) {
        const auto& pqDescr = info.PQInfo->Description;

        ++TopicsAnswered;
        auto it = TopicInfo.find(name);
        Y_ABORT_UNLESS(it != TopicInfo.end(), "topic '%s'", name.c_str());
        it->second.Config = pqDescr.GetPQTabletConfig();
        it->second.Config.SetVersion(pqDescr.GetAlterVersion());
        it->second.NumParts = pqDescr.PartitionsSize();
        Y_ABORT_UNLESS(it->second.BalancerTabletId);

        for (ui32 i = 0; i < pqDescr.PartitionsSize(); ++i) {
            ui32 part = pqDescr.GetPartitions(i).GetPartitionId();
            ui64 tabletId = pqDescr.GetPartitions(i).GetTabletId();
            if (!it->second.PartitionsToRequest.contains(part)) {
                continue;
            }
            bool res = it->second.PartitionToTablet.insert({part, tabletId}).second;
            Y_ABORT_UNLESS(res);
            if (TabletInfo.find(tabletId) == TabletInfo.end()) {
                auto& tabletInfo = TabletInfo[tabletId];
                tabletInfo.Topic = name;
                it->second.Tablets.push_back(tabletId);
                    // Tablet node resolution relies on opening a pipe

                auto retryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
                retryPolicy.RetryLimitCount = 5;
                NTabletPipe::TClientConfig clientConfig(retryPolicy);

                TActorId pipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
                tabletInfo.PipeClient = pipeClient;
                PQClient.push_back(pipeClient);
                ++PartTabletsRequested;
            }
            if (CanProcessFetchRequest) {
                ProceedFetchRequest(ctx);
            } else {
                const auto& tabletInfo = TabletInfo[tabletId];
                auto& fetchInfo = it->second.FetchInfo[part];
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "sending HasDataInfoResponse " << fetchInfo->Record.DebugString());

                NTabletPipe::SendData(ctx, tabletInfo.PipeClient, it->second.FetchInfo[part].Release());
                ++PartTabletsRequested;
            }
        }
        if (!it->second.PartitionsToRequest.empty() && it->second.PartitionsToRequest.size() != it->second.PartitionToTablet.size()) {
            auto reason = TStringBuilder() << "no one of requested partitions in topic " << name << ", Marker# PQ12";
            return SendReplyAndDie(CreateErrorReply(Ydb::StatusIds::BAD_REQUEST, reason), ctx);
        }
        Y_ABORT_UNLESS(!TabletInfo.empty()); // if TabletInfo is empty - topic is empty
    }

    bool HandlePipeError(const ui64 tabletId, const TActorContext& ctx)
    {
        auto it = TabletInfo.find(tabletId);
        if (it != TabletInfo.end()) {
            it->second.BrokenPipe = true;
            if (FetchRequestCurrentReadTablet == tabletId) {
                //fail current read
                ctx.Send(ctx.SelfID, FormEmptyCurrentRead(CurrentCookie).Release());
            }
            return true;
        }
        return false;
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        const ui64 tabletId = ev->Get()->TabletId;
        if (msg->Status != NKikimrProto::OK) {

            if (HandlePipeError(tabletId, ctx))
                return;

            auto reason = TStringBuilder() << "Client pipe to " << tabletId << " connection error, Status"
                                                           << NKikimrProto::EReplyStatus_Name(msg->Status).data()
                                                           << ", Marker# PQ6";
            return SendReplyAndDie(CreateErrorReply(Ydb::StatusIds::INTERNAL_ERROR, reason), ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        ui64 tabletId = ev->Get()->TabletId;
        if (HandlePipeError(tabletId, ctx))
            return;

        auto reason = TStringBuilder() << "Client pipe to " << tabletId << " destroyed (connection lost), Marker# PQ7";
        SendReplyAndDie(CreateErrorReply(Ydb::StatusIds::INTERNAL_ERROR, reason), ctx);
    }

    void HandleTimeout(const TActorContext& ctx) {
        TString reason = "Timeout while waiting for response, may be just slow, Marker# PQ11";
        return SendReplyAndDie(CreateErrorReply(Ydb::StatusIds::GENERIC_ERROR, reason), ctx);
    }

    void Die(const TActorContext& ctx) override {
        for (auto& actor: PQClient) {
            NTabletPipe::CloseClient(ctx, actor);
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

    void ProceedFetchRequest(const TActorContext& ctx) {
        if (FetchRequestCurrentReadTablet) { //already got active read request
            return;
        }
        CanProcessFetchRequest = true;

        if (FetchRequestReadsDone == Settings.Partitions.size()) {
            CreateOkResponse();
            return SendReplyAndDie(std::move(Response), ctx);
        }
        Y_ABORT_UNLESS(FetchRequestReadsDone < Settings.Partitions.size());
        const auto& req = Settings.Partitions[FetchRequestReadsDone];
        const auto& topic = req.Topic;
        const auto& offset = req.Offset;
        const auto& part = req.Partition;
        const auto& maxBytes = req.MaxBytes;
        const auto& readTimestampMs = req.ReadTimestampMs;
        const auto& clientId = req.ClientId;
        auto it = TopicInfo.find(CanonizePath(topic));
        Y_ABORT_UNLESS(it != TopicInfo.end());
        if (it->second.PartitionToTablet.find(part) == it->second.PartitionToTablet.end()) {
            return;
        }
        ui64 tabletId = it->second.PartitionToTablet[part];
        Y_ABORT_UNLESS(tabletId);
        FetchRequestCurrentReadTablet = tabletId;
        ++CurrentCookie;
        auto jt = TabletInfo.find(tabletId);
        Y_ABORT_UNLESS(jt != TabletInfo.end());
        if (jt->second.BrokenPipe || FetchRequestBytesLeft == 0) { //answer right now
            ctx.Send(ctx.SelfID, FormEmptyCurrentRead(CurrentCookie).Release());
            return;
        }

        //Form read request
        TAutoPtr<TEvPersQueue::TEvRequest> preq(new TEvPersQueue::TEvRequest);
        TStringBuilder reqId;
        reqId << "request" << "-id-" << FetchRequestReadsDone << "-" << Settings.Partitions.size();
        preq->Record.SetRequestId(reqId);
        auto partReq = preq->Record.MutablePartitionRequest();
        partReq->SetCookie(CurrentCookie);

        partReq->SetTopic(topic);
        partReq->SetPartition(part);
        auto read = partReq->MutableCmdRead();
        read->SetClientId(clientId);
        read->SetOffset(offset);
        read->SetCount(1000000);
        read->SetTimeoutMs(0);
        read->SetBytes(Min<ui32>(maxBytes, FetchRequestBytesLeft));
        read->SetReadTimestampMs(readTimestampMs);
        read->SetExternalOperation(true);
        NTabletPipe::SendData(ctx, jt->second.PipeClient, preq.Release());
    }

    void ProcessFetchRequestResult(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasPartitionResponse());
        if (record.GetPartitionResponse().GetCookie() != CurrentCookie || FetchRequestCurrentReadTablet == 0) {
            LOG_ERROR_S(ctx, NKikimrServices::PQ_FETCH_REQUEST, "proxy fetch error: got response from tablet " << record.GetPartitionResponse().GetCookie()
                                << " while waiting from " << CurrentCookie << " and requested tablet is " << FetchRequestCurrentReadTablet);
            return;
        }


        if (FetchRequestBytesLeft >= (ui32)record.ByteSize())
            FetchRequestBytesLeft -= (ui32)record.ByteSize();
        else
            FetchRequestBytesLeft = 0;
        FetchRequestCurrentReadTablet = 0;
        if (!Response) {
            Response = MakeHolder<TEvPQ::TEvFetchResponse>();
        }

        auto res = Response->Response.AddPartResult();
        Y_ABORT_UNLESS(FetchRequestReadsDone < Settings.Partitions.size());
        const auto& req = Settings.Partitions[FetchRequestReadsDone];
        const auto& topic = req.Topic;
        const auto& part = req.Partition;

        res->SetTopic(topic);
        res->SetPartition(part);
        auto read = res->MutableReadResult();
        if (record.HasPartitionResponse() && record.GetPartitionResponse().HasCmdReadResult())
            read->CopyFrom(record.GetPartitionResponse().GetCmdReadResult());
        if (record.HasErrorCode())
            read->SetErrorCode(record.GetErrorCode());
        if (record.HasErrorReason())
            read->SetErrorReason(record.GetErrorReason());

        ++FetchRequestReadsDone;

        auto it = TopicInfo.find(CanonizePath(topic));
        Y_ABORT_UNLESS(it != TopicInfo.end());

        SetMeteringMode(it->second.PQInfo->Description.GetPQTabletConfig().GetMeteringMode());

        if (IsQuotaRequired()) {
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
                readBytesSize += proto.GetData().Size();
            }
        }
        return readBytesSize;
    }

    bool CheckAccess(const TSecurityObject& access) {
        if (!Settings.User.Defined())
            return true;
        return access.CheckAccess(NACLib::EAccessRights::SelectRow, Settings.User.GetRef());
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
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvPersQueue::TEvResponse, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleSchemeCacheResponse);
            HFunc(TEvPersQueue::TEvHasDataInfoResponse, Handle);
            CFunc(TEvPrivate::EvTimeout, HandleTimeout);
            CFunc(NActors::TEvents::TSystem::PoisonPill, Die);
    )
};

NActors::IActor* CreatePQFetchRequestActor(
        const TFetchRequestSettings& settings, const TActorId& schemeCache, const TActorId& requester
) {
    return new TPQFetchRequestActor(settings, schemeCache, requester);
}

} // namespace NKikimr::NPQ
