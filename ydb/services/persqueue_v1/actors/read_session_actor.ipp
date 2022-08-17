#ifndef READ_SESSION_ACTOR_IMPL
#error "Do not include this file directly"
#endif

#include "read_init_auth_actor.h"
#include "helpers.h"

#include <ydb/library/persqueue/topic_parser/counters.h>

#include <library/cpp/protobuf/util/repeated_field_utils.h>

#include <contrib/libs/protobuf_std/src/google/protobuf/util/time_util.h>

#include <util/string/join.h>
#include <util/string/strip.h>
#include <util/charset/utf8.h>

#include <utility>

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr {

using namespace NMsgBusProxy;

namespace NGRpcProxy::V1 {

using namespace PersQueue::V1;

//TODO: add here tracking of bytes in/out

template<bool UseMigrationProtocol>
TReadSessionActor<UseMigrationProtocol>::TReadSessionActor(
        TEvStreamPQReadRequest* request, const ui64 cookie, const TActorId& schemeCache, const TActorId& newSchemeCache,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const TMaybe<TString> clientDC,
        const NPersQueue::TTopicsListController& topicsHandler
)
    : Request(request)
    , ClientDC(clientDC ? *clientDC : "other")
    , StartTimestamp(TInstant::Now())
    , SchemeCache(schemeCache)
    , NewSchemeCache(newSchemeCache)
    , AuthInitActor()
    , ClientId()
    , ClientPath()
    , Session()
    , CommitsDisabled(false)
    , BalancersInitStarted(false)
    , InitDone(false)
    , MaxReadMessagesCount(0)
    , MaxReadSize(0)
    , MaxTimeLagMs(0)
    , ReadTimestampMs(0)
    , ForceACLCheck(false)
    , RequestNotChecked(true)
    , LastACLCheckTimestamp(TInstant::Zero())
    , NextAssignId(1)
    , ReadOnlyLocal(false)
    , Cookie(cookie)
    , Counters(counters)
    , BytesInflight_(0)
    , RequestedBytes(0)
    , ReadsInfly(0)
    , TopicsHandler(topicsHandler)
{
    Y_ASSERT(Request);
}


template<bool UseMigrationProtocol>
TReadSessionActor<UseMigrationProtocol>::~TReadSessionActor() = default;


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Bootstrap(const TActorContext& ctx) {
    Y_VERIFY(Request);
    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        ++(*GetServiceCounters(Counters, "pqproxy|readSession")
           ->GetNamedCounter("sensor", "SessionsCreatedTotal", true));
    }

    Request->GetStreamCtx()->Attach(ctx.SelfID);
    if (!Request->GetStreamCtx()->Read()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
        Die(ctx);
        return;
    }
    StartTime = ctx.Now();

    TReadSessionActor<UseMigrationProtocol>::Become(&TReadSessionActor<UseMigrationProtocol>::TThis::StateFunc);
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::HandleDone(const TActorContext& ctx) {

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc closed");
    Die(ctx);
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(typename IContext::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {

    auto& request = ev->Get()->Record;

    if constexpr (UseMigrationProtocol) {
        auto token = request.token();
        request.set_token("");

        if (!token.empty()) { //TODO refreshtoken here
            ctx.Send(ctx.SelfID, new TEvPQProxy::TEvAuth(token));
        }
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY,
                PQ_LOG_PREFIX << " grpc read done: success: " << ev->Get()->Success << " data: " << request);

    if (!ev->Get()->Success) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed");
        ctx.Send(ctx.SelfID, new TEvPQProxy::TEvDone());
        return;
    }
    auto converterFactory = TopicsHandler.GetConverterFactory();
    auto MakePartitionId = [&](auto& request) {
        const ui64 id = [&request](){
            if constexpr (UseMigrationProtocol) {
                return request.assign_id();
            } else {
                return request.partition_session_id();
            }
        }();
        Y_VERIFY(Partitions.find(id) != Partitions.end());

        const auto& info = Partitions.at(id);
        auto topic = info.Topic->GetFederationPath();
        const auto& cluster = info.Topic->GetCluster();
        ui64 partition = info.Partition.Partition;

        auto converter = converterFactory->MakeDiscoveryConverter(
                std::move(topic), {}, cluster, Request->GetDatabaseName().GetOrElse(TString())
        );

        return TPartitionId{converter, partition, id};
    };


#define GET_PART_ID_OR_EXIT(request)             \
auto partId = MakePartitionId(request);      \
if (!partId.DiscoveryConverter->IsValid()) { \
    CloseSession(TStringBuilder() << "Invalid topic in request: " << partId.DiscoveryConverter->GetOriginalTopic() \
                                  << ", reason: " << partId.DiscoveryConverter->GetReason(),                       \
                 PersQueue::ErrorCode::BAD_REQUEST, ctx);                                                          \
    return;                                  \
}

    if constexpr (UseMigrationProtocol) {
        switch (request.request_case()) {
            case TClientMessage::kInitRequest: {
                ctx.Send(ctx.SelfID, new TEvReadInit(request, Request->GetStreamCtx()->GetPeerName()));
                break;
            }
            case TClientMessage::kStatus: {
                //const auto& req = request.status();
                GET_PART_ID_OR_EXIT(request.status());
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvGetStatus(partId));
                if (!Request->GetStreamCtx()->Read()) {
                    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
                    Die(ctx);
                    return;
                }
                break;

            }
            case TClientMessage::kRead: {
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvRead()); // Proto read message have no parameters
                break;
            }
            case TClientMessage::kReleased: {
                GET_PART_ID_OR_EXIT(request.released());
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvReleased(partId));
                if (!Request->GetStreamCtx()->Read()) {
                    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
                    Die(ctx);
                    return;
                }
                break;

            }
            case TClientMessage::kStartRead: {
                const auto& req = request.start_read();

                const ui64 readOffset = req.read_offset();
                const ui64 commitOffset = req.commit_offset();
                const bool verifyReadOffset = req.verify_read_offset();

                GET_PART_ID_OR_EXIT(request.start_read());
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvStartRead(partId, readOffset, commitOffset, verifyReadOffset));
                if (!Request->GetStreamCtx()->Read()) {
                    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
                    Die(ctx);
                    return;
                }
                break;
            }
            case TClientMessage::kCommit: {
                const auto& req = request.commit();

                if (!req.cookies_size() && !RangesMode) {
                    CloseSession(TStringBuilder() << "can't commit without cookies", PersQueue::ErrorCode::BAD_REQUEST, ctx);
                    return;
                }
                if (RangesMode && !req.offset_ranges_size()) {
                    CloseSession(TStringBuilder() << "can't commit without offsets", PersQueue::ErrorCode::BAD_REQUEST, ctx);
                    return;

                }

                THashMap<ui64, TEvPQProxy::TCommitCookie> commitCookie;
                THashMap<ui64, TEvPQProxy::TCommitRange> commitRange;

                for (auto& c: req.cookies()) {
                    commitCookie[c.assign_id()].Cookies.push_back(c.partition_cookie());
                }
                for (auto& c: req.offset_ranges()) {
                    commitRange[c.assign_id()].Ranges.push_back(std::make_pair(c.start_offset(), c.end_offset()));
                }

                for (auto& c : commitCookie) {
                    ctx.Send(ctx.SelfID, new TEvPQProxy::TEvCommitCookie(c.first, std::move(c.second)));
                }

                for (auto& c : commitRange) {
                    ctx.Send(ctx.SelfID, new TEvPQProxy::TEvCommitRange(c.first, std::move(c.second)));
                }

                if (!Request->GetStreamCtx()->Read()) {
                    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
                    Die(ctx);
                    return;
                }
                break;
            }

            default: {
                CloseSession(TStringBuilder() << "unsupported request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
                break;
            }
        }
    } else {
        switch(request.client_message_case()) {
            case TClientMessage::kInitRequest: {
                ctx.Send(ctx.SelfID, new TEvReadInit(request, Request->GetStreamCtx()->GetPeerName()));
                break;
            }
            case TClientMessage::kReadRequest: {
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvRead()); // Proto read message have no parameters
                break;
            }
            case TClientMessage::kPartitionSessionStatusRequest: {
                GET_PART_ID_OR_EXIT(request.partition_session_status_request());
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvGetStatus(partId));
                if (!Request->GetStreamCtx()->Read()) {
                    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
                    Die(ctx);
                    return;
                }
                break;

            }
            case TClientMessage::kStopPartitionSessionResponse: {
                GET_PART_ID_OR_EXIT(request.stop_partition_session_response());
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvReleased(partId));
                if (!Request->GetStreamCtx()->Read()) {
                    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
                    Die(ctx);
                    return;
                }
                break;

            }
            case TClientMessage::kStartPartitionSessionResponse: {
                const auto& req = request.start_partition_session_response();

                const ui64 readOffset = req.read_offset();
                const ui64 commitOffset = req.commit_offset();

                GET_PART_ID_OR_EXIT(req);
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvStartRead(partId, readOffset, commitOffset, req.has_read_offset()));
                if (!Request->GetStreamCtx()->Read()) {
                    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
                    Die(ctx);
                    return;
                }
                break;
            }
            case TClientMessage::kCommitOffsetRequest: {
                const auto& req = request.commit_offset_request();

                if (!RangesMode || !req.commit_offsets_size()) {
                    CloseSession(TStringBuilder() << "can't commit without offsets", PersQueue::ErrorCode::BAD_REQUEST, ctx);
                    return;
                }

                THashMap<ui64, TEvPQProxy::TCommitRange> commitRange;

                for (auto& pc: req.commit_offsets()) {
                    auto id = pc.partition_session_id();
                    for (auto& c: pc.offsets()) {
                        commitRange[id].Ranges.push_back(std::make_pair(c.start(), c.end()));
                    }
                }

                for (auto& c : commitRange) {
                    ctx.Send(ctx.SelfID, new TEvPQProxy::TEvCommitRange(c.first, std::move(c.second)));
                }

                if (!Request->GetStreamCtx()->Read()) {
                    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
                    Die(ctx);
                    return;
                }
                break;
            }
            case TClientMessage::kUpdateTokenRequest: {
                auto token = request.update_token_request().token();
                if (!token.empty()) { //TODO refreshtoken here
                    ctx.Send(ctx.SelfID, new TEvPQProxy::TEvAuth(token));
                }
                break;
            }

            default: {
                CloseSession(TStringBuilder() << "unsupported request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
                break;
            }
        }
    }
}

#undef GET_PART_ID_OR_EXIT


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(typename IContext::TEvWriteFinished::TPtr& ev, const TActorContext& ctx) {
    if (!ev->Get()->Success) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
        Die(ctx);
    }
    Y_VERIFY(!ActiveWrites.empty());
    ui64 sz = ActiveWrites.front();
    ActiveWrites.pop();
    Y_VERIFY(BytesInflight_ >= sz);
    BytesInflight_ -= sz;
    if (BytesInflight) (*BytesInflight) -= sz;

    ProcessReads(ctx);
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Die(const TActorContext& ctx) {

    ctx.Send(AuthInitActor, new TEvents::TEvPoisonPill());

    for (auto& p : Partitions) {
        ctx.Send(p.second.Actor, new TEvents::TEvPoisonPill());

        if (!p.second.Released) {
            // ToDo[counters]
            auto it = TopicCounters.find(p.second.Topic->GetInternalName());
            Y_VERIFY(it != TopicCounters.end());
            it->second.PartitionsInfly.Dec();
            it->second.PartitionsReleased.Inc();
            if (p.second.Releasing)
                it->second.PartitionsToBeReleased.Dec();
        }
    }

    for (auto& t : Topics) {
        if (t.second.PipeClient)
            NTabletPipe::CloseClient(ctx, t.second.PipeClient);
    }
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " is DEAD");

    if (BytesInflight) {
        (*BytesInflight) -= BytesInflight_;
    }
    if (SessionsActive) {
        --(*SessionsActive);
    }
    if (SessionsActive) {
        PartsPerSession.DecFor(Partitions.size(), 1);
    }

    ctx.Send(GetPQReadServiceActorID(), new TEvPQProxy::TEvSessionDead(Cookie));

    TActorBootstrapped<TReadSessionActor>::Die(ctx);
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvDone::TPtr&, const TActorContext& ctx) {
    CloseSession(TStringBuilder() << "Reads done signal - closing everything", PersQueue::ErrorCode::OK, ctx);
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvCommitCookie::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    if (CommitsDisabled) {
        CloseSession("commits in session are disabled by client option", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    const ui64& assignId = ev->Get()->AssignId;
    auto it = Partitions.find(assignId);
    if (it == Partitions.end()) //stale commit - ignore it
        return;

    for (auto& c : ev->Get()->CommitInfo.Cookies) {
        if(RangesMode) {
            CloseSession("Commits cookies in ranges commit mode is illegal", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        it->second.NextCommits.insert(c);
    }

    ctx.Send(it->second.Actor, new TEvPQProxy::TEvCommitCookie(ev->Get()->AssignId, std::move(ev->Get()->CommitInfo)));
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvCommitRange::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    if (CommitsDisabled) {
        CloseSession("commits in session are disabled by client option", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    const ui64& assignId = ev->Get()->AssignId;
    auto it = Partitions.find(assignId);
    if (it == Partitions.end()) //stale commit - ignore it
        return;

    for (auto& c : ev->Get()->CommitInfo.Ranges) {
        if(!RangesMode) {
            CloseSession("Commits ranges in cookies commit mode is illegal", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        if (c.first >= c.second || it->second.NextRanges.Intersects(c.first, c.second) || c.first < it->second.Offset) {
            CloseSession(TStringBuilder() << "Offsets range [" << c.first << ", " << c.second << ") has already committed offsets, double committing is forbiden; or incorrect", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;

        }
        it->second.NextRanges.InsertInterval(c.first, c.second);
    }

    ctx.Send(it->second.Actor, new TEvPQProxy::TEvCommitRange(ev->Get()->AssignId, std::move(ev->Get()->CommitInfo)));
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvAuth::TPtr& ev, const TActorContext& ctx) {
    ProcessAuth(ev->Get()->Auth, ctx);
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvStartRead::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    auto it = Partitions.find(ev->Get()->Partition.AssignId);
    if (it == Partitions.end()) {
        return;
    }

    if (it == Partitions.end() || it->second.Releasing) {
        //do nothing - already released partition
        LOG_WARN_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got NOTACTUAL StartRead from client for " << ev->Get()->Partition
                   << " at offset " << ev->Get()->ReadOffset);
        return;
    }
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got StartRead from client for "
               << ev->Get()->Partition <<
               " at readOffset " << ev->Get()->ReadOffset <<
               " commitOffset " << ev->Get()->CommitOffset);

    //proxy request to partition - allow initing
    //TODO: add here VerifyReadOffset too and check it againts Committed position
    ctx.Send(it->second.Actor, new TEvPQProxy::TEvLockPartition(ev->Get()->ReadOffset, ev->Get()->CommitOffset, ev->Get()->VerifyReadOffset, true));
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvReleased::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    auto it = Partitions.find(ev->Get()->Partition.AssignId);
    if (it == Partitions.end()) {
        return;
    }
    if (!it->second.Releasing) {
        CloseSession(TStringBuilder() << "Release of partition that is not requested for release is forbiden for " << it->second.Partition, PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;

    }
    Y_VERIFY(it->second.LockSent);
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got Released from client for " << ev->Get()->Partition);

    ReleasePartition(it, true, ctx);
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvGetStatus::TPtr& ev, const TActorContext& ctx) {
    auto it = Partitions.find(ev->Get()->Partition.AssignId);
    if (it == Partitions.end() || it->second.Releasing) {
        // Ignore request - client asking status after releasing of partition.
        return;
    }
    ctx.Send(it->second.Actor, new TEvPQProxy::TEvGetStatus(ev->Get()->Partition));
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::DropPartition(typename THashMap<ui64, TPartitionActorInfo>::iterator it, const TActorContext& ctx) {
    ctx.Send(it->second.Actor, new TEvents::TEvPoisonPill());
    bool res = ActualPartitionActors.erase(it->second.Actor);
    Y_VERIFY(res);

    if (--NumPartitionsFromTopic[it->second.Topic->GetInternalName()] == 0) {
        //ToDo[counters]
        bool res_ = TopicCounters.erase(it->second.Topic->GetInternalName());
        Y_VERIFY(res_);
    }

    if (SessionsActive) {
        PartsPerSession.DecFor(Partitions.size(), 1);
    }
    BalancerGeneration.erase(it->first);
    Partitions.erase(it);
    if (SessionsActive) {
        PartsPerSession.IncFor(Partitions.size(), 1);
    }
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvCommitDone::TPtr& ev, const TActorContext& ctx) {

    Y_VERIFY(!CommitsDisabled);

    if (!ActualPartitionActor(ev->Sender))
        return;

    ui64 assignId = ev->Get()->AssignId;

    auto it = Partitions.find(assignId);
    Y_VERIFY(it != Partitions.end());
    Y_VERIFY(it->second.Offset < ev->Get()->Offset);
    it->second.NextRanges.EraseInterval(it->second.Offset, ev->Get()->Offset);


    if (ev->Get()->StartCookie == Max<ui64>()) //means commit at start
        return;

    TServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);
    if (!RangesMode) {
        if constexpr (UseMigrationProtocol) {
            for (ui64 i = ev->Get()->StartCookie; i <= ev->Get()->LastCookie; ++i) {
                auto c = result.mutable_committed()->add_cookies();
                c->set_partition_cookie(i);
                c->set_assign_id(assignId);
                it->second.NextCommits.erase(i);
                it->second.ReadIdCommitted = i;
            }
        } else { // commit on cookies not supported in this case
            Y_VERIFY(false);
        }

    } else {
        if constexpr (UseMigrationProtocol) {
            auto c = result.mutable_committed()->add_offset_ranges();
            c->set_assign_id(assignId);
            c->set_start_offset(it->second.Offset);
            c->set_end_offset(ev->Get()->Offset);

        } else {
            auto c = result.mutable_commit_offset_response()->add_partitions_committed_offsets();
            c->set_partition_session_id(assignId);
            c->set_committed_offset(ev->Get()->Offset);
        }
    }

    it->second.Offset = ev->Get()->Offset;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " replying for commits from assignId " << assignId << " from " << ev->Get()->StartCookie << " to " << ev->Get()->LastCookie << " to offset " << it->second.Offset);
    if (!WriteResponse(std::move(result))) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
        Die(ctx);
        return;
    }
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvReadSessionStatus::TPtr& ev, const TActorContext& ctx) {
    THolder<TEvPQProxy::TEvReadSessionStatusResponse> result(new TEvPQProxy::TEvReadSessionStatusResponse());
    for (auto& p : Partitions) {
        auto part = result->Record.AddPartition();
        part->SetTopic(p.second.Partition.DiscoveryConverter->GetPrimaryPath());
        part->SetPartition(p.second.Partition.Partition);
        part->SetAssignId(p.second.Partition.AssignId);
        for (auto& c : p.second.NextCommits) {
            part->AddNextCommits(c);
        }
        part->SetReadIdCommitted(p.second.ReadIdCommitted);
        part->SetLastReadId(p.second.ReadIdToResponse - 1);
        part->SetTimestampMs(p.second.AssignTimestamp.MilliSeconds());
    }
    result->Record.SetSession(Session);
    result->Record.SetTimestamp(StartTimestamp.MilliSeconds());

    result->Record.SetClientNode(PeerName);
    result->Record.SetProxyNodeId(ctx.SelfID.NodeId());

    ctx.Send(ev->Sender, result.Release());
}

inline TString GetTopicSettingsPath(const PersQueue::V1::MigrationStreamingReadClientMessage::TopicReadSettings& settings) {
    return settings.topic();
}
inline TString GetTopicSettingsPath(const Topic::StreamReadMessage::InitRequest::TopicReadSettings& settings) {
    return settings.path();
}
inline i64 GetTopicSettingsReadFrom(const PersQueue::V1::MigrationStreamingReadClientMessage::TopicReadSettings& settings) {
    return settings.start_from_written_at_ms();
}
inline i64 GetTopicSettingsReadFrom(const Topic::StreamReadMessage::InitRequest::TopicReadSettings& settings) {
    return ::google::protobuf::util::TimeUtil::TimestampToMilliseconds(settings.read_from());
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(typename TEvReadInit::TPtr& ev, const TActorContext& ctx) {

    THolder<TEvReadInit> event(ev->Release());

    if (!Topics.empty()) {
        //answer error
        CloseSession("got second init request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    const auto& init = event->Request.init_request();

    if (!init.topics_read_settings_size()) {
        CloseSession("no topics in init request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    if (init.consumer().empty()) {
        CloseSession("no consumer in init request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    ClientId = NPersQueue::ConvertNewConsumerName(init.consumer(), ctx);
    if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        ClientPath = init.consumer();
    } else {
        ClientPath = NPersQueue::StripLeadSlash(NPersQueue::MakeConsumerPath(init.consumer()));
    }

    TStringBuilder session;
    session << ClientPath << "_" << ctx.SelfID.NodeId() << "_" << Cookie << "_" << TAppData::RandomProvider->GenRand64() << "_v1";
    Session = session;
    CommitsDisabled = false;

    if constexpr (UseMigrationProtocol) {
        RangesMode = init.ranges_mode();
        MaxReadMessagesCount = NormalizeMaxReadMessagesCount(init.read_params().max_read_messages_count());
        MaxReadSize = NormalizeMaxReadSize(init.read_params().max_read_size());
        MaxTimeLagMs = init.max_lag_duration_ms();
        ReadTimestampMs = static_cast<ui64>(init.start_from_written_at_ms());
        ReadOnlyLocal = init.read_only_original();
    } else {
        RangesMode = true;
        MaxReadMessagesCount = NormalizeMaxReadMessagesCount(0);
        MaxReadSize = NormalizeMaxReadSize(0);
        MaxTimeLagMs = 0; // max_lag per topic only
        ReadTimestampMs = 0; // read_from per topic only
        // MaxTimeLagMs = ::google::protobuf::util::TimeUtil::DurationToMilliseconds(init.max_lag());
        // ReadTimestampMs = ::google::protobuf::util::TimeUtil::TimestampToMilliseconds(init.read_from());
        ReadOnlyLocal = true;
    }
    if (MaxTimeLagMs < 0) {
        CloseSession("max_lag_duration_ms must be nonnegative number", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    if (ReadTimestampMs < 0) {
        CloseSession("start_from_written_at_ms must be nonnegative number", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    PeerName = event->PeerName;

    for (const auto& topic : init.topics_read_settings()) {
        TString topic_path = GetTopicSettingsPath(topic);
        if (topic_path.empty()) {
            CloseSession("empty topic in init request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        i64 read_from = GetTopicSettingsReadFrom(topic);
        if (read_from < 0) {
            CloseSession("start_from_written_at_ms must be nonnegative number", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        TopicsToResolve.insert(topic_path);
    }

    if (Request->GetInternalToken().empty()) {
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            CloseSession("Unauthenticated access is forbidden, please provide credentials", PersQueue::ErrorCode::ACCESS_DENIED, ctx);
            return;
        }
    } else {
        Y_VERIFY(Request->GetYdbToken());
        Auth = *(Request->GetYdbToken());
        Token = new NACLib::TUserToken(Request->GetInternalToken());
    }
    TopicsList = TopicsHandler.GetReadTopicsList(
            TopicsToResolve, ReadOnlyLocal, Request->GetDatabaseName().GetOrElse(TString())
    );

    if (!TopicsList.IsValid) {
        return CloseSession(
                TopicsList.Reason,
                PersQueue::ErrorCode::BAD_REQUEST, ctx
        );
    }

    for (const auto& topic : init.topics_read_settings()) {
        auto topicIter = TopicsList.ClientTopics.find(GetTopicSettingsPath(topic));
        Y_VERIFY(!topicIter.IsEnd());
        for (const auto& converter: topicIter->second) {
            const auto internalName = converter->GetOriginalPath();
            if constexpr (UseMigrationProtocol) {
                for (i64 pg: topic.partition_group_ids()) {
                    if (pg <= 0) {
                        CloseSession("partition group id must be positive number", PersQueue::ErrorCode::BAD_REQUEST,
                                    ctx);
                        return;
                    }
                    if (pg > Max<ui32>()) {
                        CloseSession(
                            TStringBuilder() << "partition group id is too big: " << pg << " > " << Max<ui32>(),
                            PersQueue::ErrorCode::BAD_REQUEST, ctx);
                        return;
                    }
                    TopicGroups[internalName].push_back(static_cast<ui32>(pg));
                }
                MaxLagByTopic[internalName] = MaxTimeLagMs;
                ReadFromTimestamp[internalName] = GetTopicSettingsReadFrom(topic);
            } else {
                for (i64 p: topic.partition_ids()) {
                    if (p < 0) {
                        CloseSession("partition id must be nonnegative number", PersQueue::ErrorCode::BAD_REQUEST,
                                    ctx);
                        return;
                    }
                    if (p + 1 > Max<ui32>()) {
                        CloseSession(
                                TStringBuilder() << "partition id is too big: " << p << " > " << Max<ui32>() - 1,
                                PersQueue::ErrorCode::BAD_REQUEST, ctx);
                        return;
                    }
                    TopicGroups[internalName].push_back(static_cast<ui32>(p + 1));
                }
                MaxLagByTopic[internalName] =
                    ::google::protobuf::util::TimeUtil::DurationToMilliseconds(topic.max_lag());;
                ReadFromTimestamp[internalName] = GetTopicSettingsReadFrom(topic);
            }
        }
    }
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " init: " << event->Request << " from " << PeerName);


    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        SetupCounters();
    }

    AuthInitActor = ctx.Register(new TReadInitAndAuthActor(
            ctx, ctx.SelfID, ClientId, Cookie, Session, SchemeCache, NewSchemeCache, Counters, Token, TopicsList,
            TopicsHandler.GetLocalCluster()
    ));


    auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");
    Aggr = {{{{"Account", ClientPath.substr(0, ClientPath.find("/"))}}, {"total"}}};

    SLIErrors = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsError"}, true, "sensor", false);
    SLITotal = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsTotal"}, true, "sensor", false);
    SLITotal.Inc();
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::RegisterSession(const TActorId& pipe, const TString& topic, const TVector<ui32>& groups, const TActorContext& ctx)
{

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " register session to " << topic);
    THolder<TEvPersQueue::TEvRegisterReadSession> request;
    request.Reset(new TEvPersQueue::TEvRegisterReadSession);
    auto& req = request->Record;
    req.SetSession(Session);
    req.SetClientNode(PeerName);
    ActorIdToProto(pipe, req.MutablePipeClient());
    req.SetClientId(ClientId);

    for (ui32 i = 0; i < groups.size(); ++i) {
        req.AddGroups(groups[i]);
    }

    NTabletPipe::SendData(ctx, pipe, request.Release());
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::RegisterSessions(const TActorContext& ctx) {
    InitDone = true;

    for (auto& t : Topics) {
        RegisterSession(t.second.PipeClient, t.second.FullConverter->GetInternalName(), t.second.Groups, ctx);
        NumPartitionsFromTopic[t.second.FullConverter->GetInternalName()] = 0;
    }
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::SetupCounters()
{
    if (SessionsCreated) {
        return;
    }

    auto subGroup = GetServiceCounters(Counters, "pqproxy|readSession");
    subGroup = subGroup->GetSubgroup("Client", ClientId)->GetSubgroup("ConsumerPath", ClientPath);
    const TString name = "sensor";

    BytesInflight = subGroup->GetExpiringNamedCounter(name, "BytesInflight", false);
    Errors = subGroup->GetExpiringNamedCounter(name, "Errors", true);
    PipeReconnects = subGroup->GetExpiringNamedCounter(name, "PipeReconnects", true);
    SessionsActive = subGroup->GetExpiringNamedCounter(name, "SessionsActive", false);
    SessionsCreated = subGroup->GetExpiringNamedCounter(name, "SessionsCreated", true);
    PartsPerSession = NKikimr::NPQ::TPercentileCounter(
        subGroup->GetSubgroup(name, "PartsPerSession"),
        {}, {}, "Count",
        TVector<std::pair<ui64, TString>>{{1, "1"}, {2, "2"}, {5, "5"},
                                          {10, "10"}, {20, "20"}, {50, "50"},
                                          {70, "70"}, {100, "100"}, {150, "150"},
                                          {300,"300"}, {99999999, "99999999"}},
        false, true);

    ++(*SessionsCreated);
    ++(*SessionsActive);
    PartsPerSession.IncFor(Partitions.size(), 1); //for 0
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::SetupTopicCounters(const NPersQueue::TTopicConverterPtr& topic)
{
    auto& topicCounters = TopicCounters[topic->GetInternalName()];
    auto subGroup = GetServiceCounters(Counters, "pqproxy|readSession");
//client/consumerPath Account/Producer OriginDC Topic/TopicPath
    auto aggr = NPersQueue::GetLabels(topic);
    TVector<std::pair<TString, TString>> cons = {{"Client", ClientId}, {"ConsumerPath", ClientPath}};

    topicCounters.PartitionsLocked       = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsLocked"}, true);
    topicCounters.PartitionsReleased     = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsReleased"}, true);
    topicCounters.PartitionsToBeReleased = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsToBeReleased"}, false);
    topicCounters.PartitionsToBeLocked   = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsToBeLocked"}, false);
    topicCounters.PartitionsInfly        = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsInfly"}, false);
    topicCounters.Errors                 = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsErrors"}, true);
    topicCounters.Commits                = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"Commits"}, true);
    topicCounters.WaitsForData           = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"WaitsForData"}, true);

    topicCounters.CommitLatency          = CommitLatency;
    topicCounters.SLIBigLatency          = SLIBigLatency;
    topicCounters.SLITotal               = SLITotal;
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::SetupTopicCounters(const NPersQueue::TTopicConverterPtr& topic, const TString& cloudId,
                                           const TString& dbId, const TString& folderId)
{
    auto& topicCounters = TopicCounters[topic->GetInternalName()];
    auto subGroup = NPersQueue::GetCountersForStream(Counters);
//client/consumerPath Account/Producer OriginDC Topic/TopicPath
    auto aggr = NPersQueue::GetLabelsForStream(topic, cloudId, dbId, folderId);
    TVector<std::pair<TString, TString>> cons{{"consumer", ClientPath}};

    topicCounters.PartitionsLocked       = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.partitions_locked_per_second"}, true, "name");
    topicCounters.PartitionsReleased     = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.partitions_released_per_second"}, true, "name");
    topicCounters.PartitionsToBeReleased = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.partitions_to_be_released"}, false, "name");
    topicCounters.PartitionsToBeLocked   = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.partitions_to_be_locked"}, false, "name");
    topicCounters.PartitionsInfly        = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.partitions_locked"}, false, "name");
    topicCounters.Errors                 = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.partitions_errors_per_second"}, true, "name");
    topicCounters.Commits                = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.commits_per_second"}, true, "name");
    topicCounters.WaitsForData           = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.waits_for_data"}, true, "name");

    topicCounters.CommitLatency          = CommitLatency;
    topicCounters.SLIBigLatency          = SLIBigLatency;
    topicCounters.SLITotal               = SLITotal;
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx) {

    LastACLCheckTimestamp = ctx.Now();

    LOG_INFO_S(
            ctx,
            NKikimrServices::PQ_READ_PROXY,
            PQ_LOG_PREFIX << " auth ok, got " << ev->Get()->TopicAndTablets.size() << " topics, init done " << InitDone
    );

    AuthInitActor = TActorId();

    if (!InitDone) {
        ui32 initBorder = AppData(ctx)->PQConfig.GetReadInitLatencyBigMs();
        ui32 readBorder = AppData(ctx)->PQConfig.GetReadLatencyBigMs();
        ui32 readBorderFromDisk = AppData(ctx)->PQConfig.GetReadLatencyFromDiskBigMs();

        auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");
        InitLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "ReadInit", initBorder, {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
        CommitLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "Commit", AppData(ctx)->PQConfig.GetCommitLatencyBigMs(), {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
        SLIBigLatency = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsBigLatency"}, true, "sensor", false);
        ReadLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "Read", readBorder, {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
        ReadLatencyFromDisk = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "ReadFromDisk", readBorderFromDisk, {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
        SLIBigReadLatency = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"ReadBigLatency"}, true, "sensor", false);
        ReadsTotal = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"ReadsTotal"}, true, "sensor", false);

        ui32 initDurationMs = (ctx.Now() - StartTime).MilliSeconds();
        InitLatency.IncFor(initDurationMs, 1);
        if (initDurationMs >= initBorder) {
            SLIBigLatency.Inc();
        }

        TServerMessage result;
        result.set_status(Ydb::StatusIds::SUCCESS);

        result.mutable_init_response()->set_session_id(Session);
        if (!WriteResponse(std::move(result))) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
            Die(ctx);
            return;
        }

        if (!Request->GetStreamCtx()->Read()) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
            Die(ctx);
            return;
        }


        Y_VERIFY(!BalancersInitStarted);
        BalancersInitStarted = true;

        for (auto& [name, t] : ev->Get()->TopicAndTablets) { // ToDo - return something from Init and Auth Actor (Full Path - ?)
            auto internalName = t.TopicNameConverter->GetInternalName();
            auto topicGrIter = TopicGroups.find(name);
            if (!topicGrIter.IsEnd()) {
                auto value = std::move(topicGrIter->second);
                TopicGroups.erase(topicGrIter);
                TopicGroups.insert(std::make_pair(internalName, std::move(value)));
            }
            auto rtfsIter = ReadFromTimestamp.find(name);
            if (!rtfsIter.IsEnd()) {
                auto value = std::move(rtfsIter->second);
                ReadFromTimestamp.erase(rtfsIter);
                ReadFromTimestamp[internalName] = value;
            }
            auto lagIter = MaxLagByTopic.find(name);
            if (!lagIter.IsEnd()) {
                auto value = std::move(lagIter->second);
                MaxLagByTopic.erase(lagIter);
                MaxLagByTopic[internalName] = value;
            }
            auto& topicHolder = Topics[internalName];
            topicHolder.TabletID = t.TabletID;
            topicHolder.FullConverter = t.TopicNameConverter;
            topicHolder.CloudId = t.CloudId;
            topicHolder.DbId = t.DbId;
            topicHolder.FolderId = t.FolderId;
            FullPathToConverter[t.TopicNameConverter->GetPrimaryPath()] = t.TopicNameConverter;
            FullPathToConverter[t.TopicNameConverter->GetSecondaryPath()] = t.TopicNameConverter;
        }

        for (auto& t : Topics) {
            NTabletPipe::TClientConfig clientConfig;

            clientConfig.CheckAliveness = false;

            clientConfig.RetryPolicy = RetryPolicyForPipes;
            t.second.PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, t.second.TabletID, clientConfig));

            Y_VERIFY(t.second.FullConverter);
            auto it = TopicGroups.find(t.second.FullConverter->GetInternalName());
            if (it != TopicGroups.end()) {
                t.second.Groups = it->second;
            }
        }

        RegisterSessions(ctx);

        ctx.Schedule(CHECK_ACL_DELAY, new TEvents::TEvWakeup());
    } else {
        for (auto& [name, t] : ev->Get()->TopicAndTablets) {
            if (Topics.find(t.TopicNameConverter->GetInternalName()) == Topics.end()) {
                CloseSession(
                        TStringBuilder() << "list of topics changed - new topic '"
                                         << t.TopicNameConverter->GetPrintableString() << "' found",
                        PersQueue::ErrorCode::BAD_REQUEST, ctx
                );
                return;
            }
        }
    }
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPersQueue::TEvLockPartition::TPtr& ev, const TActorContext& ctx) {

    auto& record = ev->Get()->Record;
    Y_VERIFY(record.GetSession() == Session);
    Y_VERIFY(record.GetClientId() == ClientId);

    TActorId pipe = ActorIdFromProto(record.GetPipeClient());
    auto path = record.GetPath();
    if (path.empty()) {
        path = record.GetTopic();
    }
    auto converterIter = FullPathToConverter.find(NPersQueue::NormalizeFullPath(path));

    if (converterIter.IsEnd()) {
        LOG_DEBUG_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                PQ_LOG_PREFIX << " ignored ev lock for path = " << record.GetPath() << ", path not recognized"
        );
        return;
    }
    //const auto& topic = converterIter->second->GetPrimaryPath();
    const auto& intName = converterIter->second->GetInternalName();
    auto jt = Topics.find(intName);
    if (jt == Topics.end() || pipe != jt->second.PipeClient) { //this is message from old version of pipe
        LOG_ALERT_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                PQ_LOG_PREFIX << " ignored ev lock for topic = " << intName
                              << " path recognized, but topic is unknown, this is unexpected"
        );
        return;
    }

    //ToDo[counters]
    if (NumPartitionsFromTopic[converterIter->second->GetInternalName()]++ == 0) {
        if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            SetupTopicCounters(converterIter->second, jt->second.CloudId, jt->second.DbId, jt->second.FolderId);
        } else {
            SetupTopicCounters(converterIter->second);
        }
    }

    //ToDo[counters]
    auto it = TopicCounters.find(converterIter->second->GetInternalName());
    Y_VERIFY(it != TopicCounters.end());

    ui64 assignId = NextAssignId++;
    BalancerGeneration[assignId] = {record.GetGeneration(), record.GetStep()};
    TPartitionId partitionId{converterIter->second, record.GetPartition(), assignId};

    IActor* partitionActor = new TPartitionActor(
            ctx.SelfID, ClientId, ClientPath, Cookie, Session, partitionId, record.GetGeneration(),
            record.GetStep(), record.GetTabletId(), it->second, CommitsDisabled, ClientDC, RangesMode,
            converterIter->second, UseMigrationProtocol);

    TActorId actorId = ctx.Register(partitionActor);
    if (SessionsActive) {
        PartsPerSession.DecFor(Partitions.size(), 1);
    }
    Y_VERIFY(record.GetGeneration() > 0);
    auto pp = Partitions.insert(std::make_pair(assignId, TPartitionActorInfo{actorId, partitionId, converterIter->second, ctx}));
    Y_VERIFY(pp.second);
    if (SessionsActive) {
        PartsPerSession.IncFor(Partitions.size(), 1);
    }

    bool res = ActualPartitionActors.insert(actorId).second;
    Y_VERIFY(res);

    it->second.PartitionsLocked.Inc();
    it->second.PartitionsInfly.Inc();

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Assign: " << record);

    ctx.Send(actorId, new TEvPQProxy::TEvLockPartition(0, 0, false, false));
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx) {
    if (!ActualPartitionActor(ev->Sender))
        return;

    auto it = Partitions.find(ev->Get()->Partition.AssignId);
    Y_VERIFY(it != Partitions.end());
    Y_VERIFY(!it->second.Releasing); // if releasing and no lock sent yet - then server must already release partition

    if (ev->Get()->Init) {
        Y_VERIFY(!it->second.LockSent);

        it->second.LockSent = true;
        it->second.Offset = ev->Get()->Offset;

        TServerMessage result;
        result.set_status(Ydb::StatusIds::SUCCESS);

        if constexpr (UseMigrationProtocol) {
            result.mutable_assigned()->mutable_topic()->set_path(it->second.Topic->GetFederationPath());
            result.mutable_assigned()->set_cluster(it->second.Topic->GetCluster());
            result.mutable_assigned()->set_partition(ev->Get()->Partition.Partition);
            result.mutable_assigned()->set_assign_id(it->first);

            result.mutable_assigned()->set_read_offset(ev->Get()->Offset);
            result.mutable_assigned()->set_end_offset(ev->Get()->EndOffset);

        } else {
            // TODO GetFederationPath() -> GetFederationPathWithDC()
            result.mutable_start_partition_session_request()->mutable_partition_session()->set_path(it->second.Topic->GetFederationPath());
            result.mutable_start_partition_session_request()->mutable_partition_session()->set_partition_id(ev->Get()->Partition.Partition);
            result.mutable_start_partition_session_request()->mutable_partition_session()->set_partition_session_id(it->first);

            result.mutable_start_partition_session_request()->set_committed_offset(ev->Get()->Offset);
            result.mutable_start_partition_session_request()->mutable_partition_offsets()->set_start(ev->Get()->Offset);
            result.mutable_start_partition_session_request()->mutable_partition_offsets()->set_end(ev->Get()->EndOffset);
        }

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " sending to client create partition stream event");

        auto pp = it->second.Partition;
        pp.AssignId = 0;
        auto jt = PartitionToControlMessages.find(pp);
        if (jt == PartitionToControlMessages.end()) {
            if (!WriteResponse(std::move(result))) {
                LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
                Die(ctx);
                return;
            }
        } else {
            Y_VERIFY(jt->second.Infly);
            jt->second.ControlMessages.push_back(result);
        }
    } else {
        Y_VERIFY(it->second.LockSent);

        TServerMessage result;
        result.set_status(Ydb::StatusIds::SUCCESS);

        if constexpr(UseMigrationProtocol) {
            result.mutable_partition_status()->mutable_topic()->set_path(it->second.Topic->GetFederationPath());
            result.mutable_partition_status()->set_cluster(it->second.Topic->GetCluster());
            result.mutable_partition_status()->set_partition(ev->Get()->Partition.Partition);
            result.mutable_partition_status()->set_assign_id(it->first);

            result.mutable_partition_status()->set_committed_offset(ev->Get()->Offset);
            result.mutable_partition_status()->set_end_offset(ev->Get()->EndOffset);
            result.mutable_partition_status()->set_write_watermark_ms(ev->Get()->WriteTimestampEstimateMs);

        } else {
            result.mutable_partition_session_status_response()->set_partition_session_id(it->first);

            result.mutable_partition_session_status_response()->set_committed_offset(ev->Get()->Offset);
            result.mutable_partition_session_status_response()->mutable_partition_offsets()->set_start(ev->Get()->Offset);
            result.mutable_partition_session_status_response()->mutable_partition_offsets()->set_end(ev->Get()->EndOffset);
            *result.mutable_partition_session_status_response()->mutable_write_time_high_watermark() =
                ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(ev->Get()->WriteTimestampEstimateMs);
        }

        auto pp = it->second.Partition;
        pp.AssignId = 0;
        auto jt = PartitionToControlMessages.find(pp);
        if (jt == PartitionToControlMessages.end()) {
            if (!WriteResponse(std::move(result))) {
                LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
                Die(ctx);
                return;
            }
        } else {
            Y_VERIFY(jt->second.Infly);
            jt->second.ControlMessages.push_back(result);
        }
    }
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPersQueue::TEvError::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->Record.GetDescription(), ConvertOldCode(ev->Get()->Record.GetCode()), ctx);
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::SendReleaseSignalToClient(const typename THashMap<ui64, TPartitionActorInfo>::iterator& it, bool kill, const TActorContext& ctx)
{
    TServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);

    if constexpr(UseMigrationProtocol) {
        result.mutable_release()->mutable_topic()->set_path(it->second.Topic->GetFederationPath());
        result.mutable_release()->set_cluster(it->second.Topic->GetCluster());
        result.mutable_release()->set_partition(it->second.Partition.Partition);
        result.mutable_release()->set_assign_id(it->second.Partition.AssignId);
        result.mutable_release()->set_forceful_release(kill);
        result.mutable_release()->set_commit_offset(it->second.Offset);

    } else {
        result.mutable_stop_partition_session_request()->set_partition_session_id(it->second.Partition.AssignId);
        result.mutable_stop_partition_session_request()->set_graceful(!kill);
        result.mutable_stop_partition_session_request()->set_committed_offset(it->second.Offset);
    }

    auto pp = it->second.Partition;
    pp.AssignId = 0;
    auto jt = PartitionToControlMessages.find(pp);
    if (jt == PartitionToControlMessages.end()) {
        if (!WriteResponse(std::move(result))) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
            Die(ctx);
            return;
        }
    } else {
        Y_VERIFY(jt->second.Infly);
        jt->second.ControlMessages.push_back(result);
    }
    Y_VERIFY(it->second.LockSent);
    it->second.ReleaseSent = true;
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPersQueue::TEvReleasePartition::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    Y_VERIFY(record.GetSession() == Session);
    Y_VERIFY(record.GetClientId() == ClientId);
    TString topicPath = NPersQueue::NormalizeFullPath(record.GetPath());

    ui32 group = record.HasGroup() ? record.GetGroup() : 0;
    auto pathIter = FullPathToConverter.find(topicPath);
    Y_VERIFY(!pathIter.IsEnd());
    auto it = Topics.find(pathIter->second->GetInternalName());
    Y_VERIFY(!it.IsEnd());
    auto& converter = it->second.FullConverter;

    TActorId pipe = ActorIdFromProto(record.GetPipeClient());

    if (pipe != it->second.PipeClient) { //this is message from old version of pipe
        return;
    }

    for (ui32 c = 0; c < record.GetCount(); ++c) {
        Y_VERIFY(!Partitions.empty());

        TActorId actorId = TActorId{};
        auto jt = Partitions.begin();
        ui32 i = 0;
        for (auto it = Partitions.begin(); it != Partitions.end(); ++it) {
            if (it->second.Topic->GetInternalName() == converter->GetInternalName()
                && !it->second.Releasing
                && (group == 0 || it->second.Partition.Partition + 1 == group)
            ) {
                ++i;
                if (rand() % i == 0) { //will lead to 1/n probability for each of n partitions
                    actorId = it->second.Actor;
                    jt = it;
                }
            }
        }
        Y_VERIFY(actorId);

        {
            //ToDo[counters]
            auto it = TopicCounters.find(converter->GetInternalName());
            Y_VERIFY(it != TopicCounters.end());
            it->second.PartitionsToBeReleased.Inc();
        }

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " releasing " << jt->second.Partition);
        jt->second.Releasing = true;
        if (!jt->second.LockSent) { //no lock yet - can release silently
            ReleasePartition(jt, true, ctx);
        } else {
            SendReleaseSignalToClient(jt, false, ctx);
        }
    }
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx) {
    if (!ActualPartitionActor(ev->Sender))
        return;

    const auto assignId = ev->Get()->Partition.AssignId;

    auto it = Partitions.find(assignId);
    Y_VERIFY(it != Partitions.end());
    Y_VERIFY(it->second.Releasing);

    ReleasePartition(it, false, ctx); //no reads could be here - this is release from partition
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::InformBalancerAboutRelease(const typename THashMap<ui64, TPartitionActorInfo>::iterator& it, const TActorContext& ctx) {

    THolder<TEvPersQueue::TEvPartitionReleased> request;
    request.Reset(new TEvPersQueue::TEvPartitionReleased);
    auto& req = request->Record;

    const auto& converter = it->second.Topic;
    auto jt = Topics.find(converter->GetInternalName());
    Y_VERIFY(jt != Topics.end());

    req.SetSession(Session);
    ActorIdToProto(jt->second.PipeClient, req.MutablePipeClient());
    req.SetClientId(ClientId);
    req.SetTopic(converter->GetPrimaryPath());
    req.SetPartition(it->second.Partition.Partition);

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " released: " << it->second.Partition);

    NTabletPipe::SendData(ctx, jt->second.PipeClient, request.Release());
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::CloseSession(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx) {

    if (errorCode != PersQueue::ErrorCode::OK) {
        if (InternalErrorCode(errorCode)) {
            SLIErrors.Inc();
        }
        if (Errors) {
            ++(*Errors);
        } else if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            ++(*GetServiceCounters(Counters, "pqproxy|readSession")->GetCounter("Errors", true));
        }

        TServerMessage result;
        result.set_status(ConvertPersQueueInternalCodeToStatus(errorCode));

        FillIssue(result.add_issues(), errorCode, errorReason);

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " closed with error reason: " << errorReason);

        if (!WriteResponse(std::move(result), true)) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
            Die(ctx);
            return;
        }
    } else {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " closed");
        if (!Request->GetStreamCtx()->Finish(std::move(grpc::Status::OK))) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc double finish failed");
            Die(ctx);
            return;
        }

    }

    Die(ctx);
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();
    if (msg->Status != NKikimrProto::OK) {
        if (msg->Dead) {
            CloseSession(TStringBuilder() << "one of topics is deleted, tablet " << msg->TabletId, PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        //TODO: remove it
        CloseSession(TStringBuilder() << "unable to connect to one of topics, tablet " << msg->TabletId, PersQueue::ErrorCode::ERROR, ctx);
        return;

#if 0
        const bool isAlive = ProcessBalancerDead(msg->TabletId, ctx); // returns false if actor died
        Y_UNUSED(isAlive);
        return;
#endif
    }
}

template<bool UseMigrationProtocol>
bool TReadSessionActor<UseMigrationProtocol>::ActualPartitionActor(const TActorId& part) {
    return ActualPartitionActors.contains(part);
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::ReleasePartition(const typename THashMap<ui64, TPartitionActorInfo>::iterator& it,
                                        bool couldBeReads, const TActorContext& ctx)
{
    {
        //ToDo[counters]
        auto jt = TopicCounters.find(it->second.Topic->GetInternalName());
        Y_VERIFY(jt != TopicCounters.end());
        jt->second.PartitionsReleased.Inc();
        jt->second.PartitionsInfly.Dec();
        if (!it->second.Released && it->second.Releasing) {
            jt->second.PartitionsToBeReleased.Dec();
        }
    }

    Y_VERIFY(couldBeReads || !it->second.Reading);
    //process reads
    typename TFormedReadResponse<TServerMessage>::TPtr formedResponseToAnswer;
    if (it->second.Reading) {
        const auto readIt = PartitionToReadResponse.find(it->second.Actor);
        Y_VERIFY(readIt != PartitionToReadResponse.end());
        if (--readIt->second->RequestsInfly == 0) {
            formedResponseToAnswer = readIt->second;
        }
    }

    InformBalancerAboutRelease(it, ctx);

    it->second.Released = true; //to force drop
    DropPartition(it, ctx); //partition will be dropped

    if (formedResponseToAnswer) {
        ProcessAnswer(ctx, formedResponseToAnswer); // returns false if actor died
    }
}


template<bool UseMigrationProtocol>
bool TReadSessionActor<UseMigrationProtocol>::ProcessBalancerDead(const ui64 tablet, const TActorContext& ctx) {
    for (auto& t : Topics) {
        if (t.second.TabletID == tablet) {
            LOG_INFO_S(
                    ctx, NKikimrServices::PQ_READ_PROXY,
                    PQ_LOG_PREFIX << " balancer for topic " << t.second.FullConverter->GetPrintableString()
                                  << " is dead, restarting all from this topic"
            );

            //Drop all partitions from this topic
            for (auto it = Partitions.begin(); it != Partitions.end();) {
                if (it->second.Topic->GetInternalName() == t.first) { //partition from this topic
                    // kill actor
                    auto jt = it;
                    ++it;
                    if (jt->second.LockSent) {
                        SendReleaseSignalToClient(jt, true, ctx);
                    }
                    ReleasePartition(jt, true, ctx);
                } else {
                    ++it;
                }
            }

            //reconnect pipe
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.CheckAliveness = false;
            clientConfig.RetryPolicy = RetryPolicyForPipes;
            t.second.PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, t.second.TabletID, clientConfig));
            if (InitDone) {
                if (PipeReconnects) {
                    ++(*PipeReconnects);
                }
                if (Errors) {
                    ++(*Errors);
                }

                RegisterSession(t.second.PipeClient, t.first, t.second.Groups, ctx);
            }
        }
    }
    return true;
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    const bool isAlive = ProcessBalancerDead(ev->Get()->TabletId, ctx); // returns false if actor died
    Y_UNUSED(isAlive);
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr &ev , const TActorContext& ctx) {
    if (ev->Get()->Authenticated && !ev->Get()->InternalToken.empty()) {
        Token = new NACLib::TUserToken(ev->Get()->InternalToken);
        ForceACLCheck = true;
        if constexpr (!UseMigrationProtocol) {
            TServerMessage result;
            result.set_status(Ydb::StatusIds::SUCCESS);
            result.mutable_update_token_response();
            if (!WriteResponse(std::move(result))) {
                LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
                Die(ctx);
                return;
            }
        }
    } else {
        Request->ReplyUnauthenticated("refreshed token is invalid");
        Die(ctx);
    }
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::ProcessAuth(const TString& auth, const TActorContext& ctx) {
    if (!auth.empty() && auth != Auth) {
        Auth = auth;
        Request->RefreshToken(auth, ctx, ctx.SelfID);
    }
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvRead::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    THolder<TEvPQProxy::TEvRead> event(ev->Release());

    if (!Request->GetStreamCtx()->Read()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
        Die(ctx);
        return;
    }


    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got read request with guid: " << event->Guid);

    Reads.emplace_back(event.Release());

    ProcessReads(ctx);
}


template<typename TServerMessage>
i64 TFormedReadResponse<TServerMessage>::ApplyResponse(TServerMessage&& resp) {
    constexpr bool UseMigrationProtocol = std::is_same_v<TServerMessage, PersQueue::V1::MigrationStreamingReadServerMessage>;
    if constexpr (UseMigrationProtocol) {
        Y_VERIFY(resp.data_batch().partition_data_size() == 1);
        Response.mutable_data_batch()->add_partition_data()->Swap(resp.mutable_data_batch()->mutable_partition_data(0));

    } else {
        Y_VERIFY(resp.read_response().partition_data_size() == 1);
        Response.mutable_read_response()->add_partition_data()->Swap(resp.mutable_read_response()->mutable_partition_data(0));
    }

    Response.set_status(Ydb::StatusIds::SUCCESS);

    i64 prev = Response.ByteSize();
    std::swap<i64>(prev, ByteSize);
    return ByteSize - prev;
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(typename TEvReadResponse::TPtr& ev, const TActorContext& ctx) {
    TActorId sender = ev->Sender;
    if (!ActualPartitionActor(sender))
        return;

    THolder<TEvReadResponse> event(ev->Release());

    ui64 partitionCookie;
    ui64 assignId;
    if constexpr (UseMigrationProtocol) {
        Y_VERIFY(event->Response.data_batch().partition_data_size() == 1);
        partitionCookie = event->Response.data_batch().partition_data(0).cookie().partition_cookie();
        Y_VERIFY(partitionCookie != 0); // cookie is assigned
        assignId = event->Response.data_batch().partition_data(0).cookie().assign_id();

    } else {
        Y_VERIFY(event->Response.read_response().partition_data_size() == 1);
        assignId = event->Response.read_response().partition_data(0).partition_session_id();
    }

    const auto partitionIt = Partitions.find(assignId);
    Y_VERIFY(partitionIt != Partitions.end());
    Y_VERIFY(partitionIt->second.Reading);
    partitionIt->second.Reading = false;

    if constexpr (UseMigrationProtocol) {
        partitionIt->second.ReadIdToResponse = partitionCookie + 1;
    }

    auto it = PartitionToReadResponse.find(sender);
    Y_VERIFY(it != PartitionToReadResponse.end());

    typename TFormedReadResponse<TServerMessage>::TPtr formedResponse = it->second;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " read done guid " << formedResponse->Guid
                                                                  << partitionIt->second.Partition
                                                                  << " size " << event->Response.ByteSize());

    const i64 diff = formedResponse->ApplyResponse(std::move(event->Response));
    if (event->FromDisk) {
        formedResponse->FromDisk = true;
    }
    formedResponse->WaitQuotaTime = Max(formedResponse->WaitQuotaTime, event->WaitQuotaTime);
    --formedResponse->RequestsInfly;

    BytesInflight_ += diff;
    if (BytesInflight) {
        (*BytesInflight) += diff;
    }

    if (formedResponse->RequestsInfly == 0) {
        ProcessAnswer(ctx, formedResponse);
    }
}

template<bool UseMigrationProtocol>
bool TReadSessionActor<UseMigrationProtocol>::WriteResponse(TServerMessage&& response, bool finish) {
    ui64 sz = response.ByteSize();
    ActiveWrites.push(sz);
    BytesInflight_ += sz;
    if (BytesInflight) {
        (*BytesInflight) += sz;
    }

    return finish ? Request->GetStreamCtx()->WriteAndFinish(std::move(response), grpc::Status::OK) : Request->GetStreamCtx()->Write(std::move(response));
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::ProcessAnswer(const TActorContext& ctx, typename TFormedReadResponse<TServerMessage>::TPtr formedResponse) {
    ui32 readDurationMs = (ctx.Now() - formedResponse->Start - formedResponse->WaitQuotaTime).MilliSeconds();
    if (formedResponse->FromDisk) {
        ReadLatencyFromDisk.IncFor(readDurationMs, 1);
    } else {
        ReadLatency.IncFor(readDurationMs, 1);
    }
    if (readDurationMs >= (formedResponse->FromDisk ? AppData(ctx)->PQConfig.GetReadLatencyFromDiskBigMs() : AppData(ctx)->PQConfig.GetReadLatencyBigMs())) {
        SLIBigReadLatency.Inc();
    }

    Y_VERIFY(formedResponse->RequestsInfly == 0);
    const ui64 diff = formedResponse->Response.ByteSize();
    bool hasMessages;
    if constexpr(UseMigrationProtocol) {
        hasMessages = RemoveEmptyMessages(*formedResponse->Response.mutable_data_batch());
    } else {
        hasMessages = RemoveEmptyMessages(*formedResponse->Response.mutable_read_response());
    }

    if (hasMessages) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " response to read " << formedResponse->Guid);

        if (!WriteResponse(std::move(formedResponse->Response))) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
            Die(ctx);
            return;
        }
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " empty read result " << formedResponse->Guid << ", start new reading");
    }

    BytesInflight_ -= diff;
    if (BytesInflight) {
        (*BytesInflight) -= diff;
    }

    for (auto& pp : formedResponse->PartitionsTookPartInControlMessages) {
        auto it = PartitionToControlMessages.find(pp);
        Y_VERIFY(it != PartitionToControlMessages.end());
        if (--it->second.Infly == 0) {
            for (auto& r : it->second.ControlMessages) {
                if (!WriteResponse(std::move(r))) {
                    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
                    Die(ctx);
                    return;
                }
            }
            PartitionToControlMessages.erase(it);
        }
    }

    for (const TActorId& p : formedResponse->PartitionsTookPartInRead) {
        PartitionToReadResponse.erase(p);
    }

    RequestedBytes -= formedResponse->RequestedBytes;
    ReadsInfly--;

    // Bring back available partitions.
    // If some partition was removed from partitions container, it is not bad because it will be checked during read processing.
    AvailablePartitions.insert(formedResponse->PartitionsBecameAvailable.begin(), formedResponse->PartitionsBecameAvailable.end());

    if (!hasMessages) {
        // process new read
        TClientMessage req;
        if constexpr(UseMigrationProtocol) {
            req.mutable_read();
        } else {
            req.mutable_read_request();
        }
        Reads.emplace_back(new TEvPQProxy::TEvRead(formedResponse->Guid)); // Start new reading request with the same guid
    }

    ProcessReads(ctx); // returns false if actor died
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->Reason, ev->Get()->ErrorCode, ctx);
}

template<bool UseMigrationProtocol>
ui32 TReadSessionActor<UseMigrationProtocol>::NormalizeMaxReadMessagesCount(ui32 sourceValue) {
    ui32 count = Min<ui32>(sourceValue, Max<i32>());
    if (count == 0) {
        count = Max<i32>();
    }
    return count;
}

template<bool UseMigrationProtocol>
ui32 TReadSessionActor<UseMigrationProtocol>::NormalizeMaxReadSize(ui32 sourceValue) {
    ui32 size = Min<ui32>(sourceValue, MAX_READ_SIZE);
    if (size == 0) {
        size = MAX_READ_SIZE;
    }
    return size;
}

template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::ProcessReads(const TActorContext& ctx) {
    while (!Reads.empty() && BytesInflight_ + RequestedBytes < MAX_INFLY_BYTES && ReadsInfly < MAX_INFLY_READS) {
        ui32 count = MaxReadMessagesCount;
        ui64 size = MaxReadSize;
        ui32 partitionsAsked = 0;

        typename TFormedReadResponse<TServerMessage>::TPtr formedResponse = new TFormedReadResponse<TServerMessage>(Reads.front()->Guid, ctx.Now());
        while (!AvailablePartitions.empty()) {
            auto part = *AvailablePartitions.begin();
            AvailablePartitions.erase(AvailablePartitions.begin());

            auto it = Partitions.find(part.AssignId);
            if (it == Partitions.end() || it->second.Releasing) { //this is already released partition
                continue;
            }
            //add this partition to reading
            ++partitionsAsked;

            const ui32 ccount = Min<ui32>(part.MsgLag * LAG_GROW_MULTIPLIER, count);
            count -= ccount;
            const ui64 csize = (ui64)Min<double>(part.SizeLag * LAG_GROW_MULTIPLIER, size);
            size -= csize;
            Y_VERIFY(csize < Max<i32>());

            auto jt = ReadFromTimestamp.find(it->second.Topic->GetInternalName());
            if (jt == ReadFromTimestamp.end()) {
                LOG_ALERT_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << "Error searching for topic: " <<  it->second.Topic->GetInternalName()
                            << " (" << it->second.Topic->GetPrintableString() << ")");
                for (const auto& [k, v] : ReadFromTimestamp) {
                    const auto& kk = k;
                    LOG_ALERT_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << "Have topic: " << kk);
                }
                CloseSession(TStringBuilder() << "Internal error", PersQueue::ErrorCode::ERROR, ctx);
                return;
            }
            ui64 readTimestampMs = Max(ReadTimestampMs, jt->second);

            auto lags_it = MaxLagByTopic.find(it->second.Topic->GetInternalName());
            Y_VERIFY(lags_it != MaxLagByTopic.end());
            ui32 maxLag = Max(MaxTimeLagMs, lags_it->second);

            TAutoPtr<TEvPQProxy::TEvRead> read = new TEvPQProxy::TEvRead(Reads.front()->Guid, ccount, csize, maxLag, readTimestampMs);

            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX
                                        << " performing read request with guid " << read->Guid
                                        << " from " << it->second.Partition << " count " << ccount << " size " << csize
                                        << " partitionsAsked " << partitionsAsked << " maxTimeLag " << maxLag << "ms");


            Y_VERIFY(!it->second.Reading);
            it->second.Reading = true;
            formedResponse->PartitionsTookPartInRead.insert(it->second.Actor);
            auto pp = it->second.Partition;
            pp.AssignId = 0;
            PartitionToControlMessages[pp].Infly++;
            bool res = formedResponse->PartitionsTookPartInControlMessages.insert(pp).second;
            Y_VERIFY(res);

            RequestedBytes += csize;
            formedResponse->RequestedBytes += csize;

            ctx.Send(it->second.Actor, read.Release());
            const auto insertResult = PartitionToReadResponse.insert(std::make_pair(it->second.Actor, formedResponse));
            Y_VERIFY(insertResult.second);

            if (count == 0 || size == 0)
                break;
        }
        if (partitionsAsked == 0)
            break;
        ReadsTotal.Inc();
        formedResponse->RequestsInfly = partitionsAsked;

        ReadsInfly++;

        i64 diff = formedResponse->Response.ByteSize();
        BytesInflight_ += diff;
        formedResponse->ByteSize = diff;
        if (BytesInflight) {
            (*BytesInflight) += diff;
        }
        Reads.pop_front();
    }
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvPartitionReady::TPtr& ev, const TActorContext& ctx) {

    if (!ActualPartitionActor(ev->Sender))
        return;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << ev->Get()->Partition
                    << " ready for read with readOffset "
                    << ev->Get()->ReadOffset << " endOffset " << ev->Get()->EndOffset << " WTime "
                    << ev->Get()->WTime << " sizeLag " << ev->Get()->SizeLag);

    const auto it = PartitionToReadResponse.find(ev->Sender); // check whether this partition is taking part in read response
    auto& container = it != PartitionToReadResponse.end() ? it->second->PartitionsBecameAvailable : AvailablePartitions;
    auto res = container.insert(TPartitionInfo{ev->Get()->Partition.AssignId, ev->Get()->WTime, ev->Get()->SizeLag,
                                 ev->Get()->EndOffset - ev->Get()->ReadOffset});
    Y_VERIFY(res.second);
    ProcessReads(ctx);
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->Reason, ev->Get()->ErrorCode, ctx);
}


template<bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::HandleWakeup(const TActorContext& ctx) {
    ctx.Schedule(CHECK_ACL_DELAY, new TEvents::TEvWakeup());
    if (Token && !AuthInitActor && (ForceACLCheck || (ctx.Now() - LastACLCheckTimestamp > TDuration::Seconds(AppData(ctx)->PQConfig.GetACLRetryTimeoutSec()) && RequestNotChecked))) {
        ForceACLCheck = false;
        RequestNotChecked = false;
        Y_VERIFY(!AuthInitActor);
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " checking auth because of timeout");

        AuthInitActor = ctx.Register(new TReadInitAndAuthActor(
                ctx, ctx.SelfID, ClientId, Cookie, Session, SchemeCache, NewSchemeCache, Counters, Token, TopicsList,
                TopicsHandler.GetLocalCluster()
        ));
    }
}

} // namespace NGRpcProxy::V1
} // namespace NKikimr
