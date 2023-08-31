#ifndef READ_SESSION_ACTOR_IMPL
    #error "Do not include this file directly"
#endif

#include "helpers.h"
#include "read_init_auth_actor.h"

#include <ydb/library/persqueue/topic_parser/counters.h>
#include <ydb/core/persqueue/user_info.h>

#include <library/cpp/protobuf/util/repeated_field_utils.h>

#include <google/protobuf/util/time_util.h>

#include <util/string/join.h>
#include <util/string/strip.h>

#include <utility>

namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimrClient;
using namespace NMsgBusProxy;
using namespace PersQueue::V1;

// TODO: add here tracking of bytes in/out

template <bool UseMigrationProtocol>
TReadSessionActor<UseMigrationProtocol>::TReadSessionActor(
        TEvStreamReadRequest* request, const ui64 cookie,
        const TActorId& schemeCache, const TActorId& newSchemeCache,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
        const TMaybe<TString> clientDC,
        const NPersQueue::TTopicsListController& topicsHandler)
    : TRlHelpers({}, request, READ_BLOCK_SIZE, false)
    , Request(request)
    , ClientDC(clientDC.GetOrElse("other"))
    , StartTimestamp(TInstant::Now())
    , SchemeCache(schemeCache)
    , NewSchemeCache(newSchemeCache)
    , CommitsDisabled(false)
    , ReadWithoutConsumer(false)
    , InitDone(false)
    , RangesMode(false)
    , MaxReadMessagesCount(0)
    , MaxReadSize(0)
    , MaxTimeLagMs(0)
    , ReadTimestampMs(0)
    , ReadSizeBudget(0)
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

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Bootstrap(const TActorContext& ctx) {
    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        ++(*GetServiceCounters(Counters, "pqproxy|readSession")
           ->GetNamedCounter("sensor", "SessionsCreatedTotal", true));
    }

    Request->GetStreamCtx()->Attach(ctx.SelfID);
    if (!ReadFromStreamOrDie(ctx)) {
        return;
    }

    StartTime = ctx.Now();
    this->Become(&TReadSessionActor<UseMigrationProtocol>::TThis::StateFunc);
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(typename IContext::TEvNotifiedWhenDone::TPtr&, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc closed");
    Die(ctx);
}

template <bool UseMigrationProtocol>
bool TReadSessionActor<UseMigrationProtocol>::ReadFromStreamOrDie(const TActorContext& ctx) {
    if (!Request->GetStreamCtx()->Read()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
        Die(ctx);
        return false;
    }

    return true;
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(typename IContext::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
    auto& request = ev->Get()->Record;

    if constexpr (UseMigrationProtocol) {
        const auto token = request.token();
        request.set_token("");

        if (!token.empty()) { // TODO: refresh token here
            ctx.Send(ctx.SelfID, new TEvPQProxy::TEvAuth(token));
        }
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read done"
        << ": success# " << ev->Get()->Success
        << ", data# " << request);

    if (!ev->Get()->Success) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed");
        ctx.Send(ctx.SelfID, new TEvPQProxy::TEvDone());
        return;
    }

    auto getAssignId = [](auto& request) {
        if constexpr (UseMigrationProtocol) {
            return request.assign_id();
        } else {
            return request.partition_session_id();
        }
    };

    if constexpr (UseMigrationProtocol) {
        switch (request.request_case()) {
            case TClientMessage::kInitRequest: {
                return (void)ctx.Send(ctx.SelfID, new TEvReadInit(request, Request->GetStreamCtx()->GetPeerName()));
            }

            case TClientMessage::kStatus: {
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvGetStatus(getAssignId(request.status())));
                return (void)ReadFromStreamOrDie(ctx);
            }

            case TClientMessage::kRead: {
                return (void)ctx.Send(ctx.SelfID, new TEvPQProxy::TEvRead());
            }

            case TClientMessage::kReleased: {
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvReleased(getAssignId(request.released())));
                return (void)ReadFromStreamOrDie(ctx);
            }

            case TClientMessage::kStartRead: {
                const auto& req = request.start_read();

                const ui64 readOffset = req.read_offset();
                const bool verifyReadOffset = req.verify_read_offset();

                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvStartRead(
                        getAssignId(request.start_read()), readOffset, req.commit_offset(), verifyReadOffset
                ));
                return (void)ReadFromStreamOrDie(ctx);
            }

            case TClientMessage::kCommit: {
                const auto& req = request.commit();

                if (!req.cookies_size() && !RangesMode) {
                    return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "can't commit without cookies", ctx);
                }

                if (RangesMode && !req.offset_ranges_size()) {
                    return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "can't commit without offsets", ctx);
                }

                THashMap<ui64, TEvPQProxy::TCommitCookie> commitCookie;
                THashMap<ui64, TEvPQProxy::TCommitRange> commitRange;

                for (const auto& c : req.cookies()) {
                    commitCookie[c.assign_id()].Cookies.push_back(c.partition_cookie());
                }

                for (const auto& c : req.offset_ranges()) {
                    commitRange[c.assign_id()].Ranges.emplace_back(c.start_offset(), c.end_offset());
                }

                for (auto& [id, cookies] : commitCookie) {
                    ctx.Send(ctx.SelfID, new TEvPQProxy::TEvCommitCookie(id, std::move(cookies)));
                }

                for (auto& [id, range] : commitRange) {
                    ctx.Send(ctx.SelfID, new TEvPQProxy::TEvCommitRange(id, std::move(range)));
                }

                return (void)ReadFromStreamOrDie(ctx);
            }

            default: {
                return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "unsupported request", ctx);
            }
        }
    } else {
        switch (request.client_message_case()) {
            case TClientMessage::kInitRequest: {
                return (void)ctx.Send(ctx.SelfID, new TEvReadInit(request, Request->GetStreamCtx()->GetPeerName()));
            }

            case TClientMessage::kReadRequest: {
                return (void)ctx.Send(ctx.SelfID, new TEvPQProxy::TEvRead(request.read_request().bytes_size()));
            }

            case TClientMessage::kPartitionSessionStatusRequest: {
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvGetStatus(getAssignId(request.partition_session_status_request())));
                return (void)ReadFromStreamOrDie(ctx);
            }

            case TClientMessage::kStopPartitionSessionResponse: {
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvReleased(getAssignId(request.stop_partition_session_response())));
                if (ReadWithoutConsumer) {
                    return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "it is forbidden to send StopPartitionSessionResponse when reading without a consumer", ctx);
                }
                return (void)ReadFromStreamOrDie(ctx);
            }

            case TClientMessage::kStartPartitionSessionResponse: {
                const auto& req = request.start_partition_session_response();

                const ui64 readOffset = req.read_offset();
                const ui64 commitOffset = req.commit_offset();
                if (ReadWithoutConsumer && req.has_commit_offset()) {
                    return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "can't commit when reading without a consumer", ctx);
                }

                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvStartRead(
                        getAssignId(req), readOffset, req.has_commit_offset() ? commitOffset : TMaybe<ui64>{},
                        req.has_read_offset()
                ));
                return (void)ReadFromStreamOrDie(ctx);
            }

            case TClientMessage::kCommitOffsetRequest: {
                const auto& req = request.commit_offset_request();

                if (ReadWithoutConsumer) {
                    return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "can't commit when reading without a consumer", ctx);
                }
                if (!RangesMode || !req.commit_offsets_size()) {
                    return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "can't commit without offsets", ctx);
                }

                THashMap<ui64, TEvPQProxy::TCommitRange> commitRange;

                for (const auto& pc : req.commit_offsets()) {
                    for (const auto& c : pc.offsets()) {
                        commitRange[pc.partition_session_id()].Ranges.emplace_back(c.start(), c.end());
                    }
                }

                for (auto& [id, range] : commitRange) {
                    ctx.Send(ctx.SelfID, new TEvPQProxy::TEvCommitRange(id, std::move(range)));
                }

                return (void)ReadFromStreamOrDie(ctx);
            }

            case TClientMessage::kUpdateTokenRequest: {
                if (const auto token = request.update_token_request().token()) { // TODO: refresh token here
                    ctx.Send(ctx.SelfID, new TEvPQProxy::TEvAuth(token));
                }
                return (void)ReadFromStreamOrDie(ctx);
            }

            default: {
                return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "unsupported request", ctx);
            }
        }
    }
}

template <bool UseMigrationProtocol>
bool TReadSessionActor<UseMigrationProtocol>::WriteToStreamOrDie(const TActorContext& ctx, TServerMessage&& response, bool finish) {
    const ui64 sz = response.ByteSize();
    ActiveWrites.push(sz);

    BytesInflight_ += sz;
    if (BytesInflight) {
        (*BytesInflight) += sz;
    }

    bool res = false;
    if (!finish) {
        res = Request->GetStreamCtx()->Write(std::move(response));
    } else {
        res = Request->GetStreamCtx()->WriteAndFinish(std::move(response), grpc::Status::OK);
    }

    if (!res) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed at start");
        Die(ctx);
    }

    return res;
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(typename IContext::TEvWriteFinished::TPtr& ev, const TActorContext& ctx) {
    if (!ev->Get()->Success) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
        return Die(ctx);
    }

    Y_VERIFY(!ActiveWrites.empty());
    const auto sz = ActiveWrites.front();
    ActiveWrites.pop();

    Y_VERIFY(BytesInflight_ >= sz);
    BytesInflight_ -= sz;
    if (BytesInflight) {
        (*BytesInflight) -= sz;
    }

    ProcessReads(ctx);
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Die(const TActorContext& ctx) {
    if (AuthInitActor) {
        ctx.Send(AuthInitActor, new TEvents::TEvPoisonPill());
    }

    for (const auto& [_, info] : Partitions) {
        if (info.Actor) {
            ctx.Send(info.Actor, new TEvents::TEvPoisonPill());
        }

        if (!info.Released) {
            // TODO: counters
            auto it = TopicCounters.find(info.Topic->GetInternalName());
            Y_VERIFY(it != TopicCounters.end());
            it->second.PartitionsInfly.Dec();
            it->second.PartitionsReleased.Inc();
            if (info.Releasing) {
                it->second.PartitionsToBeReleased.Dec();
            }
        }
    }

    for (const auto& [_, holder] : Topics) {
        if (holder.PipeClient) {
            NTabletPipe::CloseClient(ctx, holder.PipeClient);
        }
    }

    if (BytesInflight) {
        (*BytesInflight) -= BytesInflight_;
    }
    if (SessionsActive) {
        --(*SessionsActive);
    }
    if (SessionsActive) {
        PartsPerSession.DecFor(Partitions.size(), 1);
    }

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " is DEAD");
    ctx.Send(GetPQReadServiceActorID(), new TEvPQProxy::TEvSessionDead(Cookie));

    TActorBootstrapped<TReadSessionActor>::Die(ctx);
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvDone::TPtr&, const TActorContext& ctx) {
    CloseSession(PersQueue::ErrorCode::OK, "reads done signal, closing everything", ctx);
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->ErrorCode, ev->Get()->Reason, ctx);
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvDieCommand::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->ErrorCode, ev->Get()->Reason, ctx);
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvCommitCookie::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    if (CommitsDisabled) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "commits in session are disabled by client option", ctx);
    }

    auto it = Partitions.find(ev->Get()->AssignId);
    if (it == Partitions.end()) { // stale commit - ignore it
        return;
    }

    for (const auto c : ev->Get()->CommitInfo.Cookies) {
        if (RangesMode) {
            return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "commits cookies in ranges commit mode is prohibited", ctx);
        }

        it->second.NextCommits.insert(c);
    }

    ctx.Send(it->second.Actor, new TEvPQProxy::TEvCommitCookie(ev->Get()->AssignId, std::move(ev->Get()->CommitInfo)));
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvCommitRange::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    if (CommitsDisabled) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "commits in session are disabled by client option", ctx);
    }

    auto it = Partitions.find(ev->Get()->AssignId);
    if (it == Partitions.end()) { // stale commit - ignore it
        return;
    }

    for (const auto& [b, e] : ev->Get()->CommitInfo.Ranges) {
        if (!RangesMode) {
            return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "commits ranges in cookies commit mode is prohibited", ctx);
        }

        if (b >= e || it->second.NextRanges.Intersects(b, e) || b < it->second.Offset) {
            return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, TStringBuilder()
                << "offsets range [" << b << ", " << e << ")"
                << " has already committed offsets, double committing is forbiden or incorrect", ctx);
        }

        it->second.NextRanges.InsertInterval(b, e);
    }

    ctx.Send(it->second.Actor, new TEvPQProxy::TEvCommitRange(ev->Get()->AssignId, std::move(ev->Get()->CommitInfo)));
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvAuth::TPtr& ev, const TActorContext& ctx) {
    const auto& auth = ev->Get()->Auth;
    if (!auth.empty() && auth != Auth) {
        Auth = auth;
        Request->RefreshToken(auth, ctx, ctx.SelfID);
    }
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvStartRead::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    auto it = Partitions.find(ev->Get()->AssignId);
    if (it == Partitions.end() || it->second.Releasing) {
        // do nothing - already released partition
        LOG_WARN_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got irrelevant StartRead from client"
            << ": partition# " << ev->Get()->AssignId
            << ", offset# " << ev->Get()->ReadOffset);
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got StartRead from client"
        << ": partition# " << it->second.Partition
        << ", readOffset# " << ev->Get()->ReadOffset
        << ", commitOffset# " << ev->Get()->CommitOffset);

    // proxy request to partition - allow initing
    // TODO: add here VerifyReadOffset too and check it againts Committed position
    ctx.Send(it->second.Actor, new TEvPQProxy::TEvLockPartition(
        ev->Get()->ReadOffset, ev->Get()->CommitOffset, ev->Get()->VerifyReadOffset, true
    ));
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvReleased::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    auto it = Partitions.find(ev->Get()->AssignId);
    if (it == Partitions.end()) {
        return;
    }

    if (!it->second.Releasing) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, TStringBuilder()
            << "release of partition that is not requested for release is forbiden for " << it->second.Partition, ctx);
    }

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got Released from client"
        << ": partition# " << it->second.Partition);

    Y_VERIFY(it->second.LockSent);
    ReleasePartition(it, true, ctx);
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvGetStatus::TPtr& ev, const TActorContext& ctx) {
    auto it = Partitions.find(ev->Get()->AssignId);
    if (it == Partitions.end() || it->second.Releasing) {
        // Ignore request - client asking status after releasing of partition.
        return;
    }

    ctx.Send(it->second.Actor, new TEvPQProxy::TEvGetStatus(ev->Get()->AssignId));
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::DropPartition(typename TPartitionsMap::iterator it, const TActorContext& ctx) {
    ctx.Send(it->second.Actor, new TEvents::TEvPoisonPill());

    bool res = ActualPartitionActors.erase(it->second.Actor);
    Y_VERIFY(res);

    if (--NumPartitionsFromTopic[it->second.Topic->GetInternalName()] == 0) {
        // TODO: counters
        res = TopicCounters.erase(it->second.Topic->GetInternalName());
        Y_VERIFY(res);
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

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvCommitDone::TPtr& ev, const TActorContext& ctx) {
    Y_VERIFY(!CommitsDisabled);

    if (!ActualPartitionActors.contains(ev->Sender)) {
        return;
    }

    auto it = Partitions.find(ev->Get()->AssignId);
    Y_VERIFY(it != Partitions.end());
    Y_VERIFY(it->second.Offset < ev->Get()->Offset);
    it->second.NextRanges.EraseInterval(it->second.Offset, ev->Get()->Offset);

    if (ev->Get()->StartCookie == Max<ui64>()) { // means commit at start
        return;
    }

    TServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);

    if (!RangesMode) {
        if constexpr (UseMigrationProtocol) {
            for (ui64 i = ev->Get()->StartCookie; i <= ev->Get()->LastCookie; ++i) {
                auto c = result.mutable_committed()->add_cookies();
                c->set_partition_cookie(i);
                c->set_assign_id(ev->Get()->AssignId);
                it->second.NextCommits.erase(i);
                it->second.ReadIdCommitted = i;
            }
        } else { // commit on cookies not supported in this case
            Y_VERIFY(false);
        }
    } else {
        if constexpr (UseMigrationProtocol) {
            auto c = result.mutable_committed()->add_offset_ranges();
            c->set_assign_id(ev->Get()->AssignId);
            c->set_start_offset(it->second.Offset);
            c->set_end_offset(ev->Get()->Offset);
        } else {
            auto c = result.mutable_commit_offset_response()->add_partitions_committed_offsets();
            c->set_partition_session_id(ev->Get()->AssignId);
            c->set_committed_offset(ev->Get()->Offset);
        }
    }

    it->second.Offset = ev->Get()->Offset;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " replying for commits"
        << ": assignId# " << ev->Get()->AssignId
        << ", from# " << ev->Get()->StartCookie
        << ", to# " << ev->Get()->LastCookie
        << ", offset# " << it->second.Offset);
    WriteToStreamOrDie(ctx, std::move(result));
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvReadSessionStatus::TPtr& ev, const TActorContext& ctx) {
    auto result = MakeHolder<TEvPQProxy::TEvReadSessionStatusResponse>();

    for (const auto& [_, info] : Partitions) {
        auto part = result->Record.AddPartition();
        part->SetTopic(info.Partition.DiscoveryConverter->GetPrimaryPath());
        part->SetPartition(info.Partition.Partition);
        part->SetAssignId(info.Partition.AssignId);
        part->SetReadIdCommitted(info.ReadIdCommitted);
        part->SetLastReadId(info.ReadIdToResponse - 1);
        part->SetTimestampMs(info.AssignTimestamp.MilliSeconds());

        for (const auto c : info.NextCommits) {
            part->AddNextCommits(c);
        }
    }

    result->Record.SetSession(Session);
    result->Record.SetTimestamp(StartTimestamp.MilliSeconds());
    result->Record.SetClientNode(PeerName);
    result->Record.SetProxyNodeId(ctx.SelfID.NodeId());

    ctx.Send(ev->Sender, result.Release());
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(typename TEvReadInit::TPtr& ev, const TActorContext& ctx) {
    if (!Topics.empty()) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "got second init request", ctx);
    }

    const auto& init = ev->Get()->Request.init_request();

    if (!init.topics_read_settings_size()) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "no topics in init request", ctx);
    }

    ReadWithoutConsumer = init.consumer().empty();

    if (ReadWithoutConsumer) {
        ClientId = NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER;
        ClientPath = "";
    } else {
        ClientId = NPersQueue::ConvertNewConsumerName(init.consumer(), ctx);
        if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            ClientPath = init.consumer();
        } else {
            ClientPath = NPersQueue::StripLeadSlash(NPersQueue::MakeConsumerPath(init.consumer()));
        }
    }


    Session = TStringBuilder() << ClientPath
        << "_" << ctx.SelfID.NodeId()
        << "_" << Cookie
        << "_" << TAppData::RandomProvider->GenRand64()
        << "_" << "v1";
    CommitsDisabled = false;

    PeerName = ev->Get()->PeerName;

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
        ReadOnlyLocal = true;
        if (init.reader_name()) {
            PeerName = init.reader_name();
        }
    }

    if (MaxTimeLagMs < 0) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "max_lag_duration_ms must be nonnegative number", ctx);
    }

    if (ReadTimestampMs < 0) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "start_from_written_at_ms must be nonnegative number", ctx);
    }


    auto getTopicPath = [](const auto& settings) {
        if constexpr (UseMigrationProtocol) {
            return settings.topic();
        } else {
            return settings.path();
        }
    };

    auto getReadFrom = [](const auto& settings) {
        if constexpr (UseMigrationProtocol) {
            return settings.start_from_written_at_ms();
        } else {
            return ::google::protobuf::util::TimeUtil::TimestampToMilliseconds(settings.read_from());
        }
    };
    auto database = Request->GetDatabaseName().GetOrElse(TString());

    for (const auto& topic : init.topics_read_settings()) {
        const TString path = getTopicPath(topic);
        if (path.empty()) {
            return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "empty topic in init request", ctx);
        }

        const i64 read_from = getReadFrom(topic);
        if (read_from < 0) {
            return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "start_from_written_at_ms must be nonnegative number", ctx);
        }

        TopicsToResolve.insert(path);
    }

    if (Request->GetSerializedToken().empty()) {
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            return CloseSession(PersQueue::ErrorCode::ACCESS_DENIED,
                "unauthenticated access is forbidden, please provide credentials", ctx);
        }
    } else {
        Y_VERIFY(Request->GetYdbToken());
        Auth = *(Request->GetYdbToken());
        Token = new NACLib::TUserToken(Request->GetSerializedToken());
    }

    TopicsList = TopicsHandler.GetReadTopicsList(TopicsToResolve, ReadOnlyLocal, database);

    if (!TopicsList.IsValid) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, TopicsList.Reason, ctx);
    }

    for (const auto& topic : init.topics_read_settings()) {
        auto it = TopicsList.ClientTopics.find(getTopicPath(topic));
        Y_VERIFY(it != TopicsList.ClientTopics.end());

        for (const auto& converter : it->second) {
            const auto internalName = converter->GetOriginalPath();
            if constexpr (UseMigrationProtocol) {
                for (const i64 pg : topic.partition_group_ids()) {
                    if (pg <= 0) {
                        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST,
                            "partition group id must be positive number", ctx);
                    }

                    if (pg > Max<ui32>()) {
                        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, TStringBuilder()
                            << "partition group id is too big: " << pg << " > " << Max<ui32>(), ctx);
                    }

                    TopicGroups[internalName].push_back(static_cast<ui32>(pg));
                }

                MaxLagByTopic[internalName] = MaxTimeLagMs;
                ReadFromTimestamp[internalName] = getReadFrom(topic);
            } else {
                for (const i64 p : topic.partition_ids()) {
                    if (p < 0) {
                        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST,
                            "partition id must be nonnegative number", ctx);
                    }

                    if (p + 1 > Max<ui32>()) {
                        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, TStringBuilder()
                            << "partition id is too big: " << p << " > " << Max<ui32>() - 1, ctx);
                    }

                    TopicGroups[internalName].push_back(static_cast<ui32>(p + 1));
                }

                MaxLagByTopic[internalName] = ::google::protobuf::util::TimeUtil::DurationToMilliseconds(topic.max_lag());;
                ReadFromTimestamp[internalName] = getReadFrom(topic);
            }
        }
    }

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " read init"
        << ": from# " << PeerName
        << ", request# " << ev->Get()->Request);

    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        SetupCounters();
    }

    RunAuthActor(ctx);

    auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");
    Aggr = {{{{"Account", ClientPath.substr(0, ClientPath.find("/"))}}, {"total"}}};
    if (!ReadWithoutConsumer) {
        SLIErrors = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsError"}, true, "sensor", false);
        SLITotal = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsTotal"}, true, "sensor", false);
        SLITotal.Inc();
    }
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::SetupCounters() {
    if (SessionsCreated) {
        return;
    }

    auto subGroup = GetServiceCounters(Counters, "pqproxy|readSession");
    if (!ReadWithoutConsumer) {
        subGroup = subGroup->GetSubgroup("Client", ClientId)->GetSubgroup("ConsumerPath", ClientPath);
    }
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
    PartsPerSession.IncFor(Partitions.size(), 1); // for 0
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::SetupTopicCounters(const NPersQueue::TTopicConverterPtr& topic) {
    auto& topicCounters = TopicCounters[topic->GetInternalName()];
    auto subGroup = GetServiceCounters(Counters, "pqproxy|readSession");
    auto aggr = NPersQueue::GetLabels(topic);
    TVector<std::pair<TString, TString>> cons;
    if (!ReadWithoutConsumer) {
        cons = {{"Client", ClientId}, {"ConsumerPath", ClientPath}};
    }

    topicCounters.PartitionsLocked       = NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsLocked"}, true);
    topicCounters.PartitionsReleased     = NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsReleased"}, true);
    topicCounters.PartitionsToBeReleased = NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsToBeReleased"}, false);
    topicCounters.PartitionsToBeLocked   = NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsToBeLocked"}, false);
    topicCounters.PartitionsInfly        = NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsInfly"}, false);
    topicCounters.Errors                 = NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsErrors"}, true);
    topicCounters.Commits                = NPQ::TMultiCounter(subGroup, aggr, cons, {"Commits"}, true);
    topicCounters.WaitsForData           = NPQ::TMultiCounter(subGroup, aggr, cons, {"WaitsForData"}, true);

    topicCounters.CommitLatency          = CommitLatency;
    topicCounters.SLIBigLatency          = SLIBigLatency;
    topicCounters.SLITotal               = SLITotal;
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::SetupTopicCounters(const NPersQueue::TTopicConverterPtr& topic,
        const TString& cloudId, const TString& dbId, const TString& dbPath, const bool isServerless, const TString& folderId)
{
    auto& topicCounters = TopicCounters[topic->GetInternalName()];
    auto subGroup = NPersQueue::GetCountersForTopic(Counters, isServerless);
    auto subgroups = NPersQueue::GetSubgroupsForTopic(topic, cloudId, dbId, dbPath, folderId);
    if (!ReadWithoutConsumer)
        subgroups.push_back({"consumer", ClientPath});

    topicCounters.PartitionsLocked       = NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.partition_session.started"}, true, "name");
    topicCounters.PartitionsReleased     = NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.partition_session.stopped"}, true, "name");
    topicCounters.PartitionsToBeReleased = NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.partition_session.stopping_count"}, false, "name");
    topicCounters.PartitionsToBeLocked   = NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.partition_session.starting_count"}, false, "name");
    topicCounters.PartitionsInfly        = NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.partition_session.count"}, false, "name");
    topicCounters.Errors                 = NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.partition_session.errors"}, true, "name");
    topicCounters.Commits                = NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.commits"}, true, "name");

    topicCounters.CommitLatency          = CommitLatency;
    topicCounters.SLIBigLatency          = SLIBigLatency;
    topicCounters.SLITotal               = SLITotal;
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " auth ok"
        << ": topics# " << ev->Get()->TopicAndTablets.size()
        << ", initDone# " << InitDone);

    LastACLCheckTimestamp = ctx.Now();
    AuthInitActor = TActorId();

    if (!InitDone) {
        const ui32 initBorder = AppData(ctx)->PQConfig.GetReadInitLatencyBigMs();
        const ui32 readBorder = AppData(ctx)->PQConfig.GetReadLatencyBigMs();
        const ui32 readBorderFromDisk = AppData(ctx)->PQConfig.GetReadLatencyFromDiskBigMs();

        auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");
        if (!ReadWithoutConsumer) {
            InitLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "ReadInit", initBorder, {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
            CommitLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "Commit", AppData(ctx)->PQConfig.GetCommitLatencyBigMs(), {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
            SLIBigLatency = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsBigLatency"}, true, "sensor", false);
            ReadLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "Read", readBorder, {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
            ReadLatencyFromDisk = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "ReadFromDisk", readBorderFromDisk, {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
            SLIBigReadLatency = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"ReadBigLatency"}, true, "sensor", false);
            ReadsTotal = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"ReadsTotal"}, true, "sensor", false);
        }

        const ui32 initDurationMs = (ctx.Now() - StartTime).MilliSeconds();
        if (InitLatency) {
            InitLatency.IncFor(initDurationMs, 1);
        }

        if (SLIBigLatency) {
            if (initDurationMs >= initBorder) {
                SLIBigLatency.Inc();
            }
        }

        for (const auto& [name, t] : ev->Get()->TopicAndTablets) { // TODO: return something from Init and Auth Actor (Full Path - ?)
            auto internalName = t.TopicNameConverter->GetInternalName();
            {
                auto it = TopicGroups.find(name);
                if (it != TopicGroups.end()) {
                    auto value = std::move(it->second);
                    TopicGroups.erase(it);
                    TopicGroups[internalName] = std::move(value);
                }
            }
            {
                auto it = ReadFromTimestamp.find(name);
                if (it != ReadFromTimestamp.end()) {
                    auto value = std::move(it->second);
                    ReadFromTimestamp.erase(it);
                    ReadFromTimestamp[internalName] = std::move(value);
                }
            }
            {
                auto it = MaxLagByTopic.find(name);
                if (it != MaxLagByTopic.end()) {
                    auto value = std::move(it->second);
                    MaxLagByTopic.erase(it);
                    MaxLagByTopic[internalName] = std::move(value);
                }
            }

            Topics[internalName] = TTopicHolder::FromTopicInfo(t);
            FullPathToConverter[t.TopicNameConverter->GetPrimaryPath()] = t.TopicNameConverter;
            FullPathToConverter[t.TopicNameConverter->GetSecondaryPath()] = t.TopicNameConverter;

            if (!GetMeteringMode()) {
                SetMeteringMode(t.MeteringMode);
            } else if (*GetMeteringMode() != t.MeteringMode) {
                return CloseSession(PersQueue::ErrorCode::BAD_REQUEST,
                    "cannot read from topics with different metering modes", ctx);
            }
        }

        if (IsQuotaRequired()) {
            Y_VERIFY(MaybeRequestQuota(1, EWakeupTag::RlInit, ctx));
        } else {
            InitSession(ctx);
        }
    } else {
        for (const auto& [name, t] : ev->Get()->TopicAndTablets) {
            auto it = Topics.find(t.TopicNameConverter->GetInternalName());
            if (it == Topics.end()) {
                return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, TStringBuilder()
                    << "list of topics changed, new topic found: " << t.TopicNameConverter->GetPrintableString(), ctx);
            }
            if (t.MeteringMode != *GetMeteringMode()) {
                return CloseSession(PersQueue::ErrorCode::OVERLOAD, TStringBuilder()
                    << "metering mode of topic: " << name << " has been changed", ctx);
            }
        }
    }
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::InitSession(const TActorContext& ctx) {
    TServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);

    result.mutable_init_response()->set_session_id(Session);
    if (!WriteToStreamOrDie(ctx, std::move(result))) {
        return;
    }

    if (!ReadFromStreamOrDie(ctx)) {
        return;
    }

    for (auto& [_, holder] : Topics) {
        if (!ReadWithoutConsumer) {
            holder.PipeClient = CreatePipeClient(holder.TabletID, ctx);
        }
        
        Y_VERIFY(holder.FullConverter);
        auto it = TopicGroups.find(holder.FullConverter->GetInternalName());
        if (it != TopicGroups.end()) {
            holder.Groups = it->second;
        }
    }

    InitDone = true;

    for (const auto& [topicName, topic] : Topics) {
        if (ReadWithoutConsumer) {
            if (topic.Groups.size() == 0) {
                return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "explicitly specify the partitions when reading without a consumer", ctx);
            }
            for (auto group : topic.Groups) {
                SendLockPartitionToSelf(group-1, topicName, topic, ctx);
            }
        } else {
            RegisterSession(topic.FullConverter->GetInternalName(), topic.PipeClient, topic.Groups, ctx);
        }

        NumPartitionsFromTopic[topic.FullConverter->GetInternalName()] = 0;
    }

    ctx.Schedule(TDuration::Seconds(AppData(ctx)->PQConfig.GetACLRetryTimeoutSec()), new TEvents::TEvWakeup(EWakeupTag::RecheckAcl));
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::SendLockPartitionToSelf(ui32 group, TString topicName, TTopicHolder topic, const TActorContext& ctx) {
    auto partitionToTabletIt = topic.PartitionIdToTabletId.find(group);
    if (partitionToTabletIt == topic.PartitionIdToTabletId.end()) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, TStringBuilder() << "no group " << group << " in topic " << topicName, ctx);
    }
    THolder<TEvPersQueue::TEvLockPartition> res{new TEvPersQueue::TEvLockPartition};
    res->Record.SetSession(Session);
    res->Record.SetPartition(group);
    res->Record.SetTopic(topicName);
    res->Record.SetPath(topic.FullConverter->GetPrimaryPath());
    res->Record.SetGeneration(1);
    res->Record.SetStep(1);
    res->Record.SetClientId(ClientId);
    res->Record.SetTabletId(partitionToTabletIt->second);
    ctx.Send(ctx.SelfID, res.Release());
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::RegisterSession(const TString& topic, const TActorId& pipe, const TVector<ui32>& groups, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " register session"
        << ": topic# " << topic);

    auto request = MakeHolder<TEvPersQueue::TEvRegisterReadSession>();

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

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPersQueue::TEvLockPartition::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    Y_VERIFY(record.GetSession() == Session);
    Y_VERIFY(record.GetClientId() == ClientId);

    auto path = record.GetPath();
    if (path.empty()) {
        path = record.GetTopic();
    }

    auto converterIter = FullPathToConverter.find(NPersQueue::NormalizeFullPath(path));
    if (converterIter == FullPathToConverter.end()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " ignored ev lock"
            << ": path# " << path
            << ", reason# " << "path not recognized");
        return;
    }

    const auto name = converterIter->second->GetInternalName();

    {
        auto it = Topics.find(name);
        if (it == Topics.end() || (!ReadWithoutConsumer && it->second.PipeClient != ActorIdFromProto(record.GetPipeClient()))) {
            LOG_ALERT_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " ignored ev lock"
                << ": path# " << name
                << ", reason# " << "topic is unknown");
            return;
        }

        // TODO: counters
        if (NumPartitionsFromTopic[name]++ == 0) {
            if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
                SetupTopicCounters(converterIter->second, it->second.CloudId, it->second.DbId, it->second.DbPath, it->second.IsServerless, it->second.FolderId);
            } else {
                SetupTopicCounters(converterIter->second);
            }
        }
    }

    // TODO: counters
    auto it = TopicCounters.find(name);
    Y_VERIFY(it != TopicCounters.end());

    Y_VERIFY(record.GetGeneration() > 0);
    const ui64 assignId = NextAssignId++;
    BalancerGeneration[assignId] = {record.GetGeneration(), record.GetStep()};
    const TPartitionId partitionId{converterIter->second, record.GetPartition(), assignId};

    const TActorId actorId = ctx.Register(new TPartitionActor(
        ctx.SelfID, ClientId, ClientPath, Cookie, Session, partitionId, record.GetGeneration(),
        record.GetStep(), record.GetTabletId(), it->second, CommitsDisabled, ClientDC, RangesMode,
        converterIter->second, UseMigrationProtocol));

    if (SessionsActive) {
        PartsPerSession.DecFor(Partitions.size(), 1);
    }

    bool res = Partitions.emplace(assignId, TPartitionActorInfo(actorId, partitionId, converterIter->second, ctx.Now())).second;
    Y_VERIFY(res);

    if (SessionsActive) {
        PartsPerSession.IncFor(Partitions.size(), 1);
    }

    res = ActualPartitionActors.insert(actorId).second;
    Y_VERIFY(res);

    it->second.PartitionsLocked.Inc();
    it->second.PartitionsInfly.Inc();

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " assign"
        << ": record# " << record);

    ctx.Send(actorId, new TEvPQProxy::TEvLockPartition(0, {}, false, false));
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx) {
    if (!ActualPartitionActors.contains(ev->Sender)) {
        return;
    }

    auto it = Partitions.find(ev->Get()->Partition.AssignId);
    Y_VERIFY(it != Partitions.end());
    Y_VERIFY(!it->second.Releasing); // if releasing and no lock sent yet - then server must already release partition

    TServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);
    if (ev->Get()->Init) {
        Y_VERIFY(!it->second.LockSent);
        it->second.LockSent = true;
        it->second.Offset = ev->Get()->Offset;

        if constexpr (UseMigrationProtocol) {
            result.mutable_assigned()->mutable_topic()->set_path(it->second.Topic->GetFederationPath());
            result.mutable_assigned()->set_cluster(it->second.Topic->GetCluster());
            result.mutable_assigned()->set_partition(ev->Get()->Partition.Partition);
            result.mutable_assigned()->set_assign_id(it->first);

            result.mutable_assigned()->set_read_offset(ev->Get()->Offset);
            result.mutable_assigned()->set_end_offset(ev->Get()->EndOffset);
        } else {
            auto database = Request->GetDatabaseName().GetOrElse(AppData(ctx)->PQConfig.GetDatabase());
            if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen() || database == AppData(ctx)->PQConfig.GetDatabase() || database == AppData(ctx)->PQConfig.GetTestDatabaseRoot()) {
                result.mutable_start_partition_session_request()->mutable_partition_session()->set_path(it->second.Topic->GetFederationPathWithDC());
            } else {
                result.mutable_start_partition_session_request()->mutable_partition_session()->set_path(it->second.Topic->GetModernName());
            }

            result.mutable_start_partition_session_request()->mutable_partition_session()->set_partition_id(ev->Get()->Partition.Partition);
            result.mutable_start_partition_session_request()->mutable_partition_session()->set_partition_session_id(it->first);

            if (ReadWithoutConsumer) {
                result.mutable_start_partition_session_request()->set_committed_offset(0);
            } else {
                result.mutable_start_partition_session_request()->set_committed_offset(ev->Get()->Offset);
            }

            result.mutable_start_partition_session_request()->mutable_partition_offsets()->set_start(ev->Get()->Offset);
            result.mutable_start_partition_session_request()->mutable_partition_offsets()->set_end(ev->Get()->EndOffset);
        }
    } else {
        Y_VERIFY(it->second.LockSent);

        if constexpr (UseMigrationProtocol) {
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
    }

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " sending to client create partition stream event");
    SendControlMessage(it->second.Partition, std::move(result), ctx);
}

template <bool UseMigrationProtocol>
bool TReadSessionActor<UseMigrationProtocol>::SendControlMessage(TPartitionId id, TServerMessage&& message, const TActorContext& ctx) {
    id.AssignId = 0;

    auto it = PartitionToControlMessages.find(id);
    if (it == PartitionToControlMessages.end()) {
        return WriteToStreamOrDie(ctx, std::move(message));
    } else {
        Y_VERIFY(it->second.Infly);
        it->second.ControlMessages.push_back(std::move(message));
    }

    return true;
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPersQueue::TEvError::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ConvertOldCode(ev->Get()->Record.GetCode()), ev->Get()->Record.GetDescription(), ctx);
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::SendReleaseSignal(typename TPartitionsMap::iterator it, bool kill, const TActorContext& ctx) {
    TServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);

    if constexpr (UseMigrationProtocol) {
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

    if (!SendControlMessage(it->second.Partition, std::move(result), ctx)) {
        return;
    }

    Y_VERIFY(it->second.LockSent);
    it->second.ReleaseSent = true;
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPersQueue::TEvReleasePartition::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    Y_VERIFY(record.GetSession() == Session);
    Y_VERIFY(record.GetClientId() == ClientId);

    const ui32 group = record.HasGroup() ? record.GetGroup() : 0;

    auto pathIter = FullPathToConverter.find(NPersQueue::NormalizeFullPath(record.GetPath()));
    Y_VERIFY(pathIter != FullPathToConverter.end());

    auto it = Topics.find(pathIter->second->GetInternalName());
    Y_VERIFY(it != Topics.end());

    if (it->second.PipeClient != ActorIdFromProto(record.GetPipeClient())) {
        return;
    }

    auto& converter = it->second.FullConverter;

    for (ui32 c = 0; c < record.GetCount(); ++c) {
        Y_VERIFY(!Partitions.empty());

        TActorId actorId;
        auto jt = Partitions.begin();
        ui32 i = 0;

        for (auto it = Partitions.begin(); it != Partitions.end(); ++it) {
            if (it->second.Topic->GetInternalName() == converter->GetInternalName()
                && !it->second.Releasing
                && (group == 0 || it->second.Partition.Partition + 1 == group)
            ) {
                ++i;
                if (rand() % i == 0) { // will lead to 1/n probability for each of n partitions
                    actorId = it->second.Actor;
                    jt = it;
                }
            }
        }

        Y_VERIFY(actorId);

        // TODO: counters
        auto it = TopicCounters.find(converter->GetInternalName());
        Y_VERIFY(it != TopicCounters.end());
        it->second.PartitionsToBeReleased.Inc();

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " releasing"
            << ": partition# " << jt->second.Partition);
        jt->second.Releasing = true;

        if (!jt->second.LockSent) { // no lock yet - can release silently
            ReleasePartition(jt, true, ctx);
        } else {
            SendReleaseSignal(jt, false, ctx);
        }
    }
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx) {
    if (!ActualPartitionActors.contains(ev->Sender)) {
        return;
    }

    auto it = Partitions.find(ev->Get()->Partition.AssignId);
    Y_VERIFY(it != Partitions.end());
    Y_VERIFY(it->second.Releasing);

    ReleasePartition(it, false, ctx); // no reads could be here - this is release from partition
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::InformBalancerAboutRelease(typename TPartitionsMap::iterator it, const TActorContext& ctx) {
    const auto& converter = it->second.Topic;
    auto jt = Topics.find(converter->GetInternalName());
    Y_VERIFY(jt != Topics.end());

    auto request = MakeHolder<TEvPersQueue::TEvPartitionReleased>();

    auto& req = request->Record;
    req.SetSession(Session);
    ActorIdToProto(jt->second.PipeClient, req.MutablePipeClient());
    req.SetClientId(ClientId);
    req.SetTopic(converter->GetPrimaryPath());
    req.SetPartition(it->second.Partition.Partition);

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " released"
        << ": partition# " << it->second.Partition);
    NTabletPipe::SendData(ctx, jt->second.PipeClient, request.Release());
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::CloseSession(PersQueue::ErrorCode::ErrorCode code, const TString& reason, const TActorContext& ctx) {
    if (code != PersQueue::ErrorCode::OK) {
        if (InternalErrorCode(code) && SLIErrors) {
            SLIErrors.Inc();
        }

        if (Errors) {
            ++(*Errors);
        } else if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            ++(*GetServiceCounters(Counters, "pqproxy|readSession")->GetCounter("Errors", true));
        }

        TServerMessage result;
        result.set_status(ConvertPersQueueInternalCodeToStatus(code));
        FillIssue(result.add_issues(), code, reason);

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " closed with error"
            << ": reason# " << reason);
        if (!WriteToStreamOrDie(ctx, std::move(result), true)) {
            return;
        }
    } else {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " closed");
        if (!Request->GetStreamCtx()->Finish(grpc::Status::OK)) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc double finish failed");
        }
    }

    Die(ctx);
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();

    if (msg->Status != NKikimrProto::OK) {
        if (msg->Dead) {
            return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, TStringBuilder()
                << "one of topics is deleted, tablet " << msg->TabletId, ctx);
        }

        // TODO: remove it
        return CloseSession(PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED, TStringBuilder()
            << "unable to connect to one of topics, tablet " << msg->TabletId, ctx);

#if 0
        ProcessBalancerDead(msg->TabletId, ctx); // returns false if actor died
        return;
#endif
    }
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    ProcessBalancerDead(ev->Get()->TabletId, ctx);
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::ReleasePartition(typename TPartitionsMap::iterator it, bool couldBeReads, const TActorContext& ctx) {
    // TODO: counters
    auto jt = TopicCounters.find(it->second.Topic->GetInternalName());
    Y_VERIFY(jt != TopicCounters.end());

    jt->second.PartitionsReleased.Inc();
    jt->second.PartitionsInfly.Dec();

    if (!it->second.Released && it->second.Releasing) {
        jt->second.PartitionsToBeReleased.Dec();
    }

    Y_VERIFY(couldBeReads || !it->second.Reading);
    typename TFormedReadResponse<TServerMessage>::TPtr response;

    // process reads
    if (it->second.Reading) {
        auto readIt = PartitionToReadResponse.find(it->second.Actor);
        Y_VERIFY(readIt != PartitionToReadResponse.end());
        if (--readIt->second->RequestsInfly == 0) {
            response = readIt->second;
        }
    }

    InformBalancerAboutRelease(it, ctx);

    it->second.Released = true; // to force drop
    DropPartition(it, ctx); // partition will be dropped

    if (response) {
        if (const auto ru = CalcRuConsumption(PrepareResponse(response))) {
            response->RequiredQuota = ru;
            if (MaybeRequestQuota(ru, EWakeupTag::RlAllowed, ctx)) {
                Y_VERIFY(!PendingQuota);
                PendingQuota = response;
            } else {
                WaitingQuota.push_back(response);
            }
        } else {
            ProcessAnswer(response, ctx);
        }
    }
}

template <bool UseMigrationProtocol>
TActorId TReadSessionActor<UseMigrationProtocol>::CreatePipeClient(ui64 tabletId, const TActorContext& ctx) {
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.CheckAliveness = false;
    clientConfig.RetryPolicy = RetryPolicyForPipes;
    return ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::ProcessBalancerDead(ui64 tabletId, const TActorContext& ctx) {
    for (auto& t : Topics) {
        if (t.second.TabletID == tabletId) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " balancer dead, restarting all from topic"
                << ": topic# " << t.second.FullConverter->GetPrintableString());

            // Drop all partitions from this topic
            for (auto it = Partitions.begin(); it != Partitions.end();) {
                if (it->second.Topic->GetInternalName() == t.first) { // partition from this topic
                    // kill actor
                    auto jt = it;
                    ++it;

                    if (jt->second.LockSent) {
                        SendReleaseSignal(jt, true, ctx);
                    }

                    ReleasePartition(jt, true, ctx);
                } else {
                    ++it;
                }
            }

            t.second.PipeClient = CreatePipeClient(t.second.TabletID, ctx);

            if (InitDone) {
                if (PipeReconnects) {
                    ++(*PipeReconnects);
                }

                if (Errors) {
                    ++(*Errors);
                }

                RegisterSession(t.first, t.second.PipeClient, t.second.Groups, ctx);
            }
        }
    }
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev , const TActorContext& ctx) {
    if (ev->Get()->Authenticated && ev->Get()->InternalToken && !ev->Get()->InternalToken->GetSerializedToken().empty()) {
        Token = ev->Get()->InternalToken;
        ForceACLCheck = true;

        if constexpr (!UseMigrationProtocol) {
            TServerMessage result;
            result.set_status(Ydb::StatusIds::SUCCESS);
            result.mutable_update_token_response();
            WriteToStreamOrDie(ctx, std::move(result));
        }
    } else {
        if (ev->Get()->Retryable) {
            Request->ReplyUnavaliable();
        } else {
            Request->ReplyUnauthenticated("refreshed token is invalid");
        }
        Die(ctx);
    }
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvRead::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    if (!ReadFromStreamOrDie(ctx)) {
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got read request"
        << ": guid# " << ev->Get()->Guid);

    if constexpr (UseMigrationProtocol) {
        Reads.emplace_back(ev->Release());
    } else {
        ReadSizeBudget += ev->Get()->MaxSize;
    }

    ProcessReads(ctx);
}

template <typename TServerMessage>
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

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(typename TEvReadResponse::TPtr& ev, const TActorContext& ctx) {
    if (!ActualPartitionActors.contains(ev->Sender)) {
        return;
    }

    auto& response = ev->Get()->Response;
    ui64 partitionCookie;
    ui64 assignId;

    if constexpr (UseMigrationProtocol) {
        Y_VERIFY(response.data_batch().partition_data_size() == 1);
        partitionCookie = response.data_batch().partition_data(0).cookie().partition_cookie();
        Y_VERIFY(partitionCookie != 0); // cookie is assigned
        assignId = response.data_batch().partition_data(0).cookie().assign_id();
    } else {
        Y_VERIFY(response.read_response().partition_data_size() == 1);
        assignId = response.read_response().partition_data(0).partition_session_id();
    }

    typename TFormedReadResponse<TServerMessage>::TPtr formedResponse;
    {
        auto it = PartitionToReadResponse.find(ev->Sender);
        Y_VERIFY(it != PartitionToReadResponse.end());
        formedResponse = it->second;
    }

    auto it = Partitions.find(assignId);
    Y_VERIFY(it != Partitions.end());
    Y_VERIFY(it->second.Reading);
    it->second.Reading = false;

    if constexpr (UseMigrationProtocol) {
        it->second.ReadIdToResponse = partitionCookie + 1;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " read done"
        << ": guid# " << formedResponse->Guid
        << ", partition# " << it->second.Partition
        << ", size# " << response.ByteSize());

    const i64 diff = formedResponse->ApplyResponse(std::move(response));
    if (ev->Get()->FromDisk) {
        formedResponse->FromDisk = true;
    }

    formedResponse->WaitQuotaTime = Max(formedResponse->WaitQuotaTime, ev->Get()->WaitQuotaTime);
    --formedResponse->RequestsInfly;

    BytesInflight_ += diff;
    if (BytesInflight) {
        (*BytesInflight) += diff;
    }

    if (formedResponse->RequestsInfly == 0) {
        if (const auto ru = CalcRuConsumption(PrepareResponse(formedResponse))) {
            formedResponse->RequiredQuota = ru;
            if (MaybeRequestQuota(ru, EWakeupTag::RlAllowed, ctx)) {
                Y_VERIFY(!PendingQuota);
                PendingQuota = formedResponse;
            } else {
                WaitingQuota.push_back(formedResponse);
            }
        } else {
            ProcessAnswer(formedResponse, ctx);
        }
    }
}

template <bool UseMigrationProtocol>
ui64 TReadSessionActor<UseMigrationProtocol>::PrepareResponse(typename TFormedReadResponse<TServerMessage>::TPtr formedResponse) {
    formedResponse->ByteSizeBeforeFiltering = formedResponse->Response.ByteSize();

    if constexpr (UseMigrationProtocol) {
        formedResponse->HasMessages = RemoveEmptyMessages(*formedResponse->Response.mutable_data_batch());
    } else {
        formedResponse->HasMessages = RemoveEmptyMessages(*formedResponse->Response.mutable_read_response());
    }

    return formedResponse->HasMessages ? formedResponse->Response.ByteSize() : 0;
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::ProcessAnswer(typename TFormedReadResponse<TServerMessage>::TPtr formedResponse, const TActorContext& ctx) {
    ui32 readDurationMs = (ctx.Now() - formedResponse->Start - formedResponse->WaitQuotaTime).MilliSeconds();

    if (formedResponse->FromDisk) {
        if (ReadLatencyFromDisk)
            ReadLatencyFromDisk.IncFor(readDurationMs, 1);
    } else {
        if (ReadLatency)
            ReadLatency.IncFor(readDurationMs, 1);
    }

    const auto latencyThreshold = formedResponse->FromDisk
        ? AppData(ctx)->PQConfig.GetReadLatencyFromDiskBigMs()
        : AppData(ctx)->PQConfig.GetReadLatencyBigMs();
    if (readDurationMs >= latencyThreshold && SLIBigReadLatency) {
        SLIBigReadLatency.Inc();
    }

    Y_VERIFY(formedResponse->RequestsInfly == 0);
    const ui64 diff = formedResponse->ByteSizeBeforeFiltering;
    const ui64 sizeEstimation = formedResponse->HasMessages ? formedResponse->Response.ByteSize() : 0;

    if constexpr (!UseMigrationProtocol) {
        formedResponse->Response.mutable_read_response()->set_bytes_size(sizeEstimation);
    }

    if (formedResponse->HasMessages) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " response to read"
            << ": guid# " << formedResponse->Guid);
        if (!WriteToStreamOrDie(ctx, std::move(formedResponse->Response))) {
            return;
        }
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " empty read result, start new reading"
            << ": guid# " << formedResponse->Guid);
    }

    BytesInflight_ -= diff;
    if (BytesInflight) {
        (*BytesInflight) -= diff;
    }

    for (const auto& id : formedResponse->PartitionsTookPartInControlMessages) {
        auto it = PartitionToControlMessages.find(id);
        Y_VERIFY(it != PartitionToControlMessages.end());

        if (--it->second.Infly == 0) {
            for (auto& r : it->second.ControlMessages) {
                if (!WriteToStreamOrDie(ctx, std::move(r))) {
                    return;
                }
            }

            PartitionToControlMessages.erase(it);
        }
    }

    for (const auto& id : formedResponse->PartitionsTookPartInRead) {
        PartitionToReadResponse.erase(id);
    }

    RequestedBytes -= formedResponse->RequestedBytes;
    ReadsInfly--;

    if constexpr (!UseMigrationProtocol) {
        ReadSizeBudget += formedResponse->RequestedBytes;
        ReadSizeBudget -= sizeEstimation;
    }

    // Bring back available partitions.
    // If some partition was removed from partitions container, it is not bad because it will be checked during read processing.
    AvailablePartitions.insert(formedResponse->PartitionsBecameAvailable.begin(), formedResponse->PartitionsBecameAvailable.end());

    if constexpr (UseMigrationProtocol) {
        if (!formedResponse->HasMessages) {
            // process new read
            // Start new reading request with the same guid
            Reads.emplace_back(new TEvPQProxy::TEvRead(formedResponse->Guid));
        }
    }

    ProcessReads(ctx);
}

template <bool UseMigrationProtocol>
ui32 TReadSessionActor<UseMigrationProtocol>::NormalizeMaxReadMessagesCount(ui32 sourceValue) {
    ui32 count = Min<ui32>(sourceValue, Max<i32>());

    if (count == 0) {
        count = Max<i32>();
    }

    return count;
}

template <bool UseMigrationProtocol>
ui32 TReadSessionActor<UseMigrationProtocol>::NormalizeMaxReadSize(ui32 sourceValue) {
    ui32 size = Min<ui32>(sourceValue, MAX_READ_SIZE);

    if (size == 0) {
        size = MAX_READ_SIZE;
    }

    return size;
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::ProcessReads(const TActorContext& ctx) {
    auto shouldContinueReads = [this]() {
        if constexpr (UseMigrationProtocol) {
            return !Reads.empty() && ReadsInfly < MAX_INFLY_READS;
        } else {
            return ReadSizeBudget > 0;
        }
    };

    while (shouldContinueReads() && BytesInflight_ + RequestedBytes < MAX_INFLY_BYTES) {
        ui32 count = MaxReadMessagesCount;
        ui64 size = MaxReadSize;
        ui32 partitionsAsked = 0;

        TString guid;
        if constexpr (UseMigrationProtocol) {
            guid = Reads.front()->Guid;
        } else {
            guid = CreateGuidAsString();
        }

        typename TFormedReadResponse<TServerMessage>::TPtr formedResponse =
            new TFormedReadResponse<TServerMessage>(guid, ctx.Now());

        while (!AvailablePartitions.empty()) {
            auto part = *AvailablePartitions.begin();
            AvailablePartitions.erase(AvailablePartitions.begin());

            auto it = Partitions.find(part.AssignId);
            if (it == Partitions.end() || it->second.Releasing) { // this is already released partition
                continue;
            }

            ++partitionsAsked; // add this partition to reading

            const ui32 ccount = Min<ui32>(part.MsgLag * LAG_GROW_MULTIPLIER, count);
            count -= ccount;

            ui64 csize = (ui64)Min<double>(part.SizeLag * LAG_GROW_MULTIPLIER, size);
            if constexpr (!UseMigrationProtocol) {
                csize = Min<i64>(csize, ReadSizeBudget);
            }

            size -= csize;
            Y_VERIFY(csize < Max<i32>());

            auto jt = ReadFromTimestamp.find(it->second.Topic->GetInternalName());
            if (jt == ReadFromTimestamp.end()) {
                LOG_ALERT_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " error searching for topic"
                    << ": internalName# " << it->second.Topic->GetInternalName()
                    << ", prettyName# " << it->second.Topic->GetPrintableString());

                for (const auto& kv : ReadFromTimestamp) {
                    LOG_ALERT_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " have topic"
                        << ": topic# " << kv.first);
                }

                return CloseSession(PersQueue::ErrorCode::ERROR, "internal error", ctx);
            }

            ui64 readTimestampMs = Max(ReadTimestampMs, jt->second);

            auto lagsIt = MaxLagByTopic.find(it->second.Topic->GetInternalName());
            Y_VERIFY(lagsIt != MaxLagByTopic.end());
            const ui32 maxLag = lagsIt->second;

            auto ev = MakeHolder<TEvPQProxy::TEvRead>(guid, ccount, csize, maxLag, readTimestampMs);

            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " performing read request"
                << ": guid# " << ev->Guid
                << ", from# " << it->second.Partition
                << ", count# " << ccount
                << ", size# " << csize
                << ", partitionsAsked# " << partitionsAsked
                << ", maxTimeLag# " << maxLag << "ms");

            Y_VERIFY(!it->second.Reading);
            it->second.Reading = true;

            formedResponse->PartitionsTookPartInRead.insert(it->second.Actor);
            auto id = it->second.Partition;
            id.AssignId = 0;
            PartitionToControlMessages[id].Infly++;

            bool res = formedResponse->PartitionsTookPartInControlMessages.insert(id).second;
            Y_VERIFY(res);

            RequestedBytes += csize;
            formedResponse->RequestedBytes += csize;
            ReadSizeBudget -= csize;

            ctx.Send(it->second.Actor, ev.Release());
            res = PartitionToReadResponse.emplace(it->second.Actor, formedResponse).second;
            Y_VERIFY(res);

            // TODO (ildar-khisam@): Gather data from all partitions.
            // For now send messages only from single partition.
            if constexpr (!UseMigrationProtocol) {
                break;
            }

            if (count == 0 || size == 0) {
                break;
            }
        }

        if (partitionsAsked == 0) {
            break;
        }

        if (ReadsTotal)
            ReadsTotal.Inc();
        formedResponse->RequestsInfly = partitionsAsked;
        ReadsInfly++;

        i64 diff = formedResponse->Response.ByteSize();
        BytesInflight_ += diff;
        formedResponse->ByteSize = diff;

        if (BytesInflight) {
            (*BytesInflight) += diff;
        }

        if constexpr (UseMigrationProtocol) {
            Reads.pop_front();
        }
    }
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvPartitionReady::TPtr& ev, const TActorContext& ctx) {
    if (!ActualPartitionActors.contains(ev->Sender)) {
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " partition ready for read"
        << ": partition# " << ev->Get()->Partition
        << ", readOffset# " << ev->Get()->ReadOffset
        << ", endOffset# " << ev->Get()->EndOffset
        << ", WTime# " << ev->Get()->WTime
        << ", sizeLag# " << ev->Get()->SizeLag);

    auto it = PartitionToReadResponse.find(ev->Sender); // check whether this partition is taking part in read response
    auto& container = it != PartitionToReadResponse.end() ? it->second->PartitionsBecameAvailable : AvailablePartitions;

    bool res = container.emplace(
        ev->Get()->Partition.AssignId,
        ev->Get()->WTime,
        ev->Get()->SizeLag,
        ev->Get()->EndOffset - ev->Get()->ReadOffset).second;
    Y_VERIFY(res);

    ProcessReads(ctx);
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
    const auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
    OnWakeup(tag);

    switch (tag) {
        case EWakeupTag::RlInit:
            return InitSession(ctx);

        case EWakeupTag::RecheckAcl:
            return RecheckACL(ctx);

        case EWakeupTag::RlAllowed:
            if (auto counters = Request->GetCounters()) {
                counters->AddConsumedRequestUnits(PendingQuota->RequiredQuota);
            }

            ProcessAnswer(PendingQuota, ctx);

            if (!WaitingQuota.empty()) {
                PendingQuota = WaitingQuota.front();
                WaitingQuota.pop_front();
            } else {
                PendingQuota = nullptr;
            }
            if (PendingQuota) {
                Y_VERIFY(MaybeRequestQuota(PendingQuota->RequiredQuota, EWakeupTag::RlAllowed, ctx));
            }
            break;

        case EWakeupTag::RlNoResource:
        case EWakeupTag::RlInitNoResource:
            if (PendingQuota) {
                Y_VERIFY(MaybeRequestQuota(PendingQuota->RequiredQuota, EWakeupTag::RlAllowed, ctx));
            } else {
                return CloseSession(PersQueue::ErrorCode::OVERLOAD, "throughput limit exceeded", ctx);
            }
            break;
    }
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::RecheckACL(const TActorContext& ctx) {
    const auto timeout = TDuration::Seconds(AppData(ctx)->PQConfig.GetACLRetryTimeoutSec());

    ctx.Schedule(timeout, new TEvents::TEvWakeup(EWakeupTag::RecheckAcl));

    const bool authTimedOut = (ctx.Now() - LastACLCheckTimestamp) > timeout;

    if (Token && !AuthInitActor && (ForceACLCheck || (authTimedOut && RequestNotChecked))) {
        ForceACLCheck = false;
        RequestNotChecked = false;

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " checking auth because of timeout");
        RunAuthActor(ctx);
    }
}

template <bool UseMigrationProtocol>
void TReadSessionActor<UseMigrationProtocol>::RunAuthActor(const TActorContext& ctx) {
    Y_VERIFY(!AuthInitActor);
    AuthInitActor = ctx.Register(new TReadInitAndAuthActor(
        ctx, ctx.SelfID, ClientId, Cookie, Session, SchemeCache, NewSchemeCache, Counters, Token, TopicsList,
        TopicsHandler.GetLocalCluster(), ReadWithoutConsumer));
}

}
