#include "caching_service.h"
#include "deadline_map.h"

#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>
#include <ydb/core/persqueue/common/key.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/persqueue_utils.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <contrib/libs/protobuf/src/google/protobuf/util/time_util.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_READ_PROXY

namespace NKikimr::NPQ {
using namespace NActors;
using namespace Ydb::Topic;
using namespace NGRpcProxy::V1;

namespace {
constexpr ui64 ExpireDeadlineMapsWakeupTag = 1;
constexpr TDuration DeadlineMapWakeupPeriod = TDuration::Minutes(1);
} // namespace

i32 GetDataChunkCodec(const NKikimrPQClient::TDataChunk& proto) {
    if (proto.HasCodec()) {
        return proto.GetCodec() + 1;
    }
    return 0;
}

<<<<<<< HEAD
#define PQ_CPROXY_LOG_D(message) LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, TStringBuilder() << "Direct read cache: " << message);
#define PQ_CPROXY_LOG_I(message) LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, TStringBuilder() << "Direct read cache: " << message);
#define PQ_CPROXY_LOG_W(message) LOG_WARN_S(ctx, NKikimrServices::PQ_READ_PROXY, TStringBuilder() << "Direct read cache: " << message);
#define PQ_CPROXY_LOG_E(message) LOG_ERROR_S(ctx, NKikimrServices::PQ_READ_PROXY, TStringBuilder() << "Direct read cache: " << message);
#define PQ_CPROXY_LOG_A(message) LOG_ALERT_S(ctx, NKikimrServices::PQ_READ_PROXY, TStringBuilder() << "Direct read cache: " << message);

void SetKafkaBatchBaseOffsetIfNeeded(NKikimrPQClient::TDataChunk& proto, ui64 offset) {
    if (GetDataChunkCodec(proto) == Ydb::Topic::CODEC_KAFKA_BATCH) {
        NKafka::SetKafkaBatchBaseOffset(*proto.MutableData(), offset);
    }
}

=======
>>>>>>> 268c27ad174 ([YDB_LOG] Migrate ydb/core/persqueue/dread_cache_service (#45803))
class TPQDirectReadCacheService : public TActorBootstrapped<TPQDirectReadCacheService> {
public:
    TPQDirectReadCacheService(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters)
    {

    }

    void Bootstrap(const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: Created");

        Become(&TThis::StateWork);
        ctx.Schedule(DeadlineMapWakeupPeriod, new TEvents::TEvWakeup(ExpireDeadlineMapsWakeupTag));
    }

    STRICT_STFUNC(StateWork,
          hFunc(TEvPQ::TEvPublishDirectRead, HandlePublish)
          hFunc(TEvPQ::TEvStageDirectReadData, HandleFetchData)
          hFunc(TEvPQ::TEvForgetDirectRead, HandleForget)
          hFunc(TEvPQ::TEvRegisterDirectReadSession, HandleRegister)
          hFunc(TEvPQ::TEvDeregisterDirectReadSession, HandleDeregister)
          hFunc(TEvPQ::TEvGetFullDirectReadData, HandleGetData)
          hFunc(TEvPQProxy::TEvDirectReadDataSessionConnected, HandleCreateClientSession)
          hFunc(TEvPQProxy::TEvDirectReadDataSessionDead, HandleDestroyClientSession)
          hFunc(TEvPQProxy::TEvDirectReadDestroyPartitionSession, HandlePartitionSessionReleased)
          hFunc(TEvents::TEvWakeup, HandleWakeup)
    )

private:
    using TSessionsMap = THashMap<TReadSessionKey, TCacheServiceData>;

    struct TPendingStage {
        ui32 Generation = 0;
        std::shared_ptr<NKikimrClient::TResponse> Response;
    };
    struct TPendingDirectReads {
        TMap<ui64, TPendingStage> Stages;
        TMap<ui64, ui32> Publishes; // readId -> tablet generation
        TInstant Deadline;
    };
    struct TRetiredSession {
        ui32 Generation = 0;
        TInstant Deadline;
    };

    void HandleWakeup(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag != ExpireDeadlineMapsWakeupTag) {
            return;
        }
        const auto& ctx = ActorContext();
        const auto now = ctx.Now();
        const auto pendingExpired = PendingBySession.Expire(now);
        const auto retiredExpired = RetiredSessions.Expire(now);
        if (pendingExpired || retiredExpired) {
            PQ_CPROXY_LOG_I("expired deadline map entries: pending=" << pendingExpired
                            << ", retired=" << retiredExpired);
        }
        ctx.Schedule(DeadlineMapWakeupPeriod, new TEvents::TEvWakeup(ExpireDeadlineMapsWakeupTag));
    }

    void HandleCreateClientSession(TEvPQProxy::TEvDirectReadDataSessionConnected::TPtr& ev) {
        const auto& ctx = ActorContext();
        auto key = MakeSessionKey(ev->Get());
        YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: client session connected with id",
            {"sessionId", key.SessionId});
        ChangeCounterValue("CreateClientSessionRate", 1, false, true);
        auto sessionIter = ServerSessions.find(key);
        if (sessionIter.IsEnd()) {
            YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: unknown session id close session",
                {"sessionId", key.SessionId});
            CloseSession(ev->Sender, key.SessionId, Ydb::PersQueue::ErrorCode::ErrorCode::BAD_REQUEST, "Unknown session");
            return;
        }

        auto sender = ev->Sender;
        if (sessionIter->second.Generation != ev->Get()->Generation) {
            ctx.Send(
                sender,
                new TEvPQProxy::TEvDirectReadDestroyPartitionSession(key, Ydb::PersQueue::ErrorCode::ErrorCode::ERROR, "Generation mismatch")
            );
            return;
        }

        auto startingReadId = ev->Get()->StartingReadId;

        // Let the proxy respond with StartDirectReadPartitionSessionResponse right away,
        // so the client knows that the partition session has been started successfully.
        // Without this response, the client might have to wait until there are topic messages to send.
        ctx.Send(sender, new TEvPQProxy::TEvDirectReadDataSessionConnectedResponse(key.PartitionSessionId, ev->Get()->Generation));

        if (!sessionIter->second.Client.Defined()) {
            ChangeCounterValue("ActiveClientSessions", 1, false);
        } // else Its probably a misbehavior by client (or proxy) but we can handle it anyway
        sessionIter->second.Client = TCacheClientContext{sender, startingReadId};

        AssignByProxy[sender].insert(key.PartitionSessionId);
        while(SendNextReadToClient(sessionIter)) {
            // Empty
        }
    }

    void HandleDestroyClientSession(TEvPQProxy::TEvDirectReadDataSessionDead::TPtr& ev) {
        auto assignIter = AssignByProxy.find(ev->Sender);
        if (assignIter.IsEnd())
            return;
        for (auto id : assignIter->second) {
            return DestroyClientSession(ServerSessions.find(
                    TReadSessionKey{ev->Get()->Session, id}), false,
                    Ydb::PersQueue::ErrorCode::ErrorCode::OK, "", ev->Sender
            );
        }
        AssignByProxy.erase(assignIter);
    }

    void HandlePartitionSessionReleased(TEvPQProxy::TEvDirectReadDestroyPartitionSession::TPtr& ev) {
        auto assignIter = AssignByProxy.find(ev->Sender);
        if (assignIter.IsEnd())
            return;
        if (!assignIter->second.contains(ev->Get()->ReadKey.PartitionSessionId))
            return;

        assignIter->second.erase(ev->Get()->ReadKey.PartitionSessionId);
        const auto& key = ev->Get()->ReadKey;
        auto sessionIter = ServerSessions.find(key);
        if (!sessionIter.IsEnd()) {
            MarkSessionRetired(key, sessionIter->second.Generation);
            ServerSessions.erase(sessionIter);
            ChangeCounterValue("ActiveServerSessions", ServerSessions.size(), true);
        } else {
            // No server session (e.g. already Deregistered): still retire the key so late
            // Stage/Publish cannot recreate PendingBySession. Generation is unknown here —
            // use Max to block all gens until Register clears the tombstone.
            MarkSessionRetired(key, Max<ui32>());
        }
    }

    void HandleRegister(TEvPQ::TEvRegisterDirectReadSession::TPtr& ev) {
        const auto& key = ev->Get()->Session;
        RegisterServerSession(key, ev->Get()->Generation);
    }

    void HandleDeregister(TEvPQ::TEvDeregisterDirectReadSession::TPtr& ev) {
        const auto& key = ev->Get()->Session;
        const auto& ctx = ActorContext();
        const auto generation = ev->Get()->Generation;

        auto destroyDone = DestroyServerSession(ServerSessions.find(key), generation);
        // Always retire this generation so late Stage/Publish cannot re-create PendingBySession
        // after Deregister/Release (or after Stage-before-Register for a session that never
        // registered and then died).
        MarkSessionRetired(key, generation);
        if (destroyDone) {
            YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: server session",
                {"deregistered", key.SessionId});
        } else {
<<<<<<< HEAD
            PQ_CPROXY_LOG_W("attempted to deregister unknown server session: " << key.SessionId
                            << ":" << key.PartitionSessionId << " with generation " << generation << ", ignored");
=======
            YDB_LOG_WARN_CTX(ctx, "Direct read cache: attempted to deregister unknown server session with generation ignored",
                {"sessionId", key.SessionId},
                {"partitionSessionId", key.PartitionSessionId},
                {"Generation", ev->Get()->Generation});
>>>>>>> 268c27ad174 ([YDB_LOG] Migrate ydb/core/persqueue/dread_cache_service (#45803))
            return;
        }
    }

    void HandleFetchData(TEvPQ::TEvStageDirectReadData::TPtr& ev) {
        const auto& ctx = ActorContext();
        auto sessionKey = MakeSessionKey(ev->Get());
        auto sessionIter = ServerSessions.find(sessionKey);
        if (sessionIter.IsEnd()) {
<<<<<<< HEAD
            if (IsSessionGenerationRetired(sessionKey, ev->Get()->TabletGeneration)) {
                PQ_CPROXY_LOG_I("drop stage for retired session generation: session=" << sessionKey.SessionId
                                << ", partitionSessionId=" << sessionKey.PartitionSessionId
                                << ", ReadKey.ReadId=" << ev->Get()->ReadKey.ReadId
                                << ", TabletGeneration=" << ev->Get()->TabletGeneration);
                return;
            }
            // LOGBROKER-10590: CreateSession Register is fire-and-forget; Stage can arrive first.
            // Dropping it permanently leaves tablet inFlight without client-visible DirectRead.
            PQ_CPROXY_LOG_I("buffer stage for unregistered session: session=" << sessionKey.SessionId
                            << ", partitionSessionId=" << sessionKey.PartitionSessionId
                            << ", ReadKey.ReadId=" << ev->Get()->ReadKey.ReadId
                            << ", TabletGeneration=" << ev->Get()->TabletGeneration);
            BufferPendingStage(
                    sessionKey,
                    ev->Get()->ReadKey.ReadId,
                    ev->Get()->TabletGeneration,
                    ev->Get()->Response);
            return;
        }
        StageToSession(sessionIter, ev->Get()->ReadKey.ReadId, ev->Get()->TabletGeneration, ev->Get()->Response);
        TryApplyPendingPublish(sessionKey, ev->Get()->ReadKey.ReadId);
=======
            YDB_LOG_ERROR_CTX(ctx, "Direct read cache: tried to stage direct read for unregistered session",
                {"session", sessionKey.SessionId},
                {"partitionSessionId", sessionKey.PartitionSessionId});
            return;
        }
        if (sessionIter->second.Generation != ev->Get()->TabletGeneration) {
            YDB_LOG_ALERT_CTX(ctx, "Direct read cache: tried to stage direct read for session with generation previously had this session with generation Data ignored",
                {"sessionId", sessionKey.SessionId},
                {"TabletGeneration", ev->Get()->TabletGeneration},
                {"generation", sessionIter->second.Generation});
            return;
        }
        auto ins = sessionIter->second.StagedReads.insert(std::make_pair(ev->Get()->ReadKey.ReadId, ev->Get()->Response));
        if (!ins.second) {
            YDB_LOG_WARN_CTX(ctx, "Direct read cache: tried to stage duplicate direct read for session with id new data ignored",
                {"sessionId", sessionKey.SessionId},
                {"ReadKey.ReadId", ev->Get()->ReadKey.ReadId});
            return;
        }
        ChangeCounterValue("StagedReadDataSize", ins.first->second->ByteSize(), false);
        ChangeCounterValue("StagedReadsCount", 1, false);
        ChangeCounterValue("StagedReadsRate", 1, false, true);
        YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: staged direct read id",
            {"ReadKey.ReadId", ev->Get()->ReadKey.ReadId},
            {"session", sessionKey.SessionId});
>>>>>>> 268c27ad174 ([YDB_LOG] Migrate ydb/core/persqueue/dread_cache_service (#45803))
    }

    void HandlePublish(TEvPQ::TEvPublishDirectRead::TPtr& ev) {
        const auto& ctx = ActorContext();
        auto key = MakeSessionKey(ev->Get());
        const auto readId = ev->Get()->ReadKey.ReadId;
        const auto& generation = ev->Get()->TabletGeneration;
        YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: publish for session",
            {"read", readId},
            {"sessionId", key.SessionId},
            {"generation", generation});

        auto iter = ServerSessions.find(key);
        if (iter.IsEnd()) {
<<<<<<< HEAD
            if (IsSessionGenerationRetired(key, generation)) {
                PQ_CPROXY_LOG_I("drop publish for retired session generation: sessionId=" << key.SessionId
                                << ", partitionSessionId=" << key.PartitionSessionId
                                << ", readId=" << readId << ", generation=" << generation);
                return;
            }
            PQ_CPROXY_LOG_I("buffer publish for unregistered session: sessionId=" << key.SessionId
                            << ", partitionSessionId=" << key.PartitionSessionId
                            << ", readId=" << readId << ", generation=" << generation);
            BufferPendingPublish(key, readId, generation);
            return;
        }

        Y_UNUSED(PublishToSession(iter, readId, generation));
=======
            YDB_LOG_ERROR_CTX(ctx, "Direct read cache: attempt to publish read for unknow session ignored",
                {"sessionId", key.SessionId});
            return;
        }

        if (iter->second.Generation != generation)
            return;

        auto stagedIter = iter->second.StagedReads.find(readId);
        if (stagedIter == iter->second.StagedReads.end()) {
            YDB_LOG_ERROR_CTX(ctx, "Direct read cache: attempt to publish unknown read id ignored",
                {"readId", readId},
                {"sessionId", key.SessionId});
            return;
        }
        auto inserted = iter->second.Reads.insert(std::make_pair(ev->Get()->ReadKey.ReadId, stagedIter->second)).second;
        if (inserted) {
            ChangeCounterValue("PublishedReadDataSize", stagedIter->second->ByteSize(), false);
            ChangeCounterValue("PublishedReadsCount", 1, false);
            ChangeCounterValue("PublishedReadsRate", 1, false, true);
        }
        ChangeCounterValue("StagedReadDataSize", -stagedIter->second->ByteSize(), false);
        ChangeCounterValue("StagedReadsCount", -1, false);

        iter->second.StagedReads.erase(stagedIter);

        SendNextReadToClient(iter);
>>>>>>> 268c27ad174 ([YDB_LOG] Migrate ydb/core/persqueue/dread_cache_service (#45803))
    }

    void HandleForget(TEvPQ::TEvForgetDirectRead::TPtr& ev) {
        const auto& ctx = ActorContext();
        auto key = MakeSessionKey(ev->Get());
        const auto readId = ev->Get()->ReadKey.ReadId;
        const auto generation = ev->Get()->TabletGeneration;
        ForgetPending(key, readId, generation);
        auto iter = ServerSessions.find(key);
        if (iter.IsEnd()) {
            YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: attempt to forget read for unknown session ignored",
                {"session", ev->Get()->ReadKey.SessionId});
            return;
        }
<<<<<<< HEAD
        PQ_CPROXY_LOG_D("forget read: " << readId << " for session " << key.SessionId);
=======
        YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: forget for session",
            {"read", ev->Get()->ReadKey.ReadId},
            {"sessionId", key.SessionId});
>>>>>>> 268c27ad174 ([YDB_LOG] Migrate ydb/core/persqueue/dread_cache_service (#45803))

        if (iter->second.Generation != generation) { // Stale generation in event, ignore it
            return;
        }
        bool didForget = false;
        auto readIter = iter->second.Reads.find(ev->Get()->ReadKey.ReadId);
        if (readIter != iter->second.Reads.end()) {
            ChangeCounterValue("PublishedReadDataSize", -readIter->second->ByteSize(), false);
            ChangeCounterValue("PublishedReadsCount", -1, false);
            didForget = true;

            iter->second.Reads.erase(readIter);
        }
        auto stagedIter = iter->second.StagedReads.find(ev->Get()->ReadKey.ReadId);
        if (stagedIter != iter->second.StagedReads.end()) {
            ChangeCounterValue("StagedReadDataSize", -stagedIter->second->ByteSize(), false);
            ChangeCounterValue("StagedReadsCount", -1, false);
            didForget = true;
            iter->second.StagedReads.erase(stagedIter);
        }
        if (didForget) {
            ChangeCounterValue("ForgetReadsRate", 1, false, true);
        }
        iter->second.StagedReads.erase(ev->Get()->ReadKey.ReadId);
    }

    void DestroyClientSession(
            TSessionsMap::iterator sessionIter, bool doRespondToProxy, Ydb::PersQueue::ErrorCode::ErrorCode code,
            const TString& reason, const TMaybe<TActorId>& proxyId = Nothing()
    ) {
        if (sessionIter.IsEnd() || !sessionIter->second.Client.Defined())
            return;
        auto& client = sessionIter->second.Client.GetRef();
        if (proxyId.Defined() && *proxyId != client.ProxyId)
            return;

        if (doRespondToProxy) {
            DestroyPartitionSession(sessionIter, code, reason);
        }
        auto assignIter = AssignByProxy.find(sessionIter->second.Client->ProxyId);
        if (!assignIter.IsEnd()) {
            assignIter->second.erase(sessionIter->first.PartitionSessionId);
        }
        if (sessionIter->second.Client.Defined()) {
            ChangeCounterValue("ActiveClientSessions", -1, false);
        }
        sessionIter->second.Client = Nothing();
    }

    [[nodiscard]] bool DestroyServerSession(TSessionsMap::iterator sessionIter, ui64 generation) {
        if (sessionIter.IsEnd() || sessionIter->second.Generation > generation)
            return false;
        DestroyPartitionSession(sessionIter, Ydb::PersQueue::ErrorCode::READ_ERROR_NO_SESSION, "Closed by server");
        ServerSessions.erase(sessionIter);
        ChangeCounterValue("ActiveServerSessions", ServerSessions.size(), true);
        return true;
    }

    void RegisterServerSession(const TReadSessionKey& key, ui32 generation) {
        const auto& ctx = ActorContext();
        auto sessionsIter = ServerSessions.find(key);
        if (sessionsIter.IsEnd()) {
            YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: registered server with generation",
                {"sessionId", key.SessionId},
                {"partitionSessionId", key.PartitionSessionId},
                {"generation", generation});

            ClearRetiredSession(key);
            ServerSessions.insert(std::make_pair(key, TCacheServiceData{generation}));
            FlushPendingDirectReads(key);
        } else if (sessionsIter->second.Generation == generation) {
<<<<<<< HEAD
            PQ_CPROXY_LOG_W("attempted to register duplicate server session: " << key.SessionId << ":"
                            << key.PartitionSessionId << " with same generation " << generation << ", ignored");
            ClearRetiredSession(key);
            FlushPendingDirectReads(key);
        } else if (DestroyServerSession(sessionsIter, generation)) {
            PQ_CPROXY_LOG_D("registered server session: " << key.SessionId
                            << ":" << key.PartitionSessionId << " with generation " << generation
                            << ", killed existing session with older generation ");
            ClearRetiredSession(key);
=======
            YDB_LOG_WARN_CTX(ctx, "Direct read cache: attempted to register duplicate server with same generation ignored",
                {"session", key.SessionId},
                {"sessionId", key.PartitionSessionId},
                {"generation", generation});

        } else if (DestroyServerSession(sessionsIter, generation)) {
            YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: registered server with generation killed existing session with older generation",
                {"sessionId", key.SessionId},
                {"partitionSessionId", key.PartitionSessionId},
                {"generation", generation});
>>>>>>> 268c27ad174 ([YDB_LOG] Migrate ydb/core/persqueue/dread_cache_service (#45803))
            ServerSessions.insert(std::make_pair(key, TCacheServiceData{generation}));
            FlushPendingDirectReads(key);
        } else {
            YDB_LOG_INFO_CTX(ctx, "Direct read cache: attempted to register server with stale generation ignored",
                {"session", key.SessionId},
                {"sessionId", key.PartitionSessionId},
                {"generation", generation});
        }
        ChangeCounterValue("ActiveServerSessions", ServerSessions.size(), true);
    }

    void StageToSession(
            TSessionsMap::iterator sessionIter,
            ui64 readId,
            ui32 tabletGeneration,
            const std::shared_ptr<NKikimrClient::TResponse>& response)
    {
        const auto& ctx = ActorContext();
        if (sessionIter.IsEnd()) {
            return;
        }
        if (sessionIter->second.Generation != tabletGeneration) {
            PQ_CPROXY_LOG_A("Stage generation mismatch for session " << sessionIter->first.SessionId
                            << ", TabletGeneration=" << tabletGeneration
                            << ", previously had generation=" << sessionIter->second.Generation << ". Data ignored");
            return;
        }
        auto ins = sessionIter->second.StagedReads.insert(std::make_pair(readId, response));
        if (!ins.second) {
            PQ_CPROXY_LOG_W("tried to stage duplicate direct read for session " << sessionIter->first.SessionId
                            << " with id " << readId << ", new data ignored");
            return;
        }
        ChangeCounterValue("StagedReadDataSize", ins.first->second->ByteSize(), false);
        ChangeCounterValue("StagedReadsCount", 1, false);
        ChangeCounterValue("StagedReadsRate", 1, false, true);
        PQ_CPROXY_LOG_D("staged direct read id " << readId << " for session: " << sessionIter->first.SessionId);
    }

    // Returns true if the read was published (or already published). False if generation
    // mismatches or there is no staged payload for readId yet.
    [[nodiscard]] bool PublishToSession(TSessionsMap::iterator iter, ui64 readId, ui32 generation) {
        const auto& ctx = ActorContext();
        if (iter.IsEnd()) {
            return false;
        }
        if (iter->second.Generation != generation)
            return false;

        auto stagedIter = iter->second.StagedReads.find(readId);
        if (stagedIter == iter->second.StagedReads.end()) {
            PQ_CPROXY_LOG_E("attempt to publish unknown read id " << readId << " from session: "
                            << iter->first.SessionId << " ignored");
            return false;
        }
        auto inserted = iter->second.Reads.insert(std::make_pair(readId, stagedIter->second)).second;
        if (inserted) {
            ChangeCounterValue("PublishedReadDataSize", stagedIter->second->ByteSize(), false);
            ChangeCounterValue("PublishedReadsCount", 1, false);
            ChangeCounterValue("PublishedReadsRate", 1, false, true);
        }
        ChangeCounterValue("StagedReadDataSize", -stagedIter->second->ByteSize(), false);
        ChangeCounterValue("StagedReadsCount", -1, false);

        iter->second.StagedReads.erase(stagedIter);

        SendNextReadToClient(iter);
        return true;
    }

    void FlushPendingDirectReads(const TReadSessionKey& key) {
        auto* pending = PendingBySession.Find(key);
        if (!pending) {
            return;
        }
        auto sessionIter = ServerSessions.find(key);
        if (sessionIter.IsEnd()) {
            // Register always inserts the session before flush; keep pending if that invariant breaks.
            return;
        }

        const ui32 sessionGeneration = sessionIter->second.Generation;
        const auto& ctx = ActorContext();
        PQ_CPROXY_LOG_D("flush pending stage/publish after register: sessionId=" << key.SessionId
                        << ", partitionSessionId=" << key.PartitionSessionId
                        << ", sessionGeneration=" << sessionGeneration
                        << ", stages=" << pending->Stages.size()
                        << ", publishes=" << pending->Publishes.size());

        // Apply only matching generation. Drop stale lower gens; keep higher gens for a later Register.
        for (auto it = pending->Stages.begin(); it != pending->Stages.end(); ) {
            if (it->second.Generation == sessionGeneration) {
                StageToSession(sessionIter, it->first, it->second.Generation, it->second.Response);
                it = pending->Stages.erase(it);
            } else if (it->second.Generation < sessionGeneration) {
                it = pending->Stages.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending->Publishes.begin(); it != pending->Publishes.end(); ) {
            if (it->second == sessionGeneration) {
                // Keep Publish if Stage for this gen is not staged yet (may arrive after Register).
                if (sessionIter->second.StagedReads.contains(it->first)
                        && PublishToSession(sessionIter, it->first, it->second)) {
                    it = pending->Publishes.erase(it);
                } else {
                    ++it;
                }
            } else if (it->second < sessionGeneration) {
                it = pending->Publishes.erase(it);
            } else {
                ++it;
            }
        }
        if (pending->Stages.empty() && pending->Publishes.empty()) {
            PendingBySession.Erase(key);
        }
    }

    // If a Publish was buffered before its Stage (or Stage was dropped as stale lower-gen),
    // apply it once the matching Stage lands on a registered session.
    void TryApplyPendingPublish(const TReadSessionKey& key, ui64 readId) {
        auto* pending = PendingBySession.Find(key);
        if (!pending) {
            return;
        }
        auto sessionIter = ServerSessions.find(key);
        if (sessionIter.IsEnd()) {
            return;
        }
        auto publishIt = pending->Publishes.find(readId);
        if (publishIt == pending->Publishes.end()) {
            return;
        }
        if (publishIt->second != sessionIter->second.Generation) {
            return;
        }
        if (!sessionIter->second.StagedReads.contains(readId)) {
            return;
        }
        if (!PublishToSession(sessionIter, readId, publishIt->second)) {
            return;
        }
        pending->Publishes.erase(publishIt);
        if (pending->Stages.empty() && pending->Publishes.empty()) {
            PendingBySession.Erase(key);
        }
    }

    TPendingDirectReads& GetOrCreatePending(const TReadSessionKey& key) {
        return PendingBySession.FindOrInsert(
            key, TPendingDirectReads{}, ActorContext().Now());
    }

    // DirectReadIds can repeat across tablet generations; keep the highest generation
    // (first entry wins for same-generation duplicates). A lower-gen Publish for an
    // overwritten Stage is dropped.
    void BufferPendingStage(
            const TReadSessionKey& key,
            ui64 readId,
            ui32 generation,
            const std::shared_ptr<NKikimrClient::TResponse>& response)
    {
        auto& pending = GetOrCreatePending(key);
        auto it = pending.Stages.find(readId);
        if (it == pending.Stages.end()) {
            pending.Stages.emplace(readId, TPendingStage{generation, response});
            return;
        }
        if (generation <= it->second.Generation) {
            return;
        }
        it->second = TPendingStage{generation, response};
        auto publishIt = pending.Publishes.find(readId);
        if (publishIt != pending.Publishes.end() && publishIt->second < generation) {
            pending.Publishes.erase(publishIt);
        }
    }

    void BufferPendingPublish(const TReadSessionKey& key, ui64 readId, ui32 generation) {
        auto& pending = GetOrCreatePending(key);
        auto stageIt = pending.Stages.find(readId);
        if (stageIt != pending.Stages.end()) {
            if (stageIt->second.Generation > generation) {
                return;
            }
            // Publish for a newer generation invalidates a stale Stage of an older generation.
            if (stageIt->second.Generation < generation) {
                pending.Stages.erase(stageIt);
            }
        }
        auto it = pending.Publishes.find(readId);
        if (it == pending.Publishes.end()) {
            pending.Publishes.emplace(readId, generation);
            return;
        }
        if (generation > it->second) {
            it->second = generation;
        }
    }

    void ForgetPending(const TReadSessionKey& key, ui64 readId, ui32 generation) {
        auto* pending = PendingBySession.Find(key);
        if (!pending) {
            return;
        }
        auto stageIt = pending->Stages.find(readId);
        if (stageIt != pending->Stages.end() && generation >= stageIt->second.Generation) {
            pending->Stages.erase(stageIt);
        }
        auto publishIt = pending->Publishes.find(readId);
        if (publishIt != pending->Publishes.end() && generation >= publishIt->second) {
            pending->Publishes.erase(publishIt);
        }
        if (pending->Stages.empty() && pending->Publishes.empty()) {
            PendingBySession.Erase(key);
        }
    }

    void MarkSessionRetired(const TReadSessionKey& key, ui32 generation) {
        const auto now = ActorContext().Now();
        auto& retired = RetiredSessions.FindOrInsert(
            key, TRetiredSession{.Generation = generation}, now);
        retired.Generation = Max(retired.Generation, generation);
        RetiredSessions.TouchDeadline(key, now);

        const ui32 retiredGeneration = retired.Generation;

        // Drop only pending for generations <= retired. Newer Stage/Publish (e.g. Stage(N)
        // before Register, then stale Deregister(N-1)) must survive for FlushPending.
        auto* pending = PendingBySession.Find(key);
        if (!pending) {
            return;
        }
        for (auto it = pending->Stages.begin(); it != pending->Stages.end(); ) {
            if (it->second.Generation <= retiredGeneration) {
                it = pending->Stages.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending->Publishes.begin(); it != pending->Publishes.end(); ) {
            if (it->second <= retiredGeneration) {
                it = pending->Publishes.erase(it);
            } else {
                ++it;
            }
        }
        if (pending->Stages.empty() && pending->Publishes.empty()) {
            PendingBySession.Erase(key);
        }
    }

    void ClearRetiredSession(const TReadSessionKey& key) {
        RetiredSessions.Erase(key);
    }

    bool IsSessionGenerationRetired(const TReadSessionKey& key, ui32 generation) const {
        const auto* retired = RetiredSessions.Find(key);
        return retired && generation <= retired->Generation;
    }

    template<class TEv>
    const TReadSessionKey MakeSessionKey(TEv* ev) {
        return TReadSessionKey{ev->ReadKey.SessionId, ev->ReadKey.PartitionSessionId};
    }

    void HandleGetData(TEvPQ::TEvGetFullDirectReadData::TPtr& ev) {
        auto* response = new TEvPQ::TEvGetFullDirectReadData();
        auto& data = response->Data;
        auto key = MakeSessionKey(ev->Get());

        if (key.SessionId.empty()) {
            for (const auto& [k,v] : ServerSessions) {
                data.emplace_back(k, v);
            }
        } else {
            auto iter = ServerSessions.find(key);
            if (iter.IsEnd()) {
                response->Error = true;
            } else if (ev->Get()->Generation == iter->second.Generation) {
                data.emplace_back(key, iter->second);
            }
        }
        ActorContext().Send(ev->Sender, response);
    }

private:
    using TServerMessage = StreamDirectReadMessage::FromServer;
    using TClientMessage = StreamDirectReadMessage::FromClient;
    using IContext = NGRpcServer::IGRpcStreamingContext<TClientMessage, TServerMessage>;

    bool SendNextReadToClient(TSessionsMap::iterator& sessionIter) {
        if (sessionIter.IsEnd() || !sessionIter->second.Client.Defined()) {
            return false;
        }
        auto& client = sessionIter->second.Client.GetRef();
        auto nextData = sessionIter->second.Reads.lower_bound(client.NextReadId);
        if (nextData == sessionIter->second.Reads.end()) {
            return false;
        }
        auto result = SendData(sessionIter->first.SessionId, sessionIter->first.PartitionSessionId, client, nextData->first, nextData->second);
        ChangeCounterValue("SendDataRate", 1, false, true);
        if (!result) {
            //ToDo: for discuss. Error in parsing partition response - shall we kill the entire session or just the partition session?
            DestroyClientSession(sessionIter, false, Ydb::PersQueue::ErrorCode::OK, "");
            return false;
        }
        client.NextReadId = nextData->first + 1;
        return true;
    }

    [[nodiscard]] bool SendData(
            const TString& sessionId, ui64 partSessionId, TCacheClientContext& proxyClient, ui64 readId, const std::shared_ptr<NKikimrClient::TResponse>& response
    ) {
        const auto& ctx = ActorContext();
        auto message = std::make_shared<StreamDirectReadMessage::FromServer>();
        auto* directReadMessage = message->mutable_direct_read_response();
        directReadMessage->set_direct_read_id(readId);
        directReadMessage->set_partition_session_id(partSessionId);
        directReadMessage->set_bytes_size(response->GetPartitionResponse().GetCmdPrepareReadResult().GetBytesSizeEstimate());

        auto ok = VaildatePartitionResponse(sessionId, proxyClient, * response);
        if (!ok) {
            return false;
        }

        FillBatchedData(directReadMessage->mutable_partition_data(), response->GetPartitionResponse().GetCmdReadResult(),
                        partSessionId);
        message->set_status(Ydb::StatusIds::SUCCESS);

        YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: send data to client",
            {"sessionId", sessionId},
            {"assignId", partSessionId},
            {"readId", readId});

        ctx.Send(proxyClient.ProxyId, new TEvPQProxy::TEvDirectReadSendClientData(std::move(message)));
        return true;
    }

    void CloseSession(
            const TActorId& proxyId,
            const TString& sessionId,
            Ydb::PersQueue::ErrorCode::ErrorCode code,
            const TString& reason
    ) {
        const auto& ctx = ActorContext();
        ctx.Send(proxyId, new TEvPQProxy::TEvDirectReadCloseSession(code, reason));
        YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: close session for proxy",
            {"proxyId", proxyId},
            {"sessionId", sessionId});
    }

    bool DestroyPartitionSession(
            TSessionsMap::iterator sessionIter, Ydb::PersQueue::ErrorCode::ErrorCode code, const TString& reason
    ) {
        if (sessionIter.IsEnd() || !sessionIter->second.Client.Defined()) {
            return false;
        }

        const auto& ctx = ActorContext();
        ctx.Send(
                sessionIter->second.Client->ProxyId, new TEvPQProxy::TEvDirectReadDestroyPartitionSession(sessionIter->first, code, reason)
        );
        YDB_LOG_DEBUG_CTX(ctx, "Direct read cache: DestroyPartitionSession",
            {"sessionId", sessionIter->first.SessionId},
            {"proxy", sessionIter->second.Client->ProxyId});
        return true;
    }

    void ChangeCounterValue(const TString& name, i64 value, bool isAbs, bool deriv = false) {
        if (!Counters)
            return;
        auto counter = Counters->GetCounter(name, deriv);
        if (isAbs)
            counter->Set(value);
        else if (value >= 0)
            counter->Add(value);
        else
            counter->Sub(-value);
    }

    bool VaildatePartitionResponse(
            const TString& sessionId, TCacheClientContext& proxyClient, NKikimrClient::TResponse& response
    ) {
        if (response.HasErrorCode() && response.GetErrorCode() != NPersQueue::NErrorCode::OK) {
            CloseSession(
                    proxyClient.ProxyId,
                    sessionId,
                    NGRpcProxy::V1::ConvertOldCode(response.GetErrorCode()),
                    "Status is not ok: " + response.GetErrorReason()
            );
            return false;
        }

        if (response.GetStatus() != NKikimr::NMsgBusProxy::MSTATUS_OK) { //this is incorrect answer, die
            CloseSession(
                    proxyClient.ProxyId,
                    sessionId,
                    Ydb::PersQueue::ErrorCode::ERROR,
                    "Status is not ok: " + response.GetErrorReason()
            );
            return false;
        }
        if (!response.HasPartitionResponse()) { //this is incorrect answer, die
            CloseSession(
                    proxyClient.ProxyId,
                    sessionId,
                    Ydb::PersQueue::ErrorCode::ERROR,
                    "Direct read cache got empty partition response"
            );
            return false;
        }

        const auto& partResponse = response.GetPartitionResponse();
        if (!partResponse.HasCmdReadResult()) { //this is incorrect answer, die
            CloseSession(
                    proxyClient.ProxyId,
                    sessionId,
                    Ydb::PersQueue::ErrorCode::ERROR,
                    "Malformed response from partition"
            );
            return false;
        }
        return true;
    }

    void FillBatchedData(auto* partitionData, const NKikimrClient::TCmdReadResult& res, ui64 assignId) {
        partitionData->set_partition_session_id(assignId);

        i32 batchCodec = 0; // UNSPECIFIED

        StreamReadMessage::ReadResponse::Batch* currentBatch = nullptr;
        for (ui32 i = 0; i < res.ResultSize(); ++i) {
            const auto& r = res.GetResult(i);

            auto proto(GetDeserializedData(r.GetData()));
            if (proto.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
                continue; //TODO - no such chunks must be on prod
            }

            if (!proto.has_codec()) {
                proto.set_codec(NPersQueueCommon::RAW);
            }
            SetKafkaBatchBaseOffsetIfNeeded(proto, r.GetOffset());

            TString sourceId;
            if (!r.GetSourceId().empty()) {
                sourceId = NPQ::NSourceIdEncoding::Decode(r.GetSourceId());
            }

            i64 currBatchWrittenAt = currentBatch ? ::google::protobuf::util::TimeUtil::TimestampToMilliseconds(currentBatch->written_at()) : 0;
            if (currentBatch == nullptr || currBatchWrittenAt != static_cast<i64>(r.GetWriteTimestampMS()) ||
                    currentBatch->producer_id() != sourceId ||
                    GetDataChunkCodec(proto) != batchCodec
            ) {
                // If write time and source id are the same, the rest fields will be the same too.
                currentBatch = partitionData->add_batches();
                i64 write_ts = static_cast<i64>(r.GetWriteTimestampMS());
                AFL_ENSURE(write_ts >= 0);
                *currentBatch->mutable_written_at() = ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(write_ts);
                // Use shared helper to properly encode non-UTF-8 source IDs
                NGRpcProxy::V1::SetBatchSourceId(currentBatch, std::move(sourceId));
                batchCodec = GetDataChunkCodec(proto);
                currentBatch->set_codec(batchCodec);

                if (proto.HasMeta()) {
                    const auto& header = proto.GetMeta();
                    if (header.HasServer()) {
                         (*currentBatch->mutable_write_session_meta())["server"] = header.GetServer();
                    }
                    if (header.HasFile()) {
                         (*currentBatch->mutable_write_session_meta())["file"] = header.GetFile();
                    }
                    if (header.HasIdent()) {
                         (*currentBatch->mutable_write_session_meta())["ident"] = header.GetIdent();
                    }
                    if (header.HasLogType()) {
                         (*currentBatch->mutable_write_session_meta())["logtype"] = header.GetLogType();
                    }
                }
                if (proto.HasExtraFields()) {
                    const auto& map = proto.GetExtraFields();
                    for (const auto& kv : map.GetItems()) {
                         (*currentBatch->mutable_write_session_meta())[kv.GetKey()] = kv.GetValue();
                    }
                }

                if (proto.HasIp() && IsUtf(proto.GetIp())) {
                    (*currentBatch->mutable_write_session_meta())["_ip"] = proto.GetIp();
                }
            }

            auto* message = currentBatch->add_message_data();

            message->set_seq_no(r.GetSeqNo());
            message->set_offset(r.GetOffset());
            message->set_data(proto.GetData());
            message->set_uncompressed_size(r.GetUncompressedSize());

            *message->mutable_created_at() =
                ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(r.GetCreateTimestampMS());

            message->set_message_group_id(currentBatch->producer_id());
            auto* msgMeta = message->mutable_metadata_items();
            *msgMeta = (proto.GetMessageMeta());
        }
    }

    TSessionsMap ServerSessions;
    TDeadlineMap<TReadSessionKey, TPendingDirectReads> PendingBySession;
    // Highest generation for which the session was Deregistered/Released. Late Stage/Publish
    // with generation <= this value must not re-create PendingBySession after teardown.
    // Entries expire by TTL (with deadline refresh on each MarkSessionRetired).
    TDeadlineMap<TReadSessionKey, TRetiredSession> RetiredSessions;
    THashMap<TActorId, TSet<ui64>> AssignByProxy;

    ::NMonitoring::TDynamicCounterPtr Counters;
};


IActor* CreatePQDReadCacheService(const ::NMonitoring::TDynamicCounterPtr& counters) {
    if (counters) {
        return new TPQDirectReadCacheService(
            GetServiceCounters(counters, "persqueue")->GetSubgroup("subsystem", "caching_service"));
    } else {
        return new TPQDirectReadCacheService(nullptr);
    }
}

} // namespace NKikimr::NPQ
