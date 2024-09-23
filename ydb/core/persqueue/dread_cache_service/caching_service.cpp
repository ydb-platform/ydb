#include "caching_service.h"

#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>
#include <ydb/core/persqueue/key.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/persqueue_utils.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/libs/protobuf/src/google/protobuf/util/time_util.h>

namespace NKikimr::NPQ {
using namespace NActors;
using namespace Ydb::Topic;
using namespace NGRpcProxy::V1;


i32 GetDataChunkCodec(const NKikimrPQClient::TDataChunk& proto) {
    if (proto.HasCodec()) {
        return proto.GetCodec() + 1;
    }
    return 0;
}


class TPQDirectReadCacheService : public TActorBootstrapped<TPQDirectReadCacheService> {
public:
    TPQDirectReadCacheService(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters)
    {

    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "Direct read cache created");

        Become(&TThis::StateWork);
        Y_UNUSED(ctx);
        //Y_ABORT_UNLESS(Counters);
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
    )

private:
    using TSessionsMap = THashMap<TReadSessionKey, TCacheServiceData>;

    void HandleCreateClientSession(TEvPQProxy::TEvDirectReadDataSessionConnected::TPtr& ev) {
        const auto& ctx = ActorContext();
        auto key = MakeSessionKey(ev->Get());
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, TStringBuilder() << "Direct read cache: client session connected with id '" << key.SessionId << "'");
        auto sessionIter = ServerSessions.find(key);
        if (sessionIter.IsEnd()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, TStringBuilder() << "Direct read cache: unknown session id '" << key.SessionId << "', close session");
            CloseSession(ev->Sender, Ydb::PersQueue::ErrorCode::ErrorCode::BAD_REQUEST, "Unknown session");
            return;
        }
        if (sessionIter->second.Generation != ev->Get()->Generation) {
            ctx.Send(
                    sessionIter->second.Client->ProxyId,
                    new TEvPQProxy::TEvDirectReadDestroyPartitionSession(key, Ydb::PersQueue::ErrorCode::ErrorCode::ERROR, "Generation mismatch")
            );
            return;
        }

        auto sender = ev->Sender;
        auto startingReadId = ev->Get()->StartingReadId;

        // Let the proxy respond with StartDirectReadPartitionSessionResponse right away,
        // so the client knows that the partition session has been started successfully.
        // Without this response, the client might have to wait until there are topic messages to send.
        ctx.Send(sender, new TEvPQProxy::TEvDirectReadDataSessionConnectedResponse(key.PartitionSessionId, ev->Get()->Generation));

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
    }

    void HandleRegister(TEvPQ::TEvRegisterDirectReadSession::TPtr& ev) {
        const auto& key = ev->Get()->Session;
        RegisterServerSession(key, ev->Get()->Generation);
    }

    void HandleDeregister(TEvPQ::TEvDeregisterDirectReadSession::TPtr& ev) {
        const auto& key = ev->Get()->Session;
        const auto& ctx = ActorContext();

        auto destroyDone = DestroyServerSession(ServerSessions.find(key), ev->Get()->Generation);
        if (destroyDone) {
            LOG_DEBUG_S(
                    ctx, NKikimrServices::PQ_READ_PROXY,
                    TStringBuilder() << "Direct read cache: server session deregistered: " << key.SessionId
            );
        } else {
            LOG_WARN_S(
                    ctx, NKikimrServices::PQ_READ_PROXY,
                    TStringBuilder() << "Direct read cache: attempted to deregister unknown server session: " << key.SessionId
                                     << ":" << key.PartitionSessionId << " with generation " << ev->Get()->Generation << ", ignored"
            );
            return;
        }
    }

    void HandleFetchData(TEvPQ::TEvStageDirectReadData::TPtr& ev) {
        const auto& ctx = ActorContext();
        auto sessionKey = MakeSessionKey(ev->Get());
        auto sessionIter = ServerSessions.find(sessionKey);
        if (sessionIter.IsEnd()) {
            LOG_ERROR_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                TStringBuilder() << "Direct read cache: tried to stage direct read for unregistered session: "
                                 << sessionKey.SessionId << ":" << sessionKey.PartitionSessionId
            );
            return;
        }
        if (sessionIter->second.Generation != ev->Get()->TabletGeneration) {
            LOG_ALERT_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                TStringBuilder() << "Direct read cache: tried to stage direct read for session " << sessionKey.SessionId
                                 << " with generation " << ev->Get()->TabletGeneration << ", previously had this session with generation "
                                 << sessionIter->second.Generation << ". Data ignored"
            );
            return;
        }
        auto ins = sessionIter->second.StagedReads.insert(std::make_pair(ev->Get()->ReadKey.ReadId, ev->Get()->Response));
        if (!ins.second) {
            LOG_WARN_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                TStringBuilder() << "Direct read cache: tried to stage duplicate direct read for session " << sessionKey.SessionId << " with id "
                                 << ev->Get()->ReadKey.ReadId << ", new data ignored"
            );
            return;
        }
        ChangeCounterValue("StagedReadDataSize", ins.first->second->ByteSize(), false);
        ChangeCounterValue("StagedReadsCount", 1, false);
        LOG_DEBUG_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                TStringBuilder() << "Direct read cache: staged direct read id " << ev->Get()->ReadKey.ReadId << " for session: "
                                 << sessionKey.SessionId
        );
    }

    void HandlePublish(TEvPQ::TEvPublishDirectRead::TPtr& ev) {
        const auto& ctx = ActorContext();
        auto key = MakeSessionKey(ev->Get());
        const auto readId = ev->Get()->ReadKey.ReadId;
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, TStringBuilder() << "Direct read cache: publish read: " << readId << " for session " << key.SessionId);
        auto iter = ServerSessions.find(key);
        if (iter.IsEnd()) {
            LOG_ERROR_S(
                    ctx, NKikimrServices::PQ_READ_PROXY,
                    TStringBuilder() << "Direct read cache: attempt to publish read for unknow session " << key.SessionId << " ignored"
            );
            return;
        }

        const auto& generation = ev->Get()->TabletGeneration;
        if (iter->second.Generation != generation)
            return;

        auto stagedIter = iter->second.StagedReads.find(readId);
        if (stagedIter == iter->second.StagedReads.end()) {
            LOG_ERROR_S(
                    ctx, NKikimrServices::PQ_READ_PROXY,
                    TStringBuilder() << "Direct read cache: attempt to publish unknown read id " << readId << " from session: "
                                     << key.SessionId << " ignored");
            return;
        }
        auto inserted = iter->second.Reads.insert(std::make_pair(ev->Get()->ReadKey.ReadId, stagedIter->second)).second;
        if (inserted) {
            ChangeCounterValue("PublishedReadDataSize", stagedIter->second->ByteSize(), false);
            ChangeCounterValue("PublishedReadsCount", 1, false);
        }
        ChangeCounterValue("StagedReadDataSize", -stagedIter->second->ByteSize(), false);
        ChangeCounterValue("StagedReadsCount", -1, false);

        iter->second.StagedReads.erase(stagedIter);

        SendNextReadToClient(iter);
    }

    void HandleForget(TEvPQ::TEvForgetDirectRead::TPtr& ev) {
        const auto& ctx = ActorContext();
        auto key = MakeSessionKey(ev->Get());
        auto iter = ServerSessions.find(key);
        if (iter.IsEnd()) {
            LOG_DEBUG_S(
                    ctx, NKikimrServices::PQ_READ_PROXY,
                    TStringBuilder() << "Direct read cache: attempt to forget read for unknown session: "
                                     << ev->Get()->ReadKey.SessionId << " ignored"
            );
            return;
        }
        LOG_DEBUG_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                TStringBuilder() << "Direct read cache: forget read: " << ev->Get()->ReadKey.ReadId << " for session "
                                 << key.SessionId
        );

        const auto& generation = ev->Get()->TabletGeneration;
        if (iter->second.Generation != generation) { // Stale generation in event, ignore it
            return;
        }
        auto readIter = iter->second.Reads.find(ev->Get()->ReadKey.ReadId);
        if (readIter != iter->second.Reads.end()) {
            ChangeCounterValue("PublishedReadDataSize", -readIter->second->ByteSize(), false);
            ChangeCounterValue("PublishedReadsCount", -1, false);

            iter->second.Reads.erase(readIter);
        }
        auto stagedIter = iter->second.StagedReads.find(ev->Get()->ReadKey.ReadId);
        if (stagedIter != iter->second.StagedReads.end()) {
            ChangeCounterValue("StagedReadDataSize", -stagedIter->second->ByteSize(), false);
            ChangeCounterValue("StagedReadsCount", -1, false);
            iter->second.StagedReads.erase(stagedIter);
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
            LOG_DEBUG_S(
                    ctx, NKikimrServices::PQ_READ_PROXY,
                    TStringBuilder() << "Direct read cache: registered server session: " << key.SessionId
                                     << ":" << key.PartitionSessionId << " with generation " << generation
            );
            ServerSessions.insert(std::make_pair(key, TCacheServiceData{generation}));
        } else if (sessionsIter->second.Generation == generation) {
            LOG_WARN_S(
                    ctx, NKikimrServices::PQ_READ_PROXY,
                    TStringBuilder() << "Direct read cache: attempted to register duplicate server session: " << key.SessionId
                                     << ":" << key.PartitionSessionId << " with same generation " << generation << ", ignored"
            );
        } else if (DestroyServerSession(sessionsIter, generation)) {
            LOG_DEBUG_S(
                    ctx, NKikimrServices::PQ_READ_PROXY,
                    TStringBuilder() << "Direct read cache: registered server session: " << key.SessionId
                                     << ":" << key.PartitionSessionId << " with generation " << generation
                                     << ", killed existing session with older generation "
            );
            ServerSessions.insert(std::make_pair(key, TCacheServiceData{generation}));
        } else {
            LOG_INFO_S(
                        ctx, NKikimrServices::PQ_READ_PROXY,
                        TStringBuilder() << "Direct read cache: attempted to register server session: " << key.SessionId
                                        << ":" << key.PartitionSessionId << " with stale generation " << generation << ", ignored"
            );
        }
        ChangeCounterValue("ActiveServerSessions", ServerSessions.size(), true);
    }

    template<class TEv>
    const TReadSessionKey MakeSessionKey(TEv* ev) {
        return TReadSessionKey{ev->ReadKey.SessionId, ev->ReadKey.PartitionSessionId};
    }

    void HandleGetData(TEvPQ::TEvGetFullDirectReadData::TPtr& ev) {
        auto* response = new TEvPQ::TEvGetFullDirectReadData();
        auto& data = response->Data;
        auto key = MakeSessionKey(ev->Get());

        if (key.SessionId.Empty()) {
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
        auto result = SendData(sessionIter->first.PartitionSessionId, client, nextData->first, nextData->second);
        if (!result) {
            //ToDo: for discuss. Error in parsing partition response - shall we kill the entire session or just the partition session?
            DestroyClientSession(sessionIter, false, Ydb::PersQueue::ErrorCode::OK, "");
            return false;
        }
        client.NextReadId = nextData->first + 1;
        return true;
    }

    [[nodiscard]] bool SendData(
            ui64 partSessionId, TCacheClientContext& proxyClient, ui64 readId, const std::shared_ptr<NKikimrClient::TResponse>& response
    ) {
        const auto& ctx = ActorContext();
        auto message = std::make_shared<StreamDirectReadMessage::FromServer>();
        auto* directReadMessage = message->mutable_direct_read_response();
        directReadMessage->set_direct_read_id(readId);
        directReadMessage->set_partition_session_id(partSessionId);

        auto ok = VaildatePartitionResponse(proxyClient, *response);
        if (!ok) {
            return false;
        }

        FillBatchedData(directReadMessage->mutable_partition_data(), response->GetPartitionResponse().GetCmdReadResult(),
                        partSessionId);
        message->set_status(Ydb::StatusIds::SUCCESS);

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, TStringBuilder() << "Direct read cache: send data to client. AssignId: "
                                                                          << partSessionId << ", readId: " << readId);

        ctx.Send(proxyClient.ProxyId, new TEvPQProxy::TEvDirectReadSendClientData(std::move(message)));
        return true;
    }

    void CloseSession(
            const TActorId& proxyId,
            Ydb::PersQueue::ErrorCode::ErrorCode code,
            const TString& reason
    ) {
        const auto& ctx = ActorContext();
        ctx.Send(proxyId, new TEvPQProxy::TEvDirectReadCloseSession(code, reason));
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, TStringBuilder() << " Direct read cache: close session for proxy " << proxyId.ToString());
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
        LOG_DEBUG_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                TStringBuilder() << " Direct read cache: close session for proxy "
                                 << sessionIter->second.Client->ProxyId.ToString()
        );
        return true;
    }

    void ChangeCounterValue(const TString& name, i64 value, bool isAbs) {
        if (!Counters)
            return;
        auto counter = Counters->GetCounter(name, false);
        if (isAbs)
            counter->Set(value);
        else if (value >= 0)
            counter->Add(value);
        else
            counter->Sub(-value);
    }

    bool VaildatePartitionResponse(
            TCacheClientContext& proxyClient, NKikimrClient::TResponse& response
    ) {
        if (response.HasErrorCode() && response.GetErrorCode() != NPersQueue::NErrorCode::OK) {
            CloseSession(
                    proxyClient.ProxyId,
                    NGRpcProxy::V1::ConvertOldCode(response.GetErrorCode()),
                    "Status is not ok: " + response.GetErrorReason()
            );
            return false;
        }

        if (response.GetStatus() != NKikimr::NMsgBusProxy::MSTATUS_OK) { //this is incorrect answer, die
            CloseSession(
                    proxyClient.ProxyId,
                    Ydb::PersQueue::ErrorCode::ERROR,
                    "Status is not ok: " + response.GetErrorReason()
            );
            return false;
        }
        if (!response.HasPartitionResponse()) { //this is incorrect answer, die
            CloseSession(
                    proxyClient.ProxyId,
                    Ydb::PersQueue::ErrorCode::ERROR,
                    "Direct read cache got empty partition response"
            );
            return false;
        }

        const auto& partResponse = response.GetPartitionResponse();
        if (!partResponse.HasCmdReadResult()) { //this is incorrect answer, die
            CloseSession(
                    proxyClient.ProxyId,
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
                Y_ABORT_UNLESS(write_ts >= 0);
                *currentBatch->mutable_written_at() = ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(write_ts);
                currentBatch->set_producer_id(std::move(sourceId));
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
private:
    TSessionsMap ServerSessions;
    THashMap<TActorId, TSet<ui64>> AssignByProxy;

    ::NMonitoring::TDynamicCounterPtr Counters;
};


IActor* CreatePQDReadCacheService(const ::NMonitoring::TDynamicCounterPtr& counters) {
    Y_VERIFY_DEBUG(counters);
    return new TPQDirectReadCacheService(
        GetServiceCounters(counters, "persqueue")->GetSubgroup("subsystem", "caching_service"));
}

} // namespace NKikimr::NPQ
