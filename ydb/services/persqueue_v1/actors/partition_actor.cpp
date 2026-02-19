#include "partition_actor.h"
#include "persqueue_utils.h"

#include <limits>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/codecs/pqv1.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>

#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <google/protobuf/util/time_util.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <util/charset/utf8.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace PersQueue::V1;
using namespace Topic;


TPartitionActor::TPartitionActor(
        const TActorId& parentId, const TString& clientId, const TString& clientPath, const ui64 cookie,
        const TString& session, const TPartitionId& partition, const ui32 generation, const ui32 step,
        const ui64 tabletID, const TTopicCounters& counters, bool commitsDisabled,
        const TString& clientDC, bool rangesMode, const NPersQueue::TTopicConverterPtr& topic, const TString& database,
        bool directRead, bool useMigrationProtocol, ui32 maxTimeLagMs, ui64 readTimestampMs, const TTopicHolder::TPtr& topicHolder,
        const std::unordered_set<ui64>& notCommitedToFinishParents, ui64 partitionMaxInFlightBytes
)
    : ParentId(parentId)
    , ClientId(clientId)
    , ClientPath(clientPath)
    , Cookie(cookie)
    , Session(session)
    , ClientDC(clientDC)
    , Partition(partition)
    , Generation(generation)
    , Step(step)
    , TabletID(tabletID)
    , MaxTimeLagMs(maxTimeLagMs)
    , ReadTimestampMs(readTimestampMs)
    , ReadOffset(0)
    , ClientReadOffset(0)
    , ClientCommitOffset(0)
    , ClientVerifyReadOffset(false)
    , CommittedOffset(0)
    , WriteTimestampEstimateMs(0)
    , ReadIdToResponse(1)
    , ReadIdCommitted(0)
    , RangesMode(rangesMode)
    , WTime(0)
    , InitDone(false)
    , StartReading(false)
    , AllPrepareInited(false)
    , FirstInit(true)
    , PipeClient()
    , PipeGeneration(0)
    , TabletGeneration(0)
    , NodeId(0)
    , RequestInfly(false)
    , EndOffset(0)
    , SizeLag(0)
    , WaitDataCookie(0)
    , WaitForData(false)
    , LockCounted(false)
    , TopicHolder(topicHolder)
    , Counters(counters)
    , CommitsDisabled(commitsDisabled)
    , CommitCookie(1)
    , Topic(topic)
    , Database(database)
    , DirectRead(directRead)
    , PartitionInFlightMemoryController(partitionMaxInFlightBytes)
    , UseMigrationProtocol(useMigrationProtocol)
    , FirstRead(true)
    , ReadingFinishedSent(false)
    , NotCommitedToFinishParents(notCommitedToFinishParents)
{
}


void TPartitionActor::MakeCommit(const TActorContext& ctx) {
    ui64 offset = ClientReadOffset;

    if (CommitsDisabled || NotCommitedToFinishParents.size() != 0 || CommitsInfly.size() >= MAX_COMMITS_INFLY)
        return;

    //Ranges mode
    if (!NextRanges.Empty() && NextRanges.Min() == ClientCommitOffset) {
        auto firstRange = NextRanges.begin();
        offset = firstRange->second;
        NextRanges.EraseInterval(firstRange->first, firstRange->second);

        ClientCommitOffset = offset;
        ++CommitCookie;
        CommitsInfly.emplace_back(CommitCookie, TCommitInfo{CommitCookie, offset, ctx.Now()});
        if (Counters.SLITotal)
            Counters.SLITotal.Inc();

        if (PipeClient) //if not then pipe will be recreated soon and SendCommit will be done
            SendCommit(CommitCookie, offset, ctx);
        return;
    }

    //Now commits by cookies.
    ui64 readId = ReadIdCommitted;
    auto it = NextCommits.begin();
    if (it != NextCommits.end() && *it == 0) { //commit of readed in prev session data
        NextCommits.erase(NextCommits.begin());
        if (ClientReadOffset <= ClientCommitOffset) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCommitDone(Partition.AssignId, 0, 0, CommittedOffset, EndOffset, ReadingFinishedSent));
        } else {
            ClientCommitOffset = ClientReadOffset;
            CommitsInfly.emplace_back(0, TCommitInfo{0, ClientReadOffset, ctx.Now()});
            if (Counters.SLITotal)
                Counters.SLITotal.Inc();
            if (PipeClient) //if not then pipe will be recreated soon and SendCommit will be done
                SendCommit(0, ClientReadOffset, ctx);
        }
        MakeCommit(ctx);
        return;
    }
    for (;it != NextCommits.end() && (*it) == readId + 1; ++it) {
        ++readId;
    }
    if (readId == ReadIdCommitted)
        return;
    NextCommits.erase(NextCommits.begin(), it);
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " commit request from " << ReadIdCommitted + 1 << " to " << readId << " in " << Partition);

    ui64 startReadId = ReadIdCommitted + 1;

    ReadIdCommitted = readId;

    auto jt = Offsets.begin();
    while(jt != Offsets.end() && jt->ReadId != readId) ++jt;
    AFL_ENSURE(jt != Offsets.end());

    offset = Max(offset, jt->Offset);

    Offsets.erase(Offsets.begin(), ++jt);

    AFL_ENSURE(offset > ClientCommitOffset);

    ClientCommitOffset = offset;
    CommitsInfly.emplace_back(readId, TCommitInfo{startReadId, offset, ctx.Now()});
    if (Counters.SLITotal)
        Counters.SLITotal.Inc();

    if (PipeClient) //if not then pipe will be recreated soon and SendCommit will be done
        SendCommit(readId, offset, ctx);
}

TPartitionActor::~TPartitionActor() = default;


void TPartitionActor::Bootstrap(const TActorContext& ctx) {
    Become(&TThis::StateFunc);
    ctx.Schedule(PREWAIT_DATA, new TEvents::TEvWakeup());
    ctx.Schedule(READ_METRICS_UPDATE_INTERVAL, new TEvPQProxy::TEvUpdateReadMetrics());
}

const std::set<NPQ::TPartitionGraph::Node*>& TPartitionActor::GetParents(std::shared_ptr<const NPQ::TPartitionGraph> partitionGraph) const {
    const auto* partition = partitionGraph->GetPartition(Partition.Partition);
    if (partition) {
        return partition->AllParents;
    }

    static std::set<NPQ::TPartitionGraph::Node*> empty;
    return empty;
}

void TPartitionActor::SendCommit(const ui64 readId, const ui64 offset, const TActorContext& ctx) {
    // extend the lifetime for PartitionGraph
    auto partitionGraph = TopicHolder->GetPartitionGraph();
    const auto& parents = GetParents(partitionGraph);
    if (!ClientHasAnyCommits && parents.size() != 0) {
        std::vector<TDistributedCommitHelper::TCommitInfo> commits;
        for (auto& parent: parents) {
            TDistributedCommitHelper::TCommitInfo commit {.PartitionId = parent->Id, .Offset = Max<i64>(), .KillReadSession = false, .OnlyCheckCommitedToFinish = true, .ReadSessionId = Session};
            commits.push_back(commit);
        }
        TDistributedCommitHelper::TCommitInfo commit {.PartitionId = Partition.Partition, .Offset = (i64)offset, .KillReadSession = false, .OnlyCheckCommitedToFinish = false, .ReadSessionId = Session};
        commits.push_back(commit);
        auto kqp = std::make_shared<TDistributedCommitHelper>(Database, ClientId, Topic->GetPrimaryPath(), commits, readId);
        Kqps.emplace(readId, kqp);

        kqp->SendCreateSessionRequest(ctx);
    } else {
        NKikimrClient::TPersQueueRequest request;
        request.MutablePartitionRequest()->SetTopic(Topic->GetPrimaryPath());
        request.MutablePartitionRequest()->SetPartition(Partition.Partition);
        request.MutablePartitionRequest()->SetCookie(readId);

        AFL_ENSURE(PipeClient);

        ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());
        auto commit = request.MutablePartitionRequest()->MutableCmdSetClientOffset();
        commit->SetClientId(ClientId);
        commit->SetOffset(offset);
        AFL_ENSURE(!Session.empty());
        commit->SetSessionId(Session);

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " committing to position " << offset << " prev " << CommittedOffset
                            << " end " << EndOffset << " by cookie " << readId);

        TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
        req->Record.Swap(&request);

        NTabletPipe::SendData(ctx, PipeClient, req.Release());
    }
}

void TPartitionActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    auto kqpIt = Kqps.find(ev->Cookie);
    if (kqpIt == Kqps.end()) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("unexpected cookie at KQP create session response", PersQueue::ErrorCode::ERROR));
        return;
    }

    if (!kqpIt->second->Handle(ev, ctx)) {
        const auto& record = ev->Get()->Record;
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("status is not ok: " + record.GetError(), PersQueue::ErrorCode::ERROR));
    }
}

void TPartitionActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;

    auto kqpIt = Kqps.find(ev->Cookie);
    if (kqpIt == Kqps.end()) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("unexpected cookie at KQP query response", PersQueue::ErrorCode::ERROR));
        return;
    }

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        auto kqpQueryError = TStringBuilder() << "Kqp error. Status# " << record.GetYdbStatus() << ", ";

        NYql::TIssues issues;
        NYql::IssuesFromMessage(record.GetResponse().GetQueryIssues(), issues);
        kqpQueryError << issues.ToString();

        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(kqpQueryError, PersQueue::ErrorCode::ERROR));
        return;
    }

    auto step = kqpIt->second->Handle(ev, ctx);
    if (step == TDistributedCommitHelper::ECurrentStep::DONE) {
        CommitDone(ev->Cookie, ctx);
    }
}

void TPartitionActor::SendPublishDirectRead(const ui64 directReadId, const TActorContext& ctx) {
    NKikimrClient::TPersQueueRequest request;
    request.MutablePartitionRequest()->SetTopic(Topic->GetPrimaryPath());
    request.MutablePartitionRequest()->SetPartition(Partition.Partition);
    request.MutablePartitionRequest()->SetCookie(ReadOffset);

    AFL_ENSURE(PipeClient);

    ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());
    auto publish = request.MutablePartitionRequest()->MutableCmdPublishRead();
    publish->SetDirectReadId(directReadId);
    AFL_ENSURE(!Session.empty());

    publish->MutableSessionKey()->SetSessionId(Session);
    publish->MutableSessionKey()->SetPartitionSessionId(Partition.AssignId);

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " publishing direct read with id " << directReadId);

    TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
    req->Record.Swap(&request);

    NTabletPipe::SendData(ctx, PipeClient, req.Release());
}

void TPartitionActor::SendForgetDirectRead(const ui64 directReadId, const TActorContext& ctx) {
    NKikimrClient::TPersQueueRequest request;
    request.MutablePartitionRequest()->SetTopic(Topic->GetPrimaryPath());
    request.MutablePartitionRequest()->SetPartition(Partition.Partition);
    request.MutablePartitionRequest()->SetCookie(ReadOffset);

    AFL_ENSURE(PipeClient);

    ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());
    auto publish = request.MutablePartitionRequest()->MutableCmdForgetRead();
    publish->SetDirectReadId(directReadId);
    AFL_ENSURE(!Session.empty());

    publish->MutableSessionKey()->SetSessionId(Session);
    publish->MutableSessionKey()->SetPartitionSessionId(Partition.AssignId);

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " forgetting " << directReadId);

    TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
    req->Record.Swap(&request);

    NTabletPipe::SendData(ctx, PipeClient, req.Release());
}

void TPartitionActor::RestartPipe(const TActorContext& ctx, const TString& reason, const NPersQueue::NErrorCode::EErrorCode errorCode) {
    if (!PipeClient)
        return;

    Counters.Errors.Inc();

    NTabletPipe::CloseClient(ctx, PipeClient);

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                                                                  << " schedule pipe restart attempt " << PipeGeneration << " reason: " << reason << ", current pipe: " << PipeClient.ToString());
    PipeClient = TActorId{};
    if (errorCode != NPersQueue::NErrorCode::OVERLOAD)
        ++PipeGeneration;

    if (PipeGeneration == MAX_PIPE_RESTARTS) {
        // ???
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("too much attempts to restart pipe", PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED));
        return;
    }

    ctx.Schedule(TDuration::MilliSeconds(RESTART_PIPE_DELAY_MS), new TEvPQProxy::TEvRestartPipe());
}

void TPartitionActor::Handle(TEvPQProxy::TEvDirectReadAck::TPtr& ev, const TActorContext& ctx) {
    auto it = DirectReadResults.find(ev->Get()->DirectReadId);

    if (it == DirectReadResults.end() || ev->Get()->DirectReadId == DirectReadId) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "got direct read ack for uknown direct read id " << ev->Get()->DirectReadId,
                        PersQueue::ErrorCode::BAD_REQUEST));
        return;
    }
    DirectReadResults.erase(it);
    PublishedDirectReads.erase(ev->Get()->DirectReadId);

    if (DirectReadRestoreStage != EDirectReadRestoreStage::None) {
        if (RestoredDirectReadId == ev->Get()->DirectReadId) {
            // This direct read is already being restored. Have to forget it later.
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Got ack for direct read " << ev->Get()->DirectReadId
                    << " while restoring, store it to forget further");
            DirectReadsToForget.insert(ev->Get()->DirectReadId);
            return;
        }
        if (DirectReadsToRestore.contains(ev->Get()->DirectReadId)) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Got ack for direct read " << ev->Get()->DirectReadId
                    << " while restoring, remove it from restore list");
            // This direct read is pending for restore. No need to foreget - not yet prepared, just erase it;
            DirectReadsToRestore.erase(ev->Get()->DirectReadId);
            DirectReadsToPublish.erase(ev->Get()->DirectReadId);
            return;
        }
        DirectReadsToForget.insert(ev->Get()->DirectReadId);
        // This is earlier restored direct read; Will keep it and forget later within overall restore loop;
        return;
    }

    if (!PipeClient) return; //all direct reads will be cleared on pipe restart

    SendForgetDirectRead(ev->Get()->DirectReadId, ctx);

}

void TPartitionActor::Handle(const TEvPQProxy::TEvRestartPipe::TPtr&, const TActorContext& ctx) {

    AFL_ENSURE(!PipeClient);

    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = 6,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(100),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };
    PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, TabletID, clientConfig));
    AFL_ENSURE(TabletID);

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " pipe restart attempt " << PipeGeneration << " RequestInfly " << RequestInfly << " ReadOffset " << ReadOffset << " EndOffset " << EndOffset
                            << " InitDone " << InitDone << " WaitForData " << WaitForData << ", pipe: " << PipeClient);

    if (InitDone && DirectRead) {
        DirectReadsToRestore = DirectReadResults;
        DirectReadsToPublish = PublishedDirectReads;
        AFL_ENSURE(!DirectReadsToPublish.contains(DirectReadId));
        RestoredDirectReadId = 0;
        RestartDirectReadSession();
        return;
    }
    ResendRecentRequests();
}

void TPartitionActor::ResendRecentRequests() {
    const auto& ctx = ActorContext();
    if (RequestInfly) { //got read infly
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " resend " << CurrentRequest);

        TAutoPtr<TEvPersQueue::TEvRequest> event(new TEvPersQueue::TEvRequest);
        event->Record = CurrentRequest;

        ActorIdToProto(PipeClient, event->Record.MutablePartitionRequest()->MutablePipeClient());

        NTabletPipe::SendData(ctx, PipeClient, event.Release());
    }

    if (InitDone) {
        for (auto& c : CommitsInfly) { //resend all commits
            if (c.second.Offset != Max<ui64>())
                SendCommit(c.first, c.second.Offset, ctx);
        }
        if (WaitForData) { //resend wait-for-data requests
            WaitDataInfly.clear();
            WaitDataInPartition(ctx);
        }
    }
}

i64 GetBatchWriteTimestampMS(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch* batch) {
    return static_cast<i64>(batch->write_timestamp_ms());
}
i64 GetBatchWriteTimestampMS(Topic::StreamReadMessage::ReadResponse::Batch* batch) {
    return ::google::protobuf::util::TimeUtil::TimestampToMilliseconds(batch->written_at());
}

void SetBatchWriteTimestampMS(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch* batch, i64 value) {
    batch->set_write_timestamp_ms(value);
}
void SetBatchWriteTimestampMS(Topic::StreamReadMessage::ReadResponse::Batch* batch, i64 value) {
    *batch->mutable_written_at() = ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(value);
}

TString GetBatchSourceId(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch* batch) {
    AFL_ENSURE(batch);
    return batch->source_id();
}

TString GetBatchSourceId(Topic::StreamReadMessage::ReadResponse::Batch* batch) {
    AFL_ENSURE(batch);
    return batch->producer_id();
}

void SetBatchExtraField(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch* batch, TString key, TString value) {
    AFL_ENSURE(batch);
    auto* item = batch->add_extra_fields();
    item->set_key(std::move(key));
    item->set_value(std::move(value));
}

void SetBatchExtraField(Topic::StreamReadMessage::ReadResponse::Batch* batch, TString key, TString value) {
    AFL_ENSURE(batch);
    (*batch->mutable_write_session_meta())[key] = std::move(value);
}

i32 GetDataChunkCodec(const NKikimrPQClient::TDataChunk& proto) {
    if (proto.HasCodec()) {
        return proto.GetCodec() + 1;
    }
    return 0;
}

template<typename TReadResponse>
bool FillBatchedData(
        TReadResponse* data, const NKikimrClient::TCmdReadResult& res,
        const TPartitionId& Partition, ui64 ReadIdToResponse, ui64& ReadOffset, ui64& WTime, ui64 EndOffset,
        const NPersQueue::TTopicConverterPtr& topic, const TActorContext& ctx) {
    constexpr bool UseMigrationProtocol = std::is_same_v<TReadResponse, PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch>;
    auto* partitionData = data->add_partition_data();

    if constexpr (UseMigrationProtocol) {
        partitionData->mutable_topic()->set_path(topic->GetFederationPath());
        partitionData->set_cluster(topic->GetCluster());
        partitionData->set_partition(Partition.Partition);
        partitionData->set_deprecated_topic(topic->GetClientsideName());
        partitionData->mutable_cookie()->set_assign_id(Partition.AssignId);
        partitionData->mutable_cookie()->set_partition_cookie(ReadIdToResponse);

    } else {
        partitionData->set_partition_session_id(Partition.AssignId);
    }

    bool hasOffset = false;
    bool hasData = false;

    i32 batchCodec = 0; // UNSPECIFIED

    typename TReadResponse::Batch* currentBatch = nullptr;
    for (ui32 i = 0; i < res.ResultSize(); ++i) {
        const auto& r = res.GetResult(i);
        WTime = r.GetWriteTimestampMS();
        AFL_ENSURE(r.GetOffset() >= ReadOffset);
        ReadOffset = r.GetOffset() + 1;
        hasOffset = true;

        auto proto(GetDeserializedData(r.GetData()));

        if (!proto.has_codec()) {
            proto.set_codec(NPersQueueCommon::RAW);
        }

        if (proto.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
            continue; //TODO - no such chunks must be on prod
        }

        TString sourceId;
        if (!r.GetSourceId().empty()) {
            if (!NPQ::NSourceIdEncoding::IsValidEncoded(r.GetSourceId())) {
                LOG_ERROR_S(ctx, NKikimrServices::PQ_READ_PROXY, "read bad sourceId from " << Partition
                                                                                           << " offset " << r.GetOffset() << " seqNo " << r.GetSeqNo() << " sourceId '" << r.GetSourceId() << "'");
            }
            sourceId = NPQ::NSourceIdEncoding::Decode(r.GetSourceId());
        }

        if (!currentBatch || GetBatchWriteTimestampMS(currentBatch) != static_cast<i64>(r.GetWriteTimestampMS()) ||
            GetBatchSourceId(currentBatch) != sourceId ||
            (!UseMigrationProtocol && GetDataChunkCodec(proto) != batchCodec)) {
            // If write time and source id are the same, the rest fields will be the same too.
            currentBatch = partitionData->add_batches();
            i64 write_ts = static_cast<i64>(r.GetWriteTimestampMS());
            AFL_ENSURE(write_ts >= 0);
            SetBatchWriteTimestampMS(currentBatch, write_ts);
            SetBatchSourceId(currentBatch, std::move(sourceId));
            batchCodec = GetDataChunkCodec(proto);
            if constexpr (!UseMigrationProtocol) {
                currentBatch->set_codec(batchCodec);
            }

            if (proto.HasMeta()) {
                const auto& header = proto.GetMeta();
                if (header.HasServer()) {
                    SetBatchExtraField(currentBatch, "server", header.GetServer());
                }
                if (header.HasFile()) {
                    SetBatchExtraField(currentBatch, "file", header.GetFile());
                }
                if (header.HasIdent()) {
                    SetBatchExtraField(currentBatch, "ident", header.GetIdent());
                }
                if (header.HasLogType()) {
                    SetBatchExtraField(currentBatch, "logtype", header.GetLogType());
                }
            }
            if (proto.HasExtraFields()) {
                const auto& map = proto.GetExtraFields();
                for (const auto& kv : map.GetItems()) {
                    SetBatchExtraField(currentBatch, kv.GetKey(), kv.GetValue());
                }
            }

            if (proto.HasIp() && IsUtf(proto.GetIp())) {
                if constexpr (UseMigrationProtocol) {
                    currentBatch->set_ip(proto.GetIp());
                } else {
                    SetBatchExtraField(currentBatch, "_ip", proto.GetIp());
                }
            }
        }

        auto* message = currentBatch->add_message_data();

        message->set_seq_no(r.GetSeqNo());
        message->set_offset(r.GetOffset());
        message->set_data(proto.GetData());
        message->set_uncompressed_size(r.GetUncompressedSize());
        if constexpr (UseMigrationProtocol) {
            message->set_create_timestamp_ms(r.GetCreateTimestampMS());

            message->set_explicit_hash(r.GetExplicitHash());
            message->set_partition_key(r.GetPartitionKey());

            if (proto.HasCodec()) {
                message->set_codec(NPQ::ToV1Codec((NPersQueueCommon::ECodec)proto.GetCodec()));
            }
        } else {
            *message->mutable_created_at() =
                ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(r.GetCreateTimestampMS());

            message->set_message_group_id(GetBatchSourceId(currentBatch));
            auto* msgMeta = message->mutable_metadata_items();
            *msgMeta = (proto.GetMessageMeta());
        }
        hasData = true;
    }

    const ui64 realReadOffset = res.HasRealReadOffset() ? res.GetRealReadOffset() : 0;

    if (!hasOffset) { //no data could be read from partition at offset ReadOffset - no data in partition at all???
        ReadOffset = Min(Max(ReadOffset + 1, realReadOffset + 1), EndOffset);
    }
    return hasData;
}

void TPartitionActor::Handle(TEvPQProxy::TEvParentCommitedToFinish::TPtr& ev, const TActorContext& ctx) {
    NotCommitedToFinishParents.erase(ev->Get()->ParentPartitionId);
    MakeCommit(ctx);
}

void TPartitionActor::HandleInit(const NKikimrClient::TPersQueuePartitionResponse& result, const TActorContext& ctx) {
    AFL_ENSURE(DirectReadRestoreStage == EDirectReadRestoreStage::None);
    if (result.GetCookie() != InitCookie) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " unwaited response in init with cookie " << result.GetCookie());
        return;
    }
    AFL_ENSURE(RequestInfly);
    CurrentRequest.Clear();
    RequestInfly = false;

    AFL_ENSURE(result.HasCmdGetClientOffsetResult());
    const auto& resp = result.GetCmdGetClientOffsetResult();
    AFL_ENSURE(resp.HasEndOffset());
    EndOffset = resp.GetEndOffset();
    SizeLag = resp.GetSizeLag();
    WriteTimestampEstimateMs = resp.GetWriteTimestampEstimateMS();
    ClientHasAnyCommits = resp.GetClientHasAnyCommits();

    ClientCommitOffset = ReadOffset = CommittedOffset = resp.HasOffset() ? resp.GetOffset() : 0;
    AFL_ENSURE(EndOffset >= CommittedOffset);

    if (resp.HasWriteTimestampMS())
        WTime = resp.GetWriteTimestampMS();

    InitDone = true;
    PipeGeneration = 0; //reset tries counter - all ok
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " INIT DONE " << Partition
                        << " EndOffset " << EndOffset << " readOffset " << ReadOffset << " committedOffset " << CommittedOffset);


    if (!StartReading) {
        ctx.Send(ParentId, new TEvPQProxy::TEvPartitionStatus(Partition, CommittedOffset, EndOffset,
            WriteTimestampEstimateMs, NodeId, TabletGeneration, ClientHasAnyCommits, ReadOffset));
    } else {
        InitStartReading(ctx);
    }
}

void TPartitionActor::HandleDirectReadRestoreSession(const NKikimrClient::TPersQueuePartitionResponse& result, const TActorContext& ctx) {
    switch (DirectReadRestoreStage) {
        case EDirectReadRestoreStage::None:
            return;
        case EDirectReadRestoreStage::Session:
            if (result.GetCookie() != InitCookie || !result.HasCmdRestoreDirectReadResult()) {
                LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Direct read - session restarted for partition "
                    << Partition << " with unwaited cookie " << result.GetCookie());
                return;
            }
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Direct read - session restarted for partition " << Partition);
            if (!SendNextRestorePrepareOrForget()) {
                OnDirectReadsRestored();
            }
            return;
        case EDirectReadRestoreStage::Prepare:
            AFL_ENSURE(RestoredDirectReadId != 0);
            if (!result.HasCmdPrepareReadResult() || DirectReadsToRestore.empty() || DirectReadsToRestore.begin()->first != result.GetCmdPrepareReadResult().GetDirectReadId()) {
                LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Invalid response on direct read restore for "
                    << Partition << ": expect PrepareReadResult, got " << result.GetCookie());
                return;
            }

            DirectReadsToRestore.erase(DirectReadsToRestore.begin());
            {
                auto sent = SendNextRestorePublishRequest();
                if (!sent) {
                    // Read was not published previously and thus no response sent to session. Need to keep it
                    UnpublishedDirectReads.insert(result.GetCmdPrepareReadResult().GetDirectReadId());
                    sent = SendNextRestorePrepareOrForget();
                }
                if (!sent)
                    OnDirectReadsRestored();
            }
            return;
        case EDirectReadRestoreStage::Publish:
            AFL_ENSURE(RestoredDirectReadId != 0);

            AFL_ENSURE(result.HasCmdPublishReadResult());
            AFL_ENSURE(*DirectReadsToPublish.begin() == result.GetCmdPublishReadResult().GetDirectReadId());
            DirectReadsToPublish.erase(DirectReadsToPublish.begin());
            if (!SendNextRestorePrepareOrForget()) {
                OnDirectReadsRestored();
            }
            return;
        case EDirectReadRestoreStage::Forget:
            AFL_ENSURE(RestoredDirectReadId != 0);
            AFL_ENSURE(result.HasCmdForgetReadResult());
            AFL_ENSURE(*DirectReadsToForget.begin() == result.GetCmdForgetReadResult().GetDirectReadId());
            DirectReadsToForget.erase(DirectReadsToForget.begin());
            if (!SendNextRestorePrepareOrForget()) {
                OnDirectReadsRestored();
            }
            return;
    }
}

void TPartitionActor::Handle(const NKikimrClient::TPersQueuePartitionResponse::TCmdPrepareDirectReadResult& res, const TActorContext& ctx) {
    AFL_ENSURE(DirectReadRestoreStage == EDirectReadRestoreStage::None);

    AFL_ENSURE(DirectRead);
    AFL_ENSURE(res.GetDirectReadId() == DirectReadId);
    if (!PipeClient)
        return; // Pipe was already destroyed, direct read session is being restored. Will resend this request afterwards;

    EndOffset = res.GetEndOffset();
    SizeLag = res.GetSizeLag();
    WTime = res.GetWriteTimestampMS();

    if (res.GetReadOffset() > 0)
        ReadOffset = res.GetReadOffset();

    DirectReadResults[DirectReadId] = res;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " after direct read state " << Partition
                << " EndOffset " << EndOffset << " ReadOffset " << ReadOffset << " ReadGuid " << ReadGuid << " with direct read id " << DirectReadId);

    SendPublishDirectRead(DirectReadId, ctx);

    AFL_ENSURE(RequestInfly);

    CurrentRequest.Clear();
    RequestInfly = false;
}

void TPartitionActor::Handle(const NKikimrClient::TPersQueuePartitionResponse::TCmdPublishDirectReadResult& res, const TActorContext& ctx) {
    ++ReadIdToResponse;

    ReadGuid = TString();
    AFL_ENSURE(DirectReadResults.find(DirectReadId) != DirectReadResults.end());
    AFL_ENSURE(res.GetDirectReadId() == DirectReadId);
    PublishedDirectReads.insert(DirectReadId);

    AFL_ENSURE(!RequestInfly);

    const auto& dr = DirectReadResults[DirectReadId];

    auto readResponse = MakeHolder<TEvPQProxy::TEvDirectReadResponse>(
        Partition.AssignId,
        dr.GetReadOffset(),
        DirectReadId,
        dr.GetBytesSizeEstimate()
    );


    ++DirectReadId;

    ctx.Send(ParentId, readResponse.Release());

    AFL_ENSURE(!WaitForData);

    bool isInFlightMemoryOk = PartitionInFlightMemoryController.Add(dr.GetReadOffset(), dr.GetBytesSizeEstimate());
    ReadOffset = dr.GetLastOffset() + 1;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " after publish direct read " << Partition
                << " EndOffset " << EndOffset << " ReadOffset " << ReadOffset << " ReadGuid " << ReadGuid << " with direct read id " << DirectReadId
                << " isInFlightMemoryOk " << isInFlightMemoryOk);

    AFL_ENSURE(!RequestInfly);

    if (EndOffset > ReadOffset && isInFlightMemoryOk) {
        SendPartitionReady(ctx);
    } else if (EndOffset == ReadOffset) {
        WaitForData = true;
        if (PipeClient) //pipe will be recreated soon
            WaitDataInPartition(ctx);
    }
}

void TPartitionActor::Handle(const NKikimrClient::TCmdReadResult& res, const TActorContext& ctx) {
    AFL_ENSURE(res.HasMaxOffset());
    EndOffset = res.GetMaxOffset();
    SizeLag = res.GetSizeLag();

    StreamReadMessage::FromServer response;
    response.set_status(Ydb::StatusIds::SUCCESS);
    MigrationStreamingReadServerMessage migrationResponse;
    migrationResponse.set_status(Ydb::StatusIds::SUCCESS);

    bool hasData = false;
    if (UseMigrationProtocol) {
        typename MigrationStreamingReadServerMessage::DataBatch* data = migrationResponse.mutable_data_batch();
        hasData = FillBatchedData<MigrationStreamingReadServerMessage::DataBatch>(data, res, Partition, ReadIdToResponse, ReadOffset, WTime, EndOffset, Topic, ctx);
    } else {
        StreamReadMessage::ReadResponse* data = response.mutable_read_response();
        hasData = FillBatchedData<StreamReadMessage::ReadResponse>(data, res, Partition, ReadIdToResponse, ReadOffset, WTime, EndOffset, Topic, ctx);
    }

    WriteTimestampEstimateMs = Max(WriteTimestampEstimateMs, WTime);

    if (!CommitsDisabled && !RangesMode) {
        Offsets.push_back({ReadIdToResponse, ReadOffset});
    }

    if (Offsets.size() >= AppData(ctx)->PQConfig.GetMaxReadCookies() + 10) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "got more than " << AppData(ctx)->PQConfig.GetMaxReadCookies() << " uncommitted reads", PersQueue::ErrorCode::BAD_REQUEST));
        return;
    }

    CurrentRequest.Clear();
    RequestInfly = false;

    AFL_ENSURE(!WaitForData);

    for (ui32 i = 0; i < res.ResultSize(); ++i) {
        const auto& resultItem = res.GetResult(i);
        PartitionInFlightMemoryController.Add(resultItem.GetOffset(), resultItem.GetData().size());
    }

    auto isMemoryLimitReached = PartitionInFlightMemoryController.IsMemoryLimitReached();
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " isMemoryLimitReached " << isMemoryLimitReached << " EndOffset " << EndOffset << " ReadOffset " << ReadOffset
                        << " read result size " << res.ResultSize());
    if (EndOffset > ReadOffset && !isMemoryLimitReached) {
        SendPartitionReady(ctx);
    } else if (EndOffset == ReadOffset) {
        WaitForData = true;
        if (PipeClient) //pipe will be recreated soon
            WaitDataInPartition(ctx);
    }

    if (hasData) {
        ++ReadIdToResponse;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " after read state " << Partition
                << " EndOffset " << EndOffset << " ReadOffset " << ReadOffset << " ReadGuid " << ReadGuid << " has messages " << hasData);

    ReadGuid = TString();

    if (UseMigrationProtocol) {
        auto readResponse = MakeHolder<TEvPQProxy::TEvMigrationReadResponse>(
            std::move(migrationResponse),
            ReadOffset,
            res.GetBlobsFromDisk() > 0,
            TDuration::MilliSeconds(res.GetWaitQuotaTimeMs())
        );
        ctx.Send(ParentId, readResponse.Release());
    } else {
        AFL_ENSURE(!DirectRead);
        auto readResponse = MakeHolder<TEvPQProxy::TEvReadResponse>(
            std::move(response),
            ReadOffset,
            res.GetBlobsFromDisk() > 0,
            TDuration::MilliSeconds(res.GetWaitQuotaTimeMs())
        );
        ctx.Send(ParentId, readResponse.Release());
    }

    PipeGeneration = 0; //reset tries counter - all ok
}

void TPartitionActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {

    if (ev->Get()->Record.HasErrorCode() && ev->Get()->Record.GetErrorCode() != NPersQueue::NErrorCode::OK) {
        const auto errorCode = ev->Get()->Record.GetErrorCode();
        if (errorCode == NPersQueue::NErrorCode::WRONG_COOKIE
            || errorCode == NPersQueue::NErrorCode::BAD_REQUEST
            || errorCode == NPersQueue::NErrorCode::READ_ERROR_NO_SESSION) {
            Counters.Errors.Inc();
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("status is not ok: " + ev->Get()->Record.GetErrorReason(), ConvertOldCode(ev->Get()->Record.GetErrorCode())));
        } else {
            RestartPipe(ctx, TStringBuilder() << "status is not ok. Code: " << EErrorCode_Name(errorCode) << ". Reason: " << ev->Get()->Record.GetErrorReason(), errorCode);
        }
        return;
    }

    if (ev->Get()->Record.GetStatus() != NKikimr::NMsgBusProxy::MSTATUS_OK) { //this is incorrect answer, die
        AFL_ENSURE(!ev->Get()->Record.HasErrorCode());
        Counters.Errors.Inc();
        // map NMsgBusProxy::EResponseStatus to PersQueue::ErrorCode???

        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("status is not ok: " + ev->Get()->Record.GetErrorReason(), PersQueue::ErrorCode::ERROR));
        return;
    }
    if (!ev->Get()->Record.HasPartitionResponse()) { //this is incorrect answer, die
        Counters.Errors.Inc();
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("empty partition in response", PersQueue::ErrorCode::ERROR));
        return;
    }

    const auto& result = ev->Get()->Record.GetPartitionResponse();

    if (!result.HasCookie()) { //this is incorrect answer, die
        Counters.Errors.Inc();
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("no cookie in response", PersQueue::ErrorCode::ERROR));
        return;
    }

    auto MaskResult = [](const NKikimrClient::TPersQueuePartitionResponse& resp) {
            if (resp.HasCmdReadResult()) {
                auto res = resp;
                for (auto& rr : *res.MutableCmdReadResult()->MutableResult()) {
                    rr.SetData(TStringBuilder() << "... " << rr.GetData().size() << " bytes ...");
                }
                return res;
            }
            return resp;
        };

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " initDone " << InitDone << " event " << MaskResult(result));


    if (!InitDone) {
        HandleInit(result, ctx);
        return;
    }

    if (!result.HasCmdReadResult() &&
        !result.HasCmdPrepareReadResult() &&
        !result.HasCmdPublishReadResult() &&
        !result.HasCmdForgetReadResult() &&
        !result.HasCmdRestoreDirectReadResult()) {
        // this is commit response (response contains only cookie)
        CommitDone(result.GetCookie(), ctx);
        return;
    }

    if (DirectReadRestoreStage != EDirectReadRestoreStage::None) {
        HandleDirectReadRestoreSession(result, ctx);
        return;
    } else if (result.HasCmdRestoreDirectReadResult()) {
        // ignore it
        return;
    }

    if (result.HasCmdForgetReadResult()) {
        // ignore it
        return;
    }

    if (result.GetCookie() != (ui64)ReadOffset) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                    << " unwaited read-response with cookie " << result.GetCookie()
                    << "; waiting for " << ReadOffset << "; current read guid is " << ReadGuid);
        return;
    }

    if (result.HasCmdPrepareReadResult()) {
        Handle(result.GetCmdPrepareReadResult(), ctx);
        return;
    }
    if (result.HasCmdPublishReadResult()) {
        Handle(result.GetCmdPublishReadResult(), ctx);
        return;
    }
    if (result.HasCmdReadResult()) {
        Handle(result.GetCmdReadResult(), ctx);
        return;
    }

    AFL_ENSURE(false)("unexpected response", result.ShortDebugString());
}

void TPartitionActor::CommitDone(ui64 cookie, const TActorContext& ctx) {
    if (CommitsInfly.empty()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " unwaited commit-response with cookie " << cookie << "; waiting for nothing");
        return;
    }
    ui64 readId = CommitsInfly.front().first;

    if (cookie != readId) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " unwaited commit-response with cookie " << cookie << "; waiting for " << readId);
        return;
    }

    Counters.Commits.Inc();
    ClientHasAnyCommits = true;

    ui32 commitDurationMs = (ctx.Now() - CommitsInfly.front().second.StartTime).MilliSeconds();
    if (Counters.CommitLatency) {
        Counters.CommitLatency.IncFor(commitDurationMs, 1);
    }

    if (Counters.SLIBigLatency && commitDurationMs >= AppData(ctx)->PQConfig.GetCommitLatencyBigMs()) {
        Counters.SLIBigLatency.Inc();
    }

    CommittedOffset = CommitsInfly.front().second.Offset;
    
    bool wasMemoryLimitReached = PartitionInFlightMemoryController.IsMemoryLimitReached();
    bool isMemoryOkNow = PartitionInFlightMemoryController.Remove(CommittedOffset);
    if (wasMemoryLimitReached && isMemoryOkNow && EndOffset > ReadOffset) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " ready for read after commit with readOffset " << ReadOffset << " endOffset " << EndOffset);
        SendPartitionReady(ctx);
    }

    ui64 startReadId = CommitsInfly.front().second.StartReadId;
    ctx.Send(ParentId, new TEvPQProxy::TEvCommitDone(Partition.AssignId, startReadId, readId, CommittedOffset, EndOffset, ReadingFinishedSent));

    Kqps.erase(CommitsInfly.front().first);
    CommitsInfly.pop_front();

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                    << " commit done to position " << CommittedOffset << " endOffset " << EndOffset << " with cookie " << readId);

    PipeGeneration = 0; //reset tries counter - all ok
    MakeCommit(ctx);
}

void TPartitionActor::SendPartitionReady(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " ready for read with readOffset " << ReadOffset << " endOffset " << EndOffset);
    if (FirstRead) {
        ctx.Send(ParentId, new TEvPQProxy::TEvReadingStarted(Topic->GetInternalName(), Partition.Partition));
        FirstRead = false;
    }
    ctx.Send(ParentId, new TEvPQProxy::TEvPartitionReady(Partition, WTime, SizeLag, ReadOffset, EndOffset));
}


void TPartitionActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " pipe restart attempt " << PipeGeneration << " pipe creation result: " << msg->Status
                            << " TabletId: " << msg->TabletId << " Generation: " << msg->Generation << ", pipe: " << PipeClient.ToString());

    if (msg->Status != NKikimrProto::OK) {
        RestartPipe(ctx, TStringBuilder() << "pipe to tablet is dead " << msg->TabletId, NPersQueue::NErrorCode::TABLET_PIPE_DISCONNECTED);
        return;
    }

    TabletGeneration = msg->Generation;
    NodeId = msg->ServerId.NodeId();
}

void TPartitionActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    RestartPipe(ctx, TStringBuilder() << "pipe to tablet is dead " << ev->Get()->TabletId, NPersQueue::NErrorCode::TABLET_PIPE_DISCONNECTED);
}

void TPartitionActor::Handle(TEvPQProxy::TEvGetStatus::TPtr&, const TActorContext& ctx) {
    ctx.Send(ParentId, new TEvPQProxy::TEvPartitionStatus(Partition, CommittedOffset, EndOffset, WriteTimestampEstimateMs, NodeId, TabletGeneration, ClientHasAnyCommits, ReadOffset, false));
}

void TPartitionActor::Handle(TEvPQProxy::TEvUpdateReadMetrics::TPtr&, const TActorContext& ctx) {
    auto inFlightLimitReachedDuration = PartitionInFlightMemoryController.GetLimitReachedDuration();

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " update read metrics " << Partition
                        << " inFlightLimitReachedDuration " << inFlightLimitReachedDuration.MilliSeconds());

    NKikimrClient::TPersQueueRequest request;
    auto req = request.MutablePartitionRequest();
    req->SetPartition(Partition.Partition);
    request.MutablePartitionRequest()->MutableCmdUpdateReadMetrics()->SetInFlightLimitReachedDurationMs(inFlightLimitReachedDuration.MilliSeconds());
    request.MutablePartitionRequest()->MutableCmdUpdateReadMetrics()->SetClientId(ClientId);

    TAutoPtr<TEvPersQueue::TEvRequest> persqueueRequest(new TEvPersQueue::TEvRequest);
    persqueueRequest->Record.Swap(&request);

    ctx.Schedule(READ_METRICS_UPDATE_INTERVAL, new TEvPQProxy::TEvUpdateReadMetrics());
    if (!PipeClient) 
        return;

    NTabletPipe::SendData(ctx, PipeClient, persqueueRequest.Release());
}

void TPartitionActor::Handle(TEvPQProxy::TEvLockPartition::TPtr& ev, const TActorContext& ctx) {
    ClientReadOffset = ev->Get()->ReadOffset;
    ClientCommitOffset = ev->Get()->CommitOffset;
    ClientVerifyReadOffset = ev->Get()->VerifyReadOffset;

    if (StartReading) {
        AFL_ENSURE(ev->Get()->StartReading); //otherwise it is signal from actor, this could not be done
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("double partition locking", PersQueue::ErrorCode::BAD_REQUEST));
        return;
    }

    StartReading = ev->Get()->StartReading;
    InitLockPartition(ctx);
}

void TPartitionActor::InitStartReading(const TActorContext& ctx) {

    AFL_ENSURE(AllPrepareInited);
    AFL_ENSURE(!WaitForData);
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Start reading " << Partition
                        << " EndOffset " << EndOffset << " readOffset " << ReadOffset << " committedOffset " << CommittedOffset
                        << " clientCommitOffset " << ClientCommitOffset << " clientReadOffset " << ClientReadOffset);

    Counters.PartitionsToBeLocked.Dec();
    LockCounted = false;

    ReadOffset = Max<ui64>(CommittedOffset, ClientReadOffset);

    if (ClientVerifyReadOffset) {
        if (ClientReadOffset < ClientCommitOffset) {
            ctx.Send(ParentId,
                     new TEvPQProxy::TEvCloseSession(TStringBuilder()
                            << "trying to read from position that is less than position provided to commit: read " << ClientReadOffset
                            << " committed " << ClientCommitOffset,
                        PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }

        if (ClientCommitOffset.Defined() && *ClientCommitOffset < CommittedOffset) {
            ctx.Send(ParentId,
                     new TEvPQProxy::TEvCloseSession(TStringBuilder()
                            << "trying to commit to position that is less than committed: read " << ClientCommitOffset
                            << " committed " << CommittedOffset,
                        PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        if (ClientReadOffset < CommittedOffset) {
            ctx.Send(ParentId,
                     new TEvPQProxy::TEvCloseSession(TStringBuilder()
                            << "trying to read from position that is less than committed: read " << ClientReadOffset
                            << " committed " << CommittedOffset,
                        PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
    }

    if (ClientCommitOffset.GetOrElse(0) > CommittedOffset) {
        if (ClientCommitOffset > ReadOffset) {
            ctx.Send(ParentId,
                     new TEvPQProxy::TEvCloseSession(TStringBuilder()
                            << "trying to read from position that is less than provided to commit: read " << ReadOffset
                            << " commit " << ClientCommitOffset,
                        PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        if (ClientCommitOffset.GetOrElse(0) > EndOffset) {
            ctx.Send(ParentId,
                     new TEvPQProxy::TEvCloseSession(TStringBuilder()
                           << "trying to commit to future: commit " << ClientCommitOffset << " endOffset " << EndOffset,
                        PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        AFL_ENSURE(CommitsInfly.empty());
        CommitsInfly.emplace_back(Max<ui64>(), TCommitInfo{Max<ui64>(), ClientCommitOffset.GetOrElse(0), ctx.Now()});
        if (Counters.SLITotal)
            Counters.SLITotal.Inc();
        if (PipeClient) //pipe will be recreated soon
            SendCommit(CommitsInfly.back().first, CommitsInfly.back().second.Offset, ctx);
    } else {
        ClientCommitOffset = CommittedOffset;
    }

    if (EndOffset > ReadOffset && !MaxTimeLagMs && !ReadTimestampMs) {
        SendPartitionReady(ctx);
    } else {
        WaitForData = true;
        if (PipeClient) //pipe will be recreated soon
            WaitDataInPartition(ctx);
    }
}

//TODO: add here reaction on client release request
NKikimrClient::TPersQueueRequest TPartitionActor::MakeCreateSessionRequest(bool initial, ui64 cookie) const {
    NKikimrClient::TPersQueueRequest request;

    request.MutablePartitionRequest()->SetTopic(Topic->GetPrimaryPath());
    request.MutablePartitionRequest()->SetPartition(Partition.Partition);
    request.MutablePartitionRequest()->SetCookie(cookie);

    ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());

    auto cmd = request.MutablePartitionRequest()->MutableCmdCreateSession();
    cmd->SetClientId(ClientId);
    cmd->SetSessionId(Session);
    cmd->SetGeneration(Generation);
    cmd->SetStep(Step);
    cmd->SetPartitionSessionId(Partition.AssignId);
    if (!initial) {
        cmd->SetRestoreSession(true);
    }
    return request;
}

void TPartitionActor::InitLockPartition(const TActorContext& ctx) {
    if (PipeClient && AllPrepareInited) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("double partition locking", PersQueue::ErrorCode::BAD_REQUEST));
        return;
    }
    if (!LockCounted) {
        Counters.PartitionsToBeLocked.Inc();
        LockCounted = true;
    }
    if (StartReading)
        AllPrepareInited = true;



    if (FirstInit) {
        AFL_ENSURE(!PipeClient);
        FirstInit = false;
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };
        PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, TabletID, clientConfig));
        auto request = MakeCreateSessionRequest(true, ++InitCookie);

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " INITING " << Partition);

        TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
        AFL_ENSURE(!RequestInfly);
        CurrentRequest = request;
        RequestInfly = true;
        req->Record.Swap(&request);

        NTabletPipe::SendData(ctx, PipeClient, req.Release());
    } else {
        AFL_ENSURE(StartReading); //otherwise it is double locking from actor, not client - client makes lock always with StartReading == true
        AFL_ENSURE(InitDone);
        InitStartReading(ctx);
    }
}

void TPartitionActor::RestartDirectReadSession() {
    const auto& ctx = ActorContext();
    DirectReadRestoreStage = EDirectReadRestoreStage::Session;
    auto request = MakeCreateSessionRequest(false, ++InitCookie);
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Re-init direct read session for " << Partition);
    TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
    req->Record.Swap(&request);
    NTabletPipe::SendData(ctx, PipeClient, req.Release());
}

bool TPartitionActor::SendNextRestorePrepareOrForget() {
    const auto& ctx = ActorContext();
    ui64 prepareId = DirectReadsToRestore.empty() ? 0 : DirectReadsToRestore.begin()->first;
    ui64 forgetId = DirectReadsToForget.empty() ? 0 : *DirectReadsToForget.begin();
    if (prepareId == 0 && forgetId == 0)
        return false;

    bool shouldForget = forgetId != 0 && (prepareId > forgetId || prepareId == 0);
    if (shouldForget) {
        // We have something to forget from what was already restored; Do NOT change RestoredDirectReadId
        DirectReadRestoreStage = EDirectReadRestoreStage::Forget;
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Restore direct read, forget id "
                    << *DirectReadsToForget.begin() << " for partition " << Partition);
        SendForgetDirectRead(*DirectReadsToForget.begin(), ctx);
        return true;
    } else {
        auto& dr = DirectReadsToRestore.begin()->second;
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Resend prepare direct read id " << prepareId
                    << " (internal id: " << dr.GetDirectReadId() << ") for partition " << Partition);
        AFL_ENSURE(prepareId != 0);

        //Restore;
        AFL_ENSURE(prepareId == dr.GetDirectReadId());

        AFL_ENSURE(RestoredDirectReadId < dr.GetDirectReadId());
        RestoredDirectReadId = dr.GetDirectReadId();
        DirectReadRestoreStage = EDirectReadRestoreStage::Prepare;
        AFL_ENSURE(dr.GetReadOffset() <= dr.GetLastOffset());

        auto request = MakeReadRequest(dr.GetReadOffset(), dr.GetLastOffset() + 1, std::numeric_limits<i32>::max(),
                                    std::numeric_limits<i32>::max(), 0, 0, dr.GetDirectReadId(), dr.GetBytesSizeEstimate());

        if (!PipeClient) //Pipe will be recreated soon
            return true;

        TAutoPtr<TEvPersQueue::TEvRequest> event(new TEvPersQueue::TEvRequest);
        event->Record.Swap(&request);
        NTabletPipe::SendData(ctx, PipeClient, event.Release());
        return true;
    }
}

bool TPartitionActor::SendNextRestorePublishRequest() {
    const auto& ctx = ActorContext();
    if (DirectReadsToPublish.empty()) {
        AFL_ENSURE(DirectReadsToRestore.empty());
        return false;
    }
    auto id = *DirectReadsToPublish.begin();
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << "Resend publish direct read on restore, id: "
                << id << " for partition " << Partition);

    AFL_ENSURE(RestoredDirectReadId == id);
    DirectReadRestoreStage = EDirectReadRestoreStage::Publish;

    if (!PipeClient) //Pipe will be recreated soon
        return true;

    SendPublishDirectRead(id, ctx);
    return true;
}

void TPartitionActor::OnDirectReadsRestored() {
    AFL_ENSURE(DirectReadsToRestore.empty() && DirectReadsToPublish.empty() && DirectReadsToForget.empty());
    DirectReadRestoreStage = EDirectReadRestoreStage::None;

    const auto& ctx = ActorContext();
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << "Restore direct reads done, continue working");

    if (InitDone) {
        ctx.Send(ParentId, new TEvPQProxy::TEvUpdateSession(Partition, NodeId, TabletGeneration));
    }

    for (auto id: UnpublishedDirectReads) {
        SendPublishDirectRead(id, ActorContext());
    }
    UnpublishedDirectReads.clear();    ResendRecentRequests();
}

void TPartitionActor::WaitDataInPartition(const TActorContext& ctx) {
    if (WaitDataInfly.size() > 1) { //already got 2 requests inflight
        return;
    }
    if (!WaitForData) {
        return;
    }

    AFL_ENSURE(InitDone);
    AFL_ENSURE(PipeClient);
    AFL_ENSURE(ReadOffset >= EndOffset || MaxTimeLagMs || ReadTimestampMs);

    TAutoPtr<TEvPersQueue::TEvHasDataInfo> event(new TEvPersQueue::TEvHasDataInfo());
    event->Record.SetPartition(Partition.Partition);
    event->Record.SetOffset(ReadOffset);
    event->Record.SetCookie(++WaitDataCookie);
    ui64 deadline = (ctx.Now() + WAIT_DATA - WAIT_DELTA).MilliSeconds();
    event->Record.SetDeadline(deadline);
    event->Record.SetClientId(ClientId);
    if (MaxTimeLagMs) {
        event->Record.SetMaxTimeLagMs(MaxTimeLagMs);
    }
    if (ReadTimestampMs) {
        event->Record.SetReadTimestampMs(ReadTimestampMs);
    }


    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY,
            PQ_LOG_PREFIX << " " << Partition << " wait data in partition inited, cookie " << WaitDataCookie << " from offset " << ReadOffset);

    NTabletPipe::SendData(ctx, PipeClient, event.Release());

    ctx.Schedule(WAIT_DATA, new TEvPQProxy::TEvDeadlineExceeded(WaitDataCookie));

    WaitDataInfly.insert(WaitDataCookie);
}

void TPartitionActor::Handle(TEvPersQueue::TEvHasDataInfoResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    WriteTimestampEstimateMs = record.GetWriteTimestampEstimateMS();

    auto it = WaitDataInfly.find(ev->Get()->Record.GetCookie());
    if (it == WaitDataInfly.end()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " unwaited response for WaitData " << ev->Get()->Record);
        return;
    }
    WaitDataInfly.erase(it);
    if (!WaitForData)
        return;

    if (Counters.WaitsForData) {
        Counters.WaitsForData.Inc();
    }

    AFL_ENSURE(record.HasEndOffset());
    AFL_ENSURE(EndOffset <= record.GetEndOffset()); //end offset could not be changed if no data arrived, but signal will be sended anyway after timeout
    AFL_ENSURE(ReadOffset >= EndOffset || MaxTimeLagMs || ReadTimestampMs); //otherwise no WaitData were needed

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                    << " wait for data done: " << " readOffset " << ReadOffset << " EndOffset " << EndOffset << " newEndOffset "
                    << record.GetEndOffset() << " commitOffset " << CommittedOffset << " clientCommitOffset " << ClientCommitOffset
                    << " cookie " << ev->Get()->Record.GetCookie() << " readingFinished " << record.GetReadingFinished() << " firstRead " << FirstRead);

    EndOffset = record.GetEndOffset();
    SizeLag = record.GetSizeLag();

    if (!record.GetReadingFinished()) {
        if (ReadOffset < EndOffset) {
            WaitForData = false;
            WaitDataInfly.clear();
            if (!PartitionInFlightMemoryController.IsMemoryLimitReached()) {
                SendPartitionReady(ctx);
            }
        } else if (PipeClient) {
            WaitDataInPartition(ctx);
        }
    }

    if (!ReadingFinishedSent) {
        if (record.GetReadingFinished()) {
            ReadingFinishedSent = true;

            std::vector<ui32> adjacentPartitionIds;
            adjacentPartitionIds.reserve(record.GetAdjacentPartitionIds().size());
            adjacentPartitionIds.insert(adjacentPartitionIds.end(), record.GetAdjacentPartitionIds().begin(), record.GetAdjacentPartitionIds().end());

            std::vector<ui32> childPartitionIds;
            childPartitionIds.reserve(record.GetChildPartitionIds().size());
            childPartitionIds.insert(childPartitionIds.end(), record.GetChildPartitionIds().begin(), record.GetChildPartitionIds().end());

            ctx.Send(ParentId, new TEvPQProxy::TEvReadingFinished(Topic->GetInternalName(), Partition.Partition, FirstRead,
                     std::move(adjacentPartitionIds), std::move(childPartitionIds), EndOffset));
        } else if (FirstRead) {
            ctx.Send(ParentId, new TEvPQProxy::TEvReadingStarted(Topic->GetInternalName(), Partition.Partition));
        }

        FirstRead = false;
    }
}


NKikimrClient::TPersQueueRequest TPartitionActor::MakeReadRequest(
        ui64 readOffset, ui64 lastOffset, ui64 maxCount, ui64 maxSize, ui64 maxTimeLagMs, ui64 readTimestampMs, ui64 directReadId, ui64 sizeEstimate
) const {
    NKikimrClient::TPersQueueRequest request;

    request.MutablePartitionRequest()->SetTopic(Topic->GetPrimaryPath());

    request.MutablePartitionRequest()->SetPartition(Partition.Partition);
    request.MutablePartitionRequest()->SetCookie(readOffset);

    ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());
    auto read = request.MutablePartitionRequest()->MutableCmdRead();
    read->SetClientId(ClientId);
    read->SetClientDC(ClientDC);
    read->SetSessionId(Session);
    if (DirectRead) {
        read->SetDirectReadId(directReadId);
        read->SetSizeEstimate(sizeEstimate);
    }
    if (maxCount) {
        read->SetCount(maxCount);
    }
    if (maxSize) {
        read->SetBytes(maxSize);
    }
    if (maxTimeLagMs) {
        read->SetMaxTimeLagMs(maxTimeLagMs);
    }
    if (readTimestampMs) {
        read->SetReadTimestampMs(readTimestampMs);
    }

    read->SetOffset(readOffset);
    if (lastOffset) {
        read->SetLastOffset(lastOffset);
    }
    read->SetTimeoutMs(READ_TIMEOUT_DURATION.MilliSeconds());
    return request;
}

void TPartitionActor::Handle(TEvPQProxy::TEvRead::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " READ FROM " << Partition
                    << " maxCount " << ev->Get()->MaxCount << " maxSize " << ev->Get()->MaxSize << " maxTimeLagMs "
                    << ev->Get()->MaxTimeLagMs << " readTimestampMs " << ev->Get()->ReadTimestampMs
                    << " readOffset " << ReadOffset << " EndOffset " << EndOffset << " ClientCommitOffset "
                    << ClientCommitOffset << " committedOffset " << CommittedOffset << " Guid " << ev->Get()->Guid);

    AFL_ENSURE(ReadGuid.empty());
    AFL_ENSURE(!RequestInfly);

    ReadGuid = ev->Get()->Guid;

    const auto req = ev->Get();

    auto request = MakeReadRequest(ReadOffset, 0, req->MaxCount, req->MaxSize, req->MaxTimeLagMs, req->ReadTimestampMs, DirectReadId);
    RequestInfly = true;
    CurrentRequest = request;

    if (!PipeClient) //Pipe will be recreated soon
        return;

    if (DirectReadRestoreStage != EDirectReadRestoreStage::None) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " READ FROM " << Partition
                    << " store this request utill direct read is restored");
        return;
    }

    TAutoPtr<TEvPersQueue::TEvRequest> event(new TEvPersQueue::TEvRequest);
    event->Record.Swap(&request);

    NTabletPipe::SendData(ctx, PipeClient, event.Release());
}


void TPartitionActor::Handle(TEvPQProxy::TEvCommitCookie::TPtr& ev, const TActorContext& ctx) {
    //TODO: add here processing of cookie == 0 if ReadOffset > ClientCommittedOffset if any
    AFL_ENSURE(ev->Get()->AssignId == Partition.AssignId);
    for (auto& readId : ev->Get()->CommitInfo.Cookies) {
        if (readId == 0) {
            if (ReadIdCommitted > 0) {
                ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "commit of 0 allowed only as first commit in " << Partition, PersQueue::ErrorCode::BAD_REQUEST));
                return;
            }
            NextCommits.insert(0);
            continue;
        }
        if (readId <= ReadIdCommitted) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "commit of " << readId << " that is already committed in " << Partition, PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        if (readId >= ReadIdToResponse) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "commit of unknown cookie " << readId << " in " << Partition, PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        bool res = NextCommits.insert(readId).second;
        if (!res) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "double commit of cookie " << readId << " in " << Partition, PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " commit request from client for " << readId << " in " << Partition);
    }

    MakeCommit(ctx);

    if (NextCommits.size() >= AppData(ctx)->PQConfig.GetMaxReadCookies()) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "got more than " << AppData(ctx)->PQConfig.GetMaxReadCookies()
                                                            << " unordered cookies to commit in " << Partition << ", last cookie is " << ReadIdCommitted,
                        PersQueue::ErrorCode::BAD_REQUEST));
        return;
    }
}

void TPartitionActor::Handle(TEvPQProxy::TEvCommitRange::TPtr& ev, const TActorContext& ctx) {
    AFL_ENSURE(ev->Get()->AssignId == Partition.AssignId);

    for (auto& c : ev->Get()->CommitInfo.Ranges) {
        NextRanges.InsertInterval(c.first, c.second);
    }

    MakeCommit(ctx);

    if (NextRanges.GetNumIntervals() >= AppData(ctx)->PQConfig.GetMaxReadCookies()) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "got more than " << AppData(ctx)->PQConfig.GetMaxReadCookies()
                                                            << " unordered offset ranges to commit in " << Partition
                                                            << ", last to be committed offset is " << ClientCommitOffset
                                                            << ", committed offset is " << CommittedOffset,
                        PersQueue::ErrorCode::BAD_REQUEST));
        return;
    }
}



void TPartitionActor::Die(const TActorContext& ctx) {
    if (PipeClient)
        NTabletPipe::CloseClient(ctx, PipeClient);
    TActorBootstrapped<TPartitionActor>::Die(ctx);
}

bool TPartitionActor::OnUnhandledException(const std::exception& exc) {
    NPQ::DoLogUnhandledException(NKikimrServices::PQ_READ_PROXY, TStringBuilder() << "[" << Session <<"][" << Partition << "] ", exc);

    ActorContext().Send(ParentId, new TEvPQProxy::TEvCloseSession(
        TStringBuilder() << "unexpected error: " << exc.what(), PersQueue::ErrorCode::ERROR));

    this->Die(ActorContext());

    return true;
}

void TPartitionActor::HandlePoison(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    if (LockCounted)
        Counters.PartitionsToBeLocked.Dec();
    Die(ctx);
}

void TPartitionActor::Handle(TEvPQProxy::TEvDeadlineExceeded::TPtr& ev, const TActorContext& ctx) {
    if (WaitDataInfly.erase(ev->Get()->Cookie)) {
        DoWakeup(ctx);
    }
}

void TPartitionActor::HandleWakeup(const TActorContext& ctx) {
    DoWakeup(ctx);
    ctx.Schedule(PREWAIT_DATA, new TEvents::TEvWakeup());
}

void TPartitionActor::DoWakeup(const TActorContext& ctx) {
    if (WaitForData && ReadOffset >= EndOffset && WaitDataInfly.size() <= 1 && PipeClient) { //send one more
        WaitDataInPartition(ctx);
    }
}

}
