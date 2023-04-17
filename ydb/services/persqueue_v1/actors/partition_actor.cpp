#include "partition_actor.h"
#include "persqueue_utils.h"

#include <ydb/core/persqueue/codecs/pqv1.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>

#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <google/protobuf/util/time_util.h>

#include <util/charset/utf8.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace PersQueue::V1;
using namespace Topic;


TPartitionActor::TPartitionActor(
        const TActorId& parentId, const TString& clientId, const TString& clientPath, const ui64 cookie,
        const TString& session, const TPartitionId& partition, const ui32 generation, const ui32 step,
        const ui64 tabletID, const TTopicCounters& counters, bool commitsDisabled,
        const TString& clientDC, bool rangesMode, const NPersQueue::TTopicConverterPtr& topic,
        bool useMigrationProtocol
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
    , RequestInfly(false)
    , EndOffset(0)
    , SizeLag(0)
    , NeedRelease(false)
    , Released(false)
    , WaitDataCookie(0)
    , WaitForData(false)
    , LockCounted(false)
    , Counters(counters)
    , CommitsDisabled(commitsDisabled)
    , CommitCookie(1)
    , Topic(topic)
    , UseMigrationProtocol(useMigrationProtocol)
{
}


void TPartitionActor::MakeCommit(const TActorContext& ctx) {
    ui64 offset = ClientReadOffset;
    if (CommitsDisabled)
        return;
    if (CommitsInfly.size() > MAX_COMMITS_INFLY)
        return;

    //Ranges mode
    if (!NextRanges.Empty() && NextRanges.Min() == ClientCommitOffset) {
        auto first = NextRanges.begin();
        offset = first->second;
        NextRanges.EraseInterval(first->first, first->second);

        ClientCommitOffset = offset;
        ++CommitCookie;
        CommitsInfly.push_back(std::pair<ui64, TCommitInfo>(CommitCookie, {CommitCookie, offset, ctx.Now()}));
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
            ctx.Send(ParentId, new TEvPQProxy::TEvCommitDone(Partition.AssignId, 0, 0, CommittedOffset));
        } else {
            ClientCommitOffset = ClientReadOffset;
            CommitsInfly.push_back(std::pair<ui64, TCommitInfo>(0, {0, ClientReadOffset, ctx.Now()}));
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
    Y_VERIFY(jt != Offsets.end());

    offset = Max(offset, jt->Offset);

    Offsets.erase(Offsets.begin(), ++jt);

    Y_VERIFY(offset > ClientCommitOffset);

    ClientCommitOffset = offset;
    CommitsInfly.push_back(std::pair<ui64, TCommitInfo>(readId, {startReadId, offset, ctx.Now()}));
    Counters.SLITotal.Inc();

    if (PipeClient) //if not then pipe will be recreated soon and SendCommit will be done
        SendCommit(readId, offset, ctx);
}

TPartitionActor::~TPartitionActor() = default;


void TPartitionActor::Bootstrap(const TActorContext&) {
    Become(&TThis::StateFunc);
}


void TPartitionActor::CheckRelease(const TActorContext& ctx) {
    const bool hasUncommittedData = ReadOffset > ClientCommitOffset && ReadOffset > ClientReadOffset; //TODO: remove ReadOffset > ClientReadOffset - otherwise wait for commit with cookie(0)
    if (NeedRelease) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " checking release readOffset " << ReadOffset << " committedOffset " << CommittedOffset << " ReadGuid " << ReadGuid
                        << " CommitsInfly.size " << CommitsInfly.size() << " Released " << Released);
    }

    if (NeedRelease && (ReadGuid.empty() && CommitsInfly.empty() && !hasUncommittedData && !Released)) {
        Released = true;
        ctx.Send(ParentId, new TEvPQProxy::TEvPartitionReleased(Partition));
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " check release done - releasing; readOffset " << ReadOffset << " committedOffset " << CommittedOffset << " ReadGuid " << ReadGuid
                        << " CommitsInfly.size " << CommitsInfly.size() << " Released " << Released);

    }
}


void TPartitionActor::SendCommit(const ui64 readId, const ui64 offset, const TActorContext& ctx) {
    NKikimrClient::TPersQueueRequest request;
    request.MutablePartitionRequest()->SetTopic(Topic->GetPrimaryPath());
    request.MutablePartitionRequest()->SetPartition(Partition.Partition);
    request.MutablePartitionRequest()->SetCookie(readId);

    Y_VERIFY(PipeClient);

    ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());
    auto commit = request.MutablePartitionRequest()->MutableCmdSetClientOffset();
    commit->SetClientId(ClientId);
    commit->SetOffset(offset);
    Y_VERIFY(!Session.empty());
    commit->SetSessionId(Session);

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " committing to position " << offset << " prev " << CommittedOffset
                        << " end " << EndOffset << " by cookie " << readId);

    TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
    req->Record.Swap(&request);

    NTabletPipe::SendData(ctx, PipeClient, req.Release());
}

void TPartitionActor::RestartPipe(const TActorContext& ctx, const TString& reason, const NPersQueue::NErrorCode::EErrorCode errorCode) {

    if (!PipeClient)
        return;

    Counters.Errors.Inc();

    NTabletPipe::CloseClient(ctx, PipeClient);
    PipeClient = TActorId{};
    if (errorCode != NPersQueue::NErrorCode::OVERLOAD)
        ++PipeGeneration;

    if (PipeGeneration == MAX_PIPE_RESTARTS) {
        // ???
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("too much attempts to restart pipe", PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED));
        return;
    }

    ctx.Schedule(TDuration::MilliSeconds(RESTART_PIPE_DELAY_MS), new TEvPQProxy::TEvRestartPipe());

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                                                                  << " schedule pipe restart attempt " << PipeGeneration << " reason: " << reason);
}


void TPartitionActor::Handle(const TEvPQProxy::TEvRestartPipe::TPtr&, const TActorContext& ctx) {

    Y_VERIFY(!PipeClient);

    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = 6,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(100),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };
    PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, TabletID, clientConfig));
    Y_VERIFY(TabletID);

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " pipe restart attempt " << PipeGeneration << " RequestInfly " << RequestInfly << " ReadOffset " << ReadOffset << " EndOffset " << EndOffset
                            << " InitDone " << InitDone << " WaitForData " << WaitForData);

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
    Y_VERIFY(batch);
    return batch->source_id();
}

TString GetBatchSourceId(Topic::StreamReadMessage::ReadResponse::Batch* batch) {
    Y_VERIFY(batch);
    return batch->producer_id();
}

void SetBatchSourceId(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch* batch, TString value) {
    Y_VERIFY(batch);
    batch->set_source_id(std::move(value));
}

void SetBatchSourceId(Topic::StreamReadMessage::ReadResponse::Batch* batch, TString value) {
    Y_VERIFY(batch);
    batch->set_producer_id(std::move(value));
}

void SetBatchExtraField(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch* batch, TString key, TString value) {
    Y_VERIFY(batch);
    auto* item = batch->add_extra_fields();
    item->set_key(std::move(key));
    item->set_value(std::move(value));
}

void SetBatchExtraField(Topic::StreamReadMessage::ReadResponse::Batch* batch, TString key, TString value) {
    Y_VERIFY(batch);
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
        Y_VERIFY(r.GetOffset() >= ReadOffset);
        ReadOffset = r.GetOffset() + 1;
        hasOffset = true;

        auto proto(GetDeserializedData(r.GetData()));
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
            Y_VERIFY(write_ts >= 0);
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
        }
        hasData = true;
    }

    const ui64 realReadOffset = res.HasRealReadOffset() ? res.GetRealReadOffset() : 0;

    if (!hasOffset) { //no data could be read from partition at offset ReadOffset - no data in partition at all???
        ReadOffset = Min(Max(ReadOffset + 1, realReadOffset + 1), EndOffset);
    }
    return hasData;
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
        Y_VERIFY(!ev->Get()->Record.HasErrorCode());
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
        if (result.GetCookie() != INIT_COOKIE) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " unwaited response in init with cookie " << result.GetCookie());
            return;
        }
        Y_VERIFY(RequestInfly);
        CurrentRequest.Clear();
        RequestInfly = false;

        Y_VERIFY(result.HasCmdGetClientOffsetResult());
        const auto& resp = result.GetCmdGetClientOffsetResult();
        Y_VERIFY(resp.HasEndOffset());
        EndOffset = resp.GetEndOffset();
        SizeLag = resp.GetSizeLag();
        WriteTimestampEstimateMs = resp.GetWriteTimestampEstimateMS();

        ClientCommitOffset = ReadOffset = CommittedOffset = resp.HasOffset() ? resp.GetOffset() : 0;
        Y_VERIFY(EndOffset >= CommittedOffset);

        if (resp.HasWriteTimestampMS())
            WTime = resp.GetWriteTimestampMS();

        InitDone = true;
        PipeGeneration = 0; //reset tries counter - all ok
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " INIT DONE " << Partition
                            << " EndOffset " << EndOffset << " readOffset " << ReadOffset << " committedOffset " << CommittedOffset);


        if (!StartReading) {
            ctx.Send(ParentId, new TEvPQProxy::TEvPartitionStatus(Partition, CommittedOffset, EndOffset, WriteTimestampEstimateMs));
        } else {
            InitStartReading(ctx);
        }
        return;
    }

    if (!result.HasCmdReadResult()) { //this is commit response
        if (CommitsInfly.empty()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " unwaited commit-response with cookie " << result.GetCookie() << "; waiting for nothing");
            return;
        }
        ui64 readId = CommitsInfly.front().first;

        if (result.GetCookie() != readId) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " unwaited commit-response with cookie " << result.GetCookie() << "; waiting for " << readId);
            return;
        }

        Counters.Commits.Inc();

        ui32 commitDurationMs = (ctx.Now() - CommitsInfly.front().second.StartTime).MilliSeconds();
        Counters.CommitLatency.IncFor(commitDurationMs, 1);
        if (commitDurationMs >= AppData(ctx)->PQConfig.GetCommitLatencyBigMs()) {
            Counters.SLIBigLatency.Inc();
        }

        CommittedOffset = CommitsInfly.front().second.Offset;
        ui64 startReadId = CommitsInfly.front().second.StartReadId;
        ctx.Send(ParentId, new TEvPQProxy::TEvCommitDone(Partition.AssignId, startReadId, readId, CommittedOffset));

        CommitsInfly.pop_front();

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " commit done to position " << CommittedOffset << " endOffset " << EndOffset << " with cookie " << readId);

        CheckRelease(ctx);
        PipeGeneration = 0; //reset tries counter - all ok
        MakeCommit(ctx);
        return;
    }

    //This is read
    Y_VERIFY(result.HasCmdReadResult());
    const auto& res = result.GetCmdReadResult();

    if (result.GetCookie() != (ui64)ReadOffset) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                    << " unwaited read-response with cookie " << result.GetCookie() << "; waiting for " << ReadOffset << "; current read guid is " << ReadGuid);
        return;
    }

    Y_VERIFY(res.HasMaxOffset());
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

    Y_VERIFY(!WaitForData);

    if (EndOffset > ReadOffset) {
        ctx.Send(ParentId, new TEvPQProxy::TEvPartitionReady(Partition, WTime, SizeLag, ReadOffset, EndOffset));
    } else {
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
        auto readResponse = MakeHolder<TEvPQProxy::TEvReadResponse>(
            std::move(response),
            ReadOffset,
            res.GetBlobsFromDisk() > 0,
            TDuration::MilliSeconds(res.GetWaitQuotaTimeMs())
        );
        ctx.Send(ParentId, readResponse.Release());
    }
    CheckRelease(ctx);

    PipeGeneration = 0; //reset tries counter - all ok
}


void TPartitionActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " pipe restart attempt " << PipeGeneration << " pipe creation result: " << msg->Status);

    if (msg->Status != NKikimrProto::OK) {
        RestartPipe(ctx, TStringBuilder() << "pipe to tablet is dead " << msg->TabletId, NPersQueue::NErrorCode::TABLET_PIPE_DISCONNECTED);
        return;
    }
}

void TPartitionActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    RestartPipe(ctx, TStringBuilder() << "pipe to tablet is dead " << ev->Get()->TabletId, NPersQueue::NErrorCode::TABLET_PIPE_DISCONNECTED);
}


void TPartitionActor::Handle(TEvPQProxy::TEvReleasePartition::TPtr&, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " (partition)releasing " << Partition << " ReadOffset " << ReadOffset << " ClientCommitOffset " << ClientCommitOffset
                        << " CommittedOffst " << CommittedOffset);
    NeedRelease = true;
    CheckRelease(ctx);
}


void TPartitionActor::Handle(TEvPQProxy::TEvGetStatus::TPtr&, const TActorContext& ctx) {
    ctx.Send(ParentId, new TEvPQProxy::TEvPartitionStatus(Partition, CommittedOffset, EndOffset, WriteTimestampEstimateMs, false));
}


void TPartitionActor::Handle(TEvPQProxy::TEvLockPartition::TPtr& ev, const TActorContext& ctx) {
    ClientReadOffset = ev->Get()->ReadOffset;
    ClientCommitOffset = ev->Get()->CommitOffset;
    ClientVerifyReadOffset = ev->Get()->VerifyReadOffset;

    if (StartReading) {
        Y_VERIFY(ev->Get()->StartReading); //otherwise it is signal from actor, this could not be done
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("double partition locking", PersQueue::ErrorCode::BAD_REQUEST));
        return;
    }

    StartReading = ev->Get()->StartReading;
    InitLockPartition(ctx);
}

void TPartitionActor::InitStartReading(const TActorContext& ctx) {

    Y_VERIFY(AllPrepareInited);
    Y_VERIFY(!WaitForData);
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

        if (ClientCommitOffset < CommittedOffset) {
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

    if (ClientCommitOffset > CommittedOffset) {
        if (ClientCommitOffset > ReadOffset) {
            ctx.Send(ParentId,
                     new TEvPQProxy::TEvCloseSession(TStringBuilder()
                            << "trying to read from position that is less than provided to commit: read " << ReadOffset
                            << " commit " << ClientCommitOffset,
                        PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        if (ClientCommitOffset > EndOffset) {
            ctx.Send(ParentId,
                     new TEvPQProxy::TEvCloseSession(TStringBuilder()
                           << "trying to commit to future: commit " << ClientCommitOffset << " endOffset " << EndOffset,
                        PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        Y_VERIFY(CommitsInfly.empty());
        CommitsInfly.push_back(std::pair<ui64, TCommitInfo>(Max<ui64>(), {Max<ui64>(), ClientCommitOffset, ctx.Now()}));
        Counters.SLITotal.Inc();
        if (PipeClient) //pipe will be recreated soon
            SendCommit(CommitsInfly.back().first, CommitsInfly.back().second.Offset, ctx);
    } else {
        ClientCommitOffset = CommittedOffset;
    }

    if (EndOffset > ReadOffset) {
        ctx.Send(ParentId, new TEvPQProxy::TEvPartitionReady(Partition, WTime, SizeLag, ReadOffset, EndOffset));
    } else {
        WaitForData = true;
        if (PipeClient) //pipe will be recreated soon
            WaitDataInPartition(ctx);
    }
}

//TODO: add here reaction on client release request

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
        Y_VERIFY(!PipeClient);
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

        NKikimrClient::TPersQueueRequest request;

        request.MutablePartitionRequest()->SetTopic(Topic->GetPrimaryPath());
        request.MutablePartitionRequest()->SetPartition(Partition.Partition);
        request.MutablePartitionRequest()->SetCookie(INIT_COOKIE);

        ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());

        auto cmd = request.MutablePartitionRequest()->MutableCmdCreateSession();
        cmd->SetClientId(ClientId);
        cmd->SetSessionId(Session);
        cmd->SetGeneration(Generation);
        cmd->SetStep(Step);

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " INITING " << Partition);

        TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
        Y_VERIFY(!RequestInfly);
        CurrentRequest = request;
        RequestInfly = true;
        req->Record.Swap(&request);

        NTabletPipe::SendData(ctx, PipeClient, req.Release());
    } else {
        Y_VERIFY(StartReading); //otherwise it is double locking from actor, not client - client makes lock always with StartReading == true
        Y_VERIFY(InitDone);
        InitStartReading(ctx);
    }
}


void TPartitionActor::WaitDataInPartition(const TActorContext& ctx) {

    if (WaitDataInfly.size() > 1) //already got 2 requests inflight
        return;
    Y_VERIFY(InitDone);

    Y_VERIFY(PipeClient);

    if (!WaitForData)
        return;

    Y_VERIFY(ReadOffset >= EndOffset);

    TAutoPtr<TEvPersQueue::TEvHasDataInfo> event(new TEvPersQueue::TEvHasDataInfo());
    event->Record.SetPartition(Partition.Partition);
    event->Record.SetOffset(ReadOffset);
    event->Record.SetCookie(++WaitDataCookie);
    ui64 deadline = (ctx.Now() + WAIT_DATA - WAIT_DELTA).MilliSeconds();
    event->Record.SetDeadline(deadline);
    event->Record.SetClientId(ClientId);

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition << " wait data in partition inited, cookie " << WaitDataCookie);

    NTabletPipe::SendData(ctx, PipeClient, event.Release());

    ctx.Schedule(PREWAIT_DATA, new TEvents::TEvWakeup());

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

    Y_VERIFY(record.HasEndOffset());
    Y_VERIFY(EndOffset <= record.GetEndOffset()); //end offset could not be changed if no data arrived, but signal will be sended anyway after timeout
    Y_VERIFY(ReadOffset >= EndOffset); //otherwise no WaitData were needed

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                    << " wait for data done: " << " readOffset " << ReadOffset << " EndOffset " << EndOffset << " newEndOffset "
                    << record.GetEndOffset() << " commitOffset " << CommittedOffset << " clientCommitOffset " << ClientCommitOffset
                    << " cookie " << ev->Get()->Record.GetCookie());

    EndOffset = record.GetEndOffset();
    SizeLag = record.GetSizeLag();

    if (ReadOffset < EndOffset) {
        WaitForData = false;
        WaitDataInfly.clear();
        ctx.Send(ParentId, new TEvPQProxy::TEvPartitionReady(Partition, WTime, SizeLag, ReadOffset, EndOffset));
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " ready for read with readOffset " << ReadOffset << " endOffset " << EndOffset);
    } else {
        if (PipeClient)
            WaitDataInPartition(ctx);
    }
    CheckRelease(ctx); //just for logging purpose
}


void TPartitionActor::Handle(TEvPQProxy::TEvRead::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " READ FROM " << Partition
                    << "maxCount " << ev->Get()->MaxCount << " maxSize " << ev->Get()->MaxSize << " maxTimeLagMs "
                    << ev->Get()->MaxTimeLagMs << " readTimestampMs " << ev->Get()->ReadTimestampMs
                    << " readOffset " << ReadOffset << " EndOffset " << EndOffset << " ClientCommitOffset "
                    << ClientCommitOffset << " committedOffset " << CommittedOffset << " Guid " << ev->Get()->Guid);

    Y_VERIFY(!NeedRelease);
    Y_VERIFY(!Released);

    Y_VERIFY(ReadGuid.empty());
    Y_VERIFY(!RequestInfly);

    ReadGuid = ev->Get()->Guid;

    const auto req = ev->Get();

    NKikimrClient::TPersQueueRequest request;

    request.MutablePartitionRequest()->SetTopic(Topic->GetPrimaryPath());

    request.MutablePartitionRequest()->SetPartition(Partition.Partition);
    request.MutablePartitionRequest()->SetCookie((ui64)ReadOffset);

    ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());
    auto read = request.MutablePartitionRequest()->MutableCmdRead();
    read->SetClientId(ClientId);
    read->SetClientDC(ClientDC);
    read->SetSessionId(Session);
    if (req->MaxCount) {
        read->SetCount(req->MaxCount);
    }
    if (req->MaxSize) {
        read->SetBytes(req->MaxSize);
    }
    if (req->MaxTimeLagMs) {
        read->SetMaxTimeLagMs(req->MaxTimeLagMs);
    }
    if (req->ReadTimestampMs) {
        read->SetReadTimestampMs(req->ReadTimestampMs);
    }

    read->SetOffset(ReadOffset);
    read->SetTimeoutMs(READ_TIMEOUT_DURATION.MilliSeconds());
    RequestInfly = true;
    CurrentRequest = request;

    if (!PipeClient) //Pipe will be recreated soon
        return;

    TAutoPtr<TEvPersQueue::TEvRequest> event(new TEvPersQueue::TEvRequest);
    event->Record.Swap(&request);

    NTabletPipe::SendData(ctx, PipeClient, event.Release());
}


void TPartitionActor::Handle(TEvPQProxy::TEvCommitCookie::TPtr& ev, const TActorContext& ctx) {
    //TODO: add here processing of cookie == 0 if ReadOffset > ClientCommittedOffset if any
    Y_VERIFY(ev->Get()->AssignId == Partition.AssignId);
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
    Y_VERIFY(ev->Get()->AssignId == Partition.AssignId);

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

void TPartitionActor::HandlePoison(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    if (LockCounted)
        Counters.PartitionsToBeLocked.Dec();
    Die(ctx);
}

void TPartitionActor::Handle(TEvPQProxy::TEvDeadlineExceeded::TPtr& ev, const TActorContext& ctx) {

    WaitDataInfly.erase(ev->Get()->Cookie);
    if (ReadOffset >= EndOffset && WaitDataInfly.size() <= 1 && PipeClient) {
        Y_VERIFY(WaitForData);
        WaitDataInPartition(ctx);
    }

}


void TPartitionActor::HandleWakeup(const TActorContext& ctx) {
    if (ReadOffset >= EndOffset && WaitDataInfly.size() <= 1 && PipeClient) { //send one more
        Y_VERIFY(WaitForData);
        WaitDataInPartition(ctx);
    }
}

}
