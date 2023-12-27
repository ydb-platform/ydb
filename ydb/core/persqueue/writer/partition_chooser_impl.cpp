#include "common.h"
#include "partition_chooser_impl.h"
#include "ydb/public/sdk/cpp/client/ydb_proto/accessor.h"

#include <library/cpp/digest/md5/md5.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/utils.h>
#include <ydb/services/lib/sharding/sharding.h>

namespace NKikimr::NPQ {
namespace NPartitionChooser {

NYql::NDecimal::TUint128 Hash(const TString& sourceId) {
    return NKikimr::NDataStreams::V1::HexBytesToDecimal(MD5::Calc(sourceId));
}

ui32 TAsIsSharder::operator()(const TString& sourceId, ui32 totalShards) const {
    if (!sourceId) {
        return 0;
    }
    return (sourceId.at(0) - 'A') % totalShards;
}

ui32 TMd5Sharder::operator()(const TString& sourceId, ui32 totalShards) const {
    return NKikimr::NDataStreams::V1::ShardFromDecimal(Hash(sourceId), totalShards);
}

TString TAsIsConverter::operator()(const TString& sourceId) const {
    return sourceId;
}

TString TMd5Converter::operator()(const TString& sourceId) const {
    return AsKeyBound(Hash(sourceId));
}


//
// TPartitionChooserActor
//

#if defined(LOG_PREFIX) || defined(TRACE) || defined(DEBUG) || defined(INFO) || defined(ERROR)
#error "Already defined LOG_PREFIX or TRACE or DEBUG or INFO or ERROR"
#endif


#define LOG_PREFIX "TPartitionChooser " << SelfId()                  \
                    << " (SourceId=" << SourceId                     \
                    << ", PreferedPartition=" << PreferedPartition   \
                    << ", SplitMergeEnabled=" << SplitMergeEnabled_  \
                    << ") "
#define TRACE(message) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define DEBUG(message) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define INFO(message)  LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define ERROR(message) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);


TPartitionChooserActor::TPartitionChooserActor(TActorId parent,
                                                         const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                                                         std::shared_ptr<IPartitionChooser>& chooser,
                                                         NPersQueue::TTopicConverterPtr& fullConverter,
                                                         const TString& sourceId,
                                                         std::optional<ui32> preferedPartition)
    : Parent(parent)
    , FullConverter(fullConverter)
    , SourceId(sourceId)
    , PreferedPartition(preferedPartition)
    , Chooser(chooser)
    , SplitMergeEnabled_(SplitMergeEnabled(config.GetPQTabletConfig()))
    , Partition(nullptr)
    , BalancerTabletId(config.GetBalancerTabletID()) {
}

void TPartitionChooserActor::Bootstrap(const TActorContext& ctx) {
    const auto& pqConfig = AppData(ctx)->PQConfig;

    NeedUpdateTable = (!pqConfig.GetTopicsAreFirstClassCitizen() || pqConfig.GetUseSrcIdMetaMappingInFirstClass()) && SourceId;

    if (!SourceId) {
        return ChoosePartition(ctx);
    }

    TableGeneration = pqConfig.GetTopicsAreFirstClassCitizen() ? ESourceIdTableGeneration::PartitionMapping
                                                               : ESourceIdTableGeneration::SrcIdMeta2;
    try {
        EncodedSourceId = NSourceIdEncoding::EncodeSrcId(
                FullConverter->GetTopicForSrcIdHash(), SourceId, TableGeneration
        );
    } catch (yexception& e) {
        return ReplyError(ErrorCode::BAD_REQUEST, TStringBuilder() << "incorrect sourceId \"" << SourceId << "\": " << e.what(), ctx);
    }

    SelectQuery = GetSelectSourceIdQueryFromPath(pqConfig.GetSourceIdTablePath(), TableGeneration);
    UpdateQuery = GetUpdateSourceIdQueryFromPath(pqConfig.GetSourceIdTablePath(), TableGeneration);

    DEBUG("SelectQuery: " << SelectQuery);
    DEBUG("UpdateQuery: " << UpdateQuery);

    if (pqConfig.GetTopicsAreFirstClassCitizen()) {
        if (pqConfig.GetUseSrcIdMetaMappingInFirstClass()) {
            InitTable(ctx);
        } else {
            ChoosePartition(ctx);
        }
    } else {
        StartKqpSession(ctx);
    }
}

void TPartitionChooserActor::Stop(const TActorContext& ctx) {
    CloseKqpSession(ctx);
    if (PipeToBalancer) {
        NTabletPipe::CloseClient(ctx, PipeToBalancer);
    }
    if (PartitionPipe) {
        NTabletPipe::CloseClient(ctx, PartitionPipe);
    }
    IActor::Die(ctx);
}

void TPartitionChooserActor::ScheduleStop() {
    TThisActor::Become(&TThis::StateDestroy);
}

TString TPartitionChooserActor::GetDatabaseName(const NActors::TActorContext& ctx) {
    const auto& pqConfig = AppData(ctx)->PQConfig;
    switch (TableGeneration) {
        case ESourceIdTableGeneration::SrcIdMeta2:
            return NKikimr::NPQ::GetDatabaseFromConfig(pqConfig);
        case ESourceIdTableGeneration::PartitionMapping:
            return AppData(ctx)->TenantName;
    }
}

//
// StateInitTable
//

void TPartitionChooserActor::InitTable(const NActors::TActorContext& ctx) {
    DEBUG("InitTable")
    TThisActor::Become(&TThis::StateInitTable);

    ctx.Send(
            NMetadata::NProvider::MakeServiceId(ctx.SelfID.NodeId()),
            new NMetadata::NProvider::TEvPrepareManager(NGRpcProxy::V1::TSrcIdMetaInitManager::GetInstant())
    );
}

void TPartitionChooserActor::Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx) {
    StartKqpSession(ctx);
}


//
// StartKqpSession
// 

THolder<NKqp::TEvKqp::TEvCreateSessionRequest> TPartitionChooserActor::MakeCreateSessionRequest(const NActors::TActorContext& ctx) {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
    ev->Record.MutableRequest()->SetDatabase(GetDatabaseName(ctx));
    return ev;
}

void TPartitionChooserActor::StartKqpSession(const NActors::TActorContext& ctx) {
    DEBUG("StartKqpSession")
    TThisActor::Become(&TThis::StateCreateKqpSession);

    auto ev = MakeCreateSessionRequest(ctx);
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
}

void TPartitionChooserActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ53 : " <<  record, ctx);
        return;
    }

    KqpSessionId = record.GetResponse().GetSessionId();
    Y_ABORT_UNLESS(!KqpSessionId.empty());

    SendSelectRequest(ctx);
}


//
// StatePQRB
//

void TPartitionChooserActor::RequestPQRB(const NActors::TActorContext& ctx) {
    DEBUG("RequestPQRB")
    TThisActor::Become(&TThis::StatePQRB);
    Y_ABORT_UNLESS(BalancerTabletId);

    if (!PipeToBalancer) {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };
        PipeToBalancer = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, BalancerTabletId, clientConfig));
    }

    NTabletPipe::SendData(ctx, PipeToBalancer, new TEvPersQueue::TEvGetPartitionIdForWrite());
}

void TPartitionChooserActor::Handle(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const TActorContext& ctx) {
    PartitionId = ev->Get()->Record.GetPartitionId();
    DEBUG("Received partition " << PartitionId << " from PQRB for SourceId=" << SourceId);
    Partition = Chooser->GetPartition(PartitionId.value());

    OnPartitionChosen(ctx);
}

void TPartitionChooserActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);

    if (ev->Get()->Status != NKikimrProto::EReplyStatus::OK) {
        ReplyError(ErrorCode::INITIALIZING, "Pipe connection fail", ctx);
    }
}

void TPartitionChooserActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);

    ReplyError(ErrorCode::INITIALIZING, "Pipe destroyed", ctx);
}



//
// GetOwnership
//

THolder<TEvPersQueue::TEvRequest> MakeRequest(ui32 partitionId, TActorId pipe) {
    auto ev = MakeHolder<TEvPersQueue::TEvRequest>();

    ev->Record.MutablePartitionRequest()->SetPartition(partitionId);
    ActorIdToProto(pipe, ev->Record.MutablePartitionRequest()->MutablePipeClient());

    return ev;
}

void TPartitionChooserActor::GetOwnership() {
    Become(&TThis::StateOwnership);

    NTabletPipe::TClientConfig config;
    config.RetryPolicy = {
        .RetryLimitCount = 6,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(100),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };

    TRACE("GetOwnership Partition TabletId=" << Partition->TabletId);
    PartitionPipe = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), Partition->TabletId, config));

    auto ev = MakeRequest(Partition->PartitionId, PartitionPipe);

    auto& cmd = *ev->Record.MutablePartitionRequest()->MutableCmdGetOwnership();
    cmd.SetOwner(SourceId ? SourceId : CreateGuidAsString());
    cmd.SetForce(true);

    NTabletPipe::SendData(SelfId(), PartitionPipe, ev.Release());
}

void TPartitionChooserActor::HandleOwnership(TEvPersQueue::TEvResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& record = ev->Get()->Record;

    TString error;
    if (!BasicCheck(record, error)) {
        return ReplyError(ErrorCode::INITIALIZING, std::move(error), ctx);
    }

    const auto& response = record.GetPartitionResponse();
    if (!response.HasCmdGetOwnershipResult()) {
        return ReplyError(ErrorCode::INITIALIZING, "Absent Ownership result", ctx);
    }

    if (NKikimrPQ::ETopicPartitionStatus::Active != response.GetCmdGetOwnershipResult().GetStatus()) {
        return ReplyError(ErrorCode::INITIALIZING, "Absent Ownership result", ctx);
    }

    OwnerCookie = response.GetCmdGetOwnershipResult().GetOwnerCookie();

    if (NeedUpdateTable) {
        SendUpdateRequests(ctx);
    } else {
        TThisActor::Become(&TThis::StateIdle);

        ReplyResult(ctx);
    }
}


void TPartitionChooserActor::CloseKqpSession(const TActorContext& ctx) {
    if (KqpSessionId) {
        auto ev = MakeCloseSessionRequest();
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());

        KqpSessionId = "";
    }
}

void TPartitionChooserActor::SendUpdateRequests(const TActorContext& ctx) {
    TThisActor::Become(&TThis::StateUpdate);

    auto ev = MakeUpdateQueryRequest(ctx);
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());

    UpdatesInflight++;
}

void TPartitionChooserActor::SendSelectRequest(const NActors::TActorContext& ctx) {
    TThisActor::Become(&TThis::StateSelect);

    auto ev = MakeSelectQueryRequest(ctx);
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());

    SelectInflight++;
}


THolder<NKqp::TEvKqp::TEvCloseSessionRequest> TPartitionChooserActor::MakeCloseSessionRequest() {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
    ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
    return ev;
}

THolder<NKqp::TEvKqp::TEvQueryRequest> TPartitionChooserActor::MakeSelectQueryRequest(const NActors::TActorContext& ctx) {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

    ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    ev->Record.MutableRequest()->SetQuery(SelectQuery);

    ev->Record.MutableRequest()->SetDatabase(GetDatabaseName(ctx));
    // fill tx settings: set commit tx flag&  begin new serializable tx.
    ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
    ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(false);
    ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    // keep compiled query in cache.
    ev->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);

    NYdb::TParamsBuilder paramsBuilder = NYdb::TParamsBuilder();

    SetHashToTParamsBuilder(paramsBuilder, EncodedSourceId);

    paramsBuilder
        .AddParam("$Topic")
            .Utf8(FullConverter->GetClientsideName())
            .Build()
        .AddParam("$SourceId")
            .Utf8(EncodedSourceId.EscapedSourceId)
            .Build();

    NYdb::TParams params = paramsBuilder.Build();

    ev->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params)));

    return ev;
}

THolder<NKqp::TEvKqp::TEvQueryRequest> TPartitionChooserActor::MakeUpdateQueryRequest(const NActors::TActorContext& ctx) {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

    ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    ev->Record.MutableRequest()->SetQuery(UpdateQuery);
    ev->Record.MutableRequest()->SetDatabase(GetDatabaseName(ctx));
    // fill tx settings: set commit tx flag&  begin new serializable tx.
    ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
    if (KqpSessionId) {
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
    }
    if (TxId) {
        ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
        TxId = "";
    } else {
        ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    }
    // keep compiled query in cache.
    ev->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);

    NYdb::TParamsBuilder paramsBuilder = NYdb::TParamsBuilder();

    SetHashToTParamsBuilder(paramsBuilder, EncodedSourceId);

    paramsBuilder
        .AddParam("$Topic")
            .Utf8(FullConverter->GetClientsideName())
            .Build()
        .AddParam("$SourceId")
            .Utf8(EncodedSourceId.EscapedSourceId)
            .Build()
        .AddParam("$CreateTime")
            .Uint64(CreateTime)
            .Build()
        .AddParam("$AccessTime")
            .Uint64(TInstant::Now().MilliSeconds())
            .Build()
        .AddParam("$Partition")
            .Uint32(Partition->PartitionId)
            .Build();

    NYdb::TParams params = paramsBuilder.Build();

    ev->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params)));

    return ev;
}

void TPartitionChooserActor::ReplyResult(const NActors::TActorContext& ctx) {
    ctx.Send(Parent, new TEvPartitionChooser::TEvChooseResult(Partition->PartitionId, Partition->TabletId, OwnerCookie));
}

void TPartitionChooserActor::ReplyError(ErrorCode code, TString&& errorMessage, const NActors::TActorContext& ctx) {
    INFO("ReplyError: " << errorMessage);
    ctx.Send(Parent, new TEvPartitionChooser::TEvChooseError(code, std::move(errorMessage)));

    Stop(ctx);
}

void TPartitionChooserActor::HandleDestroy(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    KqpSessionId = record.GetResponse().GetSessionId();

    Stop(ctx);
}

void TPartitionChooserActor::HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record.GetRef();

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        return ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ50 : " <<  record, ctx);
    }

    auto& t = record.GetResponse().GetResults(0).GetValue().GetStruct(0);

    TxId = record.GetResponse().GetTxMeta().id();
    Y_ABORT_UNLESS(!TxId.empty());

    if (t.ListSize() != 0) {
        auto& tt = t.GetList(0).GetStruct(0);
        if (tt.HasOptional() && tt.GetOptional().HasUint32()) { //already got partition
            auto accessTime = t.GetList(0).GetStruct(2).GetOptional().GetUint64();
            if (accessTime > AccessTime) { // AccessTime
                PartitionId = tt.GetOptional().GetUint32();
                DEBUG("Received partition " << PartitionId << " from table for SourceId=" << SourceId);
                Partition = Chooser->GetPartition(PartitionId.value());
                CreateTime = t.GetList(0).GetStruct(1).GetOptional().GetUint64();
                AccessTime = accessTime;
            }
        }
    }

    if (CreateTime == 0) {
        CreateTime = TInstant::Now().MilliSeconds();
    }

    if (!Partition) {
        ChoosePartition(ctx);
    } else {
        OnPartitionChosen(ctx);
    }
}

void TPartitionChooserActor::HandleUpdate(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record.GetRef();

    if (record.GetYdbStatus() == Ydb::StatusIds::ABORTED) {
        if (!PartitionPersisted) {
            CloseKqpSession(ctx);
            StartKqpSession(ctx);
        }
        return;
    }

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        if (!PartitionPersisted) {
            ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ51 : " <<  record, ctx);
        }
        return;
    }

    if (!PartitionPersisted) {
        ReplyResult(ctx);
        PartitionPersisted = true;
        // Use tx only for query after select. Updating AccessTime without transaction.
        CloseKqpSession(ctx);
    }

    TThisActor::Become(&TThis::StateIdle);
}

void TPartitionChooserActor::HandleDestroy(NKqp::TEvKqp::TEvQueryResponse::TPtr&, const TActorContext& ctx) {
    Stop(ctx);
}

void TPartitionChooserActor::HandleIdle(TEvPartitionChooser::TEvRefreshRequest::TPtr&, const TActorContext& ctx) {
    if (PartitionPersisted) {
         // we do not update AccessTime for Split/Merge partitions because don't use table.
        SendUpdateRequests(ctx);
    }
}

void TPartitionChooserActor::ChoosePartition(const TActorContext& ctx) {
    auto [roundRobin, p] = ChoosePartitionSync(ctx);
    if (roundRobin) {
        RequestPQRB(ctx);
    } else {
        Partition = p;
        OnPartitionChosen(ctx);
    }
}

void TPartitionChooserActor::OnPartitionChosen(const TActorContext& ctx) {
    if (!Partition && PreferedPartition) {
        return ReplyError(ErrorCode::BAD_REQUEST,
                          TStringBuilder() << "Prefered partition " << (PreferedPartition.value() + 1) << " is not exists or inactive.",
                          ctx);
    }

    if (!Partition) {
        return ReplyError(ErrorCode::INITIALIZING, "Can't choose partition", ctx);
    }

    if (PreferedPartition && Partition->PartitionId != PreferedPartition.value()) {
        return ReplyError(ErrorCode::BAD_REQUEST,
                          TStringBuilder() << "MessageGroupId " << SourceId << " is already bound to PartitionGroupId "
                                    << (Partition->PartitionId + 1) << ", but client provided " << (PreferedPartition.value() + 1)
                                    << ". MessageGroupId->PartitionGroupId binding cannot be changed, either use "
                                       "another MessageGroupId, specify PartitionGroupId " << (Partition->PartitionId + 1)
                                    << ", or do not specify PartitionGroupId at all.",
                          ctx);
    }

    if (SplitMergeEnabled_ && SourceId && PartitionId) {
        if (Partition != Chooser->GetPartition(SourceId)) {
            return ReplyError(ErrorCode::BAD_REQUEST, 
                              TStringBuilder() << "Message group " << SourceId << " not in a partition boundary", ctx);
        }
    }

    GetOwnership();

/*
*/
}


std::pair<bool, const typename TPartitionChooserActor::TPartitionInfo*> TPartitionChooserActor::ChoosePartitionSync(const TActorContext& ctx) const {
    const auto& pqConfig = AppData(ctx)->PQConfig;
    if (SourceId && SplitMergeEnabled_) {
        return {false, Chooser->GetPartition(SourceId)};
    } else if (PreferedPartition) {
        return {false, Chooser->GetPartition(PreferedPartition.value())};
    } else if (pqConfig.GetTopicsAreFirstClassCitizen() && SourceId) {
        return {false, Chooser->GetPartition(SourceId)};
    } else {
        return {true, nullptr};
    }
}

} // namespace NPartitionChooser


std::shared_ptr<IPartitionChooser> CreatePartitionChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config, bool withoutHash) {
    if (SplitMergeEnabled(config.GetPQTabletConfig())) {
        if (withoutHash) {
            return std::make_shared<NPartitionChooser::TBoundaryChooser<NPartitionChooser::TAsIsConverter>>(config);
        } else {
            return std::make_shared<NPartitionChooser::TBoundaryChooser<NPartitionChooser::TMd5Converter>>(config);
        }
    } else {
        if (withoutHash) {
            return std::make_shared<NPartitionChooser::THashChooser<NPartitionChooser::TAsIsSharder>>(config);
        } else {
            return std::make_shared<NPartitionChooser::THashChooser<NPartitionChooser::TMd5Sharder>>(config);
        }
    }
}


IActor* CreatePartitionChooserActor(TActorId parentId,
                                    const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                                    NPersQueue::TTopicConverterPtr& fullConverter,
                                    const TString& sourceId,
                                    std::optional<ui32> preferedPartition,
                                    bool withoutHash) {
    auto chooser = CreatePartitionChooser(config, withoutHash);
    return new NPartitionChooser::TPartitionChooserActor(parentId, config, chooser, fullConverter, sourceId, preferedPartition);
}

#undef LOG_PREFIX
#undef TRACE
#undef DEBUG
#undef INFO
#undef ERROR

} // namespace NKikimr::NPQ
