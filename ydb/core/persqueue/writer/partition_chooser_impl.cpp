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

    NeedUpdateTable = (!pqConfig.GetTopicsAreFirstClassCitizen() || pqConfig.GetUseSrcIdMetaMappingInFirstClass()) && !SplitMergeEnabled_ && SourceId;

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

    if (pqConfig.GetTopicsAreFirstClassCitizen()) {
        if (pqConfig.GetUseSrcIdMetaMappingInFirstClass()) {
            TThisActor::Become(&TThis::StateInit);
            InitTable(ctx);
        } else {
            ChoosePartition(ctx);
        }
    } else {
        TThisActor::Become(&TThis::StateInit);
        StartKqpSession(ctx);
    }
}

void TPartitionChooserActor::Stop(const TActorContext& ctx) {
    CloseKqpSession(ctx);
    if (PipeToBalancer) {
        NTabletPipe::CloseClient(ctx, PipeToBalancer);
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

void TPartitionChooserActor::InitTable(const NActors::TActorContext& ctx) {
    ctx.Send(
            NMetadata::NProvider::MakeServiceId(ctx.SelfID.NodeId()),
            new NMetadata::NProvider::TEvPrepareManager(NGRpcProxy::V1::TSrcIdMetaInitManager::GetInstant())
    );
}

void TPartitionChooserActor::StartKqpSession(const NActors::TActorContext& ctx) {
    auto ev = MakeCreateSessionRequest(ctx);
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
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

THolder<NKqp::TEvKqp::TEvCreateSessionRequest> TPartitionChooserActor::MakeCreateSessionRequest(const NActors::TActorContext& ctx) {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
    ev->Record.MutableRequest()->SetDatabase(GetDatabaseName(ctx));
    return ev;
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

void TPartitionChooserActor::RequestPQRB(const NActors::TActorContext& ctx) {
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

    TThisActor::Become(&TThis::StateSelect);
    NTabletPipe::SendData(ctx, PipeToBalancer, new TEvPersQueue::TEvGetPartitionIdForWrite());
}

void TPartitionChooserActor::ReplyResult(const NActors::TActorContext& ctx) {
    ctx.Send(Parent, new TEvPartitionChooser::TEvChooseResult(Partition->PartitionId, Partition->TabletId));
}

void TPartitionChooserActor::ReplyError(ErrorCode code, TString&& errorMessage, const NActors::TActorContext& ctx) {
    ctx.Send(Parent, new TEvPartitionChooser::TEvChooseError(code, std::move(errorMessage)));

    Stop(ctx);
}

void TPartitionChooserActor::HandleInit(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx) {
    StartKqpSession(ctx);
}

void TPartitionChooserActor::HandleInit(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ53 : " <<  record, ctx);
        return;
    }

    KqpSessionId = record.GetResponse().GetSessionId();
    Y_ABORT_UNLESS(!KqpSessionId.empty());

    SendSelectRequest(ctx);
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

void TPartitionChooserActor::HandleSelect(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const TActorContext& ctx) {
    PartitionId = ev->Get()->Record.GetPartitionId();
    DEBUG("Received partition " << PartitionId << " from PQRB for SourceId=" << SourceId);
    Partition = Chooser->GetPartition(PartitionId.value());

    OnPartitionChosen(ctx);
}

void TPartitionChooserActor::HandleSelect(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);

    ReplyError(ErrorCode::INITIALIZING, "Pipe destroyed", ctx);
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

    if (NeedUpdateTable) {
        SendUpdateRequests(ctx);
    } else {
        TThisActor::Become(&TThis::StateIdle);

        ReplyResult(ctx);
    }
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


} // namespace NKikimr::NPQ
