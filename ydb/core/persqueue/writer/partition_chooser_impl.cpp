#include "partition_chooser_impl.h"
#include "ydb/library/actors/interconnect/types.h"

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


#if defined(LOG_PREFIX) || defined(TRACE) || defined(DEBUG) || defined(INFO) || defined(ERROR)
#error "Already defined LOG_PREFIX or TRACE or DEBUG or INFO or ERROR"
#endif


#define LOG_PREFIX "TTableHelper "
#define TRACE(message) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define DEBUG(message) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define INFO(message)  LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define ERROR(message) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);

TTableHelper::TTableHelper(const TString& topicName, const TString& topicHashName)
    : TopicName(topicName)
    , TopicHashName(topicHashName) {
}

std::optional<ui32> TTableHelper::PartitionId() const {
    return PartitionId_;
}

bool TTableHelper::Initialize(const TActorContext& ctx, const TString& sourceId) {
    const auto& pqConfig = AppData(ctx)->PQConfig;

    TableGeneration = pqConfig.GetTopicsAreFirstClassCitizen() ? ESourceIdTableGeneration::PartitionMapping
                                                                   : ESourceIdTableGeneration::SrcIdMeta2;
    try {
        EncodedSourceId = NSourceIdEncoding::EncodeSrcId(
                    TopicHashName, sourceId, TableGeneration
            );
    } catch (yexception& e) {
        return false;
    }

    SelectQuery = GetSelectSourceIdQueryFromPath(pqConfig.GetSourceIdTablePath(), TableGeneration);
    UpdateQuery = GetUpdateSourceIdQueryFromPath(pqConfig.GetSourceIdTablePath(), TableGeneration);

    DEBUG("SelectQuery: " << SelectQuery);
    DEBUG("UpdateQuery: " << UpdateQuery);

    return true;
}

TString TTableHelper::GetDatabaseName(const NActors::TActorContext& ctx) {
    const auto& pqConfig = AppData(ctx)->PQConfig;
    switch (TableGeneration) {
        case ESourceIdTableGeneration::SrcIdMeta2:
            return NKikimr::NPQ::GetDatabaseFromConfig(pqConfig);
        case ESourceIdTableGeneration::PartitionMapping:
            return AppData(ctx)->TenantName;
    }
}

void TTableHelper::SendInitTableRequest(const NActors::TActorContext& ctx) {
    ctx.Send(
        NMetadata::NProvider::MakeServiceId(ctx.SelfID.NodeId()),
        new NMetadata::NProvider::TEvPrepareManager(NGRpcProxy::V1::TSrcIdMetaInitManager::GetInstant())
    );
}

void TTableHelper::SendCreateSessionRequest(const NActors::TActorContext& ctx) {
    auto ev = MakeCreateSessionRequest(ctx);
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
}

THolder<NKqp::TEvKqp::TEvCreateSessionRequest> TTableHelper::MakeCreateSessionRequest(const NActors::TActorContext& ctx) {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
    ev->Record.MutableRequest()->SetDatabase(GetDatabaseName(ctx));
    return ev;
}

bool TTableHelper::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext&) {
    const auto& record = ev->Get()->Record;

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        return false;
    }

    KqpSessionId = record.GetResponse().GetSessionId();
    Y_ABORT_UNLESS(!KqpSessionId.empty());

    return true;
}

void TTableHelper::CloseKqpSession(const TActorContext& ctx) {
    if (KqpSessionId) {
        auto ev = MakeCloseSessionRequest();
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());

        KqpSessionId = "";
    }
}

THolder<NKqp::TEvKqp::TEvCloseSessionRequest> TTableHelper::MakeCloseSessionRequest() {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
    ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
    return ev;
}

void TTableHelper::SendSelectRequest(const NActors::TActorContext& ctx) {
    auto ev = MakeSelectQueryRequest(ctx);
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
}

THolder<NKqp::TEvKqp::TEvQueryRequest> TTableHelper::MakeSelectQueryRequest(const NActors::TActorContext& ctx) {
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
            .Utf8(TopicName)
            .Build()
        .AddParam("$SourceId")
            .Utf8(EncodedSourceId.EscapedSourceId)
            .Build();

    NYdb::TParams params = paramsBuilder.Build();

    ev->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params)));

    return ev;
}

bool TTableHelper::HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext&) {
    auto& record = ev->Get()->Record.GetRef();

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        return false;
    }

    auto& t = record.GetResponse().GetResults(0).GetValue().GetStruct(0);

    TxId = record.GetResponse().GetTxMeta().id();
    Y_ABORT_UNLESS(!TxId.empty());

    if (t.ListSize() != 0) {
        auto& tt = t.GetList(0).GetStruct(0);
        if (tt.HasOptional() && tt.GetOptional().HasUint32()) { //already got partition
            auto accessTime = t.GetList(0).GetStruct(2).GetOptional().GetUint64();
            if (accessTime > AccessTime) { // AccessTime
                PartitionId_ = tt.GetOptional().GetUint32();
                CreateTime = t.GetList(0).GetStruct(1).GetOptional().GetUint64();
                AccessTime = accessTime;
            }
        }
    }

    if (CreateTime == 0) {
        CreateTime = TInstant::Now().MilliSeconds();
    }

    return true;
}


void TTableHelper::SendUpdateRequest(ui32 partitionId, const TActorContext& ctx) {
    auto ev = MakeUpdateQueryRequest(partitionId, ctx);
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
}

THolder<NKqp::TEvKqp::TEvQueryRequest> TTableHelper::MakeUpdateQueryRequest(ui32 partitionId, const NActors::TActorContext& ctx) {
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
            .Utf8(TopicName)
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
            .Uint32(partitionId)
            .Build();

    NYdb::TParams params = paramsBuilder.Build();

    ev->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params)));

    return ev;
}

#undef LOG_PREFIX
#undef TRACE
#undef DEBUG
#undef INFO
#undef ERROR


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
    return new NPartitionChooser::TPartitionChooserActor<NTabletPipe::TPipeHelper>(parentId, config, chooser, fullConverter, sourceId, preferedPartition);
}

} // namespace NKikimr::NPQ

std::unordered_map<ui64, TActorId> NKikimr::NTabletPipe::NTest::TPipeMock::Tablets;
