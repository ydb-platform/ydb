#pragma once

#include "metadata_initializers.h"
#include "source_id_encoding.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/persqueue/pq_database.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/services/metadata/service.h>


namespace NKikimr::NPQ::NPartitionChooser {

#if defined(LOG_PREFIX) || defined(TRACE) || defined(DEBUG) || defined(INFO) || defined(ERROR)
#error "Already defined LOG_PREFIX or TRACE or DEBUG or INFO or ERROR"
#endif


#define LOG_PREFIX "TTableHelper "
#define TRACE(message) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define DEBUG(message) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define INFO(message)  LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define ERROR(message) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);


class TTableHelper {
public:
    TTableHelper(const TString& topicName, const TString& topicHashName)
        : TopicName(topicName)
        , TopicHashName(topicHashName) {
    };

    std::optional<ui32> PartitionId() const {
        return PartitionId_;
    }

    std::optional<ui64> SeqNo() const {
        return SeqNo_;
    }

    [[nodiscard]] bool Initialize(const TActorContext& ctx, const TString& sourceId) {
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
        UpdateAccessTimeQuery = GetUpdateAccessTimeQueryFromPath(pqConfig.GetSourceIdTablePath(), TableGeneration);

        DEBUG("SelectQuery: " << SelectQuery);
        DEBUG("UpdateQuery: " << UpdateQuery);
        DEBUG("UpdateAccessTimeQuery: " << UpdateAccessTimeQuery);

        return true;
    }

    TString GetDatabaseName(const TActorContext& ctx) {
        const auto& pqConfig = AppData(ctx)->PQConfig;
        switch (TableGeneration) {
            case ESourceIdTableGeneration::SrcIdMeta2:
                return NKikimr::NPQ::GetDatabaseFromConfig(pqConfig);
            case ESourceIdTableGeneration::PartitionMapping:
                return AppData(ctx)->TenantName;
        }
    }

    void SendInitTableRequest(const TActorContext& ctx) {
        ctx.Send(
            NMetadata::NProvider::MakeServiceId(ctx.SelfID.NodeId()),
            new NMetadata::NProvider::TEvPrepareManager(NGRpcProxy::V1::TSrcIdMetaInitManager::GetInstant())
        );
    }

    void SendCreateSessionRequest(const TActorContext& ctx) {
        auto ev = MakeCreateSessionRequest(ctx);
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    }

    THolder<NKqp::TEvKqp::TEvCreateSessionRequest> MakeCreateSessionRequest(const TActorContext& ctx) {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
        ev->Record.MutableRequest()->SetDatabase(GetDatabaseName(ctx));
        return ev;
    }

    bool Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& /*ctx*/)  {
        const auto& record = ev->Get()->Record;

        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            return false;
        }

        KqpSessionId = record.GetResponse().GetSessionId();
        Y_ABORT_UNLESS(!KqpSessionId.empty());

        return true;
    }

    void CloseKqpSession(const TActorContext& ctx) {
        if (KqpSessionId) {
            auto ev = MakeCloseSessionRequest();
            ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());

            KqpSessionId = "";
        }
    }

    THolder<NKqp::TEvKqp::TEvCloseSessionRequest> MakeCloseSessionRequest() {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
        return ev;
    }

    void SendSelectRequest(const TActorContext& ctx) {
        auto ev = MakeSelectQueryRequest(ctx);
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    }

    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeSelectQueryRequest(const NActors::TActorContext& ctx) {
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

    bool HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& /*ctx*/) {
        auto& record = ev->Get()->Record.GetRef();

        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            return false;
        }

        auto& t = record.GetResponse().GetResults(0).GetValue().GetStruct(0);

        TxId = record.GetResponse().GetTxMeta().id();
        Y_ABORT_UNLESS(!TxId.empty());

        if (t.ListSize() != 0) {
            auto& list = t.GetList(0);
            auto& tt = list.GetStruct(0);
            if (tt.HasOptional() && tt.GetOptional().HasUint32()) { //already got partition
                auto accessTime = list.GetStruct(2).GetOptional().GetUint64();
                if (accessTime > AccessTime) { // AccessTime
                    PartitionId_ = tt.GetOptional().GetUint32();
                    CreateTime = list.GetStruct(1).GetOptional().GetUint64();
                    AccessTime = accessTime;
                    SeqNo_ = list.GetStruct(3).GetOptional().GetUint64();
                }
            }
        }

        if (CreateTime == 0) {
            CreateTime = TInstant::Now().MilliSeconds();
        }

        return true;
    }

    void SendUpdateRequest(ui32 partitionId, std::optional<ui64> seqNo, const TActorContext& ctx) {
        auto ev = MakeUpdateQueryRequest(partitionId, seqNo, ctx);
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    }

    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeUpdateQueryRequest(ui32 partitionId, std::optional<ui64> seqNo, const NActors::TActorContext& ctx) {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        ev->Record.MutableRequest()->SetQuery(TxId ? UpdateQuery : UpdateAccessTimeQuery);
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
            .AddParam("$SeqNo")
                .Uint64(seqNo.value_or(0))
                .Build()
            .AddParam("$Partition")
                .Uint32(partitionId)
                .Build();

        NYdb::TParams params = paramsBuilder.Build();

        ev->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params)));

        return ev;
    }

private:
    const TString TopicName;
    const TString TopicHashName;

    NPQ::NSourceIdEncoding::TEncodedSourceId EncodedSourceId;

    NPQ::ESourceIdTableGeneration TableGeneration;
    TString SelectQuery;
    TString UpdateQuery;
    TString UpdateAccessTimeQuery;

    TString KqpSessionId;
    TString TxId;

    ui64 CreateTime = 0;
    ui64 AccessTime = 0;

    std::optional<ui32> PartitionId_;
    std::optional<ui64> SeqNo_;
};

#undef LOG_PREFIX
#undef TRACE
#undef DEBUG
#undef INFO
#undef ERROR

} // namespace NKikimr::NPQ::NPartitionChooser
