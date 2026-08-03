#pragma once

#include "metadata_initializers.h"
#include "source_id_encoding.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/persqueue/public/pq_database.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/services/metadata/service.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <library/cpp/time_provider/time_provider.h>
#include <ydb/library/actors/core/log.h>


namespace NKikimr::NPQ::NPartitionChooser {

class TTableHelper {
public:
    TTableHelper(const TString& topicName, const TString& topicHashName, ui64 pathId = 0)
        : TopicName(topicName)
        , TopicHashName(topicHashName)
        , PathId(pathId) {
    };

    std::optional<ui32> PartitionId() const {
        return PartitionId_;
    }

    std::optional<ui64> SeqNo() const {
        return SeqNo_;
    }

    [[nodiscard]] bool Initialize(const TActorContext& ctx, const TString& sourceId) {
        const auto& pqConfig = AppData(ctx)->PQConfig;
        const auto& featureFlags = AppData(ctx)->FeatureFlags;

        TableGeneration = pqConfig.GetTopicsAreFirstClassCitizen() ? ESourceIdTableGeneration::PartitionMapping
                                                                   : ESourceIdTableGeneration::SrcIdMeta2;
        try {
            EncodedSourceId = NSourceIdEncoding::EncodeSrcId(
                        TopicHashName, sourceId, TableGeneration
                );
        } catch (yexception& e) {
            return false;
        }

        // Primary Topic key value. When PathId is known, use the PathId-encoded value.
        // WithFallback keeps writing/reading the legacy plain-name row during migration for safe
        // rollback; it is disabled once TopicPartitionMappingByPathIdMigrationComplete is set.
        if (PathId != 0) {
            PrimaryTopic = EncodeTopicWithPathId(PathId, TopicName);
            WithFallback = !featureFlags.GetTopicPartitionMappingByPathIdMigrationComplete();
        } else {
            PrimaryTopic = TopicName;
            WithFallback = false;
        }

        SelectQuery = GetSelectSourceIdQueryFromPath(pqConfig.GetSourceIdTablePath(), TableGeneration, WithFallback);
        UpdateQuery = GetUpdateSourceIdQueryFromPath(pqConfig.GetSourceIdTablePath(), TableGeneration, WithFallback);
        UpdateAccessTimeQuery = GetUpdateAccessTimeQueryFromPath(pqConfig.GetSourceIdTablePath(), TableGeneration, WithFallback);

        YDB_LOG_DEBUG_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "TTableHelper",
            {"selectQuery", SelectQuery});
        YDB_LOG_DEBUG_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "TTableHelper",
            {"updateQuery", UpdateQuery});
        YDB_LOG_DEBUG_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "TTableHelper",
            {"updateAccessTimeQuery", UpdateAccessTimeQuery});

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
        AFL_ENSURE(!KqpSessionId.empty());

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
        ev->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);
        // keep compiled query in cache.
        ev->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);

        NYdb::TParamsBuilder paramsBuilder = NYdb::TParamsBuilder();

        SetHashToTParamsBuilder(paramsBuilder, EncodedSourceId);

        paramsBuilder
            .AddParam("$Topic")
                .Utf8(PrimaryTopic)
                .Build()
            .AddParam("$SourceId")
                .Utf8(EncodedSourceId.EscapedSourceId)
                .Build();

        if (WithFallback) {
            paramsBuilder
                .AddParam("$TopicName")
                    .Utf8(TopicName)
                    .Build();
        }

        NYdb::TParams params = paramsBuilder.Build();

        ev->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params)));

        return ev;
    }

    bool HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& /*ctx*/) {
        auto& record = ev->Get()->Record;

        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            return false;
        }

        NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));
        TxId = record.GetResponse().GetTxMeta().id();
        AFL_ENSURE(!TxId.empty());

        while(parser.TryNextRow()) {
            auto tt = parser.ColumnParser(0).GetOptionalUint32();

            if (tt.has_value()) { //already got partition
                auto accessTime = parser.ColumnParser(2).GetOptionalUint64().value_or(0);
                if (accessTime > AccessTime) { // AccessTime
                    PartitionId_ = *tt;
                    CreateTime = parser.ColumnParser(1).GetOptionalUint64().value_or(0);
                    AccessTime = accessTime;
                    SeqNo_ = parser.ColumnParser(3).GetOptionalUint64().value_or(0);
                }
            }
        }

        if (CreateTime == 0) {
            CreateTime = TAppData::TimeProvider->Now().MilliSeconds();
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
        ev->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);

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
                .Utf8(PrimaryTopic)
                .Build()
            .AddParam("$SourceId")
                .Utf8(EncodedSourceId.EscapedSourceId)
                .Build()
            .AddParam("$CreateTime")
                .Uint64(CreateTime)
                .Build()
            .AddParam("$AccessTime")
                .Uint64(TAppData::TimeProvider->Now().MilliSeconds())
                .Build()
            .AddParam("$SeqNo")
                .Uint64(seqNo.value_or(0))
                .Build()
            .AddParam("$Partition")
                .Uint32(partitionId)
                .Build();

        if (WithFallback) {
            paramsBuilder
                .AddParam("$TopicName")
                    .Utf8(TopicName)
                    .Build();
        }

        NYdb::TParams params = paramsBuilder.Build();

        ev->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params)));

        return ev;
    }

private:
    const TString TopicName;
    const TString TopicHashName;
    const ui64 PathId;

    // Primary Topic column value: PathId-encoded when PathId is known, otherwise the plain name.
    TString PrimaryTopic;
    // When true, queries also handle the legacy plain-name row (migration phase, safe rollback).
    bool WithFallback = false;

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

} // namespace NKikimr::NPQ::NPartitionChooser
