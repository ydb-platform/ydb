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
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/protos/pqconfig.pb.h>


namespace NKikimr::NPQ::NPartitionChooser {

class TTableHelper {
public:
    TTableHelper(const NKikimr::NPQ::NNameResolver::TTopicNamesPtr& fullConverter,
                  const NKikimrPQ::TPQTabletConfig::TTopicId* topicId = nullptr)
        : TopicName(fullConverter->GetClientsideName())
        , TopicHashName(fullConverter->GetTopicForSrcIdHash())
        , TopicId(topicId ? topicId->GetId() : 0)
        , IdTxStep(topicId ? topicId->GetTxStep() : 0)
        , OwnerId(topicId ? topicId->GetOwnerId() : 0) {
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

        const bool mappingByIdEnabled = TopicId != 0 && AppData(ctx)->FeatureFlags.GetEnableTopicSourceIdMappingById();

        // The composite key must be unique across all schemeshards.
        const TString topicUniqueId = ToString(OwnerId) + "+" + ToString(TopicId);
        TopicKey = mappingByIdEnabled ? topicUniqueId : TopicName;
        // The fallback to the legacy name-based key is required only when the Id was
        // back-filled by an alter on a pre-existing topic (IdTxStep != 0, the sentinel 0
        // means the Id was set at create) and only during the transition window.
        LegacyKeySelectEnabled = mappingByIdEnabled && IdTxStep != 0
            && TAppData::TimeProvider->Now() - TInstant::MilliSeconds(IdTxStep) < NPQ::SourceIdMappingTtl;

        try {
            EncodedSourceId = NSourceIdEncoding::EncodeSrcId(
                        mappingByIdEnabled ? TopicKey : TopicHashName, sourceId, TableGeneration
                );
            if (LegacyKeySelectEnabled) {
                FallbackEncodedSourceId = NSourceIdEncoding::EncodeSrcId(
                            TopicHashName, sourceId, TableGeneration
                    );
            }
        } catch (yexception& e) {
            return false;
        }

        SelectQuery = GetSelectSourceIdQueryFromPath(pqConfig.GetSourceIdTablePath(), TableGeneration);
        UpdateQuery = GetUpdateSourceIdQueryFromPath(pqConfig.GetSourceIdTablePath(), TableGeneration);
        UpdateAccessTimeQuery = GetUpdateAccessTimeQueryFromPath(pqConfig.GetSourceIdTablePath(), TableGeneration);

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
        SelectPhase = ESelectPhase::Primary;
        return MakeSelectQueryRequestImpl(TopicKey, EncodedSourceId, ctx);
    }

    // The id-keyed row was not found: within the transition window retry the select with
    // the legacy name-based key, continuing the same open transaction.
    bool NeedLegacyKeySelect() const {
        return SelectPhase == ESelectPhase::Primary && LegacyKeySelectEnabled && !PartitionId_;
    }

    void SendLegacyKeySelectRequest(const TActorContext& ctx) {
        auto ev = MakeLegacyKeySelectQueryRequest(ctx);
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    }

    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeLegacyKeySelectQueryRequest(const NActors::TActorContext& ctx) {
        SelectPhase = ESelectPhase::LegacyKey;
        return MakeSelectQueryRequestImpl(TopicName, FallbackEncodedSourceId, ctx);
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

        if (NeedLegacyKeySelect()) {
            // The final CreateTime is defined after the fallback select.
            return true;
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

        // Writes always target the primary key: the topic Id when present, the legacy
        // name otherwise. A row read via the name fallback is thus migrated to the id key.
        SetHashToTParamsBuilder(paramsBuilder, EncodedSourceId);

        paramsBuilder
            .AddParam("$Topic")
                .Utf8(TopicKey)
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

        NYdb::TParams params = paramsBuilder.Build();

        ev->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params)));

        return ev;
    }

private:
    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeSelectQueryRequestImpl(const TString& topicKey,
                                                                      const NPQ::NSourceIdEncoding::TEncodedSourceId& encodedSourceId,
                                                                      const NActors::TActorContext& ctx) {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        ev->Record.MutableRequest()->SetQuery(SelectQuery);

        ev->Record.MutableRequest()->SetDatabase(GetDatabaseName(ctx));
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
        ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(false);
        if (TxId) {
            // The name-fallback select continues the transaction opened by the primary
            // select so the whole read-modify-write stays atomic and serializable.
            ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
        } else {
            // Begin a new serializable tx; it is committed later by the update query.
            ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        }
        ev->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);
        // keep compiled query in cache.
        ev->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);

        NYdb::TParamsBuilder paramsBuilder = NYdb::TParamsBuilder();

        SetHashToTParamsBuilder(paramsBuilder, encodedSourceId);

        paramsBuilder
            .AddParam("$Topic")
                .Utf8(topicKey)
                .Build()
            .AddParam("$SourceId")
                .Utf8(encodedSourceId.EscapedSourceId)
                .Build();

        NYdb::TParams params = paramsBuilder.Build();

        ev->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params)));

        return ev;
    }

    enum class ESelectPhase {
        Primary,
        LegacyKey,
    };

    const TString TopicName;
    const TString TopicHashName;
    const ui64 TopicId;
    const ui64 IdTxStep;
    const ui64 OwnerId;

    // The key the mapping rows are written with: TopicId when set, TopicName otherwise.
    TString TopicKey;
    bool LegacyKeySelectEnabled = false;
    ESelectPhase SelectPhase = ESelectPhase::Primary;

    NPQ::NSourceIdEncoding::TEncodedSourceId EncodedSourceId;
    NPQ::NSourceIdEncoding::TEncodedSourceId FallbackEncodedSourceId;

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
