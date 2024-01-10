#pragma once

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>


namespace NKikimr::NPQ::NPartitionChooser {

class TTableHelper {
public:
    TTableHelper(const TString& topicName, const TString& topicHashName);

    std::optional<ui32> PartitionId() const;

    bool Initialize(const TActorContext& ctx, const TString& sourceId);
    TString GetDatabaseName(const TActorContext& ctx);

    void SendInitTableRequest(const TActorContext& ctx);

    void SendCreateSessionRequest(const TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvCreateSessionRequest> MakeCreateSessionRequest(const TActorContext& ctx);
    bool Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);

    void CloseKqpSession(const TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvCloseSessionRequest> MakeCloseSessionRequest();

    void SendSelectRequest(const TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeSelectQueryRequest(const NActors::TActorContext& ctx);
    bool HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);

    void SendUpdateRequest(ui32 partitionId, const TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeUpdateQueryRequest(ui32 partitionId, const NActors::TActorContext& ctx);

private:
    const TString TopicName;
    const TString TopicHashName;

    NPQ::NSourceIdEncoding::TEncodedSourceId EncodedSourceId;
   
    NPQ::ESourceIdTableGeneration TableGeneration;
    TString SelectQuery;
    TString UpdateQuery;

    TString KqpSessionId;
    TString TxId;

    ui64 CreateTime = 0;
    ui64 AccessTime = 0;

    std::optional<ui32> PartitionId_;
};


} // namespace NKikimr::NPQ::NPartitionChooser
