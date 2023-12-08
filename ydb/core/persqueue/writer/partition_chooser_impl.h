#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <util/random/random.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/pq_database.h>
#include <ydb/core/persqueue/utils.h>
#include <ydb/core/persqueue/writer/metadata_initializers.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include "partition_chooser.h"

namespace NKikimr::NPQ {
namespace NPartitionChooser {

#define DEBUG(message) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, message);    

using namespace NActors;
using namespace NSourceIdEncoding;
using namespace Ydb::PersQueue::ErrorCode;

// For testing purposes
struct TAsIsSharder {
    ui32 operator()(const TString& sourceId, ui32 totalShards) const;
};

struct TMd5Sharder {
    ui32 operator()(const TString& sourceId, ui32 totalShards) const;
};

// For testing purposes
struct TAsIsConverter {
    TString operator()(const TString& sourceId) const;
};

struct TMd5Converter {
    TString operator()(const TString& sourceId) const;
};

// Chooses the partition to produce messages using the boundaries of the partition for the SourceID distribution.
// It used for split/merge distribution and guarantee stable distribution for changing partition set.
template<class THasher = TMd5Converter>
class TBoundaryChooser {
public:
    struct TPartitionInfo {
        ui32 PartitionId;
        ui64 TabletId;
        std::optional<TString> ToBound;
    };

    TBoundaryChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config);
    TBoundaryChooser(TBoundaryChooser&&) = default;

    const TPartitionInfo* GetPartition(const TString& sourceId) const;
    const TPartitionInfo* GetPartition(ui32 partitionId) const;

private:
    const TString TopicName;
    std::vector<TPartitionInfo> Partitions;
    THasher Hasher;
};

// It is old alghoritm of choosing partition by SourceId
template<class THasher = TMd5Sharder>
class THashChooser {
public:
    struct TPartitionInfo {
        ui32 PartitionId;
        ui64 TabletId;
    };

    THashChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config);

    const TPartitionInfo* GetPartition(const TString& sourceId) const;
    const TPartitionInfo* GetPartition(ui32 partitionId) const;

private:
    std::vector<TPartitionInfo> Partitions;
    THasher Hasher;
};


template<typename TChooser>
class TPartitionChooserActor: public TActorBootstrapped<TPartitionChooserActor<TChooser>> {
    using TThis = TPartitionChooserActor<TChooser>;
    using TThisActor = TActor<TThis>;

    friend class TActorBootstrapped<TThis>;
public:
    using TPartitionInfo = typename TChooser::TPartitionInfo;

    TPartitionChooserActor(TActorId parentId,
                           const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                           NPersQueue::TTopicConverterPtr& fullConverter,
                           const TString& sourceId,
                           std::optional<ui32> preferedPartition);

    void Bootstrap(const TActorContext& ctx);

private:
    void HandleInit(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx);
    void HandleInit(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx);

    STATEFN(StateInit) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NMetadata::NProvider::TEvManagerPrepared, HandleInit);
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, HandleInit);
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }

private:
    void HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
    void HandleSelect(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const TActorContext& ctx);
    void HandleSelect(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx);

    STATEFN(StateSelect) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleSelect);
            HFunc(TEvPersQueue::TEvGetPartitionIdForWriteResponse, HandleSelect);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandleSelect);
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }

private:
    void HandleIdle(TEvPartitionChooser::TEvRefreshRequest::TPtr& ev, const TActorContext& ctx);

    STATEFN(StateIdle)  {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPartitionChooser::TEvRefreshRequest, HandleIdle);
            SFunc(TEvents::TEvPoison, Stop);
        }
    }

private:
    void HandleUpdate(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);

    STATEFN(StateUpdate) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleUpdate);
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }

private:
    void HandleDestroy(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleDestroy(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);

    STATEFN(StateDestroy) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, HandleDestroy);
            HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleDestroy);
        }
    }

private:
    void ScheduleStop();
    void Stop(const TActorContext& ctx);

    void ChoosePartition(const TActorContext& ctx);
    void OnPartitionChosen(const TActorContext& ctx);
    std::pair<bool, const TPartitionInfo*> ChoosePartitionSync(const TActorContext& ctx) const;

    TString GetDatabaseName(const NActors::TActorContext& ctx);

    void InitTable(const NActors::TActorContext& ctx);

    void StartKqpSession(const NActors::TActorContext& ctx);
    void CloseKqpSession(const TActorContext& ctx);
    void SendUpdateRequests(const TActorContext& ctx);
    void SendSelectRequest(const NActors::TActorContext& ctx);

    void RequestPQRB(const NActors::TActorContext& ctx);

    THolder<NKqp::TEvKqp::TEvCreateSessionRequest> MakeCreateSessionRequest(const NActors::TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvCloseSessionRequest> MakeCloseSessionRequest();
    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeSelectQueryRequest(const NActors::TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeUpdateQueryRequest(const NActors::TActorContext& ctx);

    void ReplyResult(const NActors::TActorContext& ctx);
    void ReplyError(ErrorCode code, TString&& errorMessage, const NActors::TActorContext& ctx);

private:
    const TActorId Parent;
    const NPersQueue::TTopicConverterPtr FullConverter;
    const TString SourceId;
    const std::optional<ui32> PreferedPartition;
    const TChooser Chooser;
    const bool SplitMergeEnabled_;

    std::optional<ui32> PartitionId;
    const TPartitionInfo* Partition;
    bool PartitionPersisted = false;
    ui64 CreateTime = 0;
    ui64 AccessTime = 0;

    bool NeedUpdateTable = false;

    NPQ::NSourceIdEncoding::TEncodedSourceId EncodedSourceId;
    TString KqpSessionId;
    TString TxId;

    NPQ::ESourceIdTableGeneration TableGeneration;
    TString SelectQuery;
    TString UpdateQuery;

    size_t UpdatesInflight = 0;
    size_t SelectInflight = 0;

    ui64 BalancerTabletId;
    TActorId PipeToBalancer;
};


//
// TBoundaryChooser
//

template<class THasher>
TBoundaryChooser<THasher>::TBoundaryChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config)
    : TopicName(config.GetPQTabletConfig().GetTopicName()) {
    for(const auto& p : config.GetPartitions()) {
        if (NKikimrPQ::ETopicPartitionStatus::Active == p.GetStatus()) {
            auto toBound = p.HasKeyRange() && p.GetKeyRange().HasToBound() ?
                            std::optional<TString>(p.GetKeyRange().GetToBound()) : std::nullopt;
            Partitions.emplace_back(TPartitionInfo{p.GetPartitionId(),
                                    p.GetTabletId(),
                                    toBound});
        }
    }

    std::sort(Partitions.begin(), Partitions.end(),
        [](const TPartitionInfo& a, const TPartitionInfo& b) { return a.ToBound && a.ToBound < b.ToBound; });
}

template<class THasher>
const typename TBoundaryChooser<THasher>::TPartitionInfo* TBoundaryChooser<THasher>::GetPartition(const TString& sourceId) const {
    const auto keyHash = Hasher(sourceId);
    auto result = std::upper_bound(Partitions.begin(), Partitions.end(), keyHash, 
                    [](const TString& value, const TPartitionInfo& partition) { return !partition.ToBound || value < partition.ToBound; });
    Y_ABORT_UNLESS(result != Partitions.end(), "Partition not found. Maybe wrong partitions bounds. Topic '%s'", TopicName.c_str());
    return result;        
}

template<class THasher>
const typename TBoundaryChooser<THasher>::TPartitionInfo* TBoundaryChooser<THasher>::GetPartition(ui32 partitionId) const {
    auto it = std::find_if(Partitions.begin(), Partitions.end(),
        [=](const TPartitionInfo& v) { return v.PartitionId == partitionId; });
    return it == Partitions.end() ? nullptr : it;
}



//
// THashChooser
//
template<class THasher>
THashChooser<THasher>::THashChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config) {
    for(const auto& p : config.GetPartitions()) {
        if (NKikimrPQ::ETopicPartitionStatus::Active == p.GetStatus()) {
            Partitions.emplace_back(TPartitionInfo{p.GetPartitionId(),
                                    p.GetTabletId()});
        }
    }

    std::sort(Partitions.begin(), Partitions.end(), 
        [](const TPartitionInfo& a, const TPartitionInfo& b) { return a.PartitionId < b.PartitionId; });
}

template<class THasher>
const typename THashChooser<THasher>::TPartitionInfo* THashChooser<THasher>::GetPartition(const TString& sourceId) const {
    return &Partitions[Hasher(sourceId, Partitions.size())];
}

template<class THasher>
const typename THashChooser<THasher>::TPartitionInfo* THashChooser<THasher>::GetPartition(ui32 partitionId) const {
    auto it = std::lower_bound(Partitions.begin(), Partitions.end(), partitionId, 
                    [](const TPartitionInfo& partition, const ui32 value) { return value > partition.PartitionId; });
    if (it == Partitions.end()) {
        return nullptr;
    }
    return it->PartitionId == partitionId ? it : nullptr;
}


//
// TPartitionChooserActor
//

template<typename TChooser>
TPartitionChooserActor<TChooser>::TPartitionChooserActor(TActorId parent,
                                                         const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                                                         NPersQueue::TTopicConverterPtr& fullConverter,
                                                         const TString& sourceId,
                                                         std::optional<ui32> preferedPartition)
    : Parent(parent)
    , FullConverter(fullConverter)
    , SourceId(sourceId)
    , PreferedPartition(preferedPartition)
    , Chooser(config)
    , SplitMergeEnabled_(SplitMergeEnabled(config.GetPQTabletConfig()))
    , Partition(nullptr)
    , BalancerTabletId(config.GetBalancerTabletID()) {
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::Bootstrap(const TActorContext& ctx) {
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

template<typename TChooser>
void TPartitionChooserActor<TChooser>::Stop(const TActorContext& ctx) {
    CloseKqpSession(ctx);
    if (PipeToBalancer) {
        NTabletPipe::CloseClient(ctx, PipeToBalancer);
    }
    IActor::Die(ctx);
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::ScheduleStop() {
    TThisActor::Become(&TThis::StateDestroy);
}

template<typename TChooser>
TString TPartitionChooserActor<TChooser>::GetDatabaseName(const NActors::TActorContext& ctx) {
    const auto& pqConfig = AppData(ctx)->PQConfig;
    switch (TableGeneration) {
        case ESourceIdTableGeneration::SrcIdMeta2:
            return NKikimr::NPQ::GetDatabaseFromConfig(pqConfig);
        case ESourceIdTableGeneration::PartitionMapping:
            return AppData(ctx)->TenantName;
    }
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::InitTable(const NActors::TActorContext& ctx) {
    ctx.Send(
            NMetadata::NProvider::MakeServiceId(ctx.SelfID.NodeId()),
            new NMetadata::NProvider::TEvPrepareManager(NGRpcProxy::V1::TSrcIdMetaInitManager::GetInstant())
    );
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::StartKqpSession(const NActors::TActorContext& ctx) {
    auto ev = MakeCreateSessionRequest(ctx);
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::CloseKqpSession(const TActorContext& ctx) {
    if (KqpSessionId) {
        auto ev = MakeCloseSessionRequest();
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());

        KqpSessionId = "";
    }
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::SendUpdateRequests(const TActorContext& ctx) {
    TThisActor::Become(&TThis::StateUpdate);

    auto ev = MakeUpdateQueryRequest(ctx);
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());

    UpdatesInflight++;
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::SendSelectRequest(const NActors::TActorContext& ctx) {
    TThisActor::Become(&TThis::StateSelect);

    auto ev = MakeSelectQueryRequest(ctx);
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());

    SelectInflight++;
}

template<typename TChooser>
THolder<NKqp::TEvKqp::TEvCreateSessionRequest> TPartitionChooserActor<TChooser>::MakeCreateSessionRequest(const NActors::TActorContext& ctx) {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
    ev->Record.MutableRequest()->SetDatabase(GetDatabaseName(ctx));
    return ev;
}

template<typename TChooser>
THolder<NKqp::TEvKqp::TEvCloseSessionRequest> TPartitionChooserActor<TChooser>::MakeCloseSessionRequest() {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
    ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
    return ev;
}

template<typename TChooser>
THolder<NKqp::TEvKqp::TEvQueryRequest> TPartitionChooserActor<TChooser>::MakeSelectQueryRequest(const NActors::TActorContext& ctx) {
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

template<typename TChooser>
THolder<NKqp::TEvKqp::TEvQueryRequest> TPartitionChooserActor<TChooser>::MakeUpdateQueryRequest(const NActors::TActorContext& ctx) {
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

template<typename TChooser>
void TPartitionChooserActor<TChooser>::RequestPQRB(const NActors::TActorContext& ctx) {
    Y_ABORT_UNLESS(!PipeToBalancer);
    Y_ABORT_UNLESS(BalancerTabletId);

    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = 6,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(100),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };
    PipeToBalancer = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, BalancerTabletId, clientConfig));

    TThisActor::Become(&TThis::StateSelect);
    NTabletPipe::SendData(ctx, PipeToBalancer, new TEvPersQueue::TEvGetPartitionIdForWrite());
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::ReplyResult(const NActors::TActorContext& ctx) {
    ctx.Send(Parent, new TEvPartitionChooser::TEvChooseResult(Partition->PartitionId, Partition->TabletId));
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::ReplyError(ErrorCode code, TString&& errorMessage, const NActors::TActorContext& ctx) {
    ctx.Send(Parent, new TEvPartitionChooser::TEvChooseError(code, std::move(errorMessage)));

    Stop(ctx);
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::HandleInit(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx) {
    StartKqpSession(ctx);
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::HandleInit(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ53 : " <<  record, ctx);
        return;
    }

    KqpSessionId = record.GetResponse().GetSessionId();
    Y_ABORT_UNLESS(!KqpSessionId.empty());

    SendSelectRequest(ctx);
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::HandleDestroy(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    KqpSessionId = record.GetResponse().GetSessionId();

    Stop(ctx);
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
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
                Partition = Chooser.GetPartition(PartitionId.value());
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

template<typename TChooser>
void TPartitionChooserActor<TChooser>::HandleSelect(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const TActorContext& ctx) {
    PartitionId = ev->Get()->Record.GetPartitionId();
    DEBUG("Received partition " << PartitionId << " from PQRB for SourceId=" << SourceId);
    Partition = Chooser.GetPartition(PartitionId.value());

    OnPartitionChosen(ctx);
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::HandleSelect(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);

    ReplyError(ErrorCode::INITIALIZING, "Pipe destroyed", ctx);
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::HandleUpdate(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
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

template<typename TChooser>
void TPartitionChooserActor<TChooser>::HandleDestroy(NKqp::TEvKqp::TEvQueryResponse::TPtr&, const TActorContext& ctx) {
    Stop(ctx);
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::HandleIdle(TEvPartitionChooser::TEvRefreshRequest::TPtr&, const TActorContext& ctx) {
    if (PartitionPersisted) {
         // we do not update AccessTime for Split/Merge partitions because don't use table.
        SendUpdateRequests(ctx);
    }
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::ChoosePartition(const TActorContext& ctx) {
    auto [roundRobin, p] = ChoosePartitionSync(ctx);
    if (roundRobin) {
        RequestPQRB(ctx);
    } else {
        Partition = p;
        OnPartitionChosen(ctx);
    }
}

template<typename TChooser>
void TPartitionChooserActor<TChooser>::OnPartitionChosen(const TActorContext& ctx) {
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
        if (Partition != Chooser.GetPartition(SourceId)) {
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

template<typename TChooser>
std::pair<bool, const typename TPartitionChooserActor<TChooser>::TPartitionInfo*> TPartitionChooserActor<TChooser>::ChoosePartitionSync(const TActorContext& ctx) const {
    const auto& pqConfig = AppData(ctx)->PQConfig;
    if (SourceId && SplitMergeEnabled_) {
        return {false, Chooser.GetPartition(SourceId)};
    } else if (PreferedPartition) {
        return {false, Chooser.GetPartition(PreferedPartition.value())};
    } else if (pqConfig.GetTopicsAreFirstClassCitizen() && SourceId) {
        return {false, Chooser.GetPartition(SourceId)};
    } else {
        return {true, nullptr};
    }
}

} // namespace NPartitionChooser
} // namespace NKikimr::NPQ
