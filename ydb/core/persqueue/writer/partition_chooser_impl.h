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

#include "common.h"
#include "pipe_utils.h"
#include "partition_chooser.h"


namespace NKikimr::NPQ {
namespace NPartitionChooser {

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
class TBoundaryChooser: public IPartitionChooser {
public:
    struct TPartitionInfo: public IPartitionChooser::TPartitionInfo {
        TPartitionInfo(ui32 partitionId, ui64 tabletId, std::optional<TString> toBound)
            : IPartitionChooser::TPartitionInfo(partitionId, tabletId)
            , ToBound(toBound) {}

        std::optional<TString> ToBound;
    };

    TBoundaryChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config);
    TBoundaryChooser(TBoundaryChooser&&) = default;

    const TPartitionInfo* GetPartition(const TString& sourceId) const override;
    const TPartitionInfo* GetPartition(ui32 partitionId) const override;

private:
    const TString TopicName;
    std::vector<TPartitionInfo> Partitions;
    THasher Hasher;
};

// It is old alghoritm of choosing partition by SourceId
template<typename THasher = TMd5Sharder>
class THashChooser: public IPartitionChooser {
public:
    THashChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config);

    const TPartitionInfo* GetPartition(const TString& sourceId) const override;
    const TPartitionInfo* GetPartition(ui32 partitionId) const override;

private:
    std::vector<TPartitionInfo> Partitions;
    THasher Hasher;
};



class TTableHelper {
public:
    bool Initialize(const TActorContext& ctx, const TString& topicName, const TString& topicHashName, const TString& sourceId);
    TString GetDatabaseName(const TActorContext& ctx);

    void SendInitTableRequest(const TActorContext& ctx);

    void SendCreateSessionRequest(const TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvCreateSessionRequest> MakeCreateSessionRequest(const TActorContext& ctx);
    bool Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);

    void CloseKqpSession(const TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvCloseSessionRequest> MakeCloseSessionRequest();

    void SendSelectRequest(const TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeSelectQueryRequest(const NActors::TActorContext& ctx);
    std::pair<bool, std::optional<ui32>> HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);

    void SendUpdateRequest(ui32 partitionId, const TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeUpdateQueryRequest(ui32 partitionId, const NActors::TActorContext& ctx);

private:
    TString TopicName;
    NPQ::NSourceIdEncoding::TEncodedSourceId EncodedSourceId;
   
    NPQ::ESourceIdTableGeneration TableGeneration;
    TString SelectQuery;
    TString UpdateQuery;

    TString KqpSessionId;
    TString TxId;

    size_t UpdatesInflight = 0;
    size_t SelectInflight = 0;

    ui64 CreateTime = 0;
    ui64 AccessTime = 0;
};


template<typename TPipe>
class TPartitionChooserActor: public TActorBootstrapped<TPartitionChooserActor<TPipe>> {
    using TThis = TPartitionChooserActor<TPipe>;
    using TThisActor = TActor<TThis>;

    friend class TActorBootstrapped<TThis>;
public:
    using TPartitionInfo = typename IPartitionChooser::TPartitionInfo;

    TPartitionChooserActor(TActorId parentId,
                           const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                           std::shared_ptr<IPartitionChooser>& chooser,
                           NPersQueue::TTopicConverterPtr& fullConverter,
                           const TString& sourceId,
                           std::optional<ui32> preferedPartition);

    void Bootstrap(const TActorContext& ctx);

    TActorIdentity SelfId() const {
        return TActor<TPartitionChooserActor<TPipe>>::SelfId();
    }

private:
    void InitTable(const NActors::TActorContext& ctx);
    void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx);

    STATEFN(StateInitTable) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NMetadata::NProvider::TEvManagerPrepared, Handle);
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }

private:
    void StartKqpSession(const NActors::TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx);

    STATEFN(StateCreateKqpSession) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }

private:
    void SendSelectRequest(const NActors::TActorContext& ctx);
    void HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);

    STATEFN(StateSelect) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleSelect);
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }

private:
    void RequestPQRB(const NActors::TActorContext& ctx);
    void Handle(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx);

    STATEFN(StatePQRB) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvGetPartitionIdForWriteResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }

private:
    void GetOwnership();
    void HandleOwnership(TEvPersQueue::TEvResponse::TPtr& ev, const NActors::TActorContext& ctx);

    STATEFN(StateOwnership) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvResponse, HandleOwnership);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle); // TODO?
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle); // TODO?
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }


private:
    void HandleIdle(TEvPartitionChooser::TEvRefreshRequest::TPtr& ev, const TActorContext& ctx);

    STATEFN(StateIdle)  {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPartitionChooser::TEvRefreshRequest, HandleIdle);
            SFunc(TEvents::TEvPoison, Stop);
        }
    }

private:
    void SendUpdateRequests(const TActorContext& ctx);
    void HandleUpdate(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);

    STATEFN(StateUpdate) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleUpdate);
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }

private:
    void HandleDestroy(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleDestroy(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);

    STATEFN(StateDestroy) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
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

    void ReplyResult(const NActors::TActorContext& ctx);
    void ReplyError(ErrorCode code, TString&& errorMessage, const NActors::TActorContext& ctx);

private:
    const TActorId Parent;
    const NPersQueue::TTopicConverterPtr FullConverter;
    const TString SourceId;
    const std::optional<ui32> PreferedPartition;
    const std::shared_ptr<IPartitionChooser> Chooser;
    const bool SplitMergeEnabled_;

    std::optional<ui32> PartitionId;
    const TPartitionInfo* Partition;
    bool PartitionPersisted = false;


    bool NeedUpdateTable = false;
    TTableHelper TableHelper;

    ui64 BalancerTabletId;
    TActorId PipeToBalancer;

    TActorId PartitionPipe;
    TString OwnerCookie;
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

#if defined(LOG_PREFIX) || defined(TRACE) || defined(DEBUG) || defined(INFO) || defined(ERROR)
#error "Already defined LOG_PREFIX or TRACE or DEBUG or INFO or ERROR"
#endif


#define LOG_PREFIX "TPartitionChooser " << SelfId()        \
                    << " (SourceId=" << SourceId                     \
                    << ", PreferedPartition=" << PreferedPartition   \
                    << ", SplitMergeEnabled=" << SplitMergeEnabled_  \
                    << ") "
#define TRACE(message) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define DEBUG(message) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define INFO(message)  LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define ERROR(message) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);

template<typename TPipe>
TPartitionChooserActor<TPipe>::TPartitionChooserActor(TActorId parent,
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

template<typename TPipe>
void TPartitionChooserActor<TPipe>::Bootstrap(const TActorContext& ctx) {
    const auto& pqConfig = AppData(ctx)->PQConfig;

    NeedUpdateTable = (!pqConfig.GetTopicsAreFirstClassCitizen() || pqConfig.GetUseSrcIdMetaMappingInFirstClass()) && SourceId;

    if (!SourceId) {
        return ChoosePartition(ctx);
    }

    TableHelper.Initialize(ctx, FullConverter->GetClientsideName(), FullConverter->GetTopicForSrcIdHash(), SourceId);

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


template<typename TPipe>
void TPartitionChooserActor<TPipe>::Stop(const TActorContext& ctx) {
    TableHelper.CloseKqpSession(ctx);
    if (PipeToBalancer) {
        NTabletPipe::CloseClient(ctx, PipeToBalancer);
    }
    if (PartitionPipe) {
        NTabletPipe::CloseClient(ctx, PartitionPipe);
    }
    IActor::Die(ctx);
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::ScheduleStop() {
    TThisActor::Become(&TThis::StateDestroy);
}

//
// StateInitTable
//

template<typename TPipe>
void TPartitionChooserActor<TPipe>::InitTable(const NActors::TActorContext& ctx) {
    DEBUG("InitTable")
    TThisActor::Become(&TThis::StateInitTable);
    TableHelper.SendInitTableRequest(ctx);
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx) {
    StartKqpSession(ctx);
}


//
// StartKqpSession
// 

template<typename TPipe>
void TPartitionChooserActor<TPipe>::StartKqpSession(const NActors::TActorContext& ctx) {
    DEBUG("StartKqpSession")
    TThisActor::Become(&TThis::StateCreateKqpSession);
    TableHelper.SendCreateSessionRequest(ctx);
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    if (!TableHelper.Handle(ev, ctx)) {
        return ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ53 : " <<  ev->Get()->Record.DebugString(), ctx);
    }

    SendSelectRequest(ctx);
}


//
// StatePQRB
//

template<typename TPipe>
void TPartitionChooserActor<TPipe>::RequestPQRB(const NActors::TActorContext& ctx) {
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
        PipeToBalancer = ctx.RegisterWithSameMailbox(TPipe::CreateClient(ctx.SelfID, BalancerTabletId, clientConfig));
    }

    NTabletPipe::SendData(ctx, PipeToBalancer, new TEvPersQueue::TEvGetPartitionIdForWrite());
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::Handle(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const TActorContext& ctx) {
    PartitionId = ev->Get()->Record.GetPartitionId();
    DEBUG("Received partition " << PartitionId << " from PQRB for SourceId=" << SourceId);
    Partition = Chooser->GetPartition(PartitionId.value());

    OnPartitionChosen(ctx);
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);

    if (ev->Get()->Status != NKikimrProto::EReplyStatus::OK) {
        ReplyError(ErrorCode::INITIALIZING, "Pipe connection fail", ctx);
    }
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);

    ReplyError(ErrorCode::INITIALIZING, "Pipe destroyed", ctx);
}



//
// GetOwnership
//

inline THolder<TEvPersQueue::TEvRequest> MakeRequest(ui32 partitionId, TActorId pipe) {
    auto ev = MakeHolder<TEvPersQueue::TEvRequest>();

    ev->Record.MutablePartitionRequest()->SetPartition(partitionId);
    ActorIdToProto(pipe, ev->Record.MutablePartitionRequest()->MutablePipeClient());

    return ev;
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::GetOwnership() {
    TThisActor::Become(&TThis::StateOwnership);

    NTabletPipe::TClientConfig config;
    config.RetryPolicy = {
        .RetryLimitCount = 6,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(100),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };

    TRACE("GetOwnership Partition TabletId=" << Partition->TabletId);
    PartitionPipe = TThisActor::RegisterWithSameMailbox(TPipe::CreateClient(SelfId(), Partition->TabletId, config));

    auto ev = MakeRequest(Partition->PartitionId, PartitionPipe);

    auto& cmd = *ev->Record.MutablePartitionRequest()->MutableCmdGetOwnership();
    cmd.SetOwner(SourceId ? SourceId : CreateGuidAsString());
    cmd.SetForce(true);

    NTabletPipe::SendData(SelfId(), PartitionPipe, ev.Release());
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::HandleOwnership(TEvPersQueue::TEvResponse::TPtr& ev, const NActors::TActorContext& ctx) {
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



template<typename TPipe>
void TPartitionChooserActor<TPipe>::SendUpdateRequests(const TActorContext& ctx) {
    TThisActor::Become(&TThis::StateUpdate);
    TableHelper.SendUpdateRequest(Partition->PartitionId, ctx);
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::SendSelectRequest(const NActors::TActorContext& ctx) {
    TThisActor::Become(&TThis::StateSelect);
    TableHelper.SendSelectRequest(ctx);
}




template<typename TPipe>
void TPartitionChooserActor<TPipe>::ReplyResult(const NActors::TActorContext& ctx) {
    ctx.Send(Parent, new TEvPartitionChooser::TEvChooseResult(Partition->PartitionId, Partition->TabletId, OwnerCookie));
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::ReplyError(ErrorCode code, TString&& errorMessage, const NActors::TActorContext& ctx) {
    INFO("ReplyError: " << errorMessage);
    ctx.Send(Parent, new TEvPartitionChooser::TEvChooseError(code, std::move(errorMessage)));

    Stop(ctx);
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::HandleDestroy(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    TableHelper.Handle(ev, ctx);
    Stop(ctx);
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    auto [success, partition] = TableHelper.HandleSelect(ev, ctx);
    if (!success) {
        return ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ50 : " <<  ev->Get()->Record.DebugString(), ctx);
    }

    if (partition) {
        PartitionId = partition;
        Partition = Chooser->GetPartition(PartitionId.value());
    }

    if (!Partition) {
        ChoosePartition(ctx);
    } else {
        OnPartitionChosen(ctx);
    }
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::HandleUpdate(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record.GetRef();

    if (record.GetYdbStatus() == Ydb::StatusIds::ABORTED) {
        if (!PartitionPersisted) {
            TableHelper.CloseKqpSession(ctx);
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
        TableHelper.CloseKqpSession(ctx);
    }

    TThisActor::Become(&TThis::StateIdle);
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::HandleDestroy(NKqp::TEvKqp::TEvQueryResponse::TPtr&, const TActorContext& ctx) {
    Stop(ctx);
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::HandleIdle(TEvPartitionChooser::TEvRefreshRequest::TPtr&, const TActorContext& ctx) {
    if (PartitionPersisted) {
         // we do not update AccessTime for Split/Merge partitions because don't use table.
        SendUpdateRequests(ctx);
    }
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::ChoosePartition(const TActorContext& ctx) {
    auto [roundRobin, p] = ChoosePartitionSync(ctx);
    if (roundRobin) {
        RequestPQRB(ctx);
    } else {
        Partition = p;
        OnPartitionChosen(ctx);
    }
}

template<typename TPipe>
void TPartitionChooserActor<TPipe>::OnPartitionChosen(const TActorContext& ctx) {
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


template<typename TPipe>
std::pair<bool, const typename TPartitionChooserActor<TPipe>::TPartitionInfo*> TPartitionChooserActor<TPipe>::ChoosePartitionSync(const TActorContext& ctx) const {
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



#undef LOG_PREFIX
#undef TRACE
#undef DEBUG
#undef INFO
#undef ERROR

inline IActor* CreatePartitionChooserActorM(TActorId parentId,
                                    const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                                    NPersQueue::TTopicConverterPtr& fullConverter,
                                    const TString& sourceId,
                                    std::optional<ui32> preferedPartition,
                                    bool withoutHash) {
    auto chooser = CreatePartitionChooser(config, withoutHash);
    return new NPartitionChooser::TPartitionChooserActor<NTabletPipe::NTest::TPipeMock>(parentId, config, chooser, fullConverter, sourceId, preferedPartition);
}


} // namespace NKikimr::NPQ
