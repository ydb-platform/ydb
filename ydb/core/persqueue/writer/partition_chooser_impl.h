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

template<typename TPipeCreator>
class TPQRBHelper {
public:
    TPQRBHelper(ui64 balancerTabletId)
        : BalancerTabletId(balancerTabletId) {
            Cerr << ">>>>> balancerTabletId = " << balancerTabletId << Endl;
    }

    std::optional<ui32> PartitionId() const {
        return PartitionId_;
    }

    void SendRequest(const NActors::TActorContext& ctx) {
        Y_ABORT_UNLESS(BalancerTabletId);

        if (!Pipe) {
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.RetryPolicy = {
                .RetryLimitCount = 6,
                .MinRetryTime = TDuration::MilliSeconds(10),
                .MaxRetryTime = TDuration::MilliSeconds(100),
                .BackoffMultiplier = 2,
                .DoFirstRetryInstantly = true
            };
            Pipe = ctx.RegisterWithSameMailbox(TPipeCreator::CreateClient(ctx.SelfID, BalancerTabletId, clientConfig));
        }

        NTabletPipe::SendData(ctx, Pipe, new TEvPersQueue::TEvGetPartitionIdForWrite());
    }

    ui32 Handle(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const TActorContext& ctx) {
        Close(ctx);

        PartitionId_ = ev->Get()->Record.GetPartitionId();
        return PartitionId_.value();
    }

    void Close(const TActorContext& ctx) {
        if (Pipe) {
            NTabletPipe::CloseClient(ctx, Pipe);
            Pipe = TActorId();
        }
    }

private:
    const ui64 BalancerTabletId;

    TActorId Pipe;
    std::optional<ui32> PartitionId_;
};


template<typename TPipeCreator>
class TPartitionHelper {
public:
    void Open(ui64 tabletId, const TActorContext& ctx) {
        Close(ctx);

        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };
        Pipe = ctx.RegisterWithSameMailbox(TPipeCreator::CreateClient(ctx.SelfID, tabletId, clientConfig));
    }

    void SendGetOwnershipRequest(ui32 partitionId, const TString& sourceId, const TActorContext& ctx) {
        auto ev = MakeRequest(partitionId, Pipe);

        auto& cmd = *ev->Record.MutablePartitionRequest()->MutableCmdGetOwnership();
        cmd.SetOwner(sourceId ? sourceId : CreateGuidAsString());
        cmd.SetForce(true);

        NTabletPipe::SendData(ctx, Pipe, ev.Release());
    }

    void Close(const TActorContext& ctx) {
        if (Pipe) {
            NTabletPipe::CloseClient(ctx, Pipe);
            Pipe = TActorId();
        }
    }

    const TString& OwnerCookie() const {
        return OwnerCookie_;
    }

private:
    THolder<TEvPersQueue::TEvRequest> MakeRequest(ui32 partitionId, TActorId pipe) {
        auto ev = MakeHolder<TEvPersQueue::TEvRequest>();

        ev->Record.MutablePartitionRequest()->SetPartition(partitionId);
        ActorIdToProto(pipe, ev->Record.MutablePartitionRequest()->MutablePipeClient());

        return ev;
    }

private:
    TActorId Pipe;
    TString OwnerCookie_;
};

#if defined(LOG_PREFIX) || defined(TRACE) || defined(DEBUG) || defined(INFO) || defined(ERROR)
#error "Already defined LOG_PREFIX or TRACE or DEBUG or INFO or ERROR"
#endif


#define LOG_PREFIX "TPartitionChooser " << SelfId()                  \
                    << " (SourceId=" << SourceId                     \
                    << ", PreferedPartition=" << PreferedPartition   \
                    << ") "
#define TRACE(message) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define DEBUG(message) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define INFO(message)  LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define ERROR(message) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);


template<typename TDerived, typename TPipeCreator>
class TAbstractPartitionChooserActor: public TActorBootstrapped<TDerived> {
public:
    using TThis = TAbstractPartitionChooserActor<TDerived, TPipeCreator>;
    using TThisActor = TActor<TThis>;
    using TPartitionInfo = typename IPartitionChooser::TPartitionInfo;

    TAbstractPartitionChooserActor(TActorId parentId,
                                   std::shared_ptr<IPartitionChooser>& chooser,
                                   NPersQueue::TTopicConverterPtr& fullConverter,
                                   const TString& sourceId,
                                   std::optional<ui32> preferedPartition)
        : Parent(parentId)
        , SourceId(sourceId)
        , PreferedPartition(preferedPartition)
        , Chooser(chooser)
        , TableHelper(fullConverter->GetClientsideName(), fullConverter->GetTopicForSrcIdHash())
        , PartitionHelper() {
    }

    TActorIdentity SelfId() const {
        return TActor<TDerived>::SelfId();
    }

    void Initialize(const NActors::TActorContext& ctx) {
        TableHelper.Initialize(ctx, SourceId);
    }

    void PassAway() {
        auto ctx = TActivationContext::ActorContextFor(SelfId());
        TableHelper.CloseKqpSession(ctx);
        PartitionHelper.Close(ctx);
    }

protected:
    void InitTable(const NActors::TActorContext& ctx) {
        const auto& pqConfig = AppData(ctx)->PQConfig;
        if (SourceId && pqConfig.GetTopicsAreFirstClassCitizen() && pqConfig.GetUseSrcIdMetaMappingInFirstClass()) {
            DEBUG("InitTable");
            TThis::Become(&TThis::StateInitTable);
            TableHelper.SendInitTableRequest(ctx);
        } else {
            StartKqpSession(ctx);
        }
    }

    void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx) {
        StartKqpSession(ctx);
    }

    STATEFN(StateInitTable) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NMetadata::NProvider::TEvManagerPrepared, Handle);
            SFunc(TEvents::TEvPoison, TThis::Die);
        }
    }

protected:
    void StartKqpSession(const NActors::TActorContext& ctx) {
        const auto& pqConfig = AppData(ctx)->PQConfig;
        if (SourceId && (!pqConfig.GetTopicsAreFirstClassCitizen() || pqConfig.GetUseSrcIdMetaMappingInFirstClass())) {
            DEBUG("StartKqpSession")
            TThis::Become(&TThis::StateCreateKqpSession);
            TableHelper.SendCreateSessionRequest(ctx);
        } else {
            OnSelected(ctx);
        }
    }

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        if (!TableHelper.Handle(ev, ctx)) {
            return ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ53 : " <<  ev->Get()->Record.DebugString(), ctx);
        }

        SendSelectRequest(ctx);
    }

    void ScheduleStop() {
        TThis::Become(&TThis::StateDestroing);
    }

    STATEFN(StateCreateKqpSession) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }

protected:
    void SendSelectRequest(const NActors::TActorContext& ctx) {
        TThis::Become(&TThis::StateSelect);
        DEBUG("Select from the table");
        TableHelper.SendSelectRequest(ctx);
    }

    void HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        if (!TableHelper.HandleSelect(ev, ctx)) {
            return ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ50 : " <<  ev->Get()->Record.DebugString(), ctx);
        }

        if (TableHelper.PartitionId()) {
            Partition = Chooser->GetPartition(TableHelper.PartitionId().value());
        }

        OnSelected(ctx);
    }

    virtual void OnSelected(const NActors::TActorContext& ctx) =  0;

    STATEFN(StateSelect) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleSelect);
            SFunc(TEvents::TEvPoison, TThis::Die);
        }
    }

protected:
    void SendUpdateRequests(const TActorContext& ctx) {
        TThis::Become(&TThis::StateUpdate);
        DEBUG("Update the table");
        TableHelper.SendUpdateRequest(Partition->PartitionId, ctx);
    }

    void HandleUpdate(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
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

        StartIdle();
    }

    STATEFN(StateUpdate) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleUpdate);
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }

protected:
    void StartGetOwnership(const TActorContext &ctx) {
        if (!Partition) {
            return ReplyError(ErrorCode::INITIALIZING, "Partition not choosed", ctx);
        }

        TThis::Become(&TThis::StateOwnership);
        TRACE("GetOwnership Partition TabletId=" << Partition->TabletId);

        //PartitionHelper.Open(Partition->TabletId, ctx);
        //PartitionHelper.SendGetOwnershipRequest(Partition->PartitionId, SourceId, ctx);
    }

    void HandleOwnership(TEvPersQueue::TEvResponse::TPtr& ev, const NActors::TActorContext& ctx) {
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
            return ReplyError(ErrorCode::INITIALIZING, "Partition is not active", ctx);
        }

        OwnerCookie = response.GetCmdGetOwnershipResult().GetOwnerCookie();

        OnOwnership(ctx);
    }

    void HandleOwnership(TEvTabletPipe::TEvClientConnected::TPtr& , const NActors::TActorContext& ) {}
    void HandleOwnership(TEvTabletPipe::TEvClientDestroyed::TPtr& , const NActors::TActorContext& ) {}

    virtual void OnOwnership(const TActorContext &ctx) = 0;

    STATEFN(StateOwnership) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvResponse, HandleOwnership);
            HFunc(TEvTabletPipe::TEvClientConnected, HandleOwnership);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandleOwnership);
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }


protected:
    void StartIdle() {
        TThis::Become(&TThis::StateIdle);        
        DEBUG("Start idle");
    }

    void HandleIdle(TEvPartitionChooser::TEvRefreshRequest::TPtr&, const TActorContext& ctx) {
        if (PartitionPersisted) {
            // we do not update AccessTime for Split/Merge partitions because don't use table.
            SendUpdateRequests(ctx);
        }        
    }

    STATEFN(StateIdle)  {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPartitionChooser::TEvRefreshRequest, HandleIdle);
            SFunc(TEvents::TEvPoison, Stop);
        }
    }


protected:
    void HandleDestroy(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        TableHelper.Handle(ev, ctx);
        TThis::Die(ctx);
    }

    STATEFN(StateDestroing) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, HandleDestroy);
        }
    }

protected:
    void ReplyResult(const NActors::TActorContext& ctx) {
        ctx.Send(Parent, new TEvPartitionChooser::TEvChooseResult(Partition->PartitionId, Partition->TabletId, PartitionHelper.OwnerCookie));
    }

    void ReplyError(ErrorCode code, TString&& errorMessage, const NActors::TActorContext& ctx) {
        INFO("ReplyError: " << errorMessage);
        ctx.Send(Parent, new TEvPartitionChooser::TEvChooseError(code, std::move(errorMessage)));

        TThis::Die(ctx);
    }
    

protected:
    const TActorId Parent;
    const TString SourceId;
    const std::optional<ui32> PreferedPartition;
    const std::shared_ptr<IPartitionChooser> Chooser;

    const TPartitionInfo* Partition;

    TTableHelper TableHelper;
    TPartitionHelper<TPipeCreator> PartitionHelper;

    bool PartitionPersisted = false;

    TString OwnerCookie;
};

#undef LOG_PREFIX
#define LOG_PREFIX "TPartitionChooser " << SelfId()                                \
                    << " (SourceId=" << TThis::SourceId                     \
                    << ", PreferedPartition=" << TThis::PreferedPartition   \
                    << ") "

template<typename TPipeCreator>
class TPartitionChooserActor: public TAbstractPartitionChooserActor<TPartitionChooserActor<TPipeCreator>, TPipeCreator> {
public:
    using TThis = TPartitionChooserActor<TPipeCreator>;
    using TThisActor = TActor<TThis>;
    using TPartitionInfo = typename IPartitionChooser::TPartitionInfo;
    using TParentActor = TAbstractPartitionChooserActor<TPartitionChooserActor<TPipeCreator>, TPipeCreator>;

    TPartitionChooserActor(TActorId parentId,
                           const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                           std::shared_ptr<IPartitionChooser>& chooser,
                           NPersQueue::TTopicConverterPtr& fullConverter,
                           const TString& sourceId,
                           std::optional<ui32> preferedPartition)
        : TAbstractPartitionChooserActor<TPartitionChooserActor<TPipeCreator>, TPipeCreator>(parentId, chooser, fullConverter, sourceId, preferedPartition)
        , PQRBHelper(config.GetBalancerTabletID()) {
    }

    void Bootstrap(const TActorContext& ctx) {
        TThis::Initialize(ctx);
        TThis::InitTable(ctx);
    }

    TActorIdentity SelfId() const {
        return TActor<TPartitionChooserActor<TPipeCreator>>::SelfId();
    }

    void OnSelected(const TActorContext &ctx) override {
        auto [roundRobin, p] = ChoosePartitionSync(ctx);
        if (roundRobin) {
            RequestPQRB(ctx);
        } else {
            Partition = p;
            OnPartitionChosen(ctx);
        }
    }

    void OnOwnership(const TActorContext &/*ctx*/) override {
        
    }


private:
    void RequestPQRB(const NActors::TActorContext& ctx) {
        DEBUG("RequestPQRB")
        TThis::Become(&TThis::StatePQRB);

        if (PQRBHelper.PartitionId()) {
            PartitionId = PQRBHelper.PartitionId();
            OnPartitionChosen(ctx);
        } else {
            PQRBHelper.SendRequest(ctx);
        }
    }

    void Handle(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const TActorContext& ctx) {
        PartitionId = PQRBHelper.Handle(ev, ctx);
        DEBUG("Received partition " << PartitionId << " from PQRB for SourceId=" << TThis::SourceId);
        Partition = TThis::Chooser->GetPartition(PQRBHelper.PartitionId().value());

        OnPartitionChosen(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ev);

        if (ev->Get()->Status != NKikimrProto::EReplyStatus::OK) {
            TThis::ReplyError(ErrorCode::INITIALIZING, "Pipe connection fail", ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ev);

        TThis::ReplyError(ErrorCode::INITIALIZING, "Pipe destroyed", ctx);
    }

    STATEFN(StatePQRB) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvGetPartitionIdForWriteResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            SFunc(TEvents::TEvPoison, TThis::Die);
        }
    }


private:
    void OnPartitionChosen(const TActorContext& ctx) {
        if (!Partition && TThis::PreferedPartition) {
            return TThis::ReplyError(ErrorCode::BAD_REQUEST,
                            TStringBuilder() << "Prefered partition " << (TThis::PreferedPartition.value() + 1) << " is not exists or inactive.",
                            ctx);
        }

        if (!Partition) {
            return TThis::ReplyError(ErrorCode::INITIALIZING, "Can't choose partition", ctx);
        }

        if (TThis::PreferedPartition && Partition->PartitionId != TThis::PreferedPartition.value()) {
            return TThis::ReplyError(ErrorCode::BAD_REQUEST,
                            TStringBuilder() << "MessageGroupId " << TThis::SourceId << " is already bound to PartitionGroupId "
                                        << (Partition->PartitionId + 1) << ", but client provided " << (TThis::PreferedPartition.value() + 1)
                                        << ". MessageGroupId->PartitionGroupId binding cannot be changed, either use "
                                        "another MessageGroupId, specify PartitionGroupId " << (Partition->PartitionId + 1)
                                        << ", or do not specify PartitionGroupId at all.",
                            ctx);
        }

        TThis::StartGetOwnership(ctx);
    }

    std::pair<bool, const TPartitionInfo*> ChoosePartitionSync(const TActorContext& ctx) const {
        const auto& pqConfig = AppData(ctx)->PQConfig;
        if (TThis::PreferedPartition) {
            return {false, TThis::Chooser->GetPartition(TThis::PreferedPartition.value())};
        } else if (pqConfig.GetTopicsAreFirstClassCitizen() && TThis::SourceId) {
            return {false, TThis::Chooser->GetPartition(TThis::SourceId)};
        } else {
            return {true, nullptr};
        }
    };


private:
    std::optional<ui32> PartitionId;
    const TPartitionInfo* Partition;
    bool PartitionPersisted = false;


    bool NeedUpdateTable = false;

    TPQRBHelper<TPipeCreator> PQRBHelper;

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
