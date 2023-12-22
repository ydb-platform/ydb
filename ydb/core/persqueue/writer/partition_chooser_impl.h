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
template<class THasher = TMd5Sharder>
class THashChooser: public IPartitionChooser {
public:
    THashChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config);

    const TPartitionInfo* GetPartition(const TString& sourceId) const override;
    const TPartitionInfo* GetPartition(ui32 partitionId) const override;

private:
    std::vector<TPartitionInfo> Partitions;
    THasher Hasher;
};


class TPartitionChooserActor: public TActorBootstrapped<TPartitionChooserActor> {
    using TThis = TPartitionChooserActor;
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
    const std::shared_ptr<IPartitionChooser> Chooser;
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

} // namespace NPartitionChooser
} // namespace NKikimr::NPQ
