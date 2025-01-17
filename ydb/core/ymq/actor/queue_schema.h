#pragma once
#include "defs.h"

#include "schema.h"
#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/public/lib/value/value.h>
#include <ydb/core/ymq/base/queue_attributes.h>

#include <util/generic/maybe.h>

namespace NKikimr::NSQS {

class TCreateQueueSchemaActorV2
    : public TActorBootstrapped<TCreateQueueSchemaActorV2>
{
public:
     TCreateQueueSchemaActorV2(const TQueuePath& path,
                               const TCreateQueueRequest& req,
                               const TActorId& sender,
                               const TString& requestId,
                               const TString& customQueueName,
                               const TString& folderId,
                               const bool isCloudMode,
                               const bool enableQueueAttributesValidation,
                               TIntrusivePtr<TUserCounters> userCounters,
                               TIntrusivePtr<TSqsEvents::TQuoterResourcesForActions> quoterResources,
                               const TString& tagsJson);

    ~TCreateQueueSchemaActorV2();

    void InitMissingQueueAttributes(const NKikimrConfig::TSqsConfig& config);

    void Bootstrap();

    void RequestQueueParams();

    STATEFN(Preamble);

    void HandleQueueId(TSqsEvents::TEvQueueId::TPtr& ev);

    void OnReadQueueParams(TSqsEvents::TEvExecuted::TPtr& ev);

    void RunAtomicCounterIncrement();
    void OnAtomicCounterIncrement(TSqsEvents::TEvAtomicCounterIncrementResult::TPtr& ev);

    void RequestCreateQueueQuota();
    void OnCreateQueueQuota(TEvQuota::TEvClearance::TPtr& ev);

    void RequestTablesFormatSettings(const TString& accountName);
    void RegisterMakeDirActor(const TString& workingDir, const TString& dirName);

    void RequestLeaderTabletId();

    void CreateComponents();

    STATEFN(CreateComponentsState);

    void Step();

    void OnExecuted(TSqsEvents::TEvExecuted::TPtr& ev);

    void OnDescribeSchemeResult(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev);

    void SendDescribeTable();
    void HandleTableDescription(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

    template <typename T>
    T GetAttribute(const TStringBuf name, const T& defaultValue) const;

    void CommitNewVersion();

    STATEFN(FinalizeAndCommit);

    void OnCommit(TSqsEvents::TEvExecuted::TPtr& ev);

    void MatchQueueAttributes(const ui64 currentVersion, const ui32 currentTablesFormat);

    STATEFN(MatchAttributes);

    void OnAttributesMatch(TSqsEvents::TEvExecuted::TPtr& ev);

    void AddRPSQuota();

    void HandleAddQuoterResource(NKesus::TEvKesus::TEvAddQuoterResourceResult::TPtr& ev);

    void PassAway() override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

public:
    enum class ECreateComponentsStep {
        GetTablesFormatSetting,
        MakeQueueDir,
        MakeQueueVersionDir,
        MakeShards,
        MakeTables,
        DiscoverLeaderTabletId,
        AddQuoterResource,
        Commit
    };

private:
    const TQueuePath QueuePath_;
    const TCreateQueueRequest Request_;
    const TActorId Sender_;
    const TString CustomQueueName_;
    const TString FolderId_;
    const TString RequestId_;
    const TString GeneratedQueueId_;
    const TInstant QueueCreationTimestamp_;

    bool IsFifo_ = false;
    bool IsCloudMode_ = false;
    bool EnableQueueAttributesValidation_ = true;

    ui32 TablesFormat_ = 0;
    ui64 Version_ = 0;

    TString VersionName_;
    TString VersionedQueueFullPath_;
    TString ExistingQueueResourceId_;
    TIntrusivePtr<TUserCounters> UserCounters_;
    TIntrusivePtr<TSqsEvents::TQuoterResourcesForActions> QuoterResources_;
    ui64 RequiredShardsCount_ = 0;
    ui64 CreatedShardsCount_ = 0;
    TVector<TTable> RequiredTables_;
    ui64 CreatedTablesCount_ = 0;
    TQueueAttributes ValidatedAttributes_;
    TString TagsJson_;

    ui64 LeaderTabletId_ = 0;
    TActorId CreateTableWithLeaderTabletActorId_;
    ui64 CreateTableWithLeaderTabletTxId_ = 0;
    std::pair<ui64, ui64> TableWithLeaderPathId_ = std::make_pair(0, 0); // (scheme shard, path id) are required for describing table

    ECreateComponentsStep CurrentCreationStep_ = ECreateComponentsStep::GetTablesFormatSetting;

    TActorId AddQuoterResourceActor_;
};

class TDeleteQueueSchemaActorV2
    : public TActorBootstrapped<TDeleteQueueSchemaActorV2>
{
public:
    TDeleteQueueSchemaActorV2(const TQueuePath& path,
                              bool isFifo,
                              ui32 tablesFormat,
                              const TActorId& sender,
                              const TString& requestId,
                              TIntrusivePtr<TUserCounters> userCounters);

    TDeleteQueueSchemaActorV2(const TQueuePath& path,
                              bool isFifo,
                              ui32 tablesFormat,
                              const TActorId& sender,
                              const TString& requestId,
                              TIntrusivePtr<TUserCounters> userCounters,
                              const ui64 advisedQueueVersion,
                              const ui64 advisedShardCount,
                              const bool advisedIsFifoFlag);

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

private:
    void PrepareCleanupPlan(const bool isFifo, const ui64 shardCount);

    void NextAction();

    void DoSuccessOperation();

    void DeleteRPSQuota();

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
            hFunc(NKesus::TEvKesus::TEvDeleteQuoterResourceResult, HandleDeleteQuoterResource);
            cFunc(TEvPoisonPill::EventType, PassAway);
        }
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);
    void HandleDeleteQuoterResource(NKesus::TEvKesus::TEvDeleteQuoterResourceResult::TPtr& ev);
    void PassAway() override;

public:
    enum class EDeleting : ui32 {
        EraseQueueRecord,
        RemoveTables,
        RemoveShards,
        RemoveQueueVersionDirectory,
        RemoveQueueDirectory,
        DeleteQuoterResource,
        Finish,
    };

private:
    const TQueuePath QueuePath_;
    const bool IsFifo_;
    const ui32 TablesFormat_;
    const TActorId Sender_;
    TVector<TTable> Tables_;
    TVector<int> Shards_;
    EDeleting DeletionStep_;
    const TString RequestId_;
    TIntrusivePtr<TUserCounters> UserCounters_;
    ui64 Version_ = 0;
    TActorId DeleteQuoterResourceActor_;
};

} // namespace NKikimr::NSQS
