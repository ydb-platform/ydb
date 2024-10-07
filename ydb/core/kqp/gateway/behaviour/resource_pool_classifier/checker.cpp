#include "checker.h"
#include "fetcher.h"

#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/kqp/workload_service/actors/actors.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/resource_pools/resource_pool_classifier_settings.h>

#include <ydb/library/query_actor/query_actor.h>


namespace NKikimr::NKqp {

namespace {

using namespace NActors;
using namespace NResourcePool;
using namespace NWorkload;


struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvRanksCheckerResponse = EventSpaceBegin(TEvents::ES_PRIVATE),

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvRanksCheckerResponse : public TEventLocal<TEvRanksCheckerResponse, EvRanksCheckerResponse> {
        TEvRanksCheckerResponse(Ydb::StatusIds::StatusCode status, i64 maxRank, ui64 numberClassifiers, NYql::TIssues issues)
            : Status(status)
            , MaxRank(maxRank)
            , NumberClassifiers(numberClassifiers)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const i64 MaxRank;
        const ui64 NumberClassifiers;
        const NYql::TIssues Issues;
    };
};

class TRanksCheckerActor : public NKikimr::TQueryBase {
    using TBase = NKikimr::TQueryBase;

public:
    TRanksCheckerActor(const TString& databaseId, const TString& sessionId, const TString& transactionId, const std::unordered_map<i64, TString>& ranksToCheck)
        : TBase(NKikimrServices::KQP_GATEWAY, sessionId)
        , DatabaseId(databaseId)
        , RanksToCheck(ranksToCheck)
    {
        TxId = transactionId;
        SetOperationInfo(__func__, DatabaseId);
    }

    void OnRunQuery() override {
        const auto& tablePath = TResourcePoolClassifierConfig::GetBehaviour()->GetStorageTablePath();

        TStringBuilder sql = TStringBuilder() << R"(
            -- TRanksCheckerActor::OnRunQuery
            DECLARE $database_id AS Text;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database_id")
                .Utf8(CanonizePath(DatabaseId))
                .Build();

        if (!RanksToCheck.empty()) {
            sql << R"(
                DECLARE $ranks AS List<Int64>;
                PRAGMA AnsiInForEmptyOrNullableItemsCollections;

                SELECT
                    rank, name
                FROM `)" << tablePath << R"(`
                WHERE database = $database_id
                  AND rank IN $ranks;
            )";

            auto& param = params.AddParam("$ranks").BeginList();
            for (const auto& [rank, _] : RanksToCheck) {
                param.AddListItem().Int64(rank);
            }
            param.EndList().Build();

            ExpectedResultSets++;
        }

        sql << R"(
            SELECT
                MAX(rank) AS MaxRank,
                COUNT(*) AS NumberClassifiers
            FROM `)" << tablePath << R"(`
            WHERE database = $database_id;
        )";

        RunDataQuery(sql, &params, TTxControl::ContinueTx());
    }

    void OnQueryResult() override {
        if (ResultSets.size() != ExpectedResultSets) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        ui64 resultSetId = 0;
        if (!RanksToCheck.empty()) {
            NYdb::TResultSetParser result(ResultSets[resultSetId++]);
            while (result.TryNextRow()) {
                TMaybe<i64> rank = result.ColumnParser("rank").GetOptionalInt64();
                if (!rank) {
                    continue;
                }

                TMaybe<TString> name = result.ColumnParser("name").GetOptionalUtf8();
                if (!name) {
                    continue;
                }

                if (auto it = RanksToCheck.find(*rank); it != RanksToCheck.end() && it->second != *name) {
                    Finish(Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder() << "Classifier with rank " << *rank << " already exists, its name " << *name);
                    return;
                }
            }
        }

        {   // Classifiers stats
            NYdb::TResultSetParser result(ResultSets[resultSetId++]);
            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
                return;
            }

            MaxRank = result.ColumnParser("MaxRank").GetOptionalInt64().GetOrElse(0);
            NumberClassifiers = result.ColumnParser("NumberClassifiers").GetUint64();
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvRanksCheckerResponse(status, MaxRank, NumberClassifiers, std::move(issues)));
    }

private:
    const TString DatabaseId;
    const std::unordered_map<i64, TString> RanksToCheck;

    ui64 ExpectedResultSets = 1;
    i64 MaxRank = 0;
    ui64 NumberClassifiers = 0;
};

class TResourcePoolClassifierPreparationActor : public TActorBootstrapped<TResourcePoolClassifierPreparationActor> {
public:
    TResourcePoolClassifierPreparationActor(std::vector<TResourcePoolClassifierConfig>&& patchedObjects, NMetadata::NModifications::IAlterPreparationController<TResourcePoolClassifierConfig>::TPtr controller, const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext)
        : Context(context)
        , AlterContext(alterContext)
        , Controller(std::move(controller))
        , PatchedObjects(std::move(patchedObjects))
    {}

    void Bootstrap() {
        Become(&TResourcePoolClassifierPreparationActor::StateFunc);
        ValidateRanks();
        GetDatabaseInfo();
    }

    void Handle(TEvPrivate::TEvRanksCheckerResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            FailAndPassAway("Resource pool classifier rank check failed", ev->Get()->Status, ev->Get()->Issues);
            return;
        }

        if (Context.GetActivityType() == NMetadata::NModifications::IOperationsManager::EActivityType::Create && ev->Get()->NumberClassifiers >= CLASSIFIER_COUNT_LIMIT) {
            FailAndPassAway(TStringBuilder() << "Number of resource pool classifiers reached limit in " << CLASSIFIER_COUNT_LIMIT);
            return;
        }

        i64 maxRank = ev->Get()->MaxRank;
        for (auto& object : PatchedObjects) {
            if (object.GetRank() != -1) {
                continue;
            }
            if (maxRank > std::numeric_limits<i64>::max() - CLASSIFIER_RANK_OFFSET) {
                FailAndPassAway(TStringBuilder() << "The rank could not be set automatically, the maximum rank of the resource pool classifier is too high: " << ev->Get()->MaxRank);
                return;
            }

            maxRank += CLASSIFIER_RANK_OFFSET;
            object.SetRank(maxRank);
        }

        RanksChecked = true;
        TryFinish();
    }

    void Handle(TEvFetchDatabaseResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            FailAndPassAway("Database check failed", ev->Get()->Status, ev->Get()->Issues);
            return;
        }

        Serverless = ev->Get()->Serverless;

        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), new NConsole::TEvConfigsDispatcher::TEvGetConfigRequest(
            (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem
        ), IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvGetConfigRequest:
                CheckFeatureFlag(AppData()->FeatureFlags);
                break;

            default:
                break;
        }
    }

    void Handle(NConsole::TEvConfigsDispatcher::TEvGetConfigResponse::TPtr& ev) {
        CheckFeatureFlag(ev->Get()->Config->GetFeatureFlags());
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        const auto& snapshot = ev->Get()->GetSnapshotAs<TResourcePoolClassifierSnapshot>();
        for (const auto& objectRecord : AlterContext.GetRestoreObjectIds().GetTableRecords()) {
            TResourcePoolClassifierConfig object;
            TResourcePoolClassifierConfig::TDecoder::DeserializeFromRecord(object, objectRecord);

            if (!snapshot->GetClassifierConfig(object.GetDatabase(), object.GetName())) {
                FailAndPassAway(TStringBuilder() << "Classifier with name " << object.GetName() << " not found in database with id " << object.GetDatabase());
                return;
            }
        }

        ExistenceChecked = true;
        TryFinish();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvRanksCheckerResponse, Handle);
        hFunc(TEvFetchDatabaseResponse, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(NConsole::TEvConfigsDispatcher::TEvGetConfigResponse, Handle);
        hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle)
    )

private:
    void GetDatabaseInfo() const {
        const auto& externalContext = Context.GetExternalData();
        const auto userToken = externalContext.GetUserToken() ? MakeIntrusive<NACLib::TUserToken>(*externalContext.GetUserToken()) : nullptr;
        Register(CreateDatabaseFetcherActor(SelfId(), externalContext.GetDatabase(), userToken, NACLib::EAccessRights::GenericFull));
    }

    void ValidateRanks() {
        if (Context.GetActivityType() == NMetadata::NModifications::IOperationsManager::EActivityType::Drop) {
            RanksChecked = true;
            TryFinish();
            return;
        }

        std::unordered_map<i64, TString> ranksToNames;
        for (const auto& object : PatchedObjects) {
            const auto rank = object.GetRank();
            if (rank == -1) {
                continue;
            }
            if (!ranksToNames.insert({rank, object.GetName()}).second) {
                FailAndPassAway(TStringBuilder() << "Found duplicate rank " << rank);
            }
        }

        Register(new TQueryRetryActor<TRanksCheckerActor, TEvPrivate::TEvRanksCheckerResponse, TString, TString, TString, std::unordered_map<i64, TString>>(
            SelfId(), Context.GetExternalData().GetDatabaseId(), AlterContext.GetSessionId(), AlterContext.GetTransactionId(), ranksToNames
        ));
    }

    void CheckFeatureFlag(const NKikimrConfig::TFeatureFlags& featureFlags) {
        if (Context.GetActivityType() == NMetadata::NModifications::IOperationsManager::EActivityType::Drop) {
            FeatureFlagChecked = true;
            ValidateExistence();
            return;
        }

        if (!featureFlags.GetEnableResourcePools()) {
            FailAndPassAway("Resource pool classifiers are disabled. Please contact your system administrator to enable it");
            return;
        }
        if (Serverless && !featureFlags.GetEnableResourcePoolsOnServerless()) {
            FailAndPassAway("Resource pool classifiers are disabled for serverless domains. Please contact your system administrator to enable it");
            return;
        }

        FeatureFlagChecked = true;
        ValidateExistence();
    }

    void ValidateExistence() {
        if (Context.GetActivityType() != NMetadata::NModifications::IOperationsManager::EActivityType::Create && NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<TResourcePoolClassifierSnapshotsFetcher>()));
            return;
        }

        ExistenceChecked = true;
        TryFinish();
    }

    void FailAndPassAway(const TString& message, Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
        FailAndPassAway(TStringBuilder() << message << ", status: " << status << ", reason: " << issues.ToOneLineString());
    }

    void FailAndPassAway(const TString& message) {
        Controller->OnPreparationProblem(message);
        PassAway();
    }

    void TryFinish() {
        if (!FeatureFlagChecked || !RanksChecked || !ExistenceChecked) {
            return;
        }

        Controller->OnPreparationFinished(std::move(PatchedObjects));
        PassAway();
    }

private:
    const NMetadata::NModifications::IOperationsManager::TInternalModificationContext Context;
    const NMetadata::NModifications::TAlterOperationContext AlterContext;

    bool Serverless = false;
    bool FeatureFlagChecked = false;
    bool RanksChecked = false;
    bool ExistenceChecked = false;

    NMetadata::NModifications::IAlterPreparationController<TResourcePoolClassifierConfig>::TPtr Controller;
    std::vector<TResourcePoolClassifierConfig> PatchedObjects;
};

}  // anonymous namespace

IActor* CreateResourcePoolClassifierPreparationActor(std::vector<TResourcePoolClassifierConfig>&& patchedObjects, NMetadata::NModifications::IAlterPreparationController<TResourcePoolClassifierConfig>::TPtr controller, const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext) {
    return new TResourcePoolClassifierPreparationActor(std::move(patchedObjects), std::move(controller), context, alterContext);
}

}  // namespace NKikimr::NKqp
