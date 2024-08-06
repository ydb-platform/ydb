#include "checker.h"

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/resource_pools/resource_pool_classifier_settings.h>

#include <ydb/library/query_actor/query_actor.h>


namespace NKikimr::NKqp {

namespace {

using namespace NActors;
using namespace NResourcePool;


struct TEvPrivate {
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
    TRanksCheckerActor(const TString& database, const TString& sessionId, const TString& transactionId, const std::unordered_map<i64, TString>& ranksToCheck)
        : TBase(NKikimrServices::KQP_GATEWAY, sessionId)
        , Database(database)
        , RanksToCheck(ranksToCheck)
    {
        TxId = transactionId;
        SetOperationInfo(__func__, Database);
    }

    void OnRunQuery() override {
        const auto& tablePath = TResourcePoolClassifierConfig::GetBehaviour()->GetStorageTablePath();

        TString sql = TStringBuilder() << R"(
            -- TRanksCheckerActor::OnRunQuery
            PRAGMA AnsiInForEmptyOrNullableItemsCollections;

            DECLARE $database AS Text;
            DECLARE $ranks AS List<Int64>;

            SELECT
                rank, name
            FROM `)" << tablePath << R"(`
            WHERE database = $database
              AND rank IN $ranks;

            SELECT
                MAX(rank) AS MaxRank,
                COUNT(*) AS NumberClassifiers
            FROM `)" << tablePath << R"(`
            WHERE database = $database;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build();

        auto& param = params.AddParam("$ranks").BeginList();
        for (const auto& [rank, _] : RanksToCheck) {
            param.AddListItem().Int64(rank);
        }
        param.EndList().Build();

        RunDataQuery(sql, &params, TTxControl::ContinueTx());
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        {   // Ranks description
            NYdb::TResultSetParser result(ResultSets[0]);
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
                    Finish(Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder() << "Classifier with rank " << *rank << " already exists (classifier name " << it->second << ")");
                    return;
                }
            }
        }

        {   // Classifiers stats
            NYdb::TResultSetParser result(ResultSets[1]);
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
    const TString Database;
    const std::unordered_map<i64, TString> RanksToCheck;

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

    void Handle(TEvPrivate::TEvRanksCheckerResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            NYql::TIssue rootIssue(TStringBuilder() << "Resource pool classifier ranks check failed, " << ev->Get()->Status);
            for (const auto& issue : ev->Get()->Issues) {
                rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
            }
            FailAndPassAway(rootIssue.ToString(true));
            return;
        }

        if (ev->Get()->NumberClassifiers >= CLASSIFIER_COUNT_LIMIT) {
            FailAndPassAway(TStringBuilder() << "Number of resource pool classifiers reached limit in " << CLASSIFIER_COUNT_LIMIT);
            return;
        }

        i64 maxRank = ev->Get()->MaxRank;
        for (auto& object : PatchedObjects) {
            if (object.GetRank() != -1) {
                continue;
            }
            if (maxRank > std::numeric_limits<i64>::max() - CLASSIFIER_RANK_OFFSET) {
                FailAndPassAway(TStringBuilder() << "The rank could not be set automatically, the maximum rank of the resource pool classifier is too high " << ev->Get()->MaxRank);
                return;
            }

            maxRank += CLASSIFIER_RANK_OFFSET;
            object.SetRank(maxRank);
        }

        RanksChecked = true;
        TryFinish();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(NConsole::TEvConfigsDispatcher::TEvGetConfigResponse, Handle);
        hFunc(TEvPrivate::TEvRanksCheckerResponse, Handle);
    )

private:
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
            SelfId(), Context.GetExternalData().GetDatabase(), AlterContext.SessionId, AlterContext.TransactionId, ranksToNames
        ));
    }

    void CheckFeatureFlag(const NKikimrConfig::TFeatureFlags& featureFlags) {
        if (!featureFlags.GetEnableResourcePools()) {
            FailAndPassAway("Resource pools are disabled. Please contact your system administrator to enable it");
            return;
        }

        FeatureFlagChecked = true;
        TryFinish();
    }

    void FailAndPassAway(const TString& message) {
        Controller->OnPreparationProblem(message);
        PassAway();
    }

    void TryFinish() {
        if (!FeatureFlagChecked || !RanksChecked) {
            return;
        }

        Controller->OnPreparationFinished(std::move(PatchedObjects));
        PassAway();
    }

private:
    const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& Context;
    const NMetadata::NModifications::TAlterOperationContext& AlterContext;

    bool FeatureFlagChecked = false;
    bool RanksChecked = false;

    NMetadata::NModifications::IAlterPreparationController<TResourcePoolClassifierConfig>::TPtr Controller;
    std::vector<TResourcePoolClassifierConfig> PatchedObjects;
};

}  // anonymous namespace

IActor* CreateResourcePoolClassifierPreparationActor(std::vector<TResourcePoolClassifierConfig>&& patchedObjects, NMetadata::NModifications::IAlterPreparationController<TResourcePoolClassifierConfig>::TPtr controller, const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext) {
    return new TResourcePoolClassifierPreparationActor(std::move(patchedObjects), std::move(controller), context, alterContext);
}

}  // namespace NKikimr::NKqp
