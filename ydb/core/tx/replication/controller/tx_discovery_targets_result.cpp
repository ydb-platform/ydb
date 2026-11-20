#include "controller_impl.h"
#include "target_cdc.h"
#include "target_transfer.h"

#include <ydb/core/base/appdata.h>

#include <util/string/join.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxDiscoveryTargetsResult: public TTxBase {
    TEvPrivate::TEvDiscoveryTargetsResult::TPtr Ev;
    TReplication::TPtr Replication;

    void AddCdcTargets(const TEvPrivate::TEvDiscoveryTargetsResult::TAddEntry& table, ui64 parentId, NIceDb::TNiceDb& db) {
        for (const auto& stream : table.CdcStreams) {
            auto childPath = [&stream](TStringBuf tablePath) -> TString {
                tablePath.ChopSuffix("/");
                return TStringBuilder() << tablePath << '/' << stream.GetName();
            };
            const auto config = std::make_shared<TTargetCdc::TConfig>(
                childPath(table.Config->GetSrcPath()), childPath(table.Config->GetDstPath()));
            const auto id = Replication->AddTarget(TReplication::ETargetKind::CdcTopic, config);

            auto& cdc = GetCdcTarget(Replication->FindTarget(id))->GetCdc();
            cdc.SetParentTargetId(parentId);
            stream.SerializeTo(*cdc.MutableSourceStream());

            db.Table<Schema::Targets>().Key(Replication->GetId(), id).Update(
                NIceDb::TUpdate<Schema::Targets::Kind>(TReplication::ETargetKind::CdcTopic),
                NIceDb::TUpdate<Schema::Targets::SrcPath>(config->GetSrcPath()),
                NIceDb::TUpdate<Schema::Targets::DstPath>(config->GetDstPath()),
                NIceDb::TUpdate<Schema::Targets::Cdc>(cdc.SerializeAsString()));
        }
    }

public:
    explicit TTxDiscoveryTargetsResult(TController* self, TEvPrivate::TEvDiscoveryTargetsResult::TPtr& ev)
        : TTxBase("TxDiscoveryTargetsResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_DISCOVERY_RESULT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"ev", Ev->Get()->ToString()});

        const auto rid = Ev->Get()->ReplicationId;

        Replication = Self->Find(rid);
        if (!Replication) {
            YDB_LOG_WARN_CTX(ctx, "Unknown replication",
                {"rid", rid});
            return true;
        }

        if (Replication->GetState() != TReplication::EState::Ready
            && Replication->GetState() != TReplication::EState::Paused)
        {
            YDB_LOG_WARN_CTX(ctx, "Replication state mismatch",
                {"rid", rid},
                {"state", Replication->GetState()});
            Replication.Reset();
            return true;
        }

        if (!Replication->IsExpectedTargetDiscoverer(Ev->Sender)) {
            YDB_LOG_DEBUG_CTX(ctx, "Ignore obsolete target discoverer",
                {"rid", rid},
                {"sender", Ev->Sender});
            Replication.Reset();
            return true;
        }

        Replication->FinishTargetDiscovery();
        NIceDb::TNiceDb db(txc.DB);

        if (Ev->Get()->IsSuccess()) {
            for (const auto& target : Ev->Get()->ToAdd) {
                const auto tid = Replication->AddTarget(target.Kind, target.Config);

                TString transformLambda;
                TString runAsUser;
                TString directoryPath;
                if (auto p = std::dynamic_pointer_cast<const TTargetTransfer::TTransferConfig>(target.Config)) {
                    transformLambda = p->GetTransformLambda();
                    runAsUser = p->GetRunAsUser();
                    directoryPath = p->GetDirectoryPath();
                }

                db.Table<Schema::Targets>().Key(rid, tid).Update(
                    NIceDb::TUpdate<Schema::Targets::Kind>(target.Kind),
                    NIceDb::TUpdate<Schema::Targets::SrcPath>(target.Config->GetSrcPath()),
                    NIceDb::TUpdate<Schema::Targets::SrcIsImpl>(target.Config->GetSrcIsImpl()),
                    NIceDb::TUpdate<Schema::Targets::DstPath>(target.Config->GetDstPath()),
                    NIceDb::TUpdate<Schema::Targets::TransformLambda>(transformLambda),
                    NIceDb::TUpdate<Schema::Targets::RunAsUser>(runAsUser),
                    NIceDb::TUpdate<Schema::Targets::DirectoryPath>(directoryPath)
                );

                const auto& config = Replication->GetConfig();
                if (target.Kind == TReplication::ETargetKind::Table
                    && config.GetInitialScan()
                    && config.GetConsistencySettings().GetLevelCase() == NKikimrReplication::TConsistencySettings::kRow
                    && AppData(ctx)->FeatureFlags.GetEnableTopicReplication()
                    && AppData(ctx)->FeatureFlags.GetEnableAsyncReplicationTableOwnership())
                {
                    AddCdcTargets(target, tid, db);
                }

                YDB_LOG_NOTICE_CTX(ctx, "Add target",
                    {"rid", rid},
                    {"tid", tid},
                    {"kind", target.Kind},
                    {"srcPath", target.Config->GetSrcPath()},
                    {"dstPath", target.Config->GetDstPath()},
                    {"bootstrap", Replication->GetConfig().GetBootstrap()});
            }
        } else {
            const auto error = JoinSeq(", ", Ev->Get()->Failed);
            Replication->SetState(TReplication::EState::Error, TStringBuilder() << "Discovery error: " << error);

            YDB_LOG_ERROR_CTX(ctx, "Discovery error",
                {"rid", rid},
                {"error", error});
        }

        db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(Replication->GetIssue()),
            NIceDb::TUpdate<Schema::Replications::NextTargetId>(Replication->GetNextTargetId())
        );

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete");

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxDiscoveryTargetsResult

void TController::RunTxDiscoveryTargetsResult(TEvPrivate::TEvDiscoveryTargetsResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDiscoveryTargetsResult(this, ev), ctx);
}

}
