#include "controller_impl.h"
#include "target_table.h"
#include "target_transfer.h"

namespace NKikimr::NReplication::NController {

class TController::TTxInit: public TTxBase {
    template <typename TRowset>
    class TSysParamLoader: public ISysParamLoader {
    public:
        explicit TSysParamLoader(TRowset& rowset)
            : Rowset(rowset)
        {
        }

        ui64 LoadInt() { return Rowset.template GetValue<Schema::SysParams::IntValue>(); }
        TString LoadText() { return Rowset.template GetValue<Schema::SysParams::TextValue>(); }
        TString LoadBinary() { return Rowset.template GetValue<Schema::SysParams::BinaryValue>(); }

    private:
        TRowset& Rowset;
    };

    bool LoadSysParams(NIceDb::TNiceDb& db) {
        auto rowset = db.Table<Schema::SysParams>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        auto loader = MakeHolder<TSysParamLoader<decltype(rowset)>>(rowset);
        while (!rowset.EndOfSet()) {
            Self->SysParams.Load(rowset.GetValue<Schema::SysParams::Id>(), loader.Get());
            if (!rowset.Next()) {
                return false;
            }
        }

        return true;
    }

    bool LoadReplications(NIceDb::TNiceDb& db) {
        auto rowset = db.Table<Schema::Replications>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const auto rid = rowset.GetValue<Schema::Replications::Id>();
            const auto pathId = TPathId(
                rowset.GetValue<Schema::Replications::PathOwnerId>(),
                rowset.GetValue<Schema::Replications::PathLocalId>()
            );
            const auto config = rowset.GetValue<Schema::Replications::Config>();
            const auto state = rowset.GetValue<Schema::Replications::State>();
            const auto issue = rowset.GetValue<Schema::Replications::Issue>();
            const auto nextTid = rowset.GetValue<Schema::Replications::NextTargetId>();
            const auto desiredState = rowset.GetValue<Schema::Replications::DesiredState>();
            const auto database = rowset.GetValue<Schema::Replications::Database>();

            auto replication = Self->Add(rid, pathId, config, database);
            replication->SetState(state, issue);
            replication->SetNextTargetId(nextTid);
            replication->SetDesiredState(desiredState);

            if (!rowset.Next()) {
                return false;
            }
        }

        return true;
    }

    bool LoadTargets(NIceDb::TNiceDb& db) {
        auto rowset = db.Table<Schema::Targets>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const auto rid = rowset.GetValue<Schema::Targets::ReplicationId>();
            const auto tid = rowset.GetValue<Schema::Targets::Id>();
            const auto kind = rowset.GetValue<Schema::Targets::Kind>();
            const auto srcPath = rowset.GetValue<Schema::Targets::SrcPath>();
            const auto dstPath = rowset.GetValue<Schema::Targets::DstPath>();
            const auto dstState = rowset.GetValue<Schema::Targets::DstState>();
            const auto issue = rowset.GetValue<Schema::Targets::Issue>();
            const auto dstPathId = TPathId(
                rowset.GetValue<Schema::Targets::DstPathOwnerId>(),
                rowset.GetValue<Schema::Targets::DstPathLocalId>()
            );
            const auto transformLambda = rowset.GetValue<Schema::Targets::TransformLambda>();
            const auto runAsUser = rowset.GetValue<Schema::Targets::RunAsUser>();
            const auto directoryPath = rowset.GetValue<Schema::Targets::DirectoryPath>();

            auto replication = Self->Find(rid);
            Y_VERIFY_S(replication, "Unknown replication: " << rid);

            TReplication::ITarget::IConfig::TPtr config;
            switch(kind) {
                case TReplication::ETargetKind::Table:
                    config = std::make_shared<TTargetTable::TTableConfig>(srcPath, dstPath);
                    break;

                case TReplication::ETargetKind::IndexTable:
                    config = std::make_shared<TTargetIndexTable::TIndexTableConfig>(srcPath, dstPath);
                    break;

                case TReplication::ETargetKind::Transfer:
                    config = std::make_shared<TTargetTransfer::TTransferConfig>(srcPath, dstPath, transformLambda, runAsUser, directoryPath);
                    break;
            }

            auto* target = replication->AddTarget(tid, kind, config);
            Y_ABORT_UNLESS(target);

            target->SetDstState(dstState);
            target->SetDstPathId(dstPathId);
            target->SetIssue(issue);

            if (!rowset.Next()) {
                return false;
            }
        }

        return true;
    }

    bool LoadSrcStreams(NIceDb::TNiceDb& db) {
        auto rowset = db.Table<Schema::SrcStreams>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const auto rid = rowset.GetValue<Schema::SrcStreams::ReplicationId>();
            const auto tid = rowset.GetValue<Schema::SrcStreams::TargetId>();
            const auto name = rowset.GetValue<Schema::SrcStreams::Name>();
            const auto state = rowset.GetValue<Schema::SrcStreams::State>();
            const auto consumerName = rowset.GetValueOrDefault<Schema::SrcStreams::ConsumerName>(ReplicationConsumerName);

            auto replication = Self->Find(rid);
            Y_VERIFY_S(replication, "Unknown replication: " << rid);

            auto* target = replication->FindTarget(tid);
            Y_VERIFY_S(target, "Unknown target"
                << ": rid# " << rid
                << ", tid# " << tid);

            target->SetStreamName(name);
            target->SetStreamState(state);
            target->SetStreamConsumerName(consumerName);

            if (!rowset.Next()) {
                return false;
            }
        }

        return true;
    }

    bool LoadTxIds(NIceDb::TNiceDb& db) {
        auto rowset = db.Table<Schema::TxIds>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const auto txId = rowset.GetValue<Schema::TxIds::WriteTxId>();
            const auto version = TRowVersion(
                rowset.GetValue<Schema::TxIds::VersionStep>(),
                rowset.GetValue<Schema::TxIds::VersionTxId>()
            );

            auto res = Self->AssignedTxIds.emplace(version, txId);
            Y_VERIFY_S(res.second, "Duplicate version: " << version);

            if (!rowset.Next()) {
                return false;
            }
        }

        return true;
    }

    bool LoadWorkers(NIceDb::TNiceDb& db) {
        auto rowset = db.Table<Schema::Workers>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const auto id = TWorkerId(
                rowset.GetValue<Schema::Workers::ReplicationId>(),
                rowset.GetValue<Schema::Workers::TargetId>(),
                rowset.GetValue<Schema::Workers::WorkerId>()
            );
            const auto version = TRowVersion(
                rowset.GetValue<Schema::Workers::HeartbeatVersionStep>(),
                rowset.GetValue<Schema::Workers::HeartbeatVersionTxId>()
            );

            auto* worker = Self->GetOrCreateWorker(id);
            worker->SetHeartbeat(version);
            Self->WorkersWithHeartbeat.insert(id);
            Self->WorkersByHeartbeat[version].insert(id);

            if (!rowset.Next()) {
                return false;
            }
        }

        return true;
    }

    inline bool Load(NIceDb::TNiceDb& db) {
        Self->Reset();
        return LoadSysParams(db)
            && LoadReplications(db)
            && LoadTargets(db)
            && LoadSrcStreams(db)
            && LoadTxIds(db)
            && LoadWorkers(db);
    }

    inline bool Load(NTable::TDatabase& toughDb) {
        NIceDb::TNiceDb db(toughDb);
        return Load(db);
    }

public:
    explicit TTxInit(TSelf* self)
        : TTxBase("TxInit", self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INIT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        CLOG_D(ctx, "Execute");
        return Load(txc.DB);
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");
        Self->SwitchToWork(ctx);
    }

}; // TTxInit

void TController::RunTxInit(const TActorContext& ctx) {
    Execute(new TTxInit(this), ctx);
}

}
