#include "controller_impl.h"
#include "target_table.h"
#include "target_transfer.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

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

            if (!database) {
                Self->UnresolvedDatabaseReplications.emplace(replication->GetId(), ResolveDatabaseAttemptsLimit);
            }

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

            auto replication = Self->Find(rid);
            Y_VERIFY_S(replication, "Unknown replication: " << rid);

            TReplication::ITarget::IConfig::TPtr config;
            switch (kind) {
            case TReplication::ETargetKind::Table:
                config = std::make_shared<TTargetTable::TTableConfig>(srcPath, dstPath);
                break;
            case TReplication::ETargetKind::IndexTable:
                config = std::make_shared<TTargetIndexTable::TIndexTableConfig>(srcPath, dstPath);
                break;
            case TReplication::ETargetKind::Transfer:
                config = std::make_shared<TTargetTransfer::TTransferConfig>(srcPath, dstPath, replication->GetConfig());
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
            // Zero is the durable "awaiting a post-schema heartbeat" marker.
            // It must not participate in the global-consistency quorum after
            // a schema operation or after recovery.
            if (version != TRowVersion::Min()) {
                worker->SetHeartbeat(version);
                Self->WorkersWithHeartbeat.insert(id);
                Self->WorkersByHeartbeat[version].insert(id);
            }

            if (!rowset.Next()) {
                return false;
            }
        }

        return true;
    }

    bool LoadSchemaBarriers(NIceDb::TNiceDb& db) {
        auto rowset = db.Table<Schema::SchemaBarriers>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const auto key = std::make_pair(
                rowset.GetValue<Schema::SchemaBarriers::ReplicationId>(),
                rowset.GetValue<Schema::SchemaBarriers::TargetId>()
            );
            auto& barrier = Self->SchemaBarriers[key];
            barrier.Phase = static_cast<TController::ESchemaBarrierPhase>(
                rowset.GetValue<Schema::SchemaBarriers::Phase>());
            Y_ABORT_UNLESS(barrier.Schema.ParseFromString(
                rowset.GetValue<Schema::SchemaBarriers::Schema>()));
            barrier.DstAlterTxId = rowset.GetValue<Schema::SchemaBarriers::DstAlterTxId>();

            if (!rowset.Next()) {
                return false;
            }
        }

        auto workers = db.Table<Schema::SchemaBarrierWorkers>().Select();
        if (!workers.IsReady()) {
            return false;
        }

        while (!workers.EndOfSet()) {
            const auto key = std::make_pair(
                workers.GetValue<Schema::SchemaBarrierWorkers::ReplicationId>(),
                workers.GetValue<Schema::SchemaBarrierWorkers::TargetId>()
            );
            auto it = Self->SchemaBarriers.find(key);
            Y_ABORT_UNLESS(it != Self->SchemaBarriers.end(), "Barrier member without barrier");

            const auto id = TWorkerId(key.first, key.second,
                workers.GetValue<Schema::SchemaBarrierWorkers::WorkerId>());
            it->second.ExpectedWorkers.insert(id);
            if (workers.GetValue<Schema::SchemaBarrierWorkers::Reported>()) {
                it->second.ReportedWorkers.insert(id);
            }
            if (workers.GetValue<Schema::SchemaBarrierWorkers::Applied>()) {
                it->second.AppliedWorkers.insert(id);
            }
            if (workers.GetValue<Schema::SchemaBarrierWorkers::Completed>()) {
                it->second.CompletedWorkers.insert(id);
            }
            it->second.WorkerOffsets[id] = workers.GetValue<Schema::SchemaBarrierWorkers::Offset>();

            if (!workers.Next()) {
                return false;
            }
        }

        auto flushes = db.Table<Schema::SchemaBarrierFlushes>().Select();
        if (!flushes.IsReady()) {
            return false;
        }
        while (!flushes.EndOfSet()) {
            const auto key = std::make_pair(
                flushes.GetValue<Schema::SchemaBarrierFlushes::ReplicationId>(),
                flushes.GetValue<Schema::SchemaBarrierFlushes::TargetId>());
            auto it = Self->SchemaBarriers.find(key);
            Y_ABORT_UNLESS(it != Self->SchemaBarriers.end(), "Flush snapshot without barrier");
            it->second.TargetFlushTxIds.push_back(
                flushes.GetValue<Schema::SchemaBarrierFlushes::WriteTxId>());
            if (!flushes.Next()) {
                return false;
            }
        }

        return true;
    }

    bool LoadWorkerSnapshots(NIceDb::TNiceDb& db) {
        auto rowset = db.Table<Schema::WorkerSnapshots>().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            Self->WorkerSnapshots.insert({
                rowset.GetValue<Schema::WorkerSnapshots::ReplicationId>(),
                rowset.GetValue<Schema::WorkerSnapshots::TargetId>()});
            if (!rowset.Next()) {
                return false;
            }
        }
        return true;
    }

    bool LoadDeferredAlters(NIceDb::TNiceDb& db) {
        auto rowset = db.Table<Schema::DeferredAlters>().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            Self->DeferredAlters.insert(rowset.GetValue<Schema::DeferredAlters::ReplicationId>());
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
            && LoadWorkers(db)
            && LoadWorkerSnapshots(db)
            && LoadSchemaBarriers(db)
            && LoadDeferredAlters(db);
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
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Execute");
        return Load(txc.DB);
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete");

        if (Self->UnresolvedDatabaseReplications.empty()) {
            Self->SwitchToWork(ctx);
        } else {
            for (auto& [rid, resolveAttempts] : Self->UnresolvedDatabaseReplications) {
                auto replication = Self->Find(rid);
                replication->ResolveDatabase(ctx);
                --resolveAttempts;
            }

            Self->SwitchToDatabaseResolve(ctx);
        }
    }

}; // TTxInit

void TController::RunTxInit(const TActorContext& ctx) {
    Execute(new TTxInit(this), ctx);
}

}
