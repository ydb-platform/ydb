#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/defs.h>
#include <ydb/core/protos/counters_backup.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NBackup {

struct TBackupControllerTabletSchema : NIceDb::Schema {
    struct BackupCollections : Table<1> {
        struct ID : Column<1, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<ID>;
    };

    using TTables = SchemaTables<BackupCollections>;
    using TSettings = SchemaSettings<ExecutorLogBatching<true>, ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>>;
};

class TBackupControllerTablet
    : public TActor<TBackupControllerTablet>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    class TTxBase
        : public NTabletFlatExecutor::TTransactionBase<TBackupControllerTablet>
    {
    public:
        TTxBase(const TString& name, TBackupControllerTablet* self)
            : TTransactionBase(self)
            , LogPrefix(name)
        {
        }

    protected:
        const TString LogPrefix;
    };
    using Schema = TBackupControllerTabletSchema;

    // local transactions
    class TTxInitSchema;
    class TTxInit;

    // tx runners
    void RunTxInitSchema(const TActorContext& ctx);
    void RunTxInit(const TActorContext& ctx);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_ACTOR; //FIXME
    }

    explicit TBackupControllerTablet(const TActorId& tablet, TTabletStorageInfo* info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    {
        Y_UNUSED(tablet, info);
    }

    STFUNC(StateInit) {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork) {
        HandleDefaultEvents(ev, SelfId());
    }

    void OnDetach(const TActorContext& ctx) override {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override {
        Y_UNUSED(ev);
        Die(ctx);
    }

    void OnActivateExecutor(const TActorContext& ctx) override {
        RunTxInitSchema(ctx);
    }

    void DefaultSignalTabletActive(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }


    void SwitchToWork(const TActorContext& ctx) {
        SignalTabletActive(ctx);
        Become(&TThis::StateWork);
    }

    void Reset() {

    }
};

} // namespace NKikimr::NBackup
