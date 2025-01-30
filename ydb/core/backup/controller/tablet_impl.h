#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/defs.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/protos/counters_backup.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/replication/service/service.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NBackup {

struct TBackupControllerSchema : NIceDb::Schema {
    struct BackupCollections : Table<1> {
        struct ID : Column<1, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<ID>;
    };

    using TTables = SchemaTables<BackupCollections>;
    using TSettings = SchemaSettings<ExecutorLogBatching<true>, ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>>;
};

class TBackupController
    : public TActor<TBackupController>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    class TTxBase
        : public NTabletFlatExecutor::TTransactionBase<TBackupController>
    {
    public:
        TTxBase(const TString& name, TBackupController* self)
            : TTransactionBase(self)
            , LogPrefix(name)
        {
        }

    protected:
        const TString LogPrefix;
    };
    using Schema = TBackupControllerSchema;

    // local transactions
    class TTxInitSchema;
    class TTxInit;

    // tx runners
    void RunTxInitSchema(const TActorContext& ctx);
    void RunTxInit(const TActorContext& ctx);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BACKUP_CONTROLLER_TABLET;
    }

    explicit TBackupController(const TActorId& tablet, TTabletStorageInfo* info);

    STFUNC(StateInit);

    STFUNC(StateWork);

    void OnDetach(const TActorContext& ctx) override;

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;

    void OnActivateExecutor(const TActorContext& ctx) override;

    void DefaultSignalTabletActive(const TActorContext& ctx) override;

    void SwitchToWork(const TActorContext& ctx);

    void Reset();
};

} // namespace NKikimr::NBackup
