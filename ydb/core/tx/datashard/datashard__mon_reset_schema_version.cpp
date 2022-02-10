#include "datashard_impl.h"
#include "operation.h"

#include <ydb/core/tablet_flat/flat_stat_table.h>
#include <ydb/core/util/pb.h>

#include <library/cpp/mime/types/mime.h>
#include <library/cpp/resource/resource.h>

#include <library/cpp/html/pcdata/pcdata.h>

namespace NKikimr {
namespace NDataShard {

class TDataShard::TTxMonitoringResetSchemaVersion
    : public NTabletFlatExecutor::TTransactionBase<TDataShard>
{
public:
    TTxMonitoringResetSchemaVersion(TDataShard *self, NMon::TEvRemoteHttpInfo::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        Updates = 0;
        for (const auto& kv : Self->GetUserTables()) {
            TUserTable::TPtr copy = new TUserTable(*kv.second);
            if (copy->ResetTableSchemaVersion()) {
                Self->PersistUserTable(db, kv.first, *copy);
                Self->AddUserTable(TPathId(Self->GetPathOwnerId(), kv.first), copy);
                ++Updates;
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        TStringStream str;
        HTML(str) {
            PRE() {
                str << "Updated " << Updates << " tables" << Endl;
            }
        }

        ctx.Send(Ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
    }

private:
    NMon::TEvRemoteHttpInfo::TPtr Ev;

    size_t Updates = 0;
};

ITransaction* TDataShard::CreateTxMonitoringResetSchemaVersion(
        TDataShard* self,
        NMon::TEvRemoteHttpInfo::TPtr ev)
{
    return new TTxMonitoringResetSchemaVersion(self, ev);
}

}
}
