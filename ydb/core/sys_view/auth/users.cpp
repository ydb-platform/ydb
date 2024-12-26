#include "auth_scan_base.h"
#include "users.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/login/protos/login.pb.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView::NAuth {

using namespace NSchemeShard;
using namespace NActors;

class TUsersScan : public TAuthScanBase<TUsersScan> {
public:
    using TScanBase = TScanActorBase<TUsersScan>;
    using TAuthBase = TAuthScanBase<TUsersScan>;

    TUsersScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TAuthBase(ownerId, scanId, tableId, tableRange, columns)
    {
    }

protected:
    void FillBatch(NKqp::TEvKqpCompute::TEvScanData& batch, const TNavigate::TResultSet& resultSet) override {
        Y_ABORT_UNLESS(resultSet.size() == 1);
        auto& entry = resultSet.back();
        Y_ABORT_UNLESS(entry.Status == TNavigate::EStatus::Ok);
        Y_ABORT_UNLESS(CanonizePath(entry.Path) == TBase::TenantName);
        
        TVector<TCell> cells(::Reserve(Columns.size()));

        // TODO: add rows according to request's sender user rights

        for (const auto& user : entry.DomainInfo->Users) {
            for (auto& column : Columns) {
                switch (column.Tag) {
                case Schema::AuthUsers::Name::ColumnId:
                    cells.push_back(TCell(user.Sid.data(), user.Sid.size()));
                    break;
                default:
                    cells.emplace_back();
                }
            }

            TArrayRef<const TCell> ref(cells);
            batch.Rows.emplace_back(TOwnedCellVec::Make(ref));
            cells.clear();
        }

        batch.Finished = true;
    }
};

THolder<NActors::IActor> CreateUsersScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TUsersScan>(ownerId, scanId, tableId, tableRange, columns);
}

}
