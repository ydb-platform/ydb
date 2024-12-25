#include "auth_scan.h"
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

class TGroupsScan : public TAuthScan<TGroupsScan> {
public:
    using TScanBase = TScanActorBase<TGroupsScan>;
    using TAuthBase = TAuthScan<TGroupsScan>;

    TGroupsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TAuthBase(ownerId, scanId, tableId, tableRange, columns)
    {
    }

protected:
    void FillBatch(NKqp::TEvKqpCompute::TEvScanData& batch, const ::NKikimrSchemeOp::TPathDescription& description) override {
        TVector<TCell> cells(::Reserve(Columns.size()));

        // TODO: add rows according to request's sender user rights

        const auto& sids = description.GetDomainDescription().GetSecurityState().GetSids();
        for (const auto& sid : sids) {
            if (sid.GetType() != NLoginProto::ESidType_SidType_GROUP) {
                continue;
            }

            for (auto& column : Columns) {
                switch (column.Tag) {
                case Schema::AuthGroups::Name::ColumnId:
                    cells.push_back(TCell(sid.GetName().data(), sid.GetName().size()));
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

THolder<NActors::IActor> CreateGroupsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TGroupsScan>(ownerId, scanId, tableId, tableRange, columns);
}

}
