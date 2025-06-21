#include "auth_scan_base.h"
#include "owners.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/login/protos/login.pb.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView::NAuth {

using namespace NSchemeShard;
using namespace NActors;

class TOwnersScan : public TAuthScanBase<TOwnersScan> {
public:
    using TScanBase = TScanActorBase<TOwnersScan>;
    using TAuthBase = TAuthScanBase<TOwnersScan>;

    TOwnersScan(const NActors::TActorId& ownerId, ui32 scanId,
        const NKikimrSysView::TSysViewDescription& sysViewInfo,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken)
        : TAuthBase(ownerId, scanId, sysViewInfo, tableRange, columns, std::move(userToken), false, true)
    {
    }

protected:
    void FillBatch(NKqp::TEvKqpCompute::TEvScanData& batch, const TNavigate::TEntry& entry) override {
        Y_ABORT_UNLESS(entry.Status == TNavigate::EStatus::Ok);

        TVector<TCell> cells(::Reserve(Columns.size()));

        auto entryPath = CanonizePath(entry.Path);

        if (StringKeyIsInTableRange({entryPath})) {
            for (auto& column : Columns) {
                switch (column.Tag) {
                case Schema::AuthOwners::Path::ColumnId:
                    cells.push_back(TCell(entryPath.data(), entryPath.size()));
                    break;
                case Schema::AuthOwners::Sid::ColumnId:
                    if (entry.SecurityObject && entry.SecurityObject->HasOwnerSID()) {
                        cells.push_back(TCell(entry.SecurityObject->GetOwnerSID().data(), entry.SecurityObject->GetOwnerSID().size()));
                    } else {
                        cells.emplace_back();
                    }
                    break;
                default:
                    cells.emplace_back();
                }
            }

            TArrayRef<const TCell> ref(cells);
            batch.Rows.emplace_back(TOwnedCellVec::Make(ref));
            cells.clear();
        }

        batch.Finished = false;
    }
};

THolder<NActors::IActor> CreateOwnersScan(const NActors::TActorId& ownerId, ui32 scanId,
    const NKikimrSysView::TSysViewDescription& sysViewInfo, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken)
{
    return MakeHolder<TOwnersScan>(ownerId, scanId, sysViewInfo, tableRange, columns, std::move(userToken));
}

}
