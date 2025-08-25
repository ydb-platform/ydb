#include "auth_scan_base.h"
#include "groups.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/login/protos/login.pb.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView::NAuth {

using namespace NSchemeShard;
using namespace NActors;

class TGroupsScan : public TAuthScanBase<TGroupsScan> {
public:
    using TScanBase = TScanActorBase<TGroupsScan>;
    using TAuthBase = TAuthScanBase<TGroupsScan>;

    TGroupsScan(const NActors::TActorId& ownerId, ui32 scanId,
        const NKikimrSysView::TSysViewDescription& sysViewInfo,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken)
        : TAuthBase(ownerId, scanId, sysViewInfo, tableRange, columns, std::move(userToken), true, false)
    {
    }

protected:
    void FillBatch(NKqp::TEvKqpCompute::TEvScanData& batch, const TNavigate::TEntry& entry) override {
        Y_ABORT_UNLESS(entry.Status == TNavigate::EStatus::Ok);
        Y_ABORT_UNLESS(CanonizePath(entry.Path) == TBase::TenantName);

        TVector<const TDomainInfo::TGroup*> groups(::Reserve(entry.DomainInfo->Groups.size()));
        for (const auto& group : entry.DomainInfo->Groups) {
            if (StringKeyIsInTableRange({group.Sid})) {
                groups.push_back(&group);
            }
        }
        SortBatch(groups, [](const auto* left, const auto* right) {
            return left->Sid < right->Sid;
        });

        TVector<TCell> cells(::Reserve(Columns.size()));

        for (const auto* group : groups) {
            for (auto& column : Columns) {
                switch (column.Tag) {
                case Schema::AuthGroups::Sid::ColumnId:
                    cells.push_back(TCell(group->Sid.data(), group->Sid.size()));
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

THolder<NActors::IActor> CreateGroupsScan(const NActors::TActorId& ownerId, ui32 scanId,
    const NKikimrSysView::TSysViewDescription& sysViewInfo, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken)
{
    return MakeHolder<TGroupsScan>(ownerId, scanId, sysViewInfo, tableRange, columns, std::move(userToken));
}

}
