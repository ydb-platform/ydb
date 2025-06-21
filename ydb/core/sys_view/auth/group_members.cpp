#include "auth_scan_base.h"
#include "group_members.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/login/protos/login.pb.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView::NAuth {

using namespace NSchemeShard;
using namespace NActors;

class TGroupMembersScan : public TAuthScanBase<TGroupMembersScan> {
public:
    using TScanBase = TScanActorBase<TGroupMembersScan>;
    using TAuthBase = TAuthScanBase<TGroupMembersScan>;

    TGroupMembersScan(const NActors::TActorId& ownerId, ui32 scanId,
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

        TVector<std::pair<const TDomainInfo::TGroup*, const TString*>> memberships;
        for (const auto& group : entry.DomainInfo->Groups) {
            for (const auto& member : group.Members) {
                if (StringKeyIsInTableRange({group.Sid, member})) {
                    memberships.emplace_back(&group, &member);
                }
            }
        }
        SortBatch(memberships, [](const auto& left, const auto& right) {
            return left.first->Sid < right.first->Sid ||
                left.first->Sid == right.first->Sid && *left.second < *right.second;
        });

        TVector<TCell> cells(::Reserve(Columns.size()));

        for (const auto& [group, member] : memberships) {
            for (auto& column : Columns) {
                switch (column.Tag) {
                case Schema::AuthGroupMembers::GroupSid::ColumnId:
                    cells.push_back(TCell(group->Sid.data(), group->Sid.size()));
                    break;
                case Schema::AuthGroupMembers::MemberSid::ColumnId:
                    cells.push_back(TCell(member->data(), member->size()));
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

THolder<NActors::IActor> CreateGroupMembersScan(const NActors::TActorId& ownerId, ui32 scanId,
    const NKikimrSysView::TSysViewDescription& sysViewInfo, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken)
{
    return MakeHolder<TGroupMembersScan>(ownerId, scanId, sysViewInfo, tableRange, columns, std::move(userToken));
}

}
