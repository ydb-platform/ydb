#include "auth_scan_base.h"
#include "permissions.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/login/protos/login.pb.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView::NAuth {

using namespace NSchemeShard;
using namespace NActors;

class TPermissionsScan : public TAuthScanBase<TPermissionsScan> {
public:
    using TScanBase = TScanActorBase<TPermissionsScan>;
    using TAuthBase = TAuthScanBase<TPermissionsScan>;

    TPermissionsScan(bool effective, const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken)
        : TAuthBase(ownerId, scanId, tableId, tableRange, columns, std::move(userToken), false, true)
        , Effective(effective)
    {
    }

protected:
    void FillBatch(NKqp::TEvKqpCompute::TEvScanData& batch, const TNavigate::TEntry& entry) override {
        Y_ABORT_UNLESS(entry.Status == TNavigate::EStatus::Ok);

        if (!entry.SecurityObject) {
            batch.Finished = false;
            return;
        }

        auto entryPath = CanonizePath(entry.Path);

        TVector<std::pair<TString, TString>> permissions;
        for (const NACLibProto::TACE& ace : entry.SecurityObject->GetACL().GetACE()) {
            if (ace.GetAccessType() != (ui32)NACLib::EAccessType::Allow) {
                continue;
            }
            if (!Effective && ace.GetInherited()) {
                continue;
            }
            if (!ace.HasSID()) {
                continue;
            }

            auto acePermissions = ConvertACLMaskToYdbPermissionNames(ace.GetAccessRight());
            for (const auto& permission : acePermissions) {
                if (StringKeyIsInTableRange({entryPath, ace.GetSID(), permission})) {
                    permissions.emplace_back(ace.GetSID(), std::move(permission));
                }
            }
        }
        // Note: due to rights inheritance permissions may be duplicated
        SortBatch(permissions, [](const auto& left, const auto& right) {
            return left.first < right.first ||
                left.first == right.first && left.second < right.second;
        }, false);

        TVector<TCell> cells(::Reserve(Columns.size()));

        for (const auto& [sid, permission] : permissions) {
            for (auto& column : Columns) {
                switch (column.Tag) {
                case Schema::AuthPermissions::Path::ColumnId:
                    cells.push_back(TCell(entryPath.data(), entryPath.size()));
                    break;
                case Schema::AuthPermissions::Sid::ColumnId:
                    if (sid) {
                        cells.push_back(TCell(sid.data(), sid.size()));
                    } else {
                        cells.emplace_back();
                    }
                    break;
                case Schema::AuthPermissions::Permission::ColumnId:
                    cells.push_back(TCell(permission.data(), permission.size()));
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

private:
    const bool Effective;
};

THolder<NActors::IActor> CreatePermissionsScan(bool effective, const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken)
{
    return MakeHolder<TPermissionsScan>(effective, ownerId, scanId, tableId, tableRange, columns, std::move(userToken));
}

}
