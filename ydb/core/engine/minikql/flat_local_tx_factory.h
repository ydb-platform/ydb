#pragma once

#include "change_collector_iface.h"

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr {
namespace NMiniKQL {

    struct TMiniKQLFactory : NTabletFlatExecutor::IMiniKQLFactory {
        using ITransaction = NTabletFlatExecutor::ITransaction;

        TAutoPtr<ITransaction> Make(TEvTablet::TEvLocalMKQL::TPtr&) override;
        TAutoPtr<ITransaction> Make(TEvTablet::TEvLocalSchemeTx::TPtr&) override;
        TAutoPtr<ITransaction> Make(TEvTablet::TEvLocalReadColumns::TPtr &ev) override;

        virtual TRowVersion GetWriteVersion(const TTableId& tableId) const;
        virtual TRowVersion GetReadVersion(const TTableId& tableId) const;
        virtual IChangeCollector* GetChangeCollector(const TTableId& tableId) const;
    };

}
}
