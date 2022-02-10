#include "flat_local_tx_factory.h"

#include "flat_local_minikql_program.h"
#include "flat_local_tx_minikql.h"
#include "flat_local_tx_read_columns.h"
#include "flat_local_tx_scheme.h"

namespace NKikimr {
namespace NMiniKQL {

using ITransaction = TMiniKQLFactory::ITransaction;

TAutoPtr<ITransaction> TMiniKQLFactory::Make(TEvTablet::TEvLocalMKQL::TPtr &ev)
{
    TLocalMiniKQLProgram program(*ev->Get());

    return new TFlatLocalMiniKQL(ev->Sender, program, this);
}

TAutoPtr<ITransaction> TMiniKQLFactory::Make(TEvTablet::TEvLocalSchemeTx::TPtr &ev)
{
    return new TFlatLocalSchemeTx(ev->Sender, ev);
}

TAutoPtr<ITransaction> TMiniKQLFactory::Make(TEvTablet::TEvLocalReadColumns::TPtr &ev)
{
    return new TFlatLocalReadColumns(ev->Sender, ev);
}

TRowVersion TMiniKQLFactory::GetWriteVersion(const TTableId& tableId) const
{
    Y_UNUSED(tableId);
    return TRowVersion::Min();
}

TRowVersion TMiniKQLFactory::GetReadVersion(const TTableId& tableId) const
{
    Y_UNUSED(tableId);
    return TRowVersion::Max();
}

IChangeCollector* TMiniKQLFactory::GetChangeCollector(const TTableId& tableId) const
{
    Y_UNUSED(tableId);
    return nullptr;
}

}
}
