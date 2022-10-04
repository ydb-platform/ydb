#include "tool.h"
#include "cook.h"
#include "mass.h"

#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/flat_row_misc.h>
#include <ydb/core/scheme/scheme_type_registry.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

NScheme::TKikimrTypeRegistry TypeRegistry;

const NScheme::TTypeRegistry* DbgRegistry()
{
    return &TypeRegistry;
}

TString PrintValue(const TCell& r, NScheme::TTypeId typeId)
{
    return DbgPrintCell(r, NScheme::TTypeInfo(typeId), TypeRegistry);
}

}
}
}
