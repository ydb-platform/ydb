#pragma once

#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/flat_row_remap.h>
#include <ydb/core/tablet_flat/flat_mem_warm.h>
#include <ydb/core/scheme/scheme_type_id.h>

namespace NKikimr {
namespace NTable {

    TString PrintRow(const TDbTupleRef& row);
    TString PrintRow(const TMemIter&);

}
}
