#pragma once

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NKikimr::NKqp {

//! Result of the wasm UDF string argument analysis for a single physical stage.
struct TWasmUdfStringColumns {
    //! Physical table column names whose values reach a string argument of
    //! Apply(Udf, ...) inside this very stage. A value materialized into WASM
    //! linear memory does not survive a channel between stages (tasks have
    //! separate allocators and repack data), so cross-stage names are useless.
    THashSet<TString> Columns;
    //! Stage owns a table read that honors PreferWasm: either a row source
    //! (KqpRowsSourceSettings) or a wide read inside the program.
    bool HasTableRead = false;
    bool HasUdfCall = false;

    bool CanMaterializeInWasm() const {
        return HasTableRead && HasUdfCall && !Columns.empty();
    }
};

//! Resolves UDF string arguments back to physical read columns of |stage|.
//! Bails out (returns no columns) on any expression shape it cannot follow.
TWasmUdfStringColumns CollectWasmUdfStringColumns(const NYql::NNodes::TDqPhyStage& stage);

} // namespace NKikimr::NKqp
