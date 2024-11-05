#pragma once

#include "kikimr_program_builder.h"

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>
#include <ydb/core/scheme_types/scheme_types.h>

#include <util/stream/output.h> // for IOutputStream
#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>


namespace NKikimr {
namespace NMiniKQL {

TReadTarget ExtractFlatReadTarget(TRuntimeNode modeInput);

struct TTableStrings {
    TTableStrings(const TTypeEnvironment& env)
        : SelectRow(env.InternName(TStringBuf("SelectRow")))
        , SelectRange(env.InternName(TStringBuf("SelectRange")))
        , UpdateRow(env.InternName(TStringBuf("UpdateRow")))
        , EraseRow(env.InternName(TStringBuf("EraseRow")))
    {
        All.reserve(10);
        All.insert(SelectRow);
        All.insert(SelectRange);
        All.insert(UpdateRow);
        All.insert(EraseRow);

        DbWrites.insert(UpdateRow);
        DbWrites.insert(EraseRow);
    }

    const TInternName SelectRow;
    const TInternName SelectRange;
    const TInternName UpdateRow;
    const TInternName EraseRow;

    THashSet<TInternName> All;
    THashSet<TInternName> DbWrites;
};

THolder<TKeyDesc> ExtractTableKey(TCallable& callable, const TTableStrings& strings, const TTypeEnvironment& env);
TVector<THolder<TKeyDesc>> ExtractTableKeys(TExploringNodeVisitor& explorer, const TTypeEnvironment& env);
TTableId ExtractTableId(const TRuntimeNode& node);

template<typename T>
TCell MakeCell(const NUdf::TUnboxedValuePod& value) {
    static_assert(TCell::CanInline(sizeof(T)), "Can't inline data in cell.");
    const auto v = value.Get<T>();
    return TCell(reinterpret_cast<const char*>(&v), sizeof(v));
}

TCell MakeCell(NScheme::TTypeInfo type, const NUdf::TUnboxedValuePod& value,
    const TTypeEnvironment& env, bool copy = true,
    i32 typmod = -1, TMaybe<TString>* error = {});

void FillKeyTupleValue(const NUdf::TUnboxedValue& row, const TVector<ui32>& rowIndices,
    const TVector<NScheme::TTypeInfo>& rowTypes, TVector<TCell>& cells, const TTypeEnvironment& env);

}
}
