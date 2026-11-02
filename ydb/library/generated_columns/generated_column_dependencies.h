#pragma once

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NGeneratedColumns {

struct TGeneratedColumnDependencyGraph {
    TMap<TString, TVector<TString>> Dependencies;
    THashMap<TString, TVector<TString>> Dependents;
    TVector<TString> EvaluationOrder;
};

bool BuildGeneratedColumnDependencyGraph(
    const TVector<TString>& columnNames,
    const TMap<TString, TVector<TString>>& directDependencies,
    TGeneratedColumnDependencyGraph& result,
    TString& error);

THashSet<TString> CollectAffectedGeneratedColumns(
    const TGeneratedColumnDependencyGraph& graph,
    const THashSet<TString>& changedColumns);

} // namespace NKikimr::NGeneratedColumns
