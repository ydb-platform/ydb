#pragma once

#include <library/cpp/yson/node/node.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>


namespace NYql {

NYT::TNode TablesSpecToInputSkiff(const NYT::TNode& spec, const THashMap<TString, ui32>& structColumns, bool rowIndex, bool rangeIndex, bool keySwitch);
NYT::TNode SingleTableSpecToInputSkiff(const NYT::TNode& spec, const THashMap<TString, ui32>& structColumns, bool rowIndex, bool rangeIndex, bool keySwitch);
NYT::TNode SingleTableSpecToInputSkiffSchema(const NYT::TNode& spec, size_t tableIndex, const THashMap<TString, ui32>& structColumns, const THashSet<TString>& auxColumns, bool rowIndex, bool rangeIndex, bool keySwitch);

NYT::TNode TablesSpecToOutputSkiff(const NYT::TNode& spec);
NYT::TNode SingleTableSpecToOutputSkiff(const NYT::TNode& spec, size_t tableIndex);

} // NYql
