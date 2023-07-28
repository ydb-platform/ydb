#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>
#include <array>

namespace NYql {

TVector<NUdf::TUnboxedValue> MakeTablePaths(const NKikimr::NMiniKQL::TTypeEnvironment& env, const TVector<TString>& paths);

class TTableState {
public:
    TTableState(const NKikimr::NMiniKQL::TTypeEnvironment& env, const TVector<TString>& paths,
        const TVector<ui64>& recordOffsets, NKikimr::NMiniKQL::TComputationContext& ctx,
        const std::array<NKikimr::NMiniKQL::IComputationExternalNode*, 5>& nodes);

    TTableState(const TVector<NUdf::TUnboxedValue>& paths, const TVector<ui64>& recordOffsets,
        NKikimr::NMiniKQL::TComputationContext& ctx,
        const std::array<NKikimr::NMiniKQL::IComputationExternalNode*, 5>& nodes);

    TTableState(const TTableState&) = default;
    TTableState(TTableState&&) = default;
    ~TTableState() = default;

    void Reset();
    void Update(ui32 tableIndex, ui64 tableRecord, bool keySwitch = false);

private:
    std::function<void(bool, ui32)> UpdateTableIndex;
    std::function<void(bool, ui32)> UpdateTablePath;
    std::function<void(bool, ui32, ui64)> UpdateTableRecord;
    std::function<void(bool)> UpdateIsKeySwitch;
    std::function<void(bool, ui32, ui64)> UpdateRowNumber;
};

} // NYql
