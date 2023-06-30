#include "yql_mkql_table.h"

#include <ydb/library/yql/public/udf/udf_value.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

TVector<NUdf::TUnboxedValue> MakeTablePaths(const TTypeEnvironment& env, const TVector<TString>& paths) {
    TVector<NUdf::TUnboxedValue> tablePaths;
    tablePaths.reserve(paths.size());
    for (auto& path: paths) {
        if (path) {
            tablePaths.emplace_back(env.NewStringValue(path));
        } else {
            tablePaths.emplace_back();
        }
    }
    return tablePaths;
}

TTableState::TTableState(const TTypeEnvironment& env, const TVector<TString>& paths, const TVector<ui64>& recordOffsets,
    TComputationContext& ctx, const std::array<IComputationExternalNode*, 5>& nodes)
    : TTableState(MakeTablePaths(env, paths), recordOffsets, ctx, nodes)
{
}

TTableState::TTableState(const TVector<NUdf::TUnboxedValue>& paths, const TVector<ui64>& rowOffsets, TComputationContext& ctx,
    const std::array<IComputationExternalNode*, 5>& nodes)
{
    auto tableIndexNode = nodes[0];
    if (tableIndexNode) {
        UpdateTableIndex = [&ctx, tableIndexNode] (bool valid, ui32 tableIndex) {
            tableIndexNode->SetValue(ctx, valid ? NUdf::TUnboxedValuePod(tableIndex) : NUdf::TUnboxedValue::Zero());
        };
    } else {
        UpdateTableIndex = [] (bool, ui32) {};
    }

    auto tablePathNode = nodes[1];
    if (tablePathNode && paths) {
        UpdateTablePath = [&ctx, tablePathNode, paths] (bool valid, ui32 tableIndex) {
            if (valid && paths.at(tableIndex)) {
                tablePathNode->SetValue(ctx, NUdf::TUnboxedValue(paths.at(tableIndex)));
            } else {
                tablePathNode->SetValue(ctx, NUdf::TUnboxedValue::Zero());
            }
        };
    } else {
        UpdateTablePath = [] (bool, ui32) {};
    }

    auto tableRecordNode = nodes[2];
    if (tableRecordNode && paths) {
        UpdateTableRecord = [&ctx, tableRecordNode, paths] (bool valid, ui32 tableIndex, ui64 rowNumber) {
            if (valid && paths.at(tableIndex)) {
                tableRecordNode->SetValue(ctx, NUdf::TUnboxedValuePod(rowNumber));
            } else {
                tableRecordNode->SetValue(ctx, NUdf::TUnboxedValue::Zero());
            }
        };
    } else {
        UpdateTableRecord = [] (bool, ui32, ui64) {};
    }

    auto isKeySwitchNode = nodes[3];
    if (isKeySwitchNode) {
        UpdateIsKeySwitch = [&ctx, isKeySwitchNode] (bool keySwitch) {
// TODO:correct set key switch (as hidden column, as example).
//          isKeySwitchNode->SetValue(ctx, NUdf::TUnboxedValuePod(keySwitch));
            ctx.MutableValues[isKeySwitchNode->GetIndex()] = NUdf::TUnboxedValuePod(keySwitch);
        };
    } else {
        UpdateIsKeySwitch = [] (bool) {};
    }

    auto rowNumberNode = nodes[4];
    if (rowNumberNode && rowOffsets) {
        UpdateRowNumber = [&ctx, rowNumberNode, rowOffsets] (bool valid, ui32 tableIndex, ui64 rowNumber) {
            if (valid) {
                rowNumberNode->SetValue(ctx, NUdf::TUnboxedValuePod(rowOffsets.at(tableIndex) + rowNumber));
            } else {
                rowNumberNode->SetValue(ctx, NUdf::TUnboxedValue::Zero());
            }
        };
    } else {
        UpdateRowNumber = [] (bool, ui32, ui64) {};
    }
}

void TTableState::Reset() {
    UpdateTableIndex(false, 0);
    UpdateTablePath(false, 0);
    UpdateTableRecord(false, 0, 0);
    UpdateIsKeySwitch(false);
    UpdateRowNumber(false, 0, 0);
}

void TTableState::Update(ui32 tableIndex, ui64 tableRecord, bool keySwitch) {
    UpdateTableIndex(true, tableIndex);
    UpdateTablePath(true, tableIndex);
    UpdateTableRecord(true, tableIndex, tableRecord);
    UpdateIsKeySwitch(keySwitch);
    UpdateRowNumber(true, tableIndex, tableRecord);
}


} // NYql
