#include "incr_restore_helpers.h"

#include <ydb/core/protos/datashard_backup.pb.h>

namespace NKikimr::NDataShard::NIncrRestoreHelpers {

std::optional<TVector<TUpdateOp>> MakeRestoreUpdates(TArrayRef<const TCell> cells, TArrayRef<const TTag> tags, const TMap<ui32, TUserTable::TUserColumn>& columns) {
    Y_ENSURE(cells.size() >= 1);
    TVector<TUpdateOp> updates(::Reserve(cells.size() - 1));

    int specialColumnCount = 0;
    NKikimrBackup::TColumnStateMap columnStateMap;
    bool deletedFlag = false;
    bool hasNullStateData = false;
    
    Y_ENSURE(cells.size() == tags.size());
    
    for (TPos pos = 0; pos < cells.size(); ++pos) {
        const auto tag = tags.at(pos);
        auto it = columns.find(tag);
        Y_ENSURE(it != columns.end());
        
        if (it->second.Name == "__ydb_incrBackupImpl_deleted") {
            if (const auto& cell = cells.at(pos); !cell.IsNull() && cell.AsValue<bool>()) {
                deletedFlag = true;
            }
            specialColumnCount++;
        } else if (it->second.Name == "__ydb_incrBackupImpl_columnStates") {
            if (const auto& cell = cells.at(pos); !cell.IsNull()) {
                TString serializedNullState(cell.Data(), cell.Size());
                if (!serializedNullState.empty()) {
                    if (columnStateMap.ParseFromString(serializedNullState)) {
                        hasNullStateData = true;
                    }
                }
            }
            specialColumnCount++;
        }
    }
    
    Y_ENSURE(specialColumnCount == 1 || specialColumnCount == 2);
    
    if (deletedFlag) {
        return std::nullopt;
    }
    
    THashMap<ui32, bool> tagToNullState;
    THashMap<ui32, bool> tagToChangedState;
    if (hasNullStateData) {
        for (const auto& columnState : columnStateMap.GetColumnStates()) {
            tagToNullState[columnState.GetTag()] = columnState.GetIsNull();
            tagToChangedState[columnState.GetTag()] = columnState.GetIsChanged();
        }
    }
    
    for (TPos pos = 0; pos < cells.size(); ++pos) {
        const auto tag = tags.at(pos);
        auto it = columns.find(tag);
        Y_ENSURE(it != columns.end());
        
        if (it->second.Name == "__ydb_incrBackupImpl_deleted" || 
            it->second.Name == "__ydb_incrBackupImpl_columnStates") {
            continue;
        }
        
        
        if (hasNullStateData) {
            auto nullStateIt = tagToNullState.find(tag);
            auto changedStateIt = tagToChangedState.find(tag);
            
            if (changedStateIt != tagToChangedState.end() && !changedStateIt->second) {
                continue;
            } else if (nullStateIt != tagToNullState.end() && nullStateIt->second) {
                updates.emplace_back(tag, ECellOp::Set, TRawTypeValue());
            } else {
                updates.emplace_back(tag, ECellOp::Set, TRawTypeValue(cells.at(pos).AsRef(), it->second.Type.GetTypeId()));
            }
        } else {
            updates.emplace_back(tag, ECellOp::Set, TRawTypeValue(cells.at(pos).AsRef(), it->second.Type.GetTypeId()));
        }
    }

    return updates;
}

} // namespace NKikimr::NDataShard::NIncrRestoreHelpers
