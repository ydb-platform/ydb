#include "incr_restore_helpers.h"

#include <ydb/core/protos/datashard_backup.pb.h>

namespace NKikimr::NDataShard::NIncrRestoreHelpers {

std::optional<TVector<TUpdateOp>> MakeRestoreUpdates(TArrayRef<const TCell> cells, TArrayRef<const TTag> tags, const TMap<ui32, TUserTable::TUserColumn>& columns) {
    Y_ENSURE(cells.size() >= 1);
    TVector<TUpdateOp> updates(::Reserve(cells.size() - 1));

    int specialColumnCount = 0;
    NKikimrBackup::TChangeMetadata changeMetadata;
    bool hasChangeMetadata = false;
    
    Y_ENSURE(cells.size() == tags.size());
    
    for (TPos pos = 0; pos < cells.size(); ++pos) {
        const auto tag = tags.at(pos);
        auto it = columns.find(tag);
        Y_ENSURE(it != columns.end());
        
        if (it->second.Name == "__ydb_incrBackupImpl_changeMetadata") {
            if (const auto& cell = cells.at(pos); !cell.IsNull()) {
                TString serializedMetadata(cell.Data(), cell.Size());
                if (!serializedMetadata.empty()) {
                    if (changeMetadata.ParseFromString(serializedMetadata)) {
                        hasChangeMetadata = true;
                    }
                }
            }
            specialColumnCount++;
        }
    }
    
    Y_ENSURE(specialColumnCount == 1);
    
    // Check if this is a deleted row
    if (hasChangeMetadata && changeMetadata.GetIsDeleted()) {
        return std::nullopt;
    }
    
    struct TColumnState {
        bool IsNull = false;
        bool IsChanged = false;
    };
    
    THashMap<ui32, TColumnState> tagToColumnState;
    if (hasChangeMetadata) {
        for (const auto& columnState : changeMetadata.GetColumnStates()) {
            tagToColumnState[columnState.GetTag()] = {columnState.GetIsNull(), columnState.GetIsChanged()};
        }
    }
    
    for (TPos pos = 0; pos < cells.size(); ++pos) {
        const auto tag = tags.at(pos);
        auto it = columns.find(tag);
        Y_ENSURE(it != columns.end());
        
        if (it->second.Name == "__ydb_incrBackupImpl_changeMetadata") {
            continue;
        }
        
        
        if (hasChangeMetadata) {
            auto columnStateIt = tagToColumnState.find(tag);
            
            if (columnStateIt != tagToColumnState.end() && !columnStateIt->second.IsChanged) {
                continue;
            } else if (columnStateIt != tagToColumnState.end() && columnStateIt->second.IsNull) {
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
