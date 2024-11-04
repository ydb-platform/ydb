#include "flat_part_laid.h"
#include "flat_stat_table.h"
#include "flat_table_subset.h"
#include "flat_stat_table_btree_index.h"
#include "flat_stat_table_mixed_index.h"

namespace NKikimr {
namespace NTable {

bool BuildStats(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, ui32 histogramBucketsCount, IPages* env, TBuildStatsYieldHandler yieldHandler) {
    stats.Clear();

    bool mixedIndex = false;
    for (const auto& part : subset.Flatten) {
        if (!part->IndexPages.HasBTree() && part->IndexPages.HasFlat()) {
            mixedIndex = true;
        }
    }

    return mixedIndex
        ? BuildStatsMixedIndex(subset, stats, rowCountResolution, dataSizeResolution, env, yieldHandler)
        : BuildStatsBTreeIndex(subset, stats, histogramBucketsCount, env, yieldHandler);
}

void GetPartOwners(const TSubset& subset, THashSet<ui64>& partOwners) {
    for (const auto& partView : subset.Flatten) {
        partOwners.insert(partView->Label.TabletID());
    }
    for (const auto& coldPart : subset.ColdParts) {
        partOwners.insert(coldPart->Label.TabletID());
    }
    for (const auto& txStatus : subset.TxStatus) {
        partOwners.insert(txStatus->Label.TabletID());
    }
}

bool HasSchemaChanges(const TPartView& partView, const TScheme::TTableInfo& tableInfo, const NTable::TRowScheme& rowScheme, bool enableBTreeIndex) {
    if (partView.Part->Stat.Rows == 0) {
        return false;
    }

    { // Check by key filter existence
        bool partByKeyFilter = bool(partView->ByKey);
        bool schemeByKeyFilter = tableInfo.ByKeyFilter;
        if (partByKeyFilter != schemeByKeyFilter) {
            return true;
        }
    }

    { // Check B-Tree index existence
        if (enableBTreeIndex && !partView->IndexPages.HasBTree()) {
            return true;
        }
    }

    { // Check families
        size_t partFamiliesCount = partView->GroupsCount;
        size_t schemeFamiliesCount = rowScheme.Families.size();
        if (partFamiliesCount != schemeFamiliesCount) {
            return true;
        }

        for (size_t index : xrange(rowScheme.Families.size())) {
            auto familyId = rowScheme.Families[index];
            static const NTable::TScheme::TFamily defaultFamilySettings;
            const auto& family = tableInfo.Families.Value(familyId, defaultFamilySettings); // Workaround for KIKIMR-17222

            const auto* schemeGroupRoom = tableInfo.Rooms.FindPtr(family.Room);
            Y_ABORT_UNLESS(schemeGroupRoom, "Cannot find room %" PRIu32 " in table %" PRIu32, family.Room, tableInfo.Id);

            ui32 partGroupChannel = partView.Part->GetGroupChannel(NPage::TGroupId(index));
            if (partGroupChannel != schemeGroupRoom->Main) {
                return true;
            }
        }
    }

    { // Check columns
        THashMap<NTable::TTag, ui32> partColumnGroups, schemeColumnGroups;
        for (const auto& column : partView->Scheme->AllColumns) {
            partColumnGroups[column.Tag] = column.Group;
        }
        for (const auto& col : rowScheme.Cols) {
            schemeColumnGroups[col.Tag] = col.Group;
        }
        if (partColumnGroups != schemeColumnGroups) {
            return true;
        }
    }

    return false;
}

bool HasSchemaChanges(const TSubset& subset, const TScheme::TTableInfo& tableInfo, bool enableBTreeIndex) {
    for (const auto& partView : subset.Flatten) {
        if (HasSchemaChanges(partView, tableInfo, *subset.Scheme, enableBTreeIndex)) {
            return true;
        }
    }

    return false;
}

}}
