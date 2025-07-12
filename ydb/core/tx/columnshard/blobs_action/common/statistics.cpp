#include "statistics.h"

namespace NKikimr::NColumnShard {

void AddTableAccessStatsToTxStats(NKikimrQueryStats::TTxStats& stats, ui64 pathId, ui64 rows, ui64 bytes,
                                  NEvWrite::EModificationType modificationType) {
    auto tableStats = stats.AddTableAccessStats();
    tableStats->MutableTableInfo()->SetPathId(pathId);
    auto row = modificationType == NEvWrite::EModificationType::Delete ? tableStats->MutableEraseRow()
                                                                       : tableStats->MutableUpdateRow();
    row->SetCount(rows);
    row->SetRows(rows);
    row->SetBytes(bytes);
}

} // namespace NKikimr::NColumnShard
