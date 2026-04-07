#include "yql_yt_table_data_service_writer.h"
#include <library/cpp/threading/future/wait/wait.h>
#include <util/string/join.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>


namespace NYql::NFmr {

void TFmrTableDataServiceWriter::PutRows() {
    if (TableContent_.Size() == 0) {
        return;
    }
    auto currentYsonContent = TString(TableContent_.Data(), TableContent_.Size());
    PutYsonByColumnGroups(currentYsonContent);
    PartIdChunkStats_.emplace_back(TChunkStats{
        .Rows = CurrentChunkRows_,
        .DataWeight = TableContent_.Size(),
        .SortedChunkStats = TSortedChunkStats{.IsSorted = false}
    });
    ClearTableData();
}

} // namespace NYql::NFmr

