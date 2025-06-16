#include "yql_yt_table_data_service_local_interface.h"

template<>
void Out<NYql::NFmr::TTableDataServiceStats>(IOutputStream& out, const NYql::NFmr::TTableDataServiceStats& stats) {
    out << "Current table data service stats: " << stats.KeysNum << " keys, " << stats.DataWeight << " data weight\n";
}
