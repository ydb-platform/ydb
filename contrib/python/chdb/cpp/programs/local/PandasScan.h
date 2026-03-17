#pragma once

#include "PybindWrapper.h"
#include "PythonUtils.h"

namespace CHDB {

class PandasScan {
public:
    static DB_CHDB::ColumnPtr scanObject(
        const DB_CHDB::ColumnWrapper & col_wrap,
        const size_t cursor,
        const size_t count,
        const DB_CHDB::FormatSettings & format_settings);

    static void scanObject(
        const size_t cursor,
        const size_t count,
        const DB_CHDB::FormatSettings & format_settings,
        const void * buf,
        DB_CHDB::MutableColumnPtr & column);

private:
    static void innerCheck(const DB_CHDB::ColumnWrapper & col_wrap);

    static void innerScanObject(
        const size_t cursor,
        const size_t count,
        const DB_CHDB::FormatSettings & format_settings,
        DB_CHDB::SerializationPtr & serialization,
        PyObject ** objects,
        DB_CHDB::MutableColumnPtr & column);
};

} // namespace CHDB
