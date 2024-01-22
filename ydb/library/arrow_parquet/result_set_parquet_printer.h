#pragma once

#include <util/system/types.h>

#include <memory>
#include <string>

namespace NYdb {

    class TResultSet;

    class TResultSetParquetPrinter {
    public:
        explicit TResultSetParquetPrinter(const std::string& outputPath, ui64 rowGroupSize = 100000);
        ~TResultSetParquetPrinter();
        void Reset();
        void Print(const TResultSet& resultSet);

    private:
        class TImpl;
        std::unique_ptr<TImpl> Impl;
    };

}
