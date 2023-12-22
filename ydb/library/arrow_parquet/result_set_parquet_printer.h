#pragma once

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb {

    class TResultSetParquetPrinter {
    public:
        explicit TResultSetParquetPrinter(const TString& outputPath, ui64 rowGroupSize = 100000);
        ~TResultSetParquetPrinter();
        void Reset();
        void Print(const TResultSet& resultSet);

    private:
        class TImpl;
        std::unique_ptr<TImpl> Impl;
    };

}
