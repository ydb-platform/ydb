#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/getopt/last_getopt.h>

enum class ECommand {
    DropIndex,
    CreateIndex,
    BuildIndex,  
    RecreateIndex, // Drop, Create, Build
    TopK,
    None,
};

ECommand Parse(std::string_view command);

struct TOptions {
    std::string Database;
    std::string Table;
    std::string IndexType;
    std::string IndexQuantizer;
    std::string PrimaryKey;
    std::string Embedding;
    std::string Distance;
    std::string Data;
    std::string Target;
    uint64_t Rows = 0;
    uint64_t TopK = 0;
};

int DropIndex(NYdb::TDriver& driver, const TOptions& options);

int CreateIndex(NYdb::TDriver& driver, const TOptions& options);

int BuildIndex(NYdb::TDriver& driver, const TOptions& options);

int TopK(NYdb::TDriver& driver, const TOptions& options);

class TVectorException: public yexception {
public:
    TVectorException(const NYdb::TStatus& status) {
        *this << "Status:" << status;
    }
};
