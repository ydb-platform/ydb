#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/getopt/last_getopt.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

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
    TString Database;
    TString Table;
    TString IndexType;
    TString IndexQuantizer;
    TString PrimaryKey;
    TString Embedding;
    TString Distance;
    TString Data;
    TString Target;
    ui64 Rows = 0;
    ui64 TopK = 0;
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
