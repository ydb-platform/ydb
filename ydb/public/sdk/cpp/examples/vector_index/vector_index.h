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
    CreateIndex,
    UpdateIndex, // Fill
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
    ui64 TopK = 0;
};

int CreateIndex(NYdb::TDriver& driver, const TOptions& options);

int UpdateIndex(NYdb::TDriver& driver, const TOptions& options);

int TopK(NYdb::TDriver& driver, const TOptions& options);

class TVectorException: public yexception {
public:
    TVectorException(NYdb::TStatus status)
        : Status(std::move(status))
    {
    }

    friend IOutputStream& operator<<(IOutputStream& out, const TVectorException& e) {
        out << "Status:" << e.Status.GetStatus();
        if (e.Status.GetIssues()) {
            out << Endl;
            e.Status.GetIssues().PrintTo(out);
        }
        return out;
    }

private:
    NYdb::TStatus Status;
};
