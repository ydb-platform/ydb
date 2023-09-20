#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

////////////////////////////////////////////////////////////////////////////////

#define TABLE_SERIES "series"
#define TABLE_SERIES_REV_VIEWS "series_rev_views"

struct TSeries {
    ui64 SeriesId;
    TString Title;
    TString SeriesInfo;
    TInstant ReleaseDate;
    ui64 Views;
};

////////////////////////////////////////////////////////////////////////////////

enum class ECmd {
    NONE,
    CREATE_TABLES,
    DROP_TABLES,
    UPDATE_VIEWS,
    LIST_SERIES,
    GENERATE_SERIES,
    DELETE_SERIES,
};

TString GetCmdList();
ECmd ParseCmd(const char* cmd);

////////////////////////////////////////////////////////////////////////////////

TString JoinPath(const TString& prefix, const TString& path);

////////////////////////////////////////////////////////////////////////////////

class TYdbErrorException : public yexception {
public:
    TYdbErrorException(NYdb::TStatus status)
        : Status(std::move(status))
    { }

    friend IOutputStream& operator<<(IOutputStream& out, const TYdbErrorException& e) {
        out << "Status: " << e.Status.GetStatus();
        if (e.Status.GetIssues()) {
            out << Endl;
            e.Status.GetIssues().PrintTo(out);
        }
        return out;
    }

private:
    NYdb::TStatus Status;
};

inline void ThrowOnError(NYdb::TStatus status) {
    if (!status.IsSuccess()) {
        throw TYdbErrorException(status) << status;
    }
}

////////////////////////////////////////////////////////////////////////////////

int RunCreateTables(NYdb::TDriver& driver, const TString& prefix, int argc, char** argv);
int RunDropTables(NYdb::TDriver& driver, const TString& prefix, int argc, char** argv);
int RunUpdateViews(NYdb::TDriver& driver, const TString& prefix, int argc, char** argv);
int RunListSeries(NYdb::TDriver& driver, const TString& prefix, int argc, char** argv);
int RunGenerateSeries(NYdb::TDriver& driver, const TString& prefix, int argc, char** argv);
int RunDeleteSeries(NYdb::TDriver& driver, const TString& prefix, int argc, char** argv);
