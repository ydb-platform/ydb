#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/getopt/last_getopt.h>

#include <format>

////////////////////////////////////////////////////////////////////////////////

#define TABLE_SERIES "series"
#define TABLE_SERIES_REV_VIEWS "series_rev_views"

struct TSeries {
    uint64_t SeriesId;
    std::string Title;
    std::string SeriesInfo;
    TInstant ReleaseDate;
    uint64_t Views;
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

std::string GetCmdList();
ECmd ParseCmd(const char* cmd);

////////////////////////////////////////////////////////////////////////////////

std::string JoinPath(const std::string& prefix, const std::string& path);

////////////////////////////////////////////////////////////////////////////////

int RunCreateTables(NYdb::TDriver& driver, const std::string& prefix, int argc, char** argv);
int RunDropTables(NYdb::TDriver& driver, const std::string& prefix, int argc, char** argv);
int RunUpdateViews(NYdb::TDriver& driver, const std::string& prefix, int argc, char** argv);
int RunListSeries(NYdb::TDriver& driver, const std::string& prefix, int argc, char** argv);
int RunGenerateSeries(NYdb::TDriver& driver, const std::string& prefix, int argc, char** argv);
int RunDeleteSeries(NYdb::TDriver& driver, const std::string& prefix, int argc, char** argv);
