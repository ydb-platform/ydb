#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/yexception.h>
#include <util/stream/output.h>

#include <library/cpp/getopt/last_getopt.h>

#include <format>

#define TABLE_USERS "users"
#define TABLE_SERIES "series"

using NYdb::TResultSetParser;

enum class TCommand {
    CREATE,
    INSERT,
    SELECT,
    DROP,
    SELECT_JOIN,
    NONE
};

struct TUser {
    uint64_t UserId;
    std::string Name;
    uint32_t Age;
    TUser(uint64_t userId = 0, std::string name = "", uint32_t age = 0)
        : UserId(userId)
        , Name(name)
        , Age(age) {}
};

struct TSeries {
    uint64_t SeriesId;
    std::string Title;
    TInstant ReleaseDate;
    std::string Info;
    uint64_t Views;
    uint64_t UploadedUserId;

    TSeries(uint64_t seriesId = 0, std::string title = "", TInstant releaseDate = TInstant::Days(0),
            std::string info = "", uint64_t views = 0, uint64_t uploadedUserId = 0)
        : SeriesId(seriesId)
        , Title(title)
        , ReleaseDate(releaseDate)
        , Info(info)
        , Views(views)
        , UploadedUserId(uploadedUserId) {}
};

std::string GetCommandsList();
TCommand Parse(const char *stringCmnd);
std::string JoinPath(const std::string& prefix, const std::string& path);

void ParseSelectSeries(std::vector<TSeries>& parseResult, TResultSetParser&& parser);

int Create(NYdb::TDriver& driver, const std::string& path);
int Insert(NYdb::TDriver& driver, const std::string& path);
int Drop(NYdb::TDriver& driver, const std::string& path);
int SelectJoin(NYdb::TDriver& driver, const std::string& path, int argc, char **argv);
int Select(NYdb::TDriver& driver, const std::string& path, int argc, char **argv);
