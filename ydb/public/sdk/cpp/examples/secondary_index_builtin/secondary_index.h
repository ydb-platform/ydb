#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/getopt/last_getopt.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

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
    ui64 UserId;
    TString Name;
    ui32 Age;
    TUser(ui64 userId = 0, TString name = "", ui32 age = 0)
        : UserId(userId)
        , Name(name)
        , Age(age) {}
};

struct TSeries {
    ui64 SeriesId;
    TString Title;
    TInstant ReleaseDate;
    TString Info;
    ui64 Views;
    ui64 UploadedUserId;

    TSeries(ui64 seriesId = 0, TString title = "", TInstant releaseDate = TInstant::Days(0),
            TString info = "", ui64 views = 0, ui64 uploadedUserId = 0)
        : SeriesId(seriesId)
        , Title(title)
        , ReleaseDate(releaseDate)
        , Info(info)
        , Views(views)
        , UploadedUserId(uploadedUserId) {}
};

class TYdbErrorException: public yexception {
public:
    TYdbErrorException(NYdb::TStatus status)
        : Status(std::move(status))
    { }

    friend IOutputStream& operator<<(IOutputStream& out, const TYdbErrorException&  e) {
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

inline void ThrowOnError(NYdb::TStatus status) {
    if (!status.IsSuccess()){
        throw TYdbErrorException(status) << status;
    }
}

TString GetCommandsList();
TCommand Parse(const char *stringCmnd);
TString JoinPath(const TString& prefix, const TString& path);

void ParseSelectSeries(TVector<TSeries>& parseResult, TResultSetParser&& parser);

int Create(NYdb::TDriver& driver, const TString& path);
int Insert(NYdb::TDriver& driver, const TString& path);
int Drop(NYdb::TDriver& driver, const TString& path);
int SelectJoin(NYdb::TDriver& driver, const TString& path, int argc, char **argv);
int Select(NYdb::TDriver& driver, const TString& path, int argc, char **argv);
