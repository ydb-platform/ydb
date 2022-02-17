#include "secondary_index.h"

#include <util/random/random.h>
#include <util/string/printf.h>

using namespace NYdb;
using namespace NYdb::NTable;

TVector<TSeries> GetSeries() {
    TVector<TSeries> series = {
        TSeries(1, "First episode", TInstant::ParseIso8601("2006-01-01"), "Pilot episode.", 1000, 0),
        TSeries(2, "Second episode", TInstant::ParseIso8601("2006-02-01"), "Jon Snow knows nothing.", 2000, 1),
        TSeries(3, "Third episode", TInstant::ParseIso8601("2006-03-01"), "Daenerys is the mother of dragons.", 3000, 2),
        TSeries(4, "Fourth episode", TInstant::ParseIso8601("2006-04-01"), "Jorah Mormont is the king of the friendzone.", 4000, 3),
        TSeries(5, "Fifth episode", TInstant::ParseIso8601("2006-05-01"), "Cercei is not good person.", 5000, 1),
        TSeries(6, "Sixth episode", TInstant::ParseIso8601("2006-06-01"), "Tyrion is not big.", 6000, 2),
        TSeries(7, "Seventh episode", TInstant::ParseIso8601("2006-07-01"), "Tywin should close the door.", 7000, 2),
        TSeries(8, "Eighth episode", TInstant::ParseIso8601("2006-08-01"), "The white walkers are well-organized.", 8000, 3),
        TSeries(9, "Ninth episode", TInstant::ParseIso8601("2006-09-01"), "Dragons can fly.", 9000, 1)
    };

    return series;
}

TVector<TUser> GetUsers() {
    TVector<TUser> users = {
        TUser(0, "Kit Harrington", 32),
        TUser(1, "Emilia Clarke", 32),
        TUser(2, "Jason Momoa", 39),
        TUser(3, "Peter Dinklage", 49)
    };
    return users;
}

TParams Build(const TVector<TSeries>& seriesList, const TVector<TUser>& usersList) {

    TParamsBuilder paramsBuilder;

    auto& seriesParam = paramsBuilder.AddParam("$seriesData");
    seriesParam.BeginList();

    for (auto& series: seriesList) {
        seriesParam.AddListItem()
            .BeginStruct()
            .AddMember("series_id")
                .Uint64(series.SeriesId)
            .AddMember("title")
                .Utf8(series.Title)
            .AddMember("info")
                .Utf8(series.Info)
            .AddMember("release_date")
                .Date(series.ReleaseDate)
            .AddMember("views")
                .Uint64(series.Views)
            .AddMember("uploaded_user_id")
                .Uint64(series.UploadedUserId)
            .EndStruct();
    }

    seriesParam.EndList();
    seriesParam.Build();

    auto& usersParam = paramsBuilder.AddParam("$usersData");
    usersParam.BeginList();

    for (auto& user: usersList) {
        usersParam.AddListItem()
            .BeginStruct()
            .AddMember("user_id")
                .Uint64(user.UserId)
            .AddMember("name")
                .Utf8(user.Name)
            .AddMember("age")
                .Uint32(user.Age)
            .EndStruct();
    }

    usersParam.EndList();
    usersParam.Build();

    return paramsBuilder.Build();
}

static TStatus FillTable(TSession session, const TString& path) {

    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $seriesData AS List<Struct<
            series_id: Uint64,
            title: Utf8,
            info: Utf8,
            release_date: Date,
            views: Uint64,
            uploaded_user_id: Uint64>>;

        DECLARE $usersData AS List<Struct<
            user_id: Uint64,
            name: Utf8,
            age: Uint32>>;


        REPLACE INTO series
        SELECT
            series_id,
            title,
            info,
            release_date,
            views,
            uploaded_user_id
        FROM AS_TABLE($seriesData);

        REPLACE INTO users
        SELECT
            user_id,
            name,
            age
        FROM AS_TABLE($usersData);)", path.c_str());

    TParams seriesParams = Build(GetSeries(), GetUsers());
    return session.ExecuteDataQuery(query,
                            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),seriesParams)
                            .GetValueSync();
}

int Insert(NYdb::TDriver& driver, const TString& path) {

    TTableClient client(driver);
    ThrowOnError(client.RetryOperationSync([path] (TSession session) {
        return FillTable(session, path);
    }));

    return 0;
}

