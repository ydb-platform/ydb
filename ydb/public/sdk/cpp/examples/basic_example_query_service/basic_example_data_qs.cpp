#include "basic_example_qs.h"

using namespace NYdb;
using namespace NYdb::NTable;

struct TSeries {
    ui64 SeriesId;
    TString Title;
    TInstant ReleaseDate;
    TString SeriesInfo;

    TSeries(ui64 seriesId, const TString& title, const TInstant& releaseDate, const TString& seriesInfo)
        : SeriesId(seriesId)
        , Title(title)
        , ReleaseDate(releaseDate)
        , SeriesInfo(seriesInfo) {}
};

struct TSeason {
    ui64 SeriesId;
    ui64 SeasonId;
    TString Title;
    TInstant FirstAired;
    TInstant LastAired;

    TSeason(ui64 seriesId, ui64 seasonId, const TString& title, const TInstant& firstAired, const TInstant& lastAired)
        : SeriesId(seriesId)
        , SeasonId(seasonId)
        , Title(title)
        , FirstAired(firstAired)
        , LastAired(lastAired) {}
};

struct TEpisode {
    ui64 SeriesId;
    ui64 SeasonId;
    ui64 EpisodeId;
    TString Title;
    TInstant AirDate;

    TEpisode(ui64 seriesId, ui64 seasonId, ui64 episodeId, const TString& title, const TInstant& airDate)
        : SeriesId(seriesId)
        , SeasonId(seasonId)
        , EpisodeId(episodeId)
        , Title(title)
        , AirDate(airDate) {}
};

TParams GetTablesDataParams() {
    TVector<TSeries> seriesData = {
        TSeries(1, "IT Crowd",  TInstant::ParseIso8601("2006-02-03"),
            "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "
            "Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."),
        TSeries(2, "Silicon Valley",  TInstant::ParseIso8601("2014-04-06"),
            "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "
            "Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.")
    };

    TVector<TSeason> seasonsData = {
        TSeason(1, 1, "Season 1", TInstant::ParseIso8601("2006-02-03"), TInstant::ParseIso8601("2006-03-03")),
        TSeason(1, 2, "Season 2", TInstant::ParseIso8601("2007-08-24"), TInstant::ParseIso8601("2007-09-28")),
        TSeason(1, 3, "Season 3", TInstant::ParseIso8601("2008-11-21"), TInstant::ParseIso8601("2008-12-26")),
        TSeason(1, 4, "Season 4", TInstant::ParseIso8601("2010-06-25"), TInstant::ParseIso8601("2010-07-30")),
        TSeason(2, 1, "Season 1", TInstant::ParseIso8601("2014-04-06"), TInstant::ParseIso8601("2014-06-01")),
        TSeason(2, 2, "Season 2", TInstant::ParseIso8601("2015-04-12"), TInstant::ParseIso8601("2015-06-14")),
        TSeason(2, 3, "Season 3", TInstant::ParseIso8601("2016-04-24"), TInstant::ParseIso8601("2016-06-26")),
        TSeason(2, 4, "Season 4", TInstant::ParseIso8601("2017-04-23"), TInstant::ParseIso8601("2017-06-25")),
        TSeason(2, 5, "Season 5", TInstant::ParseIso8601("2018-03-25"), TInstant::ParseIso8601("2018-05-13"))
    };

    TVector<TEpisode> episodesData = {
        TEpisode(1, 1, 1, "Yesterday's Jam", TInstant::ParseIso8601("2006-02-03")),
        TEpisode(1, 1, 2, "Calamity Jen", TInstant::ParseIso8601("2006-02-03")),
        TEpisode(1, 1, 3, "Fifty-Fifty", TInstant::ParseIso8601("2006-02-10")),
        TEpisode(1, 1, 4, "The Red Door", TInstant::ParseIso8601("2006-02-17")),
        TEpisode(1, 1, 5, "The Haunting of Bill Crouse", TInstant::ParseIso8601("2006-02-24")),
        TEpisode(1, 1, 6, "Aunt Irma Visits", TInstant::ParseIso8601("2006-03-03")),
        TEpisode(1, 2, 1, "The Work Outing", TInstant::ParseIso8601("2006-08-24")),
        TEpisode(1, 2, 2, "Return of the Golden Child", TInstant::ParseIso8601("2007-08-31")),
        TEpisode(1, 2, 3, "Moss and the German", TInstant::ParseIso8601("2007-09-07")),
        TEpisode(1, 2, 4, "The Dinner Party", TInstant::ParseIso8601("2007-09-14")),
        TEpisode(1, 2, 5, "Smoke and Mirrors", TInstant::ParseIso8601("2007-09-21")),
        TEpisode(1, 2, 6, "Men Without Women", TInstant::ParseIso8601("2007-09-28")),
        TEpisode(1, 3, 1, "From Hell", TInstant::ParseIso8601("2008-11-21")),
        TEpisode(1, 3, 2, "Are We Not Men?", TInstant::ParseIso8601("2008-11-28")),
        TEpisode(1, 3, 3, "Tramps Like Us", TInstant::ParseIso8601("2008-12-05")),
        TEpisode(1, 3, 4, "The Speech", TInstant::ParseIso8601("2008-12-12")),
        TEpisode(1, 3, 5, "Friendface", TInstant::ParseIso8601("2008-12-19")),
        TEpisode(1, 3, 6, "Calendar Geeks", TInstant::ParseIso8601("2008-12-26")),
        TEpisode(1, 4, 1, "Jen The Fredo", TInstant::ParseIso8601("2010-06-25")),
        TEpisode(1, 4, 2, "The Final Countdown", TInstant::ParseIso8601("2010-07-02")),
        TEpisode(1, 4, 3, "Something Happened", TInstant::ParseIso8601("2010-07-09")),
        TEpisode(1, 4, 4, "Italian For Beginners", TInstant::ParseIso8601("2010-07-16")),
        TEpisode(1, 4, 5, "Bad Boys", TInstant::ParseIso8601("2010-07-23")),
        TEpisode(1, 4, 6, "Reynholm vs Reynholm", TInstant::ParseIso8601("2010-07-30")),
        TEpisode(2, 1, 1, "Minimum Viable Product", TInstant::ParseIso8601("2014-04-06")),
        TEpisode(2, 1, 2, "The Cap Table", TInstant::ParseIso8601("2014-04-13")),
        TEpisode(2, 1, 3, "Articles of Incorporation", TInstant::ParseIso8601("2014-04-20")),
        TEpisode(2, 1, 4, "Fiduciary Duties", TInstant::ParseIso8601("2014-04-27")),
        TEpisode(2, 1, 5, "Signaling Risk", TInstant::ParseIso8601("2014-05-04")),
        TEpisode(2, 1, 6, "Third Party Insourcing", TInstant::ParseIso8601("2014-05-11")),
        TEpisode(2, 1, 7, "Proof of Concept", TInstant::ParseIso8601("2014-05-18")),
        TEpisode(2, 1, 8, "Optimal Tip-to-Tip Efficiency", TInstant::ParseIso8601("2014-06-01")),
        TEpisode(2, 2, 1, "Sand Hill Shuffle", TInstant::ParseIso8601("2015-04-12")),
        TEpisode(2, 2, 2, "Runaway Devaluation", TInstant::ParseIso8601("2015-04-19")),
        TEpisode(2, 2, 3, "Bad Money", TInstant::ParseIso8601("2015-04-26")),
        TEpisode(2, 2, 4, "The Lady", TInstant::ParseIso8601("2015-05-03")),
        TEpisode(2, 2, 5, "Server Space", TInstant::ParseIso8601("2015-05-10")),
        TEpisode(2, 2, 6, "Homicide", TInstant::ParseIso8601("2015-05-17")),
        TEpisode(2, 2, 7, "Adult Content", TInstant::ParseIso8601("2015-05-24")),
        TEpisode(2, 2, 8, "White Hat/Black Hat", TInstant::ParseIso8601("2015-05-31")),
        TEpisode(2, 2, 9, "Binding Arbitration", TInstant::ParseIso8601("2015-06-07")),
        TEpisode(2, 2, 10, "Two Days of the Condor", TInstant::ParseIso8601("2015-06-14")),
        TEpisode(2, 3, 1, "Founder Friendly", TInstant::ParseIso8601("2016-04-24")),
        TEpisode(2, 3, 2, "Two in the Box", TInstant::ParseIso8601("2016-05-01")),
        TEpisode(2, 3, 3, "Meinertzhagen's Haversack", TInstant::ParseIso8601("2016-05-08")),
        TEpisode(2, 3, 4, "Maleant Data Systems Solutions", TInstant::ParseIso8601("2016-05-15")),
        TEpisode(2, 3, 5, "The Empty Chair", TInstant::ParseIso8601("2016-05-22")),
        TEpisode(2, 3, 6, "Bachmanity Insanity", TInstant::ParseIso8601("2016-05-29")),
        TEpisode(2, 3, 7, "To Build a Better Beta", TInstant::ParseIso8601("2016-06-05")),
        TEpisode(2, 3, 8, "Bachman's Earnings Over-Ride", TInstant::ParseIso8601("2016-06-12")),
        TEpisode(2, 3, 9, "Daily Active Users", TInstant::ParseIso8601("2016-06-19")),
        TEpisode(2, 3, 10, "The Uptick", TInstant::ParseIso8601("2016-06-26")),
        TEpisode(2, 4, 1, "Success Failure", TInstant::ParseIso8601("2017-04-23")),
        TEpisode(2, 4, 2, "Terms of Service", TInstant::ParseIso8601("2017-04-30")),
        TEpisode(2, 4, 3, "Intellectual Property", TInstant::ParseIso8601("2017-05-07")),
        TEpisode(2, 4, 4, "Teambuilding Exercise", TInstant::ParseIso8601("2017-05-14")),
        TEpisode(2, 4, 5, "The Blood Boy", TInstant::ParseIso8601("2017-05-21")),
        TEpisode(2, 4, 6, "Customer Service", TInstant::ParseIso8601("2017-05-28")),
        TEpisode(2, 4, 7, "The Patent Troll", TInstant::ParseIso8601("2017-06-04")),
        TEpisode(2, 4, 8, "The Keenan Vortex", TInstant::ParseIso8601("2017-06-11")),
        TEpisode(2, 4, 9, "Hooli-Con", TInstant::ParseIso8601("2017-06-18")),
        TEpisode(2, 4, 10, "Server Error", TInstant::ParseIso8601("2017-06-25")),
        TEpisode(2, 5, 1, "Grow Fast or Die Slow", TInstant::ParseIso8601("2018-03-25")),
        TEpisode(2, 5, 2, "Reorientation", TInstant::ParseIso8601("2018-04-01")),
        TEpisode(2, 5, 3, "Chief Operating Officer", TInstant::ParseIso8601("2018-04-08")),
        TEpisode(2, 5, 4, "Tech Evangelist", TInstant::ParseIso8601("2018-04-15")),
        TEpisode(2, 5, 5, "Facial Recognition", TInstant::ParseIso8601("2018-04-22")),
        TEpisode(2, 5, 6, "Artificial Emotional Intelligence", TInstant::ParseIso8601("2018-04-29")),
        TEpisode(2, 5, 7, "Initial Coin Offering", TInstant::ParseIso8601("2018-05-06")),
        TEpisode(2, 5, 8, "Fifty-One Percent", TInstant::ParseIso8601("2018-05-13"))
    };

    TParamsBuilder paramsBuilder;

    auto& seriesParam = paramsBuilder.AddParam("$seriesData");
    seriesParam.BeginList();
    for (auto& series : seriesData) {
        seriesParam.AddListItem()
            .BeginStruct()
            .AddMember("series_id")
                .Uint64(series.SeriesId)
            .AddMember("title")
                .Utf8(series.Title)
            .AddMember("release_date")
                .Date(series.ReleaseDate)
            .AddMember("series_info")
                .Utf8(series.SeriesInfo)
            .EndStruct();
    }
    seriesParam.EndList();
    seriesParam.Build();

    auto& seasonsParam = paramsBuilder.AddParam("$seasonsData");
    seasonsParam.BeginList();
    for (auto& season : seasonsData) {
        seasonsParam.AddListItem()
            .BeginStruct()
            .AddMember("series_id")
                .Uint64(season.SeriesId)
            .AddMember("season_id")
                .Uint64(season.SeasonId)
            .AddMember("title")
                .Utf8(season.Title)
            .AddMember("first_aired")
                .Date(season.FirstAired)
            .AddMember("last_aired")
                .Date(season.LastAired)
            .EndStruct();
    }
    seasonsParam.EndList();
    seasonsParam.Build();

    auto& episodesParam = paramsBuilder.AddParam("$episodesData");
    episodesParam.BeginList();
    for (auto& episode : episodesData) {
        episodesParam.AddListItem()
            .BeginStruct()
            .AddMember("series_id")
                .Uint64(episode.SeriesId)
            .AddMember("season_id")
                .Uint64(episode.SeasonId)
            .AddMember("episode_id")
                .Uint64(episode.EpisodeId)
            .AddMember("title")
                .Utf8(episode.Title)
            .AddMember("air_date")
                .Date(episode.AirDate)
            .EndStruct();
    }
    episodesParam.EndList();
    episodesParam.Build();

    return paramsBuilder.Build();
}
