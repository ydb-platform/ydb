# -*- coding: utf-8 -*-


class Series(object):
    __slots__ = ("series_id", "title", "release_date", "series_info")

    def __init__(self, series_id, title, release_date, series_info):
        self.series_id = series_id
        self.title = title
        self.release_date = bytes(release_date, "utf8")
        self.series_info = series_info


class Season(object):
    __slots__ = ("series_id", "season_id", "title", "first_aired", "last_aired")

    def __init__(self, series_id, season_id, title, first_aired, last_aired):
        self.series_id = series_id
        self.season_id = season_id
        self.title = title
        self.first_aired = bytes(first_aired, "utf8")
        self.last_aired = bytes(last_aired, "utf8")


class Episode(object):
    __slots__ = ("series_id", "season_id", "episode_id", "title", "air_date")

    def __init__(self, series_id, season_id, episode_id, title, air_date):
        self.series_id = series_id
        self.season_id = season_id
        self.episode_id = episode_id
        self.title = title
        self.air_date = bytes(air_date, "utf8")


def get_series_data():
    return [
        Series(
            1,
            "IT Crowd",
            "2006-02-03",
            "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "
            "Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
        ),
        Series(
            2,
            "Silicon Valley",
            "2014-04-06",
            "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "
            "Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
        ),
    ]


def get_seasons_data():
    return [
        Season(1, 1, "Season 1", "2006-02-03", "2006-03-03"),
        Season(1, 2, "Season 2", "2007-08-24", "2007-09-28"),
        Season(1, 3, "Season 3", "2008-11-21", "2008-12-26"),
        Season(1, 4, "Season 4", "2010-06-25", "2010-07-30"),
        Season(2, 1, "Season 1", "2014-04-06", "2014-06-01"),
        Season(2, 2, "Season 2", "2015-04-12", "2015-06-14"),
        Season(2, 3, "Season 3", "2016-04-24", "2016-06-26"),
        Season(2, 4, "Season 4", "2017-04-23", "2017-06-25"),
        Season(2, 5, "Season 5", "2018-03-25", "2018-05-13"),
    ]


def get_episodes_data():
    return [
        Episode(1, 1, 1, "Yesterday's Jam", "2006-02-03"),
        Episode(1, 1, 2, "Calamity Jen", "2006-02-03"),
        Episode(1, 1, 3, "Fifty-Fifty", "2006-02-10"),
        Episode(1, 1, 4, "The Red Door", "2006-02-17"),
        Episode(1, 1, 5, "The Haunting of Bill Crouse", "2006-02-24"),
        Episode(1, 1, 6, "Aunt Irma Visits", "2006-03-03"),
        Episode(1, 2, 1, "The Work Outing", "2006-08-24"),
        Episode(1, 2, 2, "Return of the Golden Child", "2007-08-31"),
        Episode(1, 2, 3, "Moss and the German", "2007-09-07"),
        Episode(1, 2, 4, "The Dinner Party", "2007-09-14"),
        Episode(1, 2, 5, "Smoke and Mirrors", "2007-09-21"),
        Episode(1, 2, 6, "Men Without Women", "2007-09-28"),
        Episode(1, 3, 1, "From Hell", "2008-11-21"),
        Episode(1, 3, 2, "Are We Not Men?", "2008-11-28"),
        Episode(1, 3, 3, "Tramps Like Us", "2008-12-05"),
        Episode(1, 3, 4, "The Speech", "2008-12-12"),
        Episode(1, 3, 5, "Friendface", "2008-12-19"),
        Episode(1, 3, 6, "Calendar Geeks", "2008-12-26"),
        Episode(1, 4, 1, "Jen The Fredo", "2010-06-25"),
        Episode(1, 4, 2, "The Final Countdown", "2010-07-02"),
        Episode(1, 4, 3, "Something Happened", "2010-07-09"),
        Episode(1, 4, 4, "Italian For Beginners", "2010-07-16"),
        Episode(1, 4, 5, "Bad Boys", "2010-07-23"),
        Episode(1, 4, 6, "Reynholm vs Reynholm", "2010-07-30"),
    ]


def get_episodes_data_for_bulk_upsert():
    return [
        Episode(2, 1, 1, "Minimum Viable Product", "2014-04-06"),
        Episode(2, 1, 2, "The Cap Table", "2014-04-13"),
        Episode(2, 1, 3, "Articles of Incorporation", "2014-04-20"),
        Episode(2, 1, 4, "Fiduciary Duties", "2014-04-27"),
        Episode(2, 1, 5, "Signaling Risk", "2014-05-04"),
        Episode(2, 1, 6, "Third Party Insourcing", "2014-05-11"),
        Episode(2, 1, 7, "Proof of Concept", "2014-05-18"),
        Episode(2, 1, 8, "Optimal Tip-to-Tip Efficiency", "2014-06-01"),
        Episode(2, 2, 1, "Sand Hill Shuffle", "2015-04-12"),
        Episode(2, 2, 2, "Runaway Devaluation", "2015-04-19"),
        Episode(2, 2, 3, "Bad Money", "2015-04-26"),
        Episode(2, 2, 4, "The Lady", "2015-05-03"),
        Episode(2, 2, 5, "Server Space", "2015-05-10"),
        Episode(2, 2, 6, "Homicide", "2015-05-17"),
        Episode(2, 2, 7, "Adult Content", "2015-05-24"),
        Episode(2, 2, 8, "White Hat/Black Hat", "2015-05-31"),
        Episode(2, 2, 9, "Binding Arbitration", "2015-06-07"),
        Episode(2, 2, 10, "Two Days of the Condor", "2015-06-14"),
        Episode(2, 3, 1, "Founder Friendly", "2016-04-24"),
        Episode(2, 3, 2, "Two in the Box", "2016-05-01"),
        Episode(2, 3, 3, "Meinertzhagen's Haversack", "2016-05-08"),
        Episode(2, 3, 4, "Maleant Data Systems Solutions", "2016-05-15"),
        Episode(2, 3, 5, "The Empty Chair", "2016-05-22"),
        Episode(2, 3, 6, "Bachmanity Insanity", "2016-05-29"),
        Episode(2, 3, 7, "To Build a Better Beta", "2016-06-05"),
        Episode(2, 3, 8, "Bachman's Earnings Over-Ride", "2016-06-12"),
        Episode(2, 3, 9, "Daily Active Users", "2016-06-19"),
        Episode(2, 3, 10, "The Uptick", "2016-06-26"),
        Episode(2, 4, 1, "Success Failure", "2017-04-23"),
        Episode(2, 4, 2, "Terms of Service", "2017-04-30"),
        Episode(2, 4, 3, "Intellectual Property", "2017-05-07"),
        Episode(2, 4, 4, "Teambuilding Exercise", "2017-05-14"),
        Episode(2, 4, 5, "The Blood Boy", "2017-05-21"),
        Episode(2, 4, 6, "Customer Service", "2017-05-28"),
        Episode(2, 4, 7, "The Patent Troll", "2017-06-04"),
        Episode(2, 4, 8, "The Keenan Vortex", "2017-06-11"),
        Episode(2, 4, 9, "Hooli-Con", "2017-06-18"),
        Episode(2, 4, 10, "Server Error", "2017-06-25"),
        Episode(2, 5, 1, "Grow Fast or Die Slow", "2018-03-25"),
        Episode(2, 5, 2, "Reorientation", "2018-04-01"),
        Episode(2, 5, 3, "Chief Operating Officer", "2018-04-08"),
        Episode(2, 5, 4, "Tech Evangelist", "2018-04-15"),
        Episode(2, 5, 5, "Facial Recognition", "2018-04-22"),
        Episode(2, 5, 6, "Artificial Emotional Intelligence", "2018-04-29"),
        Episode(2, 5, 7, "Initial Coin Offering", "2018-05-06"),
        Episode(2, 5, 8, "Fifty-One Percent", "2018-05-13"),
    ]
