import iso8601
import os
import ydb
from concurrent.futures import TimeoutError


FILL_DATA_QUERY = """
--!syntax_v1
PRAGMA TablePathPrefix("{}");

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
FROM AS_TABLE($usersData);
"""

SELECT_SERIES_BY_UPLOADER = """
--!syntax_v1
PRAGMA TablePathPrefix("{}");

DECLARE $userName AS Utf8;

SELECT
 t1.series_id as series_id,
 t1.title as title,
 t1.info as info,
 t1.release_date as release_date,
 t1.views as views,
 t1.uploaded_user_id as uploaded_user_id

FROM series view users_index AS t1
INNER JOIN users view name_index AS t2
ON t1.uploaded_user_id == t2.user_id
WHERE t2.name == $userName;
"""


class Series(object):
    __slots__ = (
        "series_id",
        "title",
        "release_date",
        "info",
        "views",
        "uploaded_user_id",
    )

    def __init__(self, series_id, title, release_date, info, views, uploaded_user_id):
        self.series_id = series_id
        self.title = title
        self.release_date = (
            to_days(release_date) if isinstance(release_date, str) else release_date
        )
        self.info = info
        self.views = views
        self.uploaded_user_id = uploaded_user_id

    @classmethod
    def from_db(cls, entry):
        return cls(**entry)

    def __str__(self):
        return "Series<series_id: %s, title: %s, views: %d>" % (
            self.series_id,
            self.title,
            int(self.views),
        )


class User(object):
    __slots__ = ("user_id", "name", "age")

    def __init__(self, user_id, name, age):
        self.user_id = user_id
        self.name = name
        self.age = age


def to_days(date):
    timedelta = iso8601.parse_date(date) - iso8601.parse_date("1970-1-1")
    return timedelta.days


def is_directory_exists(driver, path):
    try:
        return driver.scheme_client.describe_path(path).is_directory()
    except ydb.SchemeError:
        return False


def ensure_path_exists(driver, database, path):
    paths_to_create = list()
    path = path.rstrip("/")
    while path not in ("", database):
        full_path = os.path.join(database, path)
        if is_directory_exists(driver, full_path):
            break
        paths_to_create.append(full_path)
        path = os.path.dirname(path).rstrip("/")

    while len(paths_to_create) > 0:
        full_path = paths_to_create.pop(-1)
        driver.scheme_client.make_directory(full_path)


def create_tables(session_pool, path):
    def callee(session):
        session.create_table(
            os.path.join(path, "series"),
            ydb.TableDescription()
            .with_primary_keys("series_id")
            .with_columns(
                ydb.Column("series_id", ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column("title", ydb.OptionalType(ydb.PrimitiveType.Utf8)),
                ydb.Column("info", ydb.OptionalType(ydb.PrimitiveType.Utf8)),
                ydb.Column(
                    "release_date", ydb.OptionalType(ydb.PrimitiveType.Datetime)
                ),
                ydb.Column("views", ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column(
                    "uploaded_user_id", ydb.OptionalType(ydb.PrimitiveType.Uint64)
                ),
            )
            .with_indexes(
                ydb.TableIndex("users_index").with_index_columns("uploaded_user_id")
            ),
        )

        session.create_table(
            os.path.join(path, "users"),
            ydb.TableDescription()
            .with_primary_keys("user_id")
            .with_columns(
                ydb.Column("user_id", ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column("name", ydb.OptionalType(ydb.PrimitiveType.Utf8)),
                ydb.Column("age", ydb.OptionalType(ydb.PrimitiveType.Uint32)),
            )
            .with_index(ydb.TableIndex("name_index").with_index_columns("name")),
        )

    session_pool.retry_operation_sync(callee)


def get_series_data():
    return [
        Series(1, "First episode", "2006-01-01", "Pilot episode.", 1000, 0),
        Series(2, "Second episode", "2006-02-01", "Jon Snow knows nothing.", 2000, 1),
        Series(
            3,
            "Third episode",
            "2006-03-01",
            "Daenerys is the mother of dragons.",
            3000,
            2,
        ),
        Series(
            4,
            "Fourth episode",
            "2006-04-01",
            "Jorah Mormont is the king of the friendzone.",
            4000,
            3,
        ),
        Series(5, "Fifth episode", "2006-05-01", "Cercei is not good person.", 5000, 1),
        Series(6, "Sixth episode", "2006-06-01", "Tyrion is not big.", 6000, 2),
        Series(
            7, "Seventh episode", "2006-07-01", "Tywin should close the door.", 7000, 2
        ),
        Series(
            8,
            "Eighth episode",
            "2006-08-01",
            "The white walkers are well-organized.",
            8000,
            3,
        ),
        Series(9, "Ninth episode", "2006-09-01", "Dragons can fly.", 9000, 1),
    ]


def get_users_data():
    return [
        User(0, "Kit Harrington", 32),
        User(1, "Emilia Clarke", 32),
        User(2, "Jason Momoa", 39),
        User(3, "Peter Dinklage", 49),
    ]


def fill_data(session_pool, path):
    def callee(session):
        session.transaction(ydb.SerializableReadWrite()).execute(
            session.prepare(FILL_DATA_QUERY.format(path)),
            commit_tx=True,
            parameters={
                "$seriesData": get_series_data(),
                "$usersData": get_users_data(),
            },
        )

    return session_pool.retry_operation_sync(callee)


def select_by_username(session_pool, path, username):
    def callee(session):
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            session.prepare(SELECT_SERIES_BY_UPLOADER.format(path)),
            commit_tx=True,
            parameters={"$userName": username},
        )

        series_rows = list(map(lambda x: Series(**x), result_sets[0].rows))
        print("Series by %s" % username)
        for series in series_rows:
            print("Series %s" % str(series))
        return series_rows

    return session_pool.retry_operation_sync(callee)


def run(endpoint, database, path):
    driver_config = ydb.DriverConfig(
        endpoint, database, credentials=ydb.construct_credentials_from_environ()
    )
    with ydb.Driver(driver_config) as driver:
        try:
            driver.wait(timeout=5)
        except TimeoutError:
            print("Connect failed to YDB")
            print("Last reported errors by discovery:")
            print(driver.discovery_debug_details())
            exit(1)

        with ydb.SessionPool(driver, size=10) as session_pool:
            ensure_path_exists(driver, database, path)
            full_path = os.path.join(database, path)

            create_tables(session_pool, full_path)

            fill_data(session_pool, full_path)

            peter_series = select_by_username(session_pool, full_path, "Peter Dinklage")

            assert len(peter_series) == 2

            emilia_series = select_by_username(session_pool, full_path, "Emilia Clarke")

            assert len(emilia_series) == 3
