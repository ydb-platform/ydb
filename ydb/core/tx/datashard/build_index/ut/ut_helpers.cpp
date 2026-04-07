#include "ut_helpers.h"

#include <ydb/core/base/table_index.h>

namespace NKikimr {
using namespace Tests;
using namespace NTableIndex::NKMeans;

const char* MainTableForOverlap = R"(UPSERT INTO `/Root/table-main`
    (key, embedding, data) VALUES

    (1, "\x10\x80\x02", "one"),
    (2, "\x80\x10\x02", "two"),
    (3, "\x10\x10\x02", "three"),

    (4, "\x11\x81\x02", "four"),
    (5, "\x11\x80\x02", "five"),

    (6, "\x81\x11\x02", "aaa"),
    (7, "\x81\x10\x02", "bbbb"),

    (8, "\x11\x10\x02", "ccccc"),
    (9, "\x10\x11\x02", "dddd"),
    (10, "\x11\x09\x02", "eee"),
    (11, "\x09\x11\x02", "ffff"),
    (12, "\x09\x09\x02", "ggggg"),
    (13, "\x11\x11\x02", "hhhh");)";

const char* MainTableForOverlapWithPrefix = R"(UPSERT INTO `/Root/table-main`
    (user, key, embedding, data) VALUES

    ("user-1", 1, "\x10\x80\x02", "one"),
    ("user-1", 2, "\x80\x10\x02", "two"),
    ("user-1", 3, "\x10\x10\x02", "three"),

    ("user-1", 4, "\x11\x81\x02", "four"),
    ("user-1", 5, "\x11\x80\x02", "five"),

    ("user-1", 6, "\x81\x11\x02", "aaa"),
    ("user-1", 7, "\x81\x10\x02", "bbbb"),

    ("user-1", 8, "\x11\x10\x02", "ccccc"),
    ("user-1", 9, "\x10\x11\x02", "dddd"),
    ("user-1", 10, "\x11\x09\x02", "eee"),
    ("user-1", 11, "\x09\x11\x02", "ffff"),
    ("user-1", 12, "\x09\x09\x02", "ggggg"),
    ("user-1", 13, "\x11\x11\x02", "hhhh"),

    ("user-2", 21, "\x10\x80\x02", "one"),
    ("user-2", 22, "\x80\x10\x02", "two"),
    ("user-2", 23, "\x10\x10\x02", "three"),

    ("user-2", 24, "\x11\x81\x02", "four"),
    ("user-2", 25, "\x11\x80\x02", "five"),

    ("user-2", 26, "\x81\x11\x02", "aaa"),
    ("user-2", 27, "\x81\x10\x02", "bbbb"),

    ("user-2", 28, "\x11\x10\x02", "ccccc"),
    ("user-2", 29, "\x10\x11\x02", "dddd"),
    ("user-2", 30, "\x11\x09\x02", "eee"),
    ("user-2", 31, "\x09\x11\x02", "ffff"),
    ("user-2", 32, "\x09\x09\x02", "ggggg"),
    ("user-2", 33, "\x11\x11\x02", "hhhh");
)";

const char* BuildTableWithOverlapIn = R"(UPSERT INTO `/Root/table-main`
    (__ydb_parent, key, __ydb_foreign, embedding, data) VALUES

    (40, 1, false, "\x10\x80\x02", "one"),
    (40, 2, false, "\x80\x10\x02", "two"),
    (40, 3, false, "\x10\x10\x02", "three"),

    (40, 4, false, "\x11\x81\x02", "four"),
    (40, 5, false, "\x11\x80\x02", "five"),

    (40, 6, false, "\x81\x11\x02", "aaa"),
    (40, 7, false, "\x81\x10\x02", "bbbb"),

    (40, 8, false, "\x11\x10\x02", "ccccc"),
    (40, 9, false, "\x10\x11\x02", "dddd"),
    (40, 10, false, "\x11\x09\x02", "eee"),
    (40, 11, false, "\x09\x11\x02", "ffff"),
    (40, 12, false, "\x09\x09\x02", "ggggg"),
    (40, 13, false, "\x11\x11\x02", "hhhh"),

    (40, 14, true, "\x12\x12\x02", "1414"),
    (40, 15, true, "\x12\x09\x02", "1515");
)";

const char* BuildToBuildWithOverlapOut =
    "key = 1, __ydb_parent = 41, __ydb_foreign = 0, __ydb_distance = 0.008950848554, embedding = \x10\x80\x02, data = one\n"
    "key = 2, __ydb_parent = 42, __ydb_foreign = 0, __ydb_distance = 0.0001086457909, embedding = \x80\x10\x02, data = two\n"
    "key = 3, __ydb_parent = 41, __ydb_foreign = 0, __ydb_distance = 0.1357536248, embedding = \x10\x10\x02, data = three\n"
    "key = 3, __ydb_parent = 42, __ydb_foreign = 1, __ydb_distance = 0.2016838042, embedding = \x10\x10\x02, data = three\n"
    "key = 4, __ydb_parent = 41, __ydb_foreign = 0, __ydb_distance = 0.008082101307, embedding = \x11\x81\x02, data = four\n"
    "key = 5, __ydb_parent = 41, __ydb_foreign = 0, __ydb_distance = 0.007954224928, embedding = \x11\x80\x02, data = five\n"
    "key = 6, __ydb_parent = 42, __ydb_foreign = 0, __ydb_distance = 3.254632953e-05, embedding = \x81\x11\x02, data = aaa\n"
    "key = 7, __ydb_parent = 42, __ydb_foreign = 0, __ydb_distance = 0.0001231662617, embedding = \x81\x10\x02, data = bbbb\n"
    "key = 8, __ydb_parent = 41, __ydb_foreign = 0, __ydb_distance = 0.1513876732, embedding = \x11\x10\x02, data = ccccc\n"
    "key = 8, __ydb_parent = 42, __ydb_foreign = 1, __ydb_distance = 0.1838088091, embedding = \x11\x10\x02, data = ccccc\n"
    "key = 9, __ydb_parent = 41, __ydb_foreign = 0, __ydb_distance = 0.1209126449, embedding = \x10\x11\x02, data = dddd\n"
    "key = 9, __ydb_parent = 42, __ydb_foreign = 1, __ydb_distance = 0.2202913675, embedding = \x10\x11\x02, data = dddd\n"
    "key = 10, __ydb_parent = 42, __ydb_foreign = 0, __ydb_distance = 0.05987630731, embedding = \x11\x09\x02, data = eee\n"
    "key = 11, __ydb_parent = 41, __ydb_foreign = 0, __ydb_distance = 0.02602604542, embedding = \x09\x11\x02, data = ffff\n"
    "key = 12, __ydb_parent = 41, __ydb_foreign = 0, __ydb_distance = 0.1357536248, embedding = \x09\x09\x02, data = ggggg\n"
    "key = 12, __ydb_parent = 42, __ydb_foreign = 1, __ydb_distance = 0.2016838042, embedding = \x09\x09\x02, data = ggggg\n"
    "key = 13, __ydb_parent = 41, __ydb_foreign = 0, __ydb_distance = 0.1357536248, embedding = \x11\x11\x02, data = hhhh\n"
    "key = 13, __ydb_parent = 42, __ydb_foreign = 1, __ydb_distance = 0.2016838042, embedding = \x11\x11\x02, data = hhhh\n"
    "key = 14, __ydb_parent = 41, __ydb_foreign = 1, __ydb_distance = 0.1357536248, embedding = \x12\x12\x02, data = 1414\n"
    "key = 14, __ydb_parent = 42, __ydb_foreign = 1, __ydb_distance = 0.2016838042, embedding = \x12\x12\x02, data = 1414\n"
    "key = 15, __ydb_parent = 42, __ydb_foreign = 1, __ydb_distance = 0.05220621233, embedding = \x12\x09\x02, data = 1515\n";

void CreateMainTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options)
{
    options.AllowSystemColumnNames(false);
    options.Columns({
        {"key", "Uint32", true, true},
        {"embedding", "String", false, false},
        {"data", "String", false, false},
    });
    CreateShardedTable(server, sender, "/Root", "table-main", options);
}

void CreateBuildTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options, const char* name)
{
    options.AllowSystemColumnNames(true);
    options.Columns({
        {ParentColumn, NTableIndex::NKMeans::ClusterIdTypeName, true, true},
        {"key", "Uint32", true, true},
        {"embedding", "String", false, false},
        {"data", "String", false, false},
    });
    CreateShardedTable(server, sender, "/Root", name, options);
}

void CreateBuildTableWithForeignIn(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options, const char* name)
{
    options.AllowSystemColumnNames(true);
    options.Columns({
        {ParentColumn, NTableIndex::NKMeans::ClusterIdTypeName, true, true},
        {"key", "Uint32", true, true},
        {IsForeignColumn, "Bool", false, true},
        {"embedding", "String", false, false},
        {"data", "String", false, false},
    });
    CreateShardedTable(server, sender, "/Root", name, options);
}

void CreateBuildTableWithForeignOut(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options, const char* name)
{
    options.AllowSystemColumnNames(true);
    options.Columns({
        {"key", "Uint32", true, true},
        {ParentColumn, NTableIndex::NKMeans::ClusterIdTypeName, true, true},
        {IsForeignColumn, "Bool", false, true},
        {DistanceColumn, "Double", false, true},
        {"embedding", "String", false, false},
        {"data", "String", false, false},
    });
    CreateShardedTable(server, sender, "/Root", name, options);
}

void CreateLevelTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options)
{
    options.AllowSystemColumnNames(true);
    options.Columns({
        {ParentColumn, NTableIndex::NKMeans::ClusterIdTypeName, true, true},
        {IdColumn, NTableIndex::NKMeans::ClusterIdTypeName, true, true},
        {CentroidColumn, "String", false, true},
    });
    CreateShardedTable(server, sender, "/Root", "table-level", options);
}

void CreatePostingTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options)
{
    options.AllowSystemColumnNames(true);
    options.Columns({
        {ParentColumn, NTableIndex::NKMeans::ClusterIdTypeName, true, true},
        {"key", "Uint32", true, true},
        {"data", "String", false, false},
    });
    CreateShardedTable(server, sender, "/Root", "table-posting", options);
}

void CreatePrefixTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options)
{
    options.AllowSystemColumnNames(true);
    options.Columns({
        {"user", "String", true, true},
        {IdColumn, NTableIndex::NKMeans::ClusterIdTypeName, true, true},
    });
    CreateShardedTable(server, sender, "/Root", "table-prefix", options);
}

void CreateBuildPrefixTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options, const char* name)
{
    options.Columns({
        {"user", "String", true, true},
        {"key", "Uint32", true, true},
        {"embedding", "String", false, false},
        {"data", "String", false, false},
    });
    CreateShardedTable(server, sender, "/Root", name, options);
}

} // namespace NKikimr
