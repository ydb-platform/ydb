#include "sql_ut.h"

#include <yql/essentials/sql/v1/translation/sql.h>

#include <util/string/split.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(ExternalTable) {
Y_UNIT_TEST(CreateExternalTable) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    CREATE EXTERNAL TABLE mytable (
                        a int
                    ) WITH (
                        DATA_SOURCE="/Root/mydatasource",
                        LOCATION="/folder1/*"
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('data_source_path (String '"/Root/mydatasource")) '('location (String '"/folder1/*")))) '('tableType 'externalTable)))))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("tablescheme"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateExternalTableWithTablePrefix) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    pragma TablePathPrefix='/aba';
                    CREATE EXTERNAL TABLE mytable (
                        a int
                    ) WITH (
                        DATA_SOURCE="mydatasource",
                        LOCATION="/folder1/*"
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "/aba/mydatasource");
            UNIT_ASSERT_STRING_CONTAINS(line, "/aba/mytable");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("tablescheme"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateExternalTableObjectStorage) {
    auto res = SqlToYql(R"sql(
                    USE plato;
                    CREATE EXTERNAL TABLE mytable (
                        a int,
                        year Int
                    ) WITH (
                        DATA_SOURCE="/Root/mydatasource",
                        LOCATION="/folder1/*",
                        FORMAT="json_as_string",
                        `projection.enabled`="true",
                        `projection.year.type`="integer",
                        `projection.year.min`="2010",
                        `projection.year.max`="2022",
                        `projection.year.interval`="1",
                        `projection.month.type`="integer",
                        `projection.month.min`="1",
                        `projection.month.max`="12",
                        `projection.month.interval`="1",
                        `projection.month.digits`="2",
                        `storage.location.template`="${year}/${month}",
                        PARTITONED_BY = "[year, month]"
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(CreateExternalTableIfNotExists) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE ydb;
                    CREATE EXTERNAL TABLE IF NOT EXISTS mytable (
                        a int
                    ) WITH (
                        DATA_SOURCE="/Root/mydatasource",
                        LOCATION="/folder1/*"
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('data_source_path (String '"/Root/mydatasource")) '('location (String '"/folder1/*")))) '('tableType 'externalTable)))))#");
            UNIT_ASSERT_STRING_CONTAINS(line, "create_if_not_exists");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateExternalTableOrReplace) {
    NYql::TAstParseResult res = SqlToYql(R"(
                    USE plato;
                    CREATE OR REPLACE EXTERNAL TABLE mytable (
                        a int
                    ) WITH (
                        DATA_SOURCE="/Root/mydatasource",
                        LOCATION="/folder1/*"
                    );
                )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('data_source_path (String '"/Root/mydatasource")) '('location (String '"/folder1/*")))) '('tableType 'externalTable)))))#");
            UNIT_ASSERT_STRING_CONTAINS(line, "create_or_replace");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterExternalTableAddColumn) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    ALTER EXTERNAL TABLE mytable
                        ADD COLUMN my_column int32,
                        RESET (LOCATION);
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('actions '('('addColumns '('('"my_column" (AsOptionalType (DataType 'Int32))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('setTableSettings '('('location)))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('tableType 'externalTable))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('mode 'alter))#");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterExternalTableDropColumn) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    ALTER EXTERNAL TABLE mytable
                        DROP COLUMN my_column,
                        SET (Location = "abc", Other_Prop = "42"),
                        SET x 'y';
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('actions '('('dropColumns '('"my_column")#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('setTableSettings '('('location (String '"abc")) '('Other_Prop (String '"42")) '('x (String '"y")))))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('tableType 'externalTable))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('mode 'alter))#");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateExternalTableWithBadArguments) {
    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL TABLE mytable;
            )sql", "<main>:3:45: Error: mismatched input ';' expecting '('\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL TABLE mytable (
                    a int
                );
            )sql", "<main>:4:23: Error: DATA_SOURCE requires key\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL TABLE mytable (
                    a int
                ) WITH (
                    DATA_SOURCE="/Root/mydatasource"
                );
            )sql", "<main>:6:33: Error: LOCATION requires key\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL TABLE mytable (
                    a int
                ) WITH (
                    LOCATION="/folder1/*"
                );
            )sql", "<main>:6:30: Error: DATA_SOURCE requires key\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL TABLE mytable (
                    a int,
                    PRIMARY KEY(a)
                ) WITH (
                    DATA_SOURCE="/Root/mydatasource",
                    LOCATION="/folder1/*"
                );
            )sql", "<main>:8:30: Error: PRIMARY KEY is not supported for external table\n");
}

Y_UNIT_TEST(DropExternalTable) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
        USE plato;
        DROP EXTERNAL TABLE MyExternalTable;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("tablescheme"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropExternalTableWithTablePrefix) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                        USE plato;
                        pragma TablePathPrefix='/aba';
                        DROP EXTERNAL TABLE MyExternalTable;
                    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "/aba/MyExternalTable");
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'tablescheme"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropExternalTableIfExists) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                        USE plato;
                        DROP EXTERNAL TABLE IF EXISTS MyExternalTable;
                    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("tablescheme"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("drop_if_exists"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}
} // Y_UNIT_TEST_SUITE(ExternalTable)

Y_UNIT_TEST_SUITE(ColumnFamily) {

Y_UNIT_TEST(CompressionLevelCorrectUsage) {
    NYql::TAstParseResult res = SqlToYql(R"( use ydb;
                    CREATE TABLE tableName (
                        Key Uint32 FAMILY default,
                        Value String FAMILY family1,
                        PRIMARY KEY (Key),
                        FAMILY default (
                             DATA = "test",
                             COMPRESSION = "lz4",
                             COMPRESSION_LEVEL = 5
                        ),
                        FAMILY family1 (
                             DATA = "test",
                             COMPRESSION = "lz4",
                             COMPRESSION_LEVEL = 3
                        )
                    );
                )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("compression_level"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("5"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("3"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("compression_level"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["compression_level"]);
}

Y_UNIT_TEST(FieldDataIsNotString) {
    NYql::TAstParseResult res = SqlToYql(R"( use plato;
                    CREATE TABLE tableName (
                        Key Uint32 FAMILY default,
                        PRIMARY KEY (Key),
                        FAMILY default (
                             DATA = 1,
                             COMPRESSION = "lz4",
                             COMPRESSION_LEVEL = 5
                        )
                    );
                )");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT(res.Issues.Size() == 1);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "DATA value should be a string literal");
}

Y_UNIT_TEST(FieldCompressionIsNotString) {
    NYql::TAstParseResult res = SqlToYql(R"( use plato;
                    CREATE TABLE tableName (
                        Key Uint32 FAMILY default,
                        PRIMARY KEY (Key),
                        FAMILY default (
                             DATA = "test",
                             COMPRESSION = 2,
                             COMPRESSION_LEVEL = 5
                        ),
                    );
                )");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT(res.Issues.Size() == 1);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "COMPRESSION value should be a string literal");
}

Y_UNIT_TEST(FieldCompressionLevelIsNotInteger) {
    NYql::TAstParseResult res = SqlToYql(R"( use plato;
                    CREATE TABLE tableName (
                        Key Uint32 FAMILY default,
                        PRIMARY KEY (Key),
                        FAMILY default (
                             DATA = "test",
                             COMPRESSION = "lz4",
                             COMPRESSION_LEVEL = "5"
                        )
                    );
                )");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT(res.Issues.Size() == 1);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "COMPRESSION_LEVEL value should be an integer");
}

Y_UNIT_TEST(FieldCacheModeCorrectUsage) {
    NYql::TAstParseResult res = SqlToYql(R"sql( use ydb;
                    CREATE TABLE tableName (
                        Key Uint32 FAMILY default,
                        Value String FAMILY family1,
                        PRIMARY KEY (Key),
                        FAMILY default (
                             DATA = "test",
                             CACHE_MODE = "regular"
                        ),
                        FAMILY family1 (
                             DATA = "test",
                             CACHE_MODE = "in_memory"
                        )
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("cache_mode"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("regular"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("in_memory"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("cache_mode"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["cache_mode"]);
}

Y_UNIT_TEST(FieldCacheModeIsNotString) {
    NYql::TAstParseResult res = SqlToYql(R"sql( use plato;
                    CREATE TABLE tableName (
                        Key Uint32 FAMILY default,
                        PRIMARY KEY (Key),
                        FAMILY default (
                             DATA = "test",
                             CACHE_MODE = 42
                        )
                    );
                )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT(res.Issues.Size() == 1);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "CACHE_MODE value should be a string literal");
}

Y_UNIT_TEST(AlterCompressionCorrectUsage) {
    NYql::TAstParseResult res = SqlToYql(R"( use ydb;
                    ALTER TABLE tableName ALTER FAMILY default SET COMPRESSION "lz4";
                )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
}

Y_UNIT_TEST(AlterCompressionFieldIsNotString) {
    NYql::TAstParseResult res = SqlToYql(R"( use ydb;
                    ALTER TABLE tableName ALTER FAMILY default SET COMPRESSION lz4;
                )");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT(res.Issues.Size() == 1);
#if ANTLR_VER == 3
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Unexpected token 'lz4' : cannot match to any predicted input");
#else
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "mismatched input 'lz4' expecting {STRING_VALUE, DIGITS, INTEGER_VALUE}");
#endif
}

Y_UNIT_TEST(AlterCompressionLevelCorrectUsage) {
    NYql::TAstParseResult res = SqlToYql(R"( use ydb;
                    ALTER TABLE tableName ALTER FAMILY default SET COMPRESSION_LEVEL 5;
                )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
}

Y_UNIT_TEST(AlterCompressionLevelFieldIsNotInteger) {
    NYql::TAstParseResult res = SqlToYql(R"( use ydb;
                    ALTER TABLE tableName ALTER FAMILY default SET COMPRESSION_LEVEL "5";
                )");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT(res.Issues.Size() == 1);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "COMPRESSION_LEVEL value should be an integer");
}

Y_UNIT_TEST(AlterCompressionLevelFieldRedefinition) {
    NYql::TAstParseResult res = SqlToYql(R"sql( use ydb;
                    ALTER TABLE tableName
                        ALTER FAMILY default SET COMPRESSION_LEVEL 3,
                        ALTER FAMILY default SET COMPRESSION_LEVEL 5;
                )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT(res.Issues.Size() == 1);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Redefinition of COMPRESSION_LEVEL setting");
}

Y_UNIT_TEST(AlterCacheModeCorrectUsage) {
    NYql::TAstParseResult res = SqlToYql(R"sql( use ydb;
                    ALTER TABLE tableName ALTER FAMILY default SET CACHE_MODE "in_memory";
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
}

Y_UNIT_TEST(AlterCacheModeFieldIsNotInteger) {
    NYql::TAstParseResult res = SqlToYql(R"sql( use ydb;
                    ALTER TABLE tableName ALTER FAMILY default SET CACHE_MODE 42;
                )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT(res.Issues.Size() == 1);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "CACHE_MODE value should be a string literal");
}

Y_UNIT_TEST(AlterCacheModeFieldRedefinition) {
    NYql::TAstParseResult res = SqlToYql(R"sql( use ydb;
                    ALTER TABLE tableName
                        ALTER FAMILY default SET CACHE_MODE "in_memory",
                        ALTER FAMILY default SET CACHE_MODE "regular";
                )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT(res.Issues.Size() == 1);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Redefinition of CACHE_MODE setting");
}

} // Y_UNIT_TEST_SUITE(ColumnFamily)

Y_UNIT_TEST_SUITE(ColumnCompression) {

Y_UNIT_TEST(CreateCompressedColumn) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            k Uint64 NOT NULL,
            v Uint64 COMPRESSION(algorithm=zstd, level=5),
            PRIMARY KEY (k)
        );
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "columnCompression");
            UNIT_ASSERT_STRING_CONTAINS(line, "algorithm (String '\"zstd");
            UNIT_ASSERT_STRING_CONTAINS(line, "level (Uint64 '\"5");
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(NoColumnCompressionAtCreationIfNotSpecified) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            k Uint64 NOT NULL,
            v Uint64,
            PRIMARY KEY (k)
        );
    )sql");

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("columnCompression"));
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateCompressedColumnEmptyAttributes) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            k Uint64 NOT NULL,
            v Uint64 COMPRESSION(),
            PRIMARY KEY (k)
        );
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "columnCompression");
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateColumnDoubleCompression) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            k Uint64 NOT NULL,
            v Uint64 COMPRESSION() COMPRESSION(),
            PRIMARY KEY (k)
        );
    )sql");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:36: Error: 'COMPRESSION' option can be specified only once\n");
}

Y_UNIT_TEST(AddCompressedColumn) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ADD COLUMN val Uint64 COMPRESSION(algorithm=zstd, level=7);
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "columnCompression");
            UNIT_ASSERT_STRING_CONTAINS(line, "algorithm (String '\"zstd");
            UNIT_ASSERT_STRING_CONTAINS(line, "level (Uint64 '\"7");
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnCompression) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET COMPRESSION(algorithm=lz4, level=1);
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "changeCompression");
            UNIT_ASSERT_STRING_CONTAINS(line, "algorithm (String '\"lz4");
            UNIT_ASSERT_STRING_CONTAINS(line, "level (Uint64 '\"1");
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(NoColumnCompressionAtAlterIfNotSpecified) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET NOT NULL;
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("columnCompression"));
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnCompressionEmptyAttributes) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET COMPRESSION();
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "changeCompression");
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnCompressionDoubleAlgorithm) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET COMPRESSION(algorithm=lz4, algorithm=zstd);
    )sql");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:73: Error: 'algorithm' setting can be specified only once\n");
}

Y_UNIT_TEST(AlterColumnCompressionDoubleLevel) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET COMPRESSION(level=1, level=2);
    )sql");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:67: Error: 'level' setting can be specified only once\n");
}

Y_UNIT_TEST(AlterColumnCompressionLevelNegative) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET COMPRESSION(level=-1);
    )sql");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "<main>:3:63: Error: extraneous input '-' expecting");
}

} // Y_UNIT_TEST_SUITE(ColumnCompression)

Y_UNIT_TEST_SUITE(ColumnDefault) {

Y_UNIT_TEST(AlterColumnSetDefault) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET DEFAULT 42;
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "setDefaultValue");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnSetDefaultString) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET DEFAULT "hello"u;
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "setDefaultValue");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnSetDefaultNull) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET DEFAULT NULL;
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "setDefaultValue");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnDropDefault) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val DROP DEFAULT;
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "dropDefault");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnSetDefaultDoesNotEmitCompression) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET DEFAULT 42;
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("columnCompression"));
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("changeColumnConstraints"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnDropDefaultDoesNotEmitCompression) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val DROP DEFAULT;
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_C(line.find("columnCompression") == TString::npos, line);
            UNIT_ASSERT_C(line.find("changeColumnConstraints") == TString::npos, line);
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnSetDefaultWithOtherActions) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl
            ALTER COLUMN val SET DEFAULT 42,
            ALTER COLUMN other DROP DEFAULT;
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "setDefaultValue");
            UNIT_ASSERT_STRING_CONTAINS(line, "dropDefault");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnSetDefaultMultipleColumns) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl
            ALTER COLUMN a SET DEFAULT 1,
            ALTER COLUMN b SET DEFAULT "hello"u,
            ALTER COLUMN c DROP DEFAULT;
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    TVector<TString> lines;
    Split(program, "\n", lines);

    int setDefaultCount = 0;
    int dropDefaultCount = 0;
    for (const auto& line : lines) {
        auto last = line.find("setDefaultValue");
        while (last != TString::npos) {
            ++setDefaultCount;
            last = line.find("setDefaultValue", last + 1);
        }
        last = line.find("dropDefault");
        while (last != TString::npos) {
            ++dropDefaultCount;
            last = line.find("dropDefault", last + 1);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(2, setDefaultCount);
    UNIT_ASSERT_VALUES_EQUAL(1, dropDefaultCount);
}

Y_UNIT_TEST(AlterColumnSetDefaultBoolLiteral) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET DEFAULT false;
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "setDefaultValue");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterExternalTableSetDefaultIsSyntaxError) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER EXTERNAL TABLE tbl ALTER COLUMN val SET DEFAULT 42;
    )sql");

    UNIT_ASSERT(!res.Root);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "mismatched input 'ALTER' expecting");
}

Y_UNIT_TEST(AlterExternalTableDropDefaultIsSyntaxError) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER EXTERNAL TABLE tbl ALTER COLUMN val DROP DEFAULT;
    )sql");

    UNIT_ASSERT(!res.Root);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "mismatched input 'ALTER' expecting");
}

Y_UNIT_TEST(AlterTableStoreSetDefaultIsSyntaxError) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLESTORE tbl ALTER COLUMN val SET DEFAULT 42;
    )sql");

    UNIT_ASSERT(!res.Root);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "mismatched input 'ALTER' expecting");
}

Y_UNIT_TEST(AlterTableStoreDropDefaultIsSyntaxError) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLESTORE tbl ALTER COLUMN val DROP DEFAULT;
    )sql");

    UNIT_ASSERT(!res.Root);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "mismatched input 'ALTER' expecting");
}

} // Y_UNIT_TEST_SUITE(ColumnDefault)

Y_UNIT_TEST_SUITE(GeneratedColumns) {

Y_UNIT_TEST(CreateTableGeneratedVirtualByDefault) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                key Uint32,
                gen Int32 AS (5),
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"5" 'virtual)))#");
    UNIT_ASSERT(!program.Contains("stored"));
    UNIT_ASSERT(!program.Contains(R"#((Int32 '"5"))#"));
}

Y_UNIT_TEST(CreateTableGeneratedExplicitVirtual) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                key Uint32,
                gen Int32 AS (5) VIRTUAL,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"5" 'virtual)))#");
    UNIT_ASSERT(!program.Contains("stored"));
}

Y_UNIT_TEST(CreateTableGeneratedStored) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                key Uint32,
                gen Int32 AS (5) STORED,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"5" 'stored)))#");
    UNIT_ASSERT(!program.Contains("virtual"));
}

Y_UNIT_TEST(CreateTableGeneratedAlwaysKeyword) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                key Uint32,
                gen Int32 GENERATED ALWAYS AS (5) STORED,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"5" 'stored)))#");
    UNIT_ASSERT(!program.Contains("virtual"));
}

Y_UNIT_TEST(CreateTableGeneratedWithoutAlways) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                id Uint32,
                Col2 Uint32,
                Col3 Uint32,
                Col1 Uint32 AS (2 + 3),
                PRIMARY KEY (id)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"2 + 3" 'virtual)))#");
    UNIT_ASSERT(!program.Contains("stored"));
}

Y_UNIT_TEST(CreateTableGeneratedDeterministicExpr) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                key Uint32,
                gen Int32 AS (1 + 2) VIRTUAL,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"1 + 2" 'virtual)))#");
    UNIT_ASSERT(!program.Contains("stored"));
}

Y_UNIT_TEST(CreateTableGeneratedNonDeterministicExpr) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                key Uint32,
                gen Uint64 AS (RandomNumber(1)) STORED,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"RandomNumber(1)" 'stored)))#");
    UNIT_ASSERT(!program.Contains("virtual"));
}

Y_UNIT_TEST(CreateTableGeneratedReferencesColumns) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                a Int32,
                b Int32,
                gen Int32 GENERATED ALWAYS AS (a + b) STORED,
                PRIMARY KEY (a)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"a + b" 'stored)))#");
    UNIT_ASSERT(!program.Contains("virtual"));
}

Y_UNIT_TEST(CreateTableMultipleGeneratedColumns) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                key Uint32,
                x Int32 AS (1) VIRTUAL,
                y Int32 AS (2) STORED,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"1" 'virtual)))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"2" 'stored)))#");
}

Y_UNIT_TEST(AlterTableAddGeneratedVirtualColumn) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            ALTER TABLE tbl ADD COLUMN gen Int32 AS (5);
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, "addColumns");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"5" 'virtual)))#");
    UNIT_ASSERT(!program.Contains("stored"));
}

Y_UNIT_TEST(AlterTableAddGeneratedStoredColumn) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            ALTER TABLE tbl ADD COLUMN gen Int32 GENERATED ALWAYS AS (a + b) STORED;
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, "addColumns");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"a + b" 'stored)))#");
    UNIT_ASSERT(!program.Contains("virtual"));
    UNIT_ASSERT(!program.Contains("(Member row"));
}

Y_UNIT_TEST(AlterTableAddGeneratedColumnWithNamedLambda) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            $f = ($x) -> ($x * 2);
            ALTER TABLE tbl ADD COLUMN gen Int32 GENERATED ALWAYS AS ($f(a)) STORED;
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, "addColumns");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n$f = ($x) -> ($x * 2);\n" '"$f(a)" 'stored)))#");
    UNIT_ASSERT(!program.Contains("virtual"));
}

Y_UNIT_TEST(CreateTableGeneratedColumnWithNamedNodeRef) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            $factor = 10;
            CREATE TABLE tbl (
                key Uint32,
                num Int32,
                gen Int32 GENERATED ALWAYS AS (num * $factor) STORED,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n$factor = 10;\n" '"num * $factor" 'stored)))#");
    UNIT_ASSERT(!program.Contains("virtual"));
    UNIT_ASSERT(!program.Contains("Read"));
}

Y_UNIT_TEST(CreateTableGeneratedColumnWithSubqueryNamedNodeRef) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            $ids = (SELECT id FROM other);
            CREATE TABLE tbl (
                key Uint32,
                num Int32,
                gen Bool GENERATED ALWAYS AS (num IN $ids) STORED,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n$ids = (SELECT id FROM other);\n" '"num IN $ids" 'stored)))#");
    UNIT_ASSERT(!program.Contains("virtual"));
    UNIT_ASSERT(!program.Contains("Read"));
}

Y_UNIT_TEST(CreateTableGeneratedColumnWithInlineSubquery) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                key Uint32,
                num Int32,
                gen Bool GENERATED ALWAYS AS (num IN (SELECT id FROM other)) STORED,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"num IN (SELECT id FROM other)" 'stored)))#");
    UNIT_ASSERT(!program.Contains("virtual"));
    UNIT_ASSERT(!program.Contains("Read"));
}

Y_UNIT_TEST(CreateTableGeneratedColumnWithScalarSubquery) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                key Uint32,
                num Int32,
                gen Int32 GENERATED ALWAYS AS ((SELECT MAX(id) FROM other)) STORED,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"(SELECT MAX(id) FROM other)" 'stored)))#");
    UNIT_ASSERT(!program.Contains("virtual"));
    UNIT_ASSERT(!program.Contains("Read"));
}

Y_UNIT_TEST(CreateTableGeneratedWithPragma) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            PRAGMA FlexibleTypes;
            CREATE TABLE tbl (
                key Uint32,
                num Int32,
                gen Int32 GENERATED ALWAYS AS (num / 2) STORED,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\nPRAGMA FlexibleTypes;\n" '"num / 2" 'stored)))#");
    UNIT_ASSERT(!program.Contains("virtual"));
}

Y_UNIT_TEST(CreateTableGeneratedNestedPath) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE `a/b/tbl` (
                key Uint32,
                num Int32,
                gen Int32 GENERATED ALWAYS AS (num / 2) STORED,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"num / 2" 'stored)))#");
}

Y_UNIT_TEST(AlterTableAddGeneratedColumnNestedPath) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            ALTER TABLE `a/b/tbl` ADD COLUMN gen Int32 GENERATED ALWAYS AS (a + b) STORED;
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"a + b" 'stored)))#");
}

Y_UNIT_TEST(CreateTableStoreGeneratedColumnFails) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLESTORE tbl (
                key Uint32 NOT NULL,
                gen Int32 AS (5) STORED,
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "GENERATED ALWAYS AS columns are supported only for CREATE TABLE and ALTER TABLE");
}

Y_UNIT_TEST(CreateExternalTableGeneratedColumnFails) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE EXTERNAL TABLE tbl (
                a Int32,
                gen Int32 AS (a) STORED
            ) WITH (
                DATA_SOURCE="/Root/mydatasource",
                LOCATION="/folder1/*"
            );
        )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "GENERATED ALWAYS AS columns are supported only for CREATE TABLE and ALTER TABLE");
}

Y_UNIT_TEST(AlterExternalTableAddGeneratedColumnFails) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            ALTER EXTERNAL TABLE tbl ADD COLUMN gen Int32 AS (5) STORED;
        )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "GENERATED ALWAYS AS columns are supported only for CREATE TABLE and ALTER TABLE");
}

Y_UNIT_TEST(AlterTableStoreAddGeneratedColumnFails) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            ALTER TABLESTORE tbl ADD COLUMN gen Int32 AS (5) STORED;
        )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "GENERATED ALWAYS AS columns are supported only for CREATE TABLE and ALTER TABLE");
}

Y_UNIT_TEST(CreateTableGeneratedColumnNonYdbProviderFails) {
    auto res = SqlToYql(R"sql(
            USE plato;
            CREATE TABLE tbl (
                a Int32,
                gen Int32 AS (a) STORED,
                PRIMARY KEY (a)
            );
        )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "GENERATED ALWAYS AS columns are supported only for the ydb provider");
}

Y_UNIT_TEST(CreateTableGeneratedColumnNonYdbProviderVirtualFails) {
    auto res = SqlToYql(R"sql(
            USE plato;
            CREATE TABLE tbl (
                key Uint32,
                gen Int32 GENERATED ALWAYS AS (5),
                PRIMARY KEY (key)
            );
        )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "GENERATED ALWAYS AS columns are supported only for the ydb provider");
}

Y_UNIT_TEST(AlterTableAddGeneratedColumnNonYdbProviderFails) {
    auto res = SqlToYql(R"sql(
            USE plato;
            ALTER TABLE tbl ADD COLUMN gen Int32 AS (5) STORED;
        )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "ALTER TABLE is not supported for yt provider");
}

Y_UNIT_TEST(CreateTableGeneratedStringFunction) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                id Int32,
                name String,
                upper_name String GENERATED ALWAYS AS (String::AsciiToUpper(name)) STORED,
                PRIMARY KEY (id)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('generated '('"USE ydb;\n" '"String::AsciiToUpper(name)" 'stored)))#");
    UNIT_ASSERT(!program.Contains("virtual"));
}

} // Y_UNIT_TEST_SUITE(GeneratedColumns)
