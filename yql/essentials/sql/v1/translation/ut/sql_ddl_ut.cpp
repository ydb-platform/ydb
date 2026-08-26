#include "sql_ut.h"

#include <yql/essentials/sql/v1/translation/sql.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(ExternalDataSource) {
Y_UNIT_TEST(CreateExternalDataSourceWithAuthNone) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"NONE") '('"location" '"my-bucket") '('"source_type" '"ObjectStorage"))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateEDSWithSecretFromName) {
    const NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="BASIC",
                LOGIN="foo_login",
                PASSWORD_SECRET_NAME="foo_secret"
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "\"password_secret_name\" '\"foo_secret\"");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateEDSWithSecretFromPathWitoutTablePathPrefix) {
    const NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="BASIC",
                LOGIN="foo_login",
                PASSWORD_SECRET_PATH="foo_secret"
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "\"password_secret_path\" '\"foo_secret\"");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateEDSWithSecretsFromNameAndPathForOneSecretType) {
    // paths and names are mutually exclusive settings for one secret type, so fail is expected
    const NYql::TAstParseResult res = SqlToYql(R"sql(
        USE plato;
        CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="my-bucket",
            AUTH_METHOD="BASIC",
            LOGIN="foo_login",
            PASSWORD_SECRET_NAME="foo_secret",
            PASSWORD_SECRET_PATH="baz_secret"
        );
    )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(
        Err2Str(res),
        "<main>:9:34: Error: Usage secrets of different types is not allowed: "
        "PASSWORD_SECRET_NAME and PASSWORD_SECRET_PATH are set\n");
}

Y_UNIT_TEST(CreateEDSWithSecretsFromNameAndPathForDifferentSecretTypes) {
    // paths and names are mutually exclusive settings for different secret types (they can not be mixed), so fail is expected
    const NYql::TAstParseResult res = SqlToYql(R"sql(
        USE plato;
        CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="my-bucket",
            AUTH_METHOD="AWS",
            AWS_ACCESS_KEY_ID_SECRET_PATH="/foo_secret",
            AWS_SECRET_ACCESS_KEY_SECRET_NAME="bar_secret",
            AWS_REGION="ru-central-1"
        );
    )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(
        Err2Str(res),
        "<main>:9:24: Error: Usage secrets of different types is not allowed: "
        "AWS_SECRET_ACCESS_KEY_SECRET_NAME and AWS_ACCESS_KEY_ID_SECRET_PATH are set\n");
}

Y_UNIT_TEST(CreateEDSWithSecretsFromNameWithTablePathPrefix) {
    // pragma should not influence on the name setting
    const NYql::TAstParseResult res = SqlToYql(R"sql(
        USE plato; PRAGMA TablePathPrefix='/PathPrefix';
        CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="my-bucket",
            AUTH_METHOD="BASIC",
            LOGIN="foo_login",
            PASSWORD_SECRET_NAME="foo_secret"
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "\"password_secret_name\" '\"foo_secret\"");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateEDSWithTwoSecretsFromPathWithTablePathPrefix) {
    const NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato; PRAGMA TablePathPrefix='/PathPrefix';
            CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_PATH="/foo_secret",
                AWS_SECRET_ACCESS_KEY_SECRET_PATH="bar_secret",
                AWS_REGION="ru-central-1"
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "\"/foo_secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "/PathPrefix/bar_secret");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateExternalDataSourceWithAuthServiceAccount) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                        SOURCE_TYPE="ObjectStorage",
                        LOCATION="my-bucket",
                        AUTH_METHOD="SERVICE_ACCOUNT",
                        SERVICE_ACCOUNT_ID="sa",
                        SERVICE_ACCOUNT_SECRET_NAME="sa_secret_name"
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"SERVICE_ACCOUNT") '('"location" '"my-bucket") '('"service_account_id" '"sa") '('"service_account_secret_name" '"sa_secret_name") '('"source_type" '"ObjectStorage"))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateExternalDataSourceWithBasic) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                        SOURCE_TYPE="PostgreSQL",
                        LOCATION="protocol://host:port/",
                        AUTH_METHOD="BASIC",
                        LOGIN="admin",
                        PASSWORD_SECRET_NAME="secret_name"
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"BASIC") '('"location" '"protocol://host:port/") '('"login" '"admin") '('"password_secret_name" '"secret_name") '('"source_type" '"PostgreSQL"))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateExternalDataSourceWithMdbBasic) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                        SOURCE_TYPE="PostgreSQL",
                        LOCATION="protocol://host:port/",
                        AUTH_METHOD="MDB_BASIC",
                        SERVICE_ACCOUNT_ID="sa",
                        SERVICE_ACCOUNT_SECRET_NAME="sa_secret_name",
                        LOGIN="admin",
                        PASSWORD_SECRET_NAME="secret_name"
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"MDB_BASIC") '('"location" '"protocol://host:port/") '('"login" '"admin") '('"password_secret_name" '"secret_name") '('"service_account_id" '"sa") '('"service_account_secret_name" '"sa_secret_name") '('"source_type" '"PostgreSQL"))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateExternalDataSourceWithAws) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                        SOURCE_TYPE="PostgreSQL",
                        LOCATION="protocol://host:port/",
                        AUTH_METHOD="AWS",
                        AWS_ACCESS_KEY_ID_SECRET_NAME="secred_id_name",
                        AWS_SECRET_ACCESS_KEY_SECRET_NAME="secret_key_name",
                        AWS_REGION="ru-central-1"
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"AWS") '('"aws_access_key_id_secret_name" '"secred_id_name") '('"aws_region" '"ru-central-1") '('"aws_secret_access_key_secret_name" '"secret_key_name") '('"location" '"protocol://host:port/") '('"source_type" '"PostgreSQL"))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateExternalDataSourceWithToken) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                        SOURCE_TYPE="YT",
                        LOCATION="protocol://host:port/",
                        AUTH_METHOD="TOKEN",
                        TOKEN_SECRET_NAME="token_name"
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"TOKEN") '('"location" '"protocol://host:port/") '('"source_type" '"YT") '('"token_secret_name" '"token_name"))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateExternalDataSourceWithTablePrefix) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    pragma TablePathPrefix='/aba';
                    CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                        SOURCE_TYPE="ObjectStorage",
                        LOCATION="my-bucket",
                        AUTH_METHOD="NONE"
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "/aba/MyDataSource");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateExternalDataSourceIfNotExists) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    CREATE EXTERNAL DATA SOURCE IF NOT EXISTS MyDataSource WITH (
                        SOURCE_TYPE="ObjectStorage",
                        LOCATION="my-bucket",
                        AUTH_METHOD="NONE"
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"NONE") '('"location" '"my-bucket") '('"source_type" '"ObjectStorage"))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObjectIfNotExists"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterExternalDataSource) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    ALTER EXTERNAL DATA SOURCE MyDataSource
                        SET (SOURCE_TYPE = "ObjectStorage", Login = "Admin"),
                        SET Location "bucket",
                        RESET (Auth_Method, Service_Account_Id, Service_Account_Secret_Name);
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('mode 'alterObject))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('features '('('"location" '"bucket") '('"login" '"Admin") '('"source_type" '"ObjectStorage"))))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('resetFeatures '('"auth_method" '"service_account_id" '"service_account_secret_name")))#");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterExternalDataSourceWithTablePathPrefix) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            PRAGMA TablePathPrefix='/Root';
            ALTER EXTERNAL DATA SOURCE MyDataSource
                SET (
                    SOURCE_TYPE = "ObjectStorage",
                    LOCATION="bucket",
                    AUTH_METHOD="AWS",
                    AWS_REGION="ru-central-1",
                    AWS_SECRET_ACCESS_KEY_SECRET_PATH="secret1",
                    AWS_ACCESS_KEY_ID_SECRET_PATH="/dir/secret2"
                ),
                RESET (Service_Account_Id, Service_Account_Secret_Path);
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(objectId (String '"/Root/MyDataSource")#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('mode 'alterObject))#");
            // TablePathPrefix should change relative paths
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"aws_secret_access_key_secret_path" '"/Root/secret1"))#");
            // TablePathPrefix should NOT change absolute paths
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"aws_access_key_id_secret_path" '"/dir/secret2"))#");
        }
    };

    TWordCountHive elementStat = {std::make_pair("Write", 0)};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateExternalDataSourceOrReplace) {
    NYql::TAstParseResult res = SqlToYql(R"(
                    USE plato;
                    CREATE OR REPLACE EXTERNAL DATA SOURCE MyDataSource WITH (
                        SOURCE_TYPE="ObjectStorage",
                        LOCATION="my-bucket",
                        AUTH_METHOD="NONE"
                    );
                )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"NONE") '('"location" '"my-bucket") '('"source_type" '"ObjectStorage"))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObjectOrReplace"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateOrReplaceForUnsupportedTableTypesShouldFail) {
    ExpectFailWithError(R"sql(
                USE plato;
                CREATE OR REPLACE TABLE t (a int32 not null, primary key(a, a));
            )sql", "<main>:3:23: Error: OR REPLACE feature is supported only for EXTERNAL DATA SOURCE and EXTERNAL TABLE\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE OR REPLACE TABLE t (
                    Key Uint64,
                    Value1 String,
                    PRIMARY KEY (Key)
                )
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
                );
            )sql", "<main>:3:23: Error: OR REPLACE feature is supported only for EXTERNAL DATA SOURCE and EXTERNAL TABLE\n");
}

Y_UNIT_TEST(CreateExternalDataSourceWithBadArguments) {
    ExpectFailWithError(R"sql(
            USE plato;
            CREATE EXTERNAL DATA SOURCE MyDataSource;
        )sql", "<main>:3:52: Error: mismatched input ';' expecting WITH\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );
            )sql", "<main>:5:33: Error: SOURCE_TYPE requires key\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket"
                );
            )sql", "<main>:5:30: Error: AUTH_METHOD requires key\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE1"
                );
            )sql", "<main>:6:33: Error: Unknown AUTH_METHOD = NONE1\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="SERVICE_ACCOUNT"
                );
            )sql", "<main>:6:33: Error: SERVICE_ACCOUNT_ID requires key\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="SERVICE_ACCOUNT",
                    SERVICE_ACCOUNT_ID="s1"
                );
            )sql", "<main>:7:40: Error: A value must be provided for either SERVICE_ACCOUNT_SECRET_NAME or SERVICE_ACCOUNT_SECRET_PATH\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="SERVICE_ACCOUNT",
                    SERVICE_ACCOUNT_SECRET_NAME="s1"
                );
            )sql", "<main>:7:49: Error: SERVICE_ACCOUNT_ID requires key\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="BASIC",
                    LOGIN="admin"
                );
            )sql", "<main>:7:27: Error: A value must be provided for either PASSWORD_SECRET_NAME or PASSWORD_SECRET_PATH\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="BASIC",
                    PASSWORD_SECRET_NAME="secret_name"
                );
            )sql", "<main>:7:42: Error: LOGIN requires key\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="MDB_BASIC",
                    SERVICE_ACCOUNT_SECRET_NAME="sa_secret_name",
                    LOGIN="admin",
                    PASSWORD_SECRET_NAME="secret_name"
                );
            )sql", "<main>:9:42: Error: SERVICE_ACCOUNT_ID requires key\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="MDB_BASIC",
                    SERVICE_ACCOUNT_ID="sa",
                    LOGIN="admin",
                    PASSWORD_SECRET_NAME="secret_name"
                );
            )sql", "<main>:9:42: Error: A value must be provided for either SERVICE_ACCOUNT_SECRET_NAME or SERVICE_ACCOUNT_SECRET_PATH\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="MDB_BASIC",
                    SERVICE_ACCOUNT_ID="sa",
                    SERVICE_ACCOUNT_SECRET_NAME="sa_secret_name",
                    PASSWORD_SECRET_NAME="secret_name"
                );
            )sql", "<main>:9:42: Error: LOGIN requires key\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="MDB_BASIC",
                    SERVICE_ACCOUNT_ID="sa",
                    SERVICE_ACCOUNT_SECRET_NAME="sa_secret_name",
                    LOGIN="admin"
                );
            )sql", "<main>:9:27: Error: A value must be provided for either PASSWORD_SECRET_NAME or PASSWORD_SECRET_PATH\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="AWS",
                    AWS_SECRET_ACCESS_KEY_SECRET_NAME="secret_key_name",
                    AWS_REGION="ru-central-1"
                );
            )sql", "<main>:8:32: Error: A value must be provided for either AWS_ACCESS_KEY_ID_SECRET_NAME or AWS_ACCESS_KEY_ID_SECRET_PATH\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="AWS",
                    AWS_ACCESS_KEY_ID_SECRET_NAME="secred_id_name",
                    AWS_REGION="ru-central-1"
                );
            )sql", "<main>:8:32: Error: A value must be provided for either AWS_SECRET_ACCESS_KEY_SECRET_NAME or AWS_SECRET_ACCESS_KEY_SECRET_PATH\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="AWS",
                    AWS_SECRET_ACCESS_KEY_SECRET_NAME="secret_key_name",
                    AWS_ACCESS_KEY_ID_SECRET_NAME="secred_id_name"
                );
            )sql", "<main>:8:51: Error: AWS_REGION requires key\n");
}

Y_UNIT_TEST(DropExternalDataSourceWithTablePrefix) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
        USE plato;
        DROP EXTERNAL DATA SOURCE MyDataSource;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropExternalDataSourceIfExists) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
        USE plato;
        DROP EXTERNAL DATA SOURCE IF EXISTS MyDataSource;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "MyDataSource");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObjectIfExists"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropExternalDataSource) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
        USE plato;
        pragma TablePathPrefix='/aba';
        DROP EXTERNAL DATA SOURCE MyDataSource;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "/aba/MyDataSource");
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}
} // Y_UNIT_TEST_SUITE(ExternalDataSource)

Y_UNIT_TEST_SUITE(TopicsDDL) {
inline void TestQuery(const TString& query, bool expectOk = true, const TVector<TString> issueSubstrings = {}) {
    TStringBuilder finalQuery;

    finalQuery << "use plato;" << Endl << query;
    auto res = SqlToYql(finalQuery, 10, "kikimr");
    if (expectOk) {
        UNIT_ASSERT_C(res.IsOk(), "Query: " << query << "\n"
                                            << "Issues: " << Err2Str(res));
    } else {
        UNIT_ASSERT(!res.IsOk());
        for (const auto& issue : issueSubstrings) {
            UNIT_ASSERT_STRING_CONTAINS_C(Err2Str(res), issue, "Query: " << query << "\n"
                                                                         << "Issues: " << Err2Str(res));
        }
    }
}

Y_UNIT_TEST(CreateTopicSimple) {
    TestQuery(R"(
                CREATE TOPIC topic1;
            )");
    TestQuery(R"(
                CREATE TOPIC `cluster1.topic1`;
            )");
    TestQuery(R"(
                CREATE TOPIC topic1 WITH (metering_mode = "str_value", partition_count_limit = 123, retention_period = Interval('PT1H'));
            )");
    TestQuery(R"(
                CREATE TOPIC topic1 WITH (metrics_level = 3);
            )");
}

Y_UNIT_TEST(CreateTopicConsumer) {
    TestQuery(R"(
                CREATE TOPIC topic1 (CONSUMER cons1);
            )");
    TestQuery(R"(
                CREATE TOPIC topic1 (CONSUMER cons1, CONSUMER cons2 WITH (important = false));
            )");
    TestQuery(R"(
                CREATE TOPIC topic1 (CONSUMER cons1, CONSUMER cons2 WITH (important = false)) WITH (supported_codecs = "1,2,3");
            )");
    TestQuery(R"(
                CREATE TOPIC topic1 (CONSUMER cons1, CONSUMER cons2 WITH (important = false, availability_period = Interval('PT9H'))) WITH (supported_codecs = "1,2,3");
            )");
    TestQuery(R"(
                CREATE TOPIC topic1 (
                    CONSUMER cons1 WITH (
                        type = 'shared',
                        receive_message_wait_time = Interval('PT5S'),
                        receive_message_delay = Interval('PT7S')
                    )
                );
            )");
}

Y_UNIT_TEST(AlterTopicConsumerReceiveMessageSettings) {
    TestQuery(R"(
                ALTER TOPIC topic1
                    ADD CONSUMER cons1 WITH (
                        type = 'shared',
                        receive_message_wait_time = Interval('PT5S'),
                        receive_message_delay = Interval('PT7S')
                    );
            )");
    TestQuery(R"(
                ALTER TOPIC topic1
                    ALTER CONSUMER cons1 SET (
                        receive_message_wait_time = Interval('PT2S'),
                        receive_message_delay = Interval('PT3S')
                    );
            )");
}

Y_UNIT_TEST(AlterTopicSimple) {
    TestQuery(R"(
                ALTER TOPIC topic1 SET (retention_period = Interval('PT1H'));
            )");
    TestQuery(R"(
                ALTER TOPIC topic1 SET (retention_storage_mb = 3, partition_count_limit = 50);
            )");
    TestQuery(R"(
                ALTER TOPIC topic1 SET (metrics_level = 2);
            )");
    TestQuery(R"(
                ALTER TOPIC topic1 RESET (supported_codecs, retention_period);
            )");
    TestQuery(R"(
                ALTER TOPIC topic1 RESET (metrics_level);
            )");
    TestQuery(R"(
                ALTER TOPIC topic1 RESET (partition_write_speed_bytes_per_second),
                     SET (partition_write_burst_bytes = 11111, min_active_partitions = 1);
            )");
}
Y_UNIT_TEST(AlterTopicConsumer) {
    TestQuery(R"(
                ALTER TOPIC topic1 ADD CONSUMER consumer1,
                    ADD CONSUMER consumer2 WITH (important = false, supported_codecs = "RAW"),
                    ALTER CONSUMER consumer3 SET (important = false, read_from = 1),
                    ALTER CONSUMER consumer3 RESET (supported_codecs),
                    DROP CONSUMER consumer4,
                    ALTER CONSUMER consumer5 SET (availability_period = Interval('PT9H')),
                    ALTER CONSUMER consumer6 RESET (availability_period),
                    SET (partition_count_limit = 11, retention_period = Interval('PT1H')),
                    RESET(metering_mode)
            )");
}
Y_UNIT_TEST(DropTopic) {
    TestQuery(R"(
                DROP TOPIC topic1;
            )");
}

Y_UNIT_TEST(TopicBadRequests) {
    TestQuery(R"(
            CREATE TOPIC topic1();
        )", /*expectOk=*/false);
    TestQuery(R"(
            CREATE TOPIC topic1 SET setting1 = value1;
        )", /*expectOk=*/false);
    TestQuery(R"(
            ALTER TOPIC topic1 SET setting1 value1;
        )", /*expectOk=*/false);
    TestQuery(R"(
            ALTER TOPIC topic1 RESET setting1;
        )", /*expectOk=*/false);

    TestQuery(R"(
            ALTER TOPIC topic1 DROP CONSUMER consumer4 WITH (k1 = v1);
        )", /*expectOk=*/false);

    TestQuery(R"(
            CREATE TOPIC topic1 WITH (retention_period = 123);
        )", /*expectOk=*/false,
              {"3:58: Error: Literal of Interval type is expected for retention"});
    TestQuery(R"(
            CREATE TOPIC topic1 WITH (metrics_level = "1");
        )", /*expectOk=*/false,
              {"3:55: Error: METRICS_LEVEL value should be an integer"});
    TestQuery(R"(
            CREATE TOPIC topic1 (CONSUMER cons1, CONSUMER cons1 WITH (important = false));
        )", /*expectOk=*/false,
              {"3:59: Error: Consumer cons1 defined more than once"});
    TestQuery(R"(
            CREATE TOPIC topic1 (CONSUMER cons1 WITH (bad_option = false));
        )", /*expectOk=*/false,
              {"3:68: Error: BAD_OPTION: unknown option for consumer"});
    TestQuery(R"(
            CREATE TOPIC topic1 (CONSUMER cons1 WITH (important = false, important = true));
        )", /*expectOk=*/false,
              {"3:86: Error: IMPORTANT specified multiple times in CONSUMER statement for single consumer"});
    TestQuery(R"(
            ALTER TOPIC topic1 SET (metrics_level = "1");
        )", /*expectOk=*/false,
              {"3:53: Error: METRICS_LEVEL value should be an integer"});
    TestQuery(R"(
            ALTER TOPIC topic1 ADD CONSUMER cons1, ALTER CONSUMER cons1 RESET (important);
        )", /*expectOk=*/false,
              {"3:80: Error: IMPORTANT reset is not supported"});
    TestQuery(R"(
            ALTER TOPIC topic1 ADD CONSUMER consumer1,
                ALTER CONSUMER consumer3 SET (supported_codecs = "RAW", read_from = 1),
                ALTER CONSUMER consumer3 RESET (supported_codecs);
        )", /*expectOk=*/false,
              {"5:49: Error: SUPPORTED_CODECS specified multiple times in ALTER CONSUMER statement for single consumer"});
    TestQuery(R"(
            ALTER TOPIC topic1 ADD CONSUMER consumer1,
                ALTER CONSUMER consumer3 SET (supported_codecs = "RAW", read_from = 1),
                ALTER CONSUMER consumer3 SET (read_from = 2);
        )", /*expectOk=*/false,
              {"5:59: Error: READ_FROM specified multiple times in CONSUMER statement for single consumer"});
    TestQuery(R"(
            CREATE TOPIC topic1 (CONSUMER cons1 WITH (availability_period = 3600));
        )", /*expectOk=*/false,
              {"3:77: Error: Literal of Interval type is expected for AVAILABILITY_PERIOD setting"});
    TestQuery(R"(
            ALTER TOPIC topic1
                ALTER CONSUMER consumer3 SET (availability_period = false);
        )", /*expectOk=*/false,
              {"4:69: Error: Literal of Interval type is expected for AVAILABILITY_PERIOD setting"});
}

Y_UNIT_TEST(TopicWithPrefix) {
    NYql::TAstParseResult res = SqlToYql(R"(
                USE plato;
                PRAGMA TablePathPrefix = '/database/path/to/tables';
                ALTER TOPIC `my_table/my_feed` ADD CONSUMER `my_consumer`;
            )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("/database/path/to/tables/my_table/my_feed"), 0}, {"topic", 0}};
    VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["topic"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["/database/path/to/tables/my_table/my_feed"]);
}
} // Y_UNIT_TEST_SUITE(TopicsDDL)

Y_UNIT_TEST_SUITE(ResourcePool) {

Y_UNIT_TEST(CreateResourcePool) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    CREATE RESOURCE POOL MyResourcePool WITH (
                        CONCURRENT_QUERY_LIMIT=20,
                        QUERY_CANCEL_AFTER_SECONDS=86400,
                        QUEUE_TYPE="FIFO"
                    );
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"concurrent_query_limit" (Int32 '"20")) '('"query_cancel_after_seconds" (Int32 '"86400")) '('"queue_type" '"FIFO"))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateResourcePoolWithBadArguments) {
    ExpectFailWithError(R"sql(
            USE plato;
            CREATE RESOURCE POOL MyResourcePool;
        )sql", "<main>:3:47: Error: mismatched input ';' expecting WITH\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE RESOURCE POOL MyResourcePool WITH (
                    DUPLICATE_SETTING="first_value",
                    DUPLICATE_SETTING="second_value"
                );
            )sql", "<main>:5:21: Error: DUPLICATE_SETTING duplicate keys\n");
}

Y_UNIT_TEST(AlterResourcePool) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    ALTER RESOURCE POOL MyResourcePool
                        SET (CONCURRENT_QUERY_LIMIT = 30, Weight = 5, QUEUE_TYPE = "UNORDERED"),
                        RESET (Query_Cancel_After_Seconds, Query_Count_Limit);
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('mode 'alterObject))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('features '('('"concurrent_query_limit" (Int32 '"30")) '('"queue_type" '"UNORDERED") '('"weight" (Int32 '"5")))))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('resetFeatures '('"query_cancel_after_seconds" '"query_count_limit")))#");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropResourcePool) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                    USE plato;
                    DROP RESOURCE POOL MyResourcePool;
                )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}
} // Y_UNIT_TEST_SUITE(ResourcePool)

Y_UNIT_TEST_SUITE(BackupCollection) {

Y_UNIT_TEST(CreateBackupCollection) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
        USE plato;
        CREATE BACKUP COLLECTION TestCollection WITH (
            STORAGE="local",
            TAG="test" -- for testing purposes, not a real thing
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"#('"TestCollection")#"));
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('"storage" (String '"local")))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('"tag" (String '"test"))))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('entries '()))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'create"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateBackupCollectionWithDatabase) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
        USE plato;
        CREATE BACKUP COLLECTION TestCollection DATABASE WITH (
            STORAGE="local",
            TAG="test" -- for testing purposes, not a real thing
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"#('"TestCollection")#"));
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('"storage" (String '"local")))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('"tag" (String '"test"))))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('entries '('('('type 'database)))))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'create"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateBackupCollectionWithTables) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
        USE plato;
        CREATE BACKUP COLLECTION TestCollection (
            TABLE someTable,
            TABLE `prefix/anotherTable`
        ) WITH (
            STORAGE="local",
            TAG="test" -- for testing purposes, not a real thing
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"#('"TestCollection")#"));
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('"storage" (String '"local")))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('"tag" (String '"test"))))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('entries '('('('type 'table) '('path '"someTable")) '('('type 'table) '('path '"prefix/anotherTable")))))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'create"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateBackupCollectionWithBadArguments) {
    ExpectFailWithError(R"sql(
                USE plato;
                CREATE BACKUP COLLECTION TestCollection;
            )sql", "<main>:3:55: Error: mismatched input ';' expecting {'(', DATABASE, WITH}\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE BACKUP COLLECTION TABLE TestCollection;
            )sql", "<main>:3:47: Error: mismatched input 'TestCollection' expecting {'(', DATABASE, WITH}\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE BACKUP COLLECTION DATABASE `test` TestCollection;
            )sql", "<main>:3:50: Error: mismatched input '`test`' expecting {'(', DATABASE, WITH}\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE BACKUP COLLECTION TestCollection WITH (
                    DUPLICATE_SETTING="first_value",
                    DUPLICATE_SETTING="second_value"
                );
            )sql", "<main>:5:21: Error: DUPLICATE_SETTING duplicate keys\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE BACKUP COLLECTION TestCollection WITH (
                    INT_SETTING=1
                );
            )sql", "<main>:4:21: Error: INT_SETTING value should be a string literal\n");
}

Y_UNIT_TEST(AlterBackupCollection) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                ALTER BACKUP COLLECTION TestCollection
                    SET (STORAGE="remote"), -- also just for test
                    SET (TAG1 = "123"),
                    RESET (TAG2, TAG3);
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"#('"TestCollection")#"));
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('mode 'alter))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('"storage" (String '"remote")))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('"tag1" (String '"123"))))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('resetSettings '('"tag2" '"tag3")))#");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterBackupCollectionEntries) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                ALTER BACKUP COLLECTION TestCollection
                    DROP TABLE `test`,
                    ADD DATABASE;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"#('"TestCollection")#"));
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('mode 'alter))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('alterEntries)#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('type 'table) '('path '"test") '('action 'drop)))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('type 'database) '('action 'add)))#");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropBackupCollection) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                DROP BACKUP COLLECTION TestCollection;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"#('"TestCollection")#"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("drop"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}
} // Y_UNIT_TEST_SUITE(BackupCollection)

Y_UNIT_TEST_SUITE(ResourcePoolClassifier) {

Y_UNIT_TEST(CreateResourcePoolClassifier) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                    RANK=20,
                    RESOURCE_POOL='wgUserQueries',
                    MEMBER_NAME='yandex_query@abc'
                );
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"member_name" '"yandex_query@abc") '('"rank" (Int32 '"20")) '('"resource_pool" '"wgUserQueries"))#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateResourcePoolClassifierWithBadArguments) {
    ExpectFailWithError(R"sql(
                USE plato;
                CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier;
            )sql", "<main>:3:72: Error: mismatched input ';' expecting WITH\n");

    ExpectFailWithError(R"sql(
                USE plato;
                CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                    DUPLICATE_SETTING="first_value",
                    DUPLICATE_SETTING="second_value"
                );
            )sql", "<main>:5:21: Error: DUPLICATE_SETTING duplicate keys\n");
}

Y_UNIT_TEST(AlterResourcePoolClassifier) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                ALTER RESOURCE POOL CLASSIFIER MyResourcePoolClassifier
                    SET (RANK = 30, Weight = 5, MEMBER_NAME = "test@user"),
                    RESET (Resource_Pool);
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#(('mode 'alterObject))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('features '('('"member_name" '"test@user") '('"rank" (Int32 '"30")) '('"weight" (Int32 '"5")))))#");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('resetFeatures '('"resource_pool")))#");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropResourcePoolClassifier) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                DROP RESOURCE POOL CLASSIFIER MyResourcePoolClassifier;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(BacktickMatching) {
    auto req = "select\n"
               "    1 as `Schema has \\`RealCost\\``\n"
               "    -- foo`bar";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
    res = SqlToYqlWithAnsiLexer(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);

    req = R"(select 1 as `a``b`, 2 as ````, 3 as `\x60a\x60`, 4 as ```b```, 5 as `\`c\``)";
    res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
    res = SqlToYqlWithAnsiLexer(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
}
} // Y_UNIT_TEST_SUITE(ResourcePoolClassifier)

Y_UNIT_TEST_SUITE(Backup) {

Y_UNIT_TEST(Simple) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                BACKUP TestCollection;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"#('"TestCollection")#"));
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'Incremental"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'backup"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(Incremental) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                BACKUP TestCollection INCREMENTAL;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"#('"TestCollection")#"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'backupIncremental"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}
} // Y_UNIT_TEST_SUITE(Backup)

Y_UNIT_TEST_SUITE(Restore) {

Y_UNIT_TEST(Simple) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                RESTORE TestCollection;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"#('"TestCollection")#"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'restore"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AtPoint) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                RESTORE TestCollection AT '2024-06-16_20-14-02';
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"#('"TestCollection")#"));
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('at '"2024-06-16_20-14-02")#");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'restore"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}
} // Y_UNIT_TEST_SUITE(Restore)

Y_UNIT_TEST_SUITE(Transfer) {

Y_UNIT_TEST(Lambda) {
    NYql::TAstParseResult res = SqlToYql(R"( use plato;
            -- Русский коммент, empty statement
            ;

            -- befor comment
            $a = "А";

            SELECT * FROM Input;

            $b = ($x) -> { return $a || $x; };

            CREATE TRANSFER `TransferName`
                FROM `TopicName` TO `TableName`
                USING ($x) -> {
                -- internal comment
                return $b($x);
                }
                WITH (
                CONNECTION_STRING = "grpc://localhost:2135/?database=/Root"
                );
    )");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_VALUES_EQUAL_C(res.Issues.Size(), 0, Err2Str(res));

    const auto programm = GetPrettyPrint(res);

    auto expected = R"('transformLambda 'use plato;
-- befor comment
            $a = "А";
$b = ($x) -> { return $a || $x; };
$__ydb_transfer_lambda = ($x) -> {
                -- internal comment
                return $b($x);
                };
))";

    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, programm.find(expected));
}

Y_UNIT_TEST(CreateWithSecretsWithoutTablePathPrefix) {
    const NYql::TAstParseResult res = SqlToYql(R"sql(
            use plato;
            SELECT * FROM Input;

            $b = ($x) -> { return $x; };

            CREATE TRANSFER `TransferName`
                FROM `TopicName` TO `TableName`
                USING ($x) -> {
                    return $b($x);
                }
                WITH (
                    TOKEN_SECRET_NAME = "foo_secret",
                    PASSWORD_SECRET_PATH = "bar_secret"
                );
        )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "settings") {
            UNIT_ASSERT_STRING_CONTAINS(line, "\"foo_secret"); // names remain unchanged
            UNIT_ASSERT_STRING_CONTAINS(line, "\"bar_secret"); // paths remain unchanged
        }
    };

    TWordCountHive elementStat = {{TString("settings"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["settings"]);
}

Y_UNIT_TEST(CreateWithSecretsWithTablePathPrefix) {
    const NYql::TAstParseResult res = SqlToYql(R"sql(
            use plato;
            PRAGMA TablePathPrefix = "/PathPrefix";
            SELECT * FROM Input;

            $b = ($x) -> { return $x; };

            CREATE TRANSFER `TransferName`
                FROM `TopicName` TO `TableName`
                USING ($x) -> {
                    return $b($x);
                }
                WITH (
                    TOKEN_SECRET_NAME = "foo_secret",
                    PASSWORD_SECRET_PATH = "bar_secret",
                    INITIAL_TOKEN_SECRET_PATH = "/baz_secret"
                );
        )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "settings") {
            UNIT_ASSERT_STRING_CONTAINS(line, "\"foo_secret");           // names remain unchanged
            UNIT_ASSERT_STRING_CONTAINS(line, "/PathPrefix/bar_secret"); // paths are adjusted if not absolute
            UNIT_ASSERT_STRING_CONTAINS(line, "\"/baz_secret");          // absolute paths remain unchanged
        }
    };

    TWordCountHive elementStat = {{TString("settings"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["settings"]);
}

Y_UNIT_TEST(CreateWithSecretsWithNameAndPath) {
    // these are mutually exclusive settings
    const auto requestTemplate = R"sql(
        USE plato;
        SELECT * FROM Input;

        $b = ($x) -> { return $x; };

        CREATE TRANSFER `TransferName`
        FROM `TopicName` TO `TableName`
        USING ($x) -> {
            return $b($x);
        }
        WITH (
            %s = "",
            %s = ""
        );
    )sql";

    static const TVector<std::pair<TString, TString>> MutuallyExclusiveSettings = {
        {"TOKEN_SECRET_NAME", "TOKEN_SECRET_PATH"},
        {"PASSWORD_SECRET_NAME", "PASSWORD_SECRET_PATH"},
        {"INITIAL_TOKEN_SECRET_NAME", "INITIAL_TOKEN_SECRET_PATH"},
    };

    for (const auto& settings : MutuallyExclusiveSettings) {
        auto req = Sprintf(requestTemplate, settings.first.c_str(), settings.second.c_str());
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(
            Err2Str(res),
            std::format(
                "<main>:14:{}: Error: {} and {} are mutually exclusive\n",
                16 + settings.first.size(), settings.first.c_str(), settings.second.c_str()));
    }
}

Y_UNIT_TEST(AlterWithSecretsWithTablePathPrefix) {
    const NYql::TAstParseResult res = SqlToYql(R"sql(
            use plato;
            PRAGMA TablePathPrefix = "/PathPrefix";
            ALTER TRANSFER `TransferName`
                SET (
                    TOKEN_SECRET_NAME = "foo_secret",
                    PASSWORD_SECRET_PATH = "bar_secret",
                    INITIAL_TOKEN_SECRET_PATH = "/baz_secret"
                );
        )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "settings") {
            UNIT_ASSERT_STRING_CONTAINS(line, "\"foo_secret");           // names remain unchanged
            UNIT_ASSERT_STRING_CONTAINS(line, "/PathPrefix/bar_secret"); // paths are adjusted if not absolute
            UNIT_ASSERT_STRING_CONTAINS(line, "\"/baz_secret");          // absolute paths remain unchanged
        }
    };

    TWordCountHive elementStat = {{TString("settings"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["settings"]);
}

Y_UNIT_TEST(MetricsLevel) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            $b = ($x) -> { return "A" || $x; };

            CREATE TRANSFER `TransferName`
            FROM `TopicName` TO `TableName`
            USING ($x) -> { return $b($x); }
            WITH (
                CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                METRICS_LEVEL = "DETAILED"
             );
        )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_VALUES_EQUAL_C(res.Issues.Size(), 0, Err2Str(res));

    const auto programm = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(
        GetPrettyPrint(res),
        R"('"metrics_level" (String '"DETAILED"))");
}

Y_UNIT_TEST(MetricsLevelCaseSensivity) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            $b = ($x) -> { return "A" || $x; };

            CREATE TRANSFER `TransferName`
            FROM `TopicName` TO `TableName`
            USING ($x) -> { return $b($x); }
            WITH (
                CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                METRICS_LEVEL = "object"
             );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(MetricsLevelValueValidation) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            $b = ($x) -> { return "A" || $x; };

            CREATE TRANSFER `TransferName`
            FROM `TopicName` TO `TableName`
            USING ($x) -> { return $b($x); }
            WITH (
                CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                METRICS_LEVEL = "BAD"
             );
        )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Invalid metrics_level value");
}

Y_UNIT_TEST(MetricsLevelNumericValue) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            $b = ($x) -> { return "A" || $x; };

            CREATE TRANSFER `TransferName`
            FROM `TopicName` TO `TableName`
            USING ($x) -> { return $b($x); }
            WITH (
                CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                METRICS_LEVEL = "1"
             );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(MetricsLevelEmptyValue) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE TRANSFER `TransferName`
            FROM `TopicName` TO `TableName`
            USING ($x) -> { return $x; }
            WITH (METRICS_LEVEL = "");
        )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "metrics_level value must be a string literal");
}

Y_UNIT_TEST(MetricsLevelBadNumericValue) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE TRANSFER `TransferName`
            FROM `TopicName` TO `TableName`
            USING ($x) -> { return $x; }
            WITH (METRICS_LEVEL = 11);
        )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Invalid numeric value for metrics_value");
}

Y_UNIT_TEST(TransferCPULimit) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            $b = ($x) -> { return "A" || $x; };

            CREATE TRANSFER `TransferName`
            FROM `TopicName` TO `TableName`
            USING ($x) -> { return $b($x); }
            WITH (
                CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                V_CPU_RATE_LIMIT = 1.5
             );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AlterTransferWithCPULimit) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            $b = ($x) -> { return "A" || $x; };

            ALTER TRANSFER `TransferName`
            SET (
                V_CPU_RATE_LIMIT = 4
             );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}
} // Y_UNIT_TEST_SUITE(Transfer)

Y_UNIT_TEST_SUITE(OlapPartitionCount) {
Y_UNIT_TEST(CorrectUsage) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE ydb;
                CREATE TABLE `mytable` (id Uint32, PRIMARY KEY (id))
                PARTITION BY HASH(id)
                WITH (STORE = COLUMN, PARTITION_COUNT = 8);
            )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(UseWithoutColumnStore) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE ydb;
                CREATE TABLE `mytable` (id Uint32, PRIMARY KEY (id))
                WITH (PARTITION_COUNT = 8);
            )sql");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT(res.Issues.Size() == 1);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "PARTITION_COUNT can be used only with STORE=COLUMN");
}
} // Y_UNIT_TEST_SUITE(OlapPartitionCount)

Y_UNIT_TEST_SUITE(StreamingQuery) {
Y_UNIT_TEST(CreateStreamingQueryBasic) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
USE ydb;
CREATE TABLE test (Key Int32 NOT NULL, PRIMARY KEY (Key));
-- Some comment
CREATE STREAMING QUERY MyQuery AS DO BEGIN
PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;
USE plato;
$source = SELECT * FROM Input;
INSERT INTO Output1 SELECT * FROM $source;
INSERT INTO Output2 SELECT * FROM $source;END DO;
USE hahn;
-- Other comment
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "createObject") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_ast" (block '()#");
        }

        if (word == "__query_text") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_text" '"\nPRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;\nUSE plato;\n$source = SELECT * FROM Input;\nINSERT INTO Output1 SELECT * FROM $source;\nINSERT INTO Output2 SELECT * FROM $source;")))#");
        }
    };

    TWordCountHive elementStat = {
        {TString("createObject"), 0},
        {TString("__query_text"), 0},
        {TString("(let world (World))"), 0},
    };
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["createObject"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["__query_text"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["(let world (World))"]);
}

Y_UNIT_TEST(CreateStreamingQueryCrlfCheck) {
    NYql::TAstParseResult res = SqlToYql(TStringBuilder() << R"sql(
USE plato;
-- Some comment
CREATE STREAMING QUERY MyQuery AS DO )sql" << "\r" << R"sql(BEGIN
USE plato;
$source = SELECT * FROM Input;
INSERT INTO Output1 SELECT * FROM $source;
INSERT INTO Output2 SELECT * FROM $source;
END DO;
USE hahn;
-- Other comment
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "createObject") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_ast" (block '()#");
        }

        if (word == "__query_text") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_text" '"\nUSE plato;\n$source = SELECT * FROM Input;\nINSERT INTO Output1 SELECT * FROM $source;\nINSERT INTO Output2 SELECT * FROM $source;\n")))#");
        }
    };

    TWordCountHive elementStat = {{TString("createObject"), 0}, {TString("__query_text"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["createObject"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["__query_text"]);
}

Y_UNIT_TEST(CreateStreamingQueryWithSettings) {
    NYql::TAstParseResult res = SqlToYql(TStringBuilder() << R"sql(
USE plato;
-- Some comment
CREATE STREAMING QUERY MyQuery WITH (
    RUN = TRUE,
    RESOURCE_POOL = my_pool,
    STREAMING_DISPOSITION = (
        FROM_TIME = "2025-05-04T11:30:34.336938Z"
    )
) AS DO )sql" << "\r" << R"sql(BEGIN
USE plato;
$source = SELECT * FROM Input;
INSERT INTO Output1 SELECT * FROM $source;
INSERT INTO Output2 SELECT * FROM $source;
END DO;
USE hahn;
-- Other comment
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "createObject") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_ast" (block '()#");
        }

        if (word == "__query_text") {
            UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder()
                                                  << R"#('('"__query_text" '"\nUSE plato;\n$source = SELECT * FROM Input;\nINSERT INTO Output1 SELECT * FROM $source;\nINSERT INTO Output2 SELECT * FROM $source;\n") )#"
                                                  << R"#('('"resource_pool" '"my_pool") '('"run" (Bool '"true")) '('"streaming_disposition" '('('"from_time" '"2025-05-04T11:30:34.336938Z"))))#");
        }
    };

    TWordCountHive elementStat = {{TString("createObject"), 0}, {TString("__query_text"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["createObject"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["__query_text"]);
}

Y_UNIT_TEST(CreateOrReplaceStreamingQuery) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                CREATE OR REPLACE STREAMING QUERY MyQuery AS DO BEGIN /* create or replace */ SELECT 42; END DO;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "createObjectOrReplace") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_ast" (block '()#");
        }

        if (word == "__query_text") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_text" '" /* create or replace */ SELECT 42; ")))#");
        }
    };

    TWordCountHive elementStat = {{TString("createObjectOrReplace"), 0}, {TString("__query_text"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["createObjectOrReplace"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["__query_text"]);
}

Y_UNIT_TEST(CreateStreamingQueryIfNotExists) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                CREATE STREAMING QUERY IF NOT EXISTS MyQuery AS DO BEGIN /* create if not exists */ SELECT 42; END DO;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "createObjectIfNotExists") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_ast" (block '()#");
        }

        if (word == "__query_text") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_text" '" /* create if not exists */ SELECT 42; ")))#");
        }
    };

    TWordCountHive elementStat = {{TString("createObjectIfNotExists"), 0}, {TString("__query_text"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["createObjectIfNotExists"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["__query_text"]);
}

Y_UNIT_TEST(CreateStreamingQueryWithTablePrefix) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                PRAGMA TablePathPrefix='/aba';
                CREATE STREAMING QUERY MyQuery AS DO BEGIN SELECT * FROM hahn.Input; END DO;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "createObject") {
            UNIT_ASSERT_STRING_CONTAINS(line, "/aba/MyQuery");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_ast" (block '()#");
        }

        if (word == "__query_text") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_text" '" SELECT * FROM hahn.Input; ")))#");
        }
    };

    TWordCountHive elementStat = {{TString("createObject"), 0}, {TString("__query_text"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["createObject"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["__query_text"]);
}

Y_UNIT_TEST(CreateStreamingQueryWithBadArguments) {
    ExpectFailWithError(R"sql(
            USE plato;
            CREATE STREAMING QUERY MyQuery WITH (OPTION = "VALUE");
        )sql", "<main>:3:66: Error: mismatched input ';' expecting AS\n");

    ExpectFailWithError(R"sql(
            USE plato;
            CREATE STREAMING QUERY MyQuery WITH (
                DUPLICATE_SETTING = "first_value",
                DUPLICATE_SETTING = "second_value"
            ) AS
            DO BEGIN
                USE plato;
                INSERT INTO Output SELECT * FROM Input;
            END DO;
        )sql", "<main>:5:17: Error: Found duplicated parameter: DUPLICATE_SETTING\n");

    ExpectFailWithError(R"sql(
            USE plato;
            CREATE STREAMING QUERY MyQuery WITH (
                `__QUERY_TEXT` = "SELECT 42"
            ) AS
            DO BEGIN
                USE plato;
                INSERT INTO Output SELECT * FROM Input;
            END DO;
        )sql", "<main>:4:17: Error: Streaming query parameter name should not start with prefix '__': __QUERY_TEXT\n");

    ExpectFailWithError(R"sql(
            USE plato;
            $named_node = 42;
            CREATE STREAMING QUERY MyQuery AS
            DO BEGIN
                SELECT $named_node;
            END DO;
        )sql", "<main>:6:24: Error: Unknown name: $named_node\n");
}

Y_UNIT_TEST(AlterStreamingQuerySetQuery) {
    NYql::TAstParseResult res = SqlToYql(TStringBuilder() << R"sql(
USE ydb;
CREATE TABLE test (Key Int32 NOT NULL, PRIMARY KEY (Key));
-- Some comment
ALTER STREAMING QUERY MyQuery AS DO )sql" << "\r" << R"sql(BEGIN
PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;
USE plato;
$source = SELECT * FROM Input;
INSERT INTO Output1 SELECT * FROM $source;
INSERT INTO Output2 SELECT * FROM $source;
END DO;
USE hahn;
-- Other comment
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "alterObject") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_ast" (block '()#");
        }

        if (word == "__query_text") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_text" '"\nPRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;\nUSE plato;\n$source = SELECT * FROM Input;\nINSERT INTO Output1 SELECT * FROM $source;\nINSERT INTO Output2 SELECT * FROM $source;\n")))#");
        }
    };

    TWordCountHive elementStat = {
        {TString("alterObject"), 0},
        {TString("__query_text"), 0},
        {TString("(let world (World))"), 0},
    };
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["alterObject"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["__query_text"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["(let world (World))"]);
}

Y_UNIT_TEST(AlterStreamingQuerySetOptions) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                ALTER STREAMING QUERY MyQuery SET (
                    WAIT_CHECKPOINT = TRUE,
                    RESOURCE_POOL = other_pool,
                    STREAMING_DISPOSITION = (
                        TIME_AGO = "PT1H"
                    )
                );
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"resource_pool" '"other_pool") '('"streaming_disposition" '('('"time_ago" '"PT1H"))) '('"wait_checkpoint" (Bool '"true"))))#");
            UNIT_ASSERT_STRING_CONTAINS(line, "alterObject");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterStreamingQuerySetBothOptionsAndQuery) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                ALTER STREAMING QUERY MyQuery SET (
                    WAIT_CHECKPOINT = TRUE,
                    RESOURCE_POOL = other_pool
                ) AS DO BEGIN /* alter */ SELECT 42; END DO;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "alterObject") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_ast" (block '()#");
        }

        if (word == "__query_text") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_text" '" /* alter */ SELECT 42; ") '('"resource_pool" '"other_pool") '('"wait_checkpoint" (Bool '"true"))))#");
        }
    };

    TWordCountHive elementStat = {{TString("alterObject"), 0}, {TString("__query_text"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["alterObject"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["__query_text"]);
}

Y_UNIT_TEST(AlterStreamingQueryIfExists) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                ALTER STREAMING QUERY IF EXISTS MyQuery AS DO BEGIN /* alter if exists */ SELECT 42; END DO;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "alterObjectIfExists") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_ast" (block '()#");
        }

        if (word == "__query_text") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_text" '" /* alter if exists */ SELECT 42; ")))#");
        }
    };

    TWordCountHive elementStat = {{TString("alterObjectIfExists"), 0}, {TString("__query_text"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["alterObjectIfExists"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["__query_text"]);
}

Y_UNIT_TEST(AlterStreamingQueryWithTablePrefix) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                PRAGMA TablePathPrefix='/aba';
                ALTER STREAMING QUERY MyQuery AS DO BEGIN SELECT * FROM hahn.Input; END DO;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "alterObject") {
            UNIT_ASSERT_STRING_CONTAINS(line, "/aba/MyQuery");
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_ast" (block '()#");
        }

        if (word == "__query_text") {
            UNIT_ASSERT_STRING_CONTAINS(line, R"#('('"__query_text" '" SELECT * FROM hahn.Input; ")))#");
        }
    };

    TWordCountHive elementStat = {{TString("alterObject"), 0}, {TString("__query_text"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["alterObject"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["__query_text"]);
}

Y_UNIT_TEST(AlterStreamingQueryWithBadArguments) {
    ExpectFailWithError(R"sql(
            USE plato;
            ALTER STREAMING QUERY MyQuery;
        )sql", "<main>:3:41: Error: mismatched input ';' expecting {AS, SET}\n");

    ExpectFailWithError(R"sql(
            USE plato;
            ALTER STREAMING QUERY MyQuery SET (
                DUPLICATE_SETTING = "first_value",
                DUPLICATE_SETTING = "second_value"
            );
        )sql", "<main>:5:17: Error: Found duplicated parameter: DUPLICATE_SETTING\n");

    ExpectFailWithError(R"sql(
            USE plato;
            ALTER STREAMING QUERY MyQuery SET (
                `__QUERY_TEXT` = "SELECT 42"
            );
        )sql", "<main>:4:17: Error: Streaming query parameter name should not start with prefix '__': __QUERY_TEXT\n");

    ExpectFailWithError(R"sql(
            USE plato;
            $named_node = 42;
            ALTER STREAMING QUERY MyQuery AS
            DO BEGIN
                SELECT $named_node;
            END DO;
        )sql", "<main>:6:24: Error: Unknown name: $named_node\n");
}

Y_UNIT_TEST(DropStreamingQueryBasic) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                DROP STREAMING QUERY MyQuery;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
            UNIT_ASSERT_STRING_CONTAINS(line, "dropObject");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropStreamingQueryIfExists) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                DROP STREAMING QUERY IF EXISTS MyQuery;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
            UNIT_ASSERT_STRING_CONTAINS(line, "dropObjectIfExists");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}
} // Y_UNIT_TEST_SUITE(StreamingQuery)
