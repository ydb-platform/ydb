#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/protos/table_stats.pb.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;

namespace NKikimr {
namespace {

namespace NTypeIds = NScheme::NTypeIds;
using TTypeInfo = NScheme::TTypeInfo;

static const TString defaultStoreSchema = R"(
    Name: "OlapStore"
    ColumnShardCount: 1
    SchemaPresets {
        Name: "default"
        Schema {
            Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
            Columns { Name: "data" Type: "Utf8" }
            KeyColumnNames: "timestamp"
        }
    }
)";

static const TString invalidStoreSchema = R"(
    Name: "OlapStore"
    ColumnShardCount: 1
    SchemaPresets {
        Name: "default"
        Schema {
            Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
            Columns { Name: "data" Type: "Utf8" }
            Columns { Name: "mess age" Type: "Utf8" }
            KeyColumnNames: "timestamp"
        }
    }
)";

static const TString defaultTableSchema = R"(
    Name: "ColumnTable"
    ColumnShardCount: 1
    Schema {
        Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
        Columns { Name: "data" Type: "Utf8" }
        KeyColumnNames: "timestamp"
    }
)";

static const TVector<NArrow::NTest::TTestColumn> defaultYdbSchema = {
    NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp)).SetNullable(false),
    NArrow::NTest::TTestColumn("data", TTypeInfo(NTypeIds::Utf8) )
};

}}

Y_UNIT_TEST_SUITE(TOlap_Naming) {

    Y_UNIT_TEST(CreateColumnTableOk) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // const TString& olapSchema = defaultStoreSchema;

        // TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        // env.TestWaitNotification(runtime, txId);

        // TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathExist);

        // TestMkDir(runtime, ++txId, "/MyRoot/OlapStore", "MyDir");
        // env.TestWaitNotification(runtime, txId);

        // TestLs(runtime, "/MyRoot/OlapStore/MyDir", false, NLs::PathExist);

        // TString tableSchema = R"(
        //     Name: "ColumnTable"
        //     ColumnShardCount: 1
        // )";

        TString allowedChars = "_-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

        TString tableSchema = Sprintf(R"(
            Name: "TestTable"
            Schema {
                Columns {
                    Name: "Id"
                    Type: "Int32"
                    NotNull: True
                }
                Columns {
                    Name: "%s"
                    Type: "Utf8"
                }
                KeyColumnNames: ["Id"]
            }
        )", allowedChars.c_str());

        TestCreateColumnTable(runtime, ++txId, "/MyRoot", tableSchema, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CreateColumnTableFailed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        
        TVector<TString> notAllowedNames = {"mess age", "~!@#$%^&*()+=asdfa"};
        
        for (const auto& colName: notAllowedNames) {
            TString tableSchema = Sprintf(R"(
                Name: "TestTable"
                Schema {
                    Columns {
                        Name: "Id"
                        Type: "Int32"
                        NotNull: True
                    }
                    Columns {
                        Name: "%s"
                        Type: "Utf8"
                    }
                    KeyColumnNames: ["Id"]
                }
            )", colName.c_str());

            TestCreateColumnTable(runtime, ++txId, "/MyRoot", tableSchema, {NKikimrScheme::StatusSchemeError});
            env.TestWaitNotification(runtime, txId);
        }
    }

    Y_UNIT_TEST(CreateColumnStoreOk) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const TString& storeSchema = defaultStoreSchema;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", storeSchema, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathExist);
    }

    Y_UNIT_TEST(CreateColumnStoreFailed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const TString& storeSchema = invalidStoreSchema;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", storeSchema, {NKikimrScheme::StatusSchemeError});
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(AlterColumnTableOk) {
        TTestBasicRuntime runtime;
        TTestEnvOptions options;
        TTestEnv env(runtime, options);
        ui64 txId = 100;

        TString tableSchema = R"(
            Name: "TestTable"
            Schema {
                Columns {
                    Name: "Id"
                    Type: "Int32"
                    NotNull: True
                }
                Columns {
                    Name: "message"
                    Type: "Utf8"
                }
                KeyColumnNames: ["Id"]
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot", tableSchema, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        TestAlterColumnTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TestTable"
            AlterSchema {
                AddColumns {
                    Name: "NewColumn"
                    Type: "Int32"
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AlterColumnTableFailed) {
        TTestBasicRuntime runtime;
        TTestEnvOptions options;
        TTestEnv env(runtime, options);
        ui64 txId = 100;

        TString tableSchema = R"(
            Name: "TestTable"
            Schema {
                Columns {
                    Name: "Id"
                    Type: "Int32"
                    NotNull: True
                }
                Columns {
                    Name: "message"
                    Type: "Utf8"
                }
                KeyColumnNames: ["Id"]
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot", tableSchema, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        TestAlterColumnTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TestTable"
            AlterSchema {
                AddColumns {
                    Name: "New Column"
                    Type: "Int32"
                }
            }
        )", {NKikimrScheme::StatusSchemeError});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AlterColumnStoreOk) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const TString& olapSchema = defaultStoreSchema;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        const TString& tableSchema = defaultTableSchema;

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchema);
        env.TestWaitNotification(runtime, txId);

        TestAlterOlapStore(runtime, ++txId, "/MyRoot", R"(
            Name: "OlapStore"
            AlterSchemaPresets {
                Name: "default"
                AlterSchema {
                    AddColumns { Name: "comment" Type: "Utf8" }
                }
            }
        )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AlterColumnStoreFailed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const TString& olapSchema = defaultStoreSchema;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        const TString& tableSchema = defaultTableSchema;

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchema);
        env.TestWaitNotification(runtime, txId);

        TestAlterOlapStore(runtime, ++txId, "/MyRoot", R"(
            Name: "OlapStore"
            AlterSchemaPresets {
                Name: "default"
                AlterSchema {
                    AddColumns { Name: "mess age" Type: "Utf8" }
                }
            }
        )", {NKikimrScheme::StatusSchemeError});

        env.TestWaitNotification(runtime, txId);
    }

}
