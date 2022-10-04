
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dummy.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include "flat_cxx_database.h"
#include "flat_dbase_scheme.h"
#include "flat_dbase_apply.h"

namespace NKikimr {

namespace NTable {

Y_UNIT_TEST_SUITE(TFlatCxxDatabaseTest) {
    using TDummyEnv = NTable::TDummyEnv;

    enum ESomeEnum : ui8 {
        SomeValue0,
        SomeValue1,
        SomeValue2,
    };

    struct Schema : NIceDb::Schema {
        struct TestTable : Table<1> {
            struct ID : Column<1, NScheme::NTypeIds::Uint64> {};
            struct Value : Column<2, NScheme::NTypeIds::Uint64> {};
            struct Name : Column<3, NScheme::NTypeIds::Utf8> {};
            struct BoolValue : Column<4, NScheme::NTypeIds::Bool> {};
            struct EmptyValue : Column<5, NScheme::NTypeIds::Uint64> { static constexpr ui64 Default = 13; };
            struct ProtoValue : Column<6, NScheme::NTypeIds::String> { using Type = NKikimrMiniKQL::TValue; };
            struct EnumValue : Column<7, NScheme::NTypeIds::Uint64> { using Type = ESomeEnum; };
            struct InstantValue : Column<8, NScheme::NTypeIds::Uint64> { using Type = TInstant; };

            using TKey = TableKey<ID>;
            using TColumns = TableColumns<ID, Value, Name, BoolValue, EmptyValue, ProtoValue, EnumValue, InstantValue>;
        };

        struct TestTable2 : Table<2> {
            struct ID1 : Column<1, NScheme::NTypeIds::Uint64> {};
            struct ID2 : Column<2, NScheme::NTypeIds::Uint64> {};
            struct Value : Column<3, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<ID1, ID2>;
            using TColumns = TableColumns<ID1, ID2, Value>;

            using Precharge = NoAutoPrecharge;
        };

        struct TestTable3 : Table<3> {
            struct ID1 : Column<1, NScheme::NTypeIds::Utf8> {};
            struct ID2 : Column<2, NScheme::NTypeIds::String> {};
            struct Value : Column<3, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<ID1, ID2>;
            using TColumns = TableColumns<ID1, ID2, Value>;
        };

        struct TestTable4 : Table<4> {
            struct ID : Column<1, NScheme::NTypeIds::Uint64> {};
            struct Value : Column<2, NScheme::NTypeIds::String> { using Type = TStringBuf; };
            struct ProtoValue : Column<3, NScheme::NTypeIds::String> { using Type = NKikimrMiniKQL::TValue; };

            using TKey = TableKey<ID>;
            using TColumns = TableColumns<ID, Value, ProtoValue>;
        };

        using TTables = SchemaTables<TestTable, TestTable2, TestTable3, TestTable4>;
    };

    Y_UNIT_TEST(BasicSchemaTest) {
        TDatabase DB;
        NIceDb::TNiceDb db(DB);
        ui64 stamp = 0;

        TInstant timestamp = TInstant::Now();

        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            db.Materialize<Schema>();
            DB.Commit(stamp, true);
        }

        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            for (ui64 i = 0; i < 1000; ++i) {
                NKikimrMiniKQL::TValue protoValue;
                protoValue.SetUint64(i);
                protoValue.SetText(ToString(i));
                protoValue.SetBool(i % 2 == 0);

                db.Table<Schema::TestTable>().Key(i).Update(NIceDb::TNull<Schema::TestTable::Value>());

                db.Table<Schema::TestTable>().Key(i).Update<Schema::TestTable::EnumValue>(ESomeEnum::SomeValue0);

                // old syntax:
                db.Table<Schema::TestTable>().Key(i).Update(NIceDb::TUpdate<Schema::TestTable::Value>(i),
                                                            NIceDb::TUpdate<Schema::TestTable::Name>(ToString(i)),
                                                            NIceDb::TUpdate<Schema::TestTable::BoolValue>(i % 2 == 0),
                                                            NIceDb::TUpdate<Schema::TestTable::ProtoValue>(protoValue),
                                                            NIceDb::TUpdate<Schema::TestTable::EnumValue>(ESomeEnum::SomeValue1),
                                                            NIceDb::TUpdate<Schema::TestTable::InstantValue>(timestamp));

                // modern syntax:
                db.Table<Schema::TestTable>().Key(i).Update<
                        Schema::TestTable::Value,
                        Schema::TestTable::Name,
                        Schema::TestTable::BoolValue,
                        Schema::TestTable::ProtoValue,
                        Schema::TestTable::EnumValue,
                        Schema::TestTable::InstantValue>(i, ToString(i), i % 2 == 0, protoValue, ESomeEnum::SomeValue1,
                            timestamp);

                // also modern syntax:
                db.Table<Schema::TestTable>().Key(i)
                        .Update<Schema::TestTable::Value>(i)
                        .Update<Schema::TestTable::Name>(ToString(i))
                        .Update<Schema::TestTable::BoolValue>(i % 2 == 0)
                        .Update<Schema::TestTable::ProtoValue>(protoValue)
                        .Update<Schema::TestTable::EnumValue>(ESomeEnum::SomeValue1)
                        .Update<Schema::TestTable::InstantValue>(timestamp);
            }
            DB.Commit(stamp, true);
        }

        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            for (ui64 i = 0; i < 100; ++i) {
                for (ui64 j = 0; j < 10; ++j) {
                    db.Table<Schema::TestTable2>().Key(i, j).Update(NIceDb::TUpdate<Schema::TestTable2::Value>(i * 10 + j));
                }
            }
            DB.Commit(stamp, true);
        }

        // NoCopy
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            NKikimrMiniKQL::TValue protoValue;
            protoValue.SetText("test");
            db.Table<Schema::TestTable4>().Key(1).Update<Schema::TestTable4::Value, Schema::TestTable4::ProtoValue>("test", protoValue);
            DB.Commit(stamp, true);
        }
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            {
                auto row = db.Table<Schema::TestTable4>().Key(1).Select<Schema::TestTable4::Value>();
            }
            {
                auto row = db.Table<Schema::TestTable4>().Key(1).Select<Schema::TestTable4::ProtoValue>();
            }
            auto row = db.Table<Schema::TestTable4>().Key(1).Select<Schema::TestTable4::Value, Schema::TestTable4::ProtoValue>();
            UNIT_ASSERT(row.IsReady());
            UNIT_ASSERT(row.IsValid());
            UNIT_ASSERT(!row.EndOfSet());
            auto value = row.GetValue<Schema::TestTable4::Value>();
            UNIT_ASSERT(typeid(value) == typeid(TStringBuf));
            UNIT_ASSERT(value == "test");
            auto protoValue = row.GetValue<Schema::TestTable4::ProtoValue>();
            UNIT_ASSERT(typeid(protoValue) == typeid(NKikimrMiniKQL::TValue));
            UNIT_ASSERT(protoValue.GetText() == "test");
            DB.Commit(stamp, true);
        }

        // SelectRow
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            for (ui64 i = 0; i < 1000; ++i) {
                auto row = db.Table<Schema::TestTable>().Key(i).Select<
                        Schema::TestTable::Value,
                        Schema::TestTable::Name,
                        Schema::TestTable::BoolValue,
                        Schema::TestTable::EmptyValue,
                        Schema::TestTable::EnumValue,
                        Schema::TestTable::InstantValue>();
                // move semantics test
                decltype(row) new_row = std::move(row);
                row = std::move(new_row);
                ////////////////////////
                UNIT_ASSERT(row.IsReady());
                UNIT_ASSERT(row.IsValid());
                ui64 value = row.GetValue<Schema::TestTable::Value>();
                TString name = row.GetValue<Schema::TestTable::Name>();
                bool boolValue = row.GetValue<Schema::TestTable::BoolValue>();
                ESomeEnum enumValue = row.GetValue<Schema::TestTable::EnumValue>();
                TInstant instantValue = row.GetValue<Schema::TestTable::InstantValue>();
                UNIT_ASSERT_EQUAL(value, i);
                UNIT_ASSERT_EQUAL(ToString(value), name);
                UNIT_ASSERT_EQUAL(boolValue, (i % 2 == 0));
                UNIT_ASSERT_EQUAL(enumValue, ESomeEnum::SomeValue1);
                UNIT_ASSERT_EQUAL(instantValue, timestamp);
                UNIT_ASSERT_EQUAL(row.GetValue<Schema::TestTable::EmptyValue>(), 13);
                UNIT_ASSERT_EQUAL(row.GetValueOrDefault<Schema::TestTable::EmptyValue>(), 13);
                UNIT_ASSERT_EQUAL(row.GetValueOrDefault<Schema::TestTable::EmptyValue>(i), i);
            }
            DB.Commit(stamp, true);
        }

        // All
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            UNIT_ASSERT(db.Table<Schema::TestTable>().Precharge());
            auto table = db.Table<Schema::TestTable>();
            // move semantics test
            decltype(table) new_table = std::move(table);
            table = std::move(new_table);
            ////////////////////////
            auto rowset = table.Select<Schema::TestTable::Value, Schema::TestTable::Name, Schema::TestTable::BoolValue>();
            UNIT_ASSERT(rowset.IsReady());
            // move semantics test
            decltype(rowset) new_rowset = std::move(rowset);
            rowset = std::move(new_rowset);
            ////////////////////////
            for (ui64 i = 0; i < 1000; ++i) {
                UNIT_ASSERT(!rowset.EndOfSet());
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                TString name = rowset.GetValue<Schema::TestTable::Name>();
                bool boolValue = rowset.GetValue<Schema::TestTable::BoolValue>();
                UNIT_ASSERT_EQUAL(value, i);
                UNIT_ASSERT_EQUAL(ToString(value), name);
                UNIT_ASSERT_EQUAL(boolValue, (i % 2 == 0));
                UNIT_ASSERT(rowset.Next());
            }
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        // All in reverse
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            UNIT_ASSERT(db.Table<Schema::TestTable>().Reverse().Precharge());
            auto table = db.Table<Schema::TestTable>();
            // move semantics test
            decltype(table) new_table = std::move(table);
            table = std::move(new_table);
            ////////////////////////
            auto rowset = table.Reverse().Select<Schema::TestTable::Value, Schema::TestTable::Name, Schema::TestTable::BoolValue>();
            UNIT_ASSERT(rowset.IsReady());
            // move semantics test
            decltype(rowset) new_rowset = std::move(rowset);
            rowset = std::move(new_rowset);
            ////////////////////////
            for (int i = 999; i >= 0; --i) {
                ui64 expected = i;
                UNIT_ASSERT(!rowset.EndOfSet());
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                TString name = rowset.GetValue<Schema::TestTable::Name>();
                bool boolValue = rowset.GetValue<Schema::TestTable::BoolValue>();
                UNIT_ASSERT_VALUES_EQUAL(value, expected);
                UNIT_ASSERT_VALUES_EQUAL(ToString(value), name);
                UNIT_ASSERT_VALUES_EQUAL(boolValue, (expected % 2 == 0));
                UNIT_ASSERT(rowset.Next());
            }
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        // Prefix
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            UNIT_ASSERT(db.Table<Schema::TestTable2>().Prefix(50).Precharge<Schema::TestTable2::Value>());
            auto rowset = db.Table<Schema::TestTable2>().Prefix(50).Select<Schema::TestTable2::Value>();
            UNIT_ASSERT(rowset.IsReady());
            // move semantics test
            decltype(rowset) new_rowset = std::move(rowset);
            rowset = std::move(new_rowset);
            ////////////////////////
            for (ui64 i = 0; i < 10; ++i) {
                UNIT_ASSERT(!rowset.EndOfSet());
                UNIT_ASSERT_EQUAL(rowset.GetValue<Schema::TestTable2::Value>(), 50 * 10 + i);
                UNIT_ASSERT(rowset.Next());
            }
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        // Prefix in reverse
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            UNIT_ASSERT(db.Table<Schema::TestTable2>().Reverse().Prefix(50).Precharge<Schema::TestTable2::Value>());
            auto rowset = db.Table<Schema::TestTable2>().Reverse().Prefix(50).Select<Schema::TestTable2::Value>();
            UNIT_ASSERT(rowset.IsReady());
            // move semantics test
            decltype(rowset) new_rowset = std::move(rowset);
            rowset = std::move(new_rowset);
            ////////////////////////
            for (int i = 9; i >= 0; --i) {
                ui64 expected = 50 * 10 + i;
                UNIT_ASSERT(!rowset.EndOfSet());
                UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::TestTable2::Value>(), expected);
                UNIT_ASSERT(rowset.Next());
            }
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        // GreaterOrEqual
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            auto rowset = db.Table<Schema::TestTable>().GreaterOrEqual(500).Select();
            UNIT_ASSERT(rowset.IsReady());
            // move semantics test
            decltype(rowset) new_rowset = std::move(rowset);
            rowset = std::move(new_rowset);
            ////////////////////////
            for (ui64 i = 500; i < 1000; ++i) {
                UNIT_ASSERT(!rowset.EndOfSet());
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                TString name = rowset.GetValue<Schema::TestTable::Name>();
                bool boolValue = rowset.GetValue<Schema::TestTable::BoolValue>();
                UNIT_ASSERT_EQUAL(value, i);
                UNIT_ASSERT_EQUAL(ToString(value), name);
                UNIT_ASSERT_EQUAL(boolValue, (i % 2 == 0));
                UNIT_ASSERT(rowset.Next());
            }
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        // GreaterOrEqual in reverse
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            auto rowset = db.Table<Schema::TestTable>().Reverse().GreaterOrEqual(500).Select();
            UNIT_ASSERT(rowset.IsReady());
            // move semantics test
            decltype(rowset) new_rowset = std::move(rowset);
            rowset = std::move(new_rowset);
            ////////////////////////
            for (ui64 i = 999; i >= 500; --i) {
                UNIT_ASSERT(!rowset.EndOfSet());
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                TString name = rowset.GetValue<Schema::TestTable::Name>();
                bool boolValue = rowset.GetValue<Schema::TestTable::BoolValue>();
                UNIT_ASSERT_VALUES_EQUAL(value, i);
                UNIT_ASSERT_VALUES_EQUAL(ToString(value), name);
                UNIT_ASSERT_VALUES_EQUAL(boolValue, (i % 2 == 0));
                UNIT_ASSERT(rowset.Next());
            }
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        // LessOrEqual
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            auto rowset = db.Table<Schema::TestTable>().LessOrEqual(500).Select();
            UNIT_ASSERT(rowset.IsReady());
            // move semantics test
            decltype(rowset) new_rowset = std::move(rowset);
            rowset = std::move(new_rowset);
            ////////////////////////
            for (ui64 i = 0; i <= 500; ++i) {
                UNIT_ASSERT(!rowset.EndOfSet());
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                TString name = rowset.GetValue<Schema::TestTable::Name>();
                bool boolValue = rowset.GetValue<Schema::TestTable::BoolValue>();
                UNIT_ASSERT_EQUAL(value, i);
                UNIT_ASSERT_EQUAL(ToString(value), name);
                UNIT_ASSERT_EQUAL(boolValue, (i % 2 == 0));
                UNIT_ASSERT(rowset.Next());
            }
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        // LessOrEqual in reverse
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            auto rowset = db.Table<Schema::TestTable>().Reverse().LessOrEqual(500).Select();
            UNIT_ASSERT(rowset.IsReady());
            // move semantics test
            decltype(rowset) new_rowset = std::move(rowset);
            rowset = std::move(new_rowset);
            ////////////////////////
            for (int i = 500; i >= 0; --i) {
                UNIT_ASSERT(!rowset.EndOfSet());
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                TString name = rowset.GetValue<Schema::TestTable::Name>();
                bool boolValue = rowset.GetValue<Schema::TestTable::BoolValue>();
                UNIT_ASSERT_VALUES_EQUAL(value, i);
                UNIT_ASSERT_VALUES_EQUAL(ToString(value), name);
                UNIT_ASSERT_VALUES_EQUAL(boolValue, (i % 2 == 0));
                UNIT_ASSERT(rowset.Next());
            }
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        // GreaterOrEqual + LessOrEqual
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            auto operation = db.Table<Schema::TestTable>().GreaterOrEqual(500).LessOrEqual(501);
            // move semantics test
            decltype(operation) new_operation = std::move(operation);
            operation = std::move(new_operation);
            ////////////////////////
            auto rowset = operation.Select();
            UNIT_ASSERT(rowset.IsReady());
            // move semantics test
            decltype(rowset) new_rowset = std::move(rowset);
            rowset = std::move(new_rowset);
            ////////////////////////
            for (ui64 i = 500; i <= 501; ++i) {
                UNIT_ASSERT(!rowset.EndOfSet());
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                TString name = rowset.GetValue<Schema::TestTable::Name>();
                bool boolValue = rowset.GetValue<Schema::TestTable::BoolValue>();
                UNIT_ASSERT_EQUAL(value, i);
                UNIT_ASSERT_EQUAL(ToString(value), name);
                UNIT_ASSERT_EQUAL(boolValue, (i % 2 == 0));
                UNIT_ASSERT(rowset.Next());
            }
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        // GreaterOrEqual + LessOrEqual in reverse
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            auto operation = db.Table<Schema::TestTable>().Reverse().GreaterOrEqual(500).LessOrEqual(501);
            // move semantics test
            decltype(operation) new_operation = std::move(operation);
            operation = std::move(new_operation);
            ////////////////////////
            auto rowset = operation.Select();
            UNIT_ASSERT(rowset.IsReady());
            // move semantics test
            decltype(rowset) new_rowset = std::move(rowset);
            rowset = std::move(new_rowset);
            ////////////////////////
            for (ui64 i = 501; i >= 500; --i) {
                UNIT_ASSERT(!rowset.EndOfSet());
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                TString name = rowset.GetValue<Schema::TestTable::Name>();
                bool boolValue = rowset.GetValue<Schema::TestTable::BoolValue>();
                UNIT_ASSERT_VALUES_EQUAL(value, i);
                UNIT_ASSERT_VALUES_EQUAL(ToString(value), name);
                UNIT_ASSERT_VALUES_EQUAL(boolValue, (i % 2 == 0));
                UNIT_ASSERT(rowset.Next());
            }
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        // LessOrEqual + GreaterOrEqual
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            auto operation = db.Table<Schema::TestTable>().LessOrEqual(501).GreaterOrEqual(500);
            // move semantics test
            decltype(operation) new_operation = std::move(operation);
            operation = std::move(new_operation);
            ////////////////////////
            auto rowset = operation.Select();
            UNIT_ASSERT(rowset.IsReady());
            // move semantics test
            decltype(rowset) new_rowset = std::move(rowset);
            rowset = std::move(new_rowset);
            ////////////////////////
            for (ui64 i = 500; i <= 501; ++i) {
                UNIT_ASSERT(!rowset.EndOfSet());
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                TString name = rowset.GetValue<Schema::TestTable::Name>();
                bool boolValue = rowset.GetValue<Schema::TestTable::BoolValue>();
                UNIT_ASSERT_EQUAL(value, i);
                UNIT_ASSERT_EQUAL(ToString(value), name);
                UNIT_ASSERT_EQUAL(boolValue, (i % 2 == 0));
                UNIT_ASSERT(rowset.Next());
            }
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        // LessOrEqual + GreaterOrEqual in reverse
        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            auto operation = db.Table<Schema::TestTable>().Reverse().LessOrEqual(501).GreaterOrEqual(500);
            // move semantics test
            decltype(operation) new_operation = std::move(operation);
            operation = std::move(new_operation);
            ////////////////////////
            auto rowset = operation.Select();
            UNIT_ASSERT(rowset.IsReady());
            // move semantics test
            decltype(rowset) new_rowset = std::move(rowset);
            rowset = std::move(new_rowset);
            ////////////////////////
            for (ui64 i = 501; i >= 500; --i) {
                UNIT_ASSERT(!rowset.EndOfSet());
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                TString name = rowset.GetValue<Schema::TestTable::Name>();
                bool boolValue = rowset.GetValue<Schema::TestTable::BoolValue>();
                UNIT_ASSERT_VALUES_EQUAL(value, i);
                UNIT_ASSERT_VALUES_EQUAL(ToString(value), name);
                UNIT_ASSERT_VALUES_EQUAL(boolValue, (i % 2 == 0));
                UNIT_ASSERT(rowset.Next());
            }
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            for (ui64 i = 0; i < 1000; ++i) {
                db.Table<Schema::TestTable>().Key(i).Delete();
            }
            DB.Commit(stamp, true);
        }

        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            auto rowset = db.Table<Schema::TestTable>().Select<Schema::TestTable::Value, Schema::TestTable::Name>();
            UNIT_ASSERT(rowset.IsReady());
            UNIT_ASSERT(rowset.EndOfSet());
            DB.Commit(stamp, true);
        }

        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            for (ui64 i = 0; i < 1000; ++i) {
                db.Table<Schema::TestTable2>().Key(i / 100, i % 100).Update();
            }
            DB.Commit(stamp, true);
        }

        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            auto rowset = db.Table<Schema::TestTable2>().GreaterOrEqual(5,0).Select();
            UNIT_ASSERT(rowset.IsReady());
            for (ui64 i = 500; i < 1000; ++i) {
                UNIT_ASSERT(!rowset.EndOfSet());
                UNIT_ASSERT_EQUAL(rowset.GetKey(), std::make_tuple(i / 100, i % 100));
                UNIT_ASSERT(rowset.Next());
            }
            DB.Commit(stamp, true);
        }

        {
            TDummyEnv env;
            DB.Begin(++stamp, env);

            for (ui64 i = 0; i < 1000; ++i) {
                db.Table<Schema::TestTable3>().Key(IntToString<16>(i / 4), IntToString<16>(i % 4)).Update(NIceDb::TUpdate<Schema::TestTable3::Value>(i));
            }

            DB.Commit(stamp, true);
        }

        {
            TDummyEnv env;
            DB.Begin(++stamp, env);
            for (ui64 i = 250; i > 0; --i) {
                auto rowset = db.Table<Schema::TestTable3>().Range(IntToString<16>(i - 1)).Select();
                UNIT_ASSERT(rowset.IsReady());
                for (ui64 j = 0; j < 4; ++j) {
                    UNIT_ASSERT(rowset.IsValid());
                    TString should = IntToString<16>(j);
                    TString have = rowset.GetValue<Schema::TestTable3::ID2>();
                    UNIT_ASSERT_EQUAL(should, have);
                    UNIT_ASSERT_EQUAL((i - 1) * 4 + j, rowset.GetValue<Schema::TestTable3::Value>());
                    UNIT_ASSERT(rowset.Next());
                }
                UNIT_ASSERT(rowset.EndOfSet());
            }
            DB.Commit(stamp, true);
        }
    }

    Y_UNIT_TEST(RenameColumnSchemaTest) {
        TScheme scheme;
        TSchemeModifier applier(scheme);
        TAlter delta;

        delta.AddTable("test", 1);
        delta.AddColumn(1, "test", 1, 1, { });
        applier.Apply(*delta.Flush());

        delta.AddTable("testtest", 1);
        delta.AddColumn(1, "testtest", 1, 1, { });
        applier.Apply(*delta.Flush());

        UNIT_ASSERT(scheme.Tables.find(1)->second.Name == "testtest");
        UNIT_ASSERT(scheme.TableNames.find("testtest")->second == 1);
        UNIT_ASSERT(scheme.TableNames.find("test") == scheme.TableNames.end());
        UNIT_ASSERT(scheme.Tables.find(1)->second.Columns.find(1)->second.Name == "testtest");
        UNIT_ASSERT(scheme.Tables.find(1)->second.ColumnNames.find("testtest")->second == 1);
        UNIT_ASSERT(scheme.Tables.find(1)->second.ColumnNames.find("test") == scheme.Tables.find(1)->second.ColumnNames.end());
    }

    Y_UNIT_TEST(SchemaFillerTest) {
        NTable::TScheme::TTableSchema schema;
        NIceDb::NHelpers::TStaticSchemaFiller<Schema::TestTable>::Fill(schema);

        UNIT_ASSERT_VALUES_EQUAL(schema.Columns.size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(schema.ColumnNames.size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(schema.KeyColumns.size(), 1);
        const TVector<std::pair<TString, NScheme::TTypeId>> columns = {
            {"ID", NScheme::NTypeIds::Uint64},
            {"Value", NScheme::NTypeIds::Uint64},
            {"Name", NScheme::NTypeIds::Utf8},
            {"BoolValue", NScheme::NTypeIds::Bool},
            {"EmptyValue", NScheme::NTypeIds::Uint64},
            {"ProtoValue", NScheme::NTypeIds::String},
            {"EnumValue", NScheme::NTypeIds::Uint64},
            {"InstantValue", NScheme::NTypeIds::Uint64}
            };
        for (const auto& col : columns) {
            ui32 id = schema.ColumnNames.at(col.first);
            UNIT_ASSERT_VALUES_EQUAL(schema.Columns.at(id).Name, col.first);
            UNIT_ASSERT_VALUES_EQUAL(schema.Columns.at(id).PType.GetTypeId(), col.second);
        }
    }
}

}
}
