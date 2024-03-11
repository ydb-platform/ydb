#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

const char* FormatPragma(bool disableOpt) {
    if (disableOpt) {
        return "PRAGMA Kikimr.OptDisableSqlInToJoin = 'True';";
    }
    return "";
}

const bool DisableOpt = true;
const bool EnableOpt = false;

} // namespace


Y_UNIT_TEST_SUITE(KqpSqlIn) {

    Y_UNIT_TEST(TableSource) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, std::function<void(const TDataQueryResult&)> assertFn) {
            TString query = TStringBuilder()
                << "--!syntax_v1" << Endl
                << FormatPragma(disableOpt)
                << Q_(R"(
                    DECLARE $in AS List<Struct<k: Uint64>>;
                    SELECT Key, Value
                    FROM `/Root/KeyValue` WHERE Key IN (SELECT k FROM AS_TABLE($in)) ORDER BY Key
                )");

            auto params = TParamsBuilder().AddParam("$in").BeginList()
                    .AddListItem().BeginStruct().AddMember("k").Uint64(1).EndStruct()
                    .AddListItem().BeginStruct().AddMember("k").Uint64(2).EndStruct()
                    .AddListItem().BeginStruct().AddMember("k").Uint64(5).EndStruct()
                    .AddListItem().BeginStruct().AddMember("k").Uint64(10).EndStruct()
                    .EndList().Build().Build();

            auto result = ExecQueryAndTestResult(session, query, params,
                    R"([[[1u];["One"]];
                        [[2u];["Two"]];
                        [[10u];["Ten"]]])");
            assertFn(result);
        };

        test(DisableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/KeyValue", 3);
        });

        test(EnableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/KeyValue", 3);
        });
    }

    Y_UNIT_TEST(CantRewrite) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto query = TStringBuilder()
            << "--!syntax_v1" << Endl
            << Q_(R"(
                DECLARE $in AS List<Uint64>;
                SELECT Key, Value FROM `/Root/KeyValue`
                WHERE Value = "One" AND Key IN $in
            )");

        // empty parameters
        {
            TMap<TString, TType> paramsType;
            paramsType.emplace("$in", TTypeBuilder().BeginList().Primitive(EPrimitiveType::Uint64).EndList().Build());
            auto params = TParamsBuilder(paramsType);

            auto result = ExecQueryAndTestResult(session, query, params.Build(), R"([])");
            AssertTableReads(result, "/Root/KeyValue", 0);
        }

        // not empty parameters
        {
            auto params = TParamsBuilder();
            auto& pl = params.AddParam("$in").BeginList();
            for (auto v : {1, 2, 3, 42, 50, 100}) {
                pl.AddListItem().Uint64(v);
            }
            pl.EndList().Build();

            auto result = ExecQueryAndTestResult(session, query, params.Build(), R"([[[1u];["One"]]])");
            AssertTableReads(result, "/Root/KeyValue", 3);
        }
    }

    Y_UNIT_TEST(SimpleKey) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, bool optionalParam, std::function<void(const TDataQueryResult&)> assertFn) {
            auto query = TStringBuilder()
                << "--!syntax_v1" << Endl
                << FormatPragma(disableOpt) << Endl
                << Q_(Sprintf(R"(
                    DECLARE $in AS List<Uint64%s>;
                    SELECT Key, Value FROM `/Root/KeyValue` WHERE Key IN $in
                    ORDER BY Key
                )", (optionalParam ? "?" : "")));

            auto params = TParamsBuilder();
            auto& pl = params.AddParam("$in").BeginList();
            for (auto v : {1, 2, 3, 42, 50, 100}) {
                if (optionalParam) {
                    pl.AddListItem().OptionalUint64(v);
                } else {
                    pl.AddListItem().Uint64(v);
                }
            }
            if (optionalParam) {
                pl.AddListItem().OptionalUint64(Nothing());
            }
            pl.EndList().Build();

            auto result = ExecQueryAndTestResult(session, query, params.Build(),
                    R"([[[1u];["One"]];
                        [[2u];["Two"]];
                        [[3u];["Three"]]])");
            assertFn(result);
        };

        test(DisableOpt, true /* optionalParams */, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/KeyValue", 3);
        });
        test(DisableOpt, false /* optionalParams */, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/KeyValue", 3);
        });

        test(EnableOpt, true /* optionalParams */, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/KeyValue", 3);
        });
        test(EnableOpt, false /* optionalParams */, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/KeyValue", 3);
        });
    }

    Y_UNIT_TEST(SimpleKey_Negated) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto query = TStringBuilder()
            << "--!syntax_v1" << Endl
            << Q_(R"(
                DECLARE $in AS List<Uint64?>;
                SELECT Key, Value FROM `/Root/KeyValue` WHERE Key NOT IN $in
            )");
        auto params = TParamsBuilder().AddParam("$in").BeginList()
                .AddListItem().OptionalUint64(1)
                .AddListItem().OptionalUint64(2)
                .AddListItem().OptionalUint64(42)
                .AddListItem().OptionalUint64(Nothing())
                .EndList().Build().Build();
        auto result = ExecQueryAndTestResult(session, query, params,
                R"([[[3u];["Three"]];
                    [[4u];["Four"]];
                    [[10u];["Ten"]]])");
        AssertTableReads(result, "/Root/KeyValue", 6); // not optimized
    }

    Y_UNIT_TEST(KeySuffix) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, bool optionalParam, std::function<void(const TDataQueryResult&)> assertFn) {
            auto query = TStringBuilder()
                << "--!syntax_v1" << Endl
                << Q_(Sprintf(R"(
                    %s
                    DECLARE $in AS List<String%s>;
                    SELECT Group, Name, Amount, Comment
                    FROM `/Root/Test`
                    WHERE Group = 1 AND Name IN $in
                    ORDER BY Name
                )", FormatPragma(disableOpt), (optionalParam ? "?" : "")));

            auto params = TParamsBuilder();
            auto& pl = params.AddParam("$in").BeginList();
            for (auto& v : {"Anna", "Tony", "Bob", "Muhammad", "Jack"}) {
                if (optionalParam) {
                    pl.AddListItem().OptionalString(v);
                } else {
                    pl.AddListItem().String(v);
                }
            }
            if (optionalParam) {
                pl.AddListItem().OptionalString(Nothing());
            }
            pl.EndList().Build();

            auto result = ExecQueryAndTestResult(session, query, params.Build(),
                    R"([[[1u];["Anna"];[3500u];["None"]];
                        [[1u];["Jack"];[100500u];["Just Jack"]]])");
            assertFn(result);
        };

        test(DisableOpt, true /* optionalParam */, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/Test", 2); // 3 rows have Group == 1
        });
        test(DisableOpt, false /* optionalParam */, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/Test", 2);
        });

        test(EnableOpt, true /* optionalParam */, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/Test", 2);
        });
        test(EnableOpt, false /* optionalParam */, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/Test", 2);
        });
    }

    Y_UNIT_TEST(KeySuffix_OnlyTail) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto query = TStringBuilder()
            << "--!syntax_v1" << Endl
            << Q_(R"(
                DECLARE $in AS List<String?>;
                SELECT Group, Name, Amount, Comment
                FROM `/Root/Test`
                WHERE Name IN $in
                ORDER BY Group, Name
            )");
        auto params = TParamsBuilder().AddParam("$in").BeginList()
                .AddListItem().OptionalString("Anna")
                .AddListItem().OptionalString("Tony")
                .AddListItem().OptionalString("Hugo")
                .AddListItem().OptionalString("Logan")
                .AddListItem().OptionalString(Nothing())
                .EndList().Build().Build();
        auto result = ExecQueryAndTestResult(session, query, params,
                R"([[[1u];["Anna"];[3500u];["None"]];
                    [[2u];["Tony"];[7200u];["None"]];
                    [[4u];["Hugo"];[77u];["Boss"]]])");
        AssertTableReads(result, "/Root/Test", 8); // not optimized
    }

    Y_UNIT_TEST(KeySuffix_NotPointPrefix) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto query = TStringBuilder()
            << "--!syntax_v1" << Endl
            << Q_(R"(
                DECLARE $in AS List<String?>;
                SELECT Group, Name, Amount, Comment
                FROM `/Root/Test`
                WHERE (Group > 1 AND Group < 4) AND Name IN $in
            )");
        auto params = TParamsBuilder().AddParam("$in").BeginList()
                .AddListItem().OptionalString("Anna")
                .AddListItem().OptionalString("Tony")
                .AddListItem().OptionalString("Harry")
                .AddListItem().OptionalString("Hugo")
                .AddListItem().OptionalString(Nothing())
                .EndList().Build().Build();
        auto result = ExecQueryAndTestResult(session, query, params,
                R"([[[2u];["Tony"];[7200u];["None"]];
                    [[3u];["Harry"];[5600u];["Not Potter"]]])");
        AssertTableReads(result, "/Root/Test", 4); // not optimized
    }

    Y_UNIT_TEST(ComplexKey) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto query = TStringBuilder()
            << "--!syntax_v1" << Endl
            << Q_(R"(
                DECLARE $groups AS List<Uint32?>;
                DECLARE $names AS List<String?>;
                SELECT Group, Name, Amount, Comment
                FROM `/Root/Test`
                WHERE Group IN $groups AND Name IN $names;
            )");

        auto params = TParamsBuilder()
                .AddParam("$groups").BeginList()
                    .AddListItem().OptionalUint32(1)
                    .AddListItem().OptionalUint32(3)
                    .AddListItem().OptionalUint32(50)
                    .EndList().Build()
                .AddParam("$names").BeginList()
                    .AddListItem().OptionalString("Anna")
                    .AddListItem().OptionalString("Tony")
                    .AddListItem().OptionalString("Jack")
                    .AddListItem().OptionalString("Hugo")
                    .AddListItem().OptionalString(Nothing())
                    .EndList().Build()
                .Build();

        auto result = ExecQueryAndTestResult(session, query, params,
                R"([[[1u];["Anna"];[3500u];["None"]];
                    [[1u];["Jack"];[100500u];["Just Jack"]]])");
        AssertTableReads(result, "/Root/Test", 2);
    }

    Y_UNIT_TEST(KeyTypeMissmatch_Int) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto query = TStringBuilder()
            << "--!syntax_v1" << Endl
            << Q_(R"(
                DECLARE $groups AS List<Int32>; -- not Uint32
                SELECT Group, Name, Amount, Comment
                FROM `/Root/Test`
                WHERE Group IN $groups;
            )");

        auto params = TParamsBuilder()
            .AddParam("$groups").BeginList()
                .AddListItem().Int32(1)
                .AddListItem().Int32(-1)
                .EndList().Build()
            .Build();

        auto result = ExecQueryAndTestResult(session, query, params,
            R"([[[1u];["Anna"];[3500u];["None"]];
                [[1u];["Jack"];[100500u];["Just Jack"]];
                [[1u];["Paul"];[300u];["None"]]])");
        AssertTableReads(result, "/Root/Test", 3);
    }

    Y_UNIT_TEST(KeyTypeMissmatch_Str) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = TStringBuilder()
            << "--!syntax_v1" << Endl
            << Q_(R"(
                DECLARE $keys AS List<Utf8>; -- not String
                SELECT Key, Value
                FROM `/Root/KeyValue2`
                WHERE Key IN $keys;
                )");

        auto params = TParamsBuilder()
            .AddParam("$keys").BeginList()
                .AddListItem().Utf8("1")
                .AddListItem().Utf8("¿cómo estás?")
                .EndList().Build()
            .Build();

        auto result = ExecQueryAndTestResult(session, query, params, R"([[["1"];["One"]]])");
        AssertTableReads(result, "/Root/KeyValue2", 1);
    }

    Y_UNIT_TEST(Dict) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, std::function<void(const TDataQueryResult&)> assertFn) {
            auto query = TStringBuilder()
                << "--!syntax_v1" << Endl
                << Q_(Sprintf(R"(
                    %s
                    DECLARE $in AS Dict<Uint64, String>;
                    SELECT Key, Value FROM `/Root/KeyValue` WHERE Key IN $in
                    ORDER BY Key
                )", FormatPragma(disableOpt)));

            auto params = TParamsBuilder().AddParam("$in").BeginDict()
                    .AddDictItem().DictKey().Uint64(1).DictPayload().String("any-1")
                    .AddDictItem().DictKey().Uint64(2).DictPayload().String("any-2")
                    .AddDictItem().DictKey().Uint64(7).DictPayload().String("any-7")
                    .EndDict().Build().Build();

            auto result = ExecQueryAndTestResult(session, query, params,
                    R"([[[1u];["One"]];
                        [[2u];["Two"]]])");
            assertFn(result);
        };

        test(DisableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/KeyValue", 2);
        });

        test(EnableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/KeyValue", 2);
        });
    }

    Y_UNIT_TEST(TupleParameter) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, std::function<void(const TDataQueryResult&)> assertFn) {
            auto query = TStringBuilder()
                << "--!syntax_v1" << Endl
                << Q_(Sprintf(R"(
                    %s
                    DECLARE $in AS List<Tuple<String, Uint32>>;
                    SELECT Group AS g, Amount AS a, Comment
                    FROM `/Root/Test`
                    WHERE (Name, Group) IN $in     -- reverse key prefix
                    ORDER BY g, a
                )", FormatPragma(disableOpt)));

            auto params = TParamsBuilder().AddParam("$in").BeginList()
                    .AddListItem().BeginTuple().AddElement().String("Anna").AddElement().Uint32(1).EndTuple()
                    .AddListItem().BeginTuple().AddElement().String("Jack").AddElement().Uint32(1).EndTuple()
                    .AddListItem().BeginTuple().AddElement().String("Hugo").AddElement().Uint32(4).EndTuple()
                    .AddListItem().BeginTuple().AddElement().String("Harry").AddElement().Uint32(9).EndTuple()
                    .EndList().Build().Build();

            auto result = ExecQueryAndTestResult(session, query, params,
                    R"([[[1u];[3500u];["None"]];
                        [[1u];[100500u];["Just Jack"]];
                        [[4u];[77u];["Boss"]]])");
            assertFn(result);
        };

        test(DisableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/Test", 3);
        });

        test(EnableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/Test", 3);
        });
    }

    Y_UNIT_TEST(TupleLiteral) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, std::function<void(const TDataQueryResult&)> assertFn) {
            auto query = TStringBuilder()
                << "--!syntax_v1" << Endl
                << Q_(Sprintf(R"(
                    %s
                    SELECT Group AS g, Amount AS a, Comment
                    FROM `/Root/Test`
                    WHERE (Group, Name) IN AsList(AsTuple(1u, "Anna"), AsTuple(1u, "Jack"),
                                                  AsTuple(4u, "Hugo"), AsTuple(9u, "Harry"))
                    ORDER BY g, a
                )", FormatPragma(disableOpt)));

            auto result = ExecQueryAndTestResult(session, query, TParamsBuilder().Build(),
                    R"([[[1u];[3500u];["None"]];
                        [[1u];[100500u];["Just Jack"]];
                        [[4u];[77u];["Boss"]]])");
            assertFn(result);
        };

        test(DisableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/Test", 3);
        });

        test(EnableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/Test", 3);
        });
    }

    Y_UNIT_TEST(TupleSelect) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);
        auto query = Q1_(R"(
                PRAGMA Kikimr.OptDisableSqlInToJoin = "True";
                DECLARE $in AS List<Struct<k: Uint32, v: String>>;
                SELECT Group AS g, Amount AS a, Comment
                FROM `/Root/Test`
                WHERE (Group, Name) IN (SELECT (k, v) FROM AS_TABLE($in)) -- table source
                ORDER BY g, a
            )");

        auto params = TParamsBuilder().AddParam("$in").BeginList()
            .AddListItem().BeginStruct().AddMember("k").Uint32(1).AddMember("v").String("Anna").EndStruct()
            .AddListItem().BeginStruct().AddMember("k").Uint32(1).AddMember("v").String("Jack").EndStruct()
            .AddListItem().BeginStruct().AddMember("k").Uint32(4).AddMember("v").String("Hugo").EndStruct()
            .AddListItem().BeginStruct().AddMember("k").Uint32(9).AddMember("v").String("Harry").EndStruct()
            .EndList().Build().Build();

        auto result = ExecQueryAndTestResult(session, query, params,
            R"(
                [[[1u];[3500u];["None"]];
                [[1u];[100500u];["Just Jack"]];
                [[4u];[77u];["Boss"]]]
            )");
            
        AssertTableReads(result, "/Root/Test", 3);
    }

    Y_UNIT_TEST(SelectNotAllElements) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        const TString query = Q1_(R"(
                DECLARE $in AS List<Int32?>;
                DECLARE $in2 AS List<String?>;
                SELECT Value AS V FROM `/Root/SecondaryKeys`
                WHERE Key IN $in AND Value IN $in2
                ORDER BY V
            )");

        auto params = TParamsBuilder()
                .AddParam("$in").BeginList()
                    .AddListItem().OptionalInt32(1)
                    .AddListItem().OptionalInt32(2)
                    .AddListItem().OptionalInt32(42)
                    .AddListItem().OptionalInt32(Nothing())
                .EndList().Build()
                .AddParam("$in2").BeginList()
                    .AddListItem().OptionalString("Payload1")
                    .AddListItem().OptionalString("Payload2")
                    .AddListItem().OptionalString("Payload0")
                    .AddListItem().OptionalString(Nothing())
                .EndList().Build()
                .Build();

        auto result = ExecQueryAndTestResult(session, query, params, R"([[["Payload1"]];[["Payload2"]]])");
        AssertTableReads(result, "/Root/SecondaryKeys", 2);
    }

    Y_UNIT_TEST(SecondaryIndex_SimpleKey) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, std::function<void(const TDataQueryResult&)> assertFn) {
            const TString query = Q1_(Sprintf(R"(
                    %s
                    DECLARE $in AS List<Int32?>;
                    SELECT Value FROM `/Root/SecondaryKeys` VIEW Index WHERE Fk IN $in
                    ORDER BY Value
                )", FormatPragma(disableOpt)));

            auto params = TParamsBuilder().AddParam("$in").BeginList()
                    .AddListItem().OptionalInt32(1)
                    .AddListItem().OptionalInt32(2)
                    .AddListItem().OptionalInt32(42)
                    .AddListItem().OptionalInt32(Nothing())
                    .EndList().Build().Build();

            auto result = ExecQueryAndTestResult(session, query, params, R"([[["Payload1"]];[["Payload2"]]])");
            assertFn(result);
        };

        test(DisableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryKeys/Index/indexImplTable", 2);
            AssertTableReads(result, "/Root/SecondaryKeys", 2);
        });

        test(EnableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryKeys/Index/indexImplTable", 2);
            AssertTableReads(result, "/Root/SecondaryKeys", 2);
        });
    }

    Y_UNIT_TEST(SecondaryIndex_SimpleKey_In_And) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, std::function<void(const TDataQueryResult&)> assertFn) {
            const TString query = Q1_(Sprintf(R"(
                    PRAGMA AnsiInForEmptyOrNullableItemsCollections;
                    %s
                    DECLARE $in AS List<Int32?>;
                    SELECT * FROM `/Root/SecondaryKeys` VIEW Index WHERE Fk IN $in
                    AND Value == "Payload1"
                )", FormatPragma(disableOpt)));

            auto params = TParamsBuilder().AddParam("$in").BeginList()
                    .AddListItem().OptionalInt32(1)
                    .AddListItem().OptionalInt32(2)
                    .AddListItem().OptionalInt32(42)
                    .AddListItem().OptionalInt32(Nothing())
                    .EndList().Build().Build();

            auto result = ExecQueryAndTestResult(session, query, params, R"([[[1];[1];["Payload1"]]])");
            assertFn(result);
        };

        test(DisableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryKeys/Index/indexImplTable", 2);
            AssertTableReads(result, "/Root/SecondaryKeys", 2);
            UNIT_ASSERT_C(result.GetIssues().Empty(), result.GetIssues().ToString());
        });

        test(EnableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryKeys/Index/indexImplTable", 2);
            AssertTableReads(result, "/Root/SecondaryKeys", 2);
            UNIT_ASSERT_C(result.GetIssues().Empty(), result.GetIssues().ToString());
        });
    }

    Y_UNIT_TEST(SimpleKey_In_And_In) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, std::function<void(const TDataQueryResult&)> assertFn) {
            const TString query = Q1_(Sprintf(R"(
                    PRAGMA AnsiInForEmptyOrNullableItemsCollections;
                    %s
                    DECLARE $in1 AS List<Int32>;
                    SELECT Key, FROM `/Root/SecondaryComplexKeys`
                    WHERE Key IN $in1 AND Value IN ("active", "Payload2");
                )", FormatPragma(disableOpt)));

            auto params = TParamsBuilder()
                    .AddParam("$in1")
                        .BeginList()
                            .AddListItem().Int32(1)
                            .AddListItem().Int32(2)
                            .AddListItem().Int32(42)
                        .EndList().Build()
                    .Build();

            auto result = ExecQueryAndTestResult(session, query, params, R"([[[2]]])");
            assertFn(result);
        };

        test(DisableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryComplexKeys", 2);
        });

        test(EnableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryComplexKeys", 2);
            UNIT_ASSERT_C(result.GetIssues().Empty(), result.GetIssues().ToString());
        });
    }

    Y_UNIT_TEST(SecondaryIndex_SimpleKey_In_And_In) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, std::function<void(const TDataQueryResult&)> assertFn) {
            const TString query = Q1_(Sprintf(R"(
                    PRAGMA AnsiInForEmptyOrNullableItemsCollections;
                    %s
                    DECLARE $in1 AS List<Int32>;
                    SELECT Key, FROM `/Root/SecondaryComplexKeys` VIEW Index
                        WHERE Fk1 IN $in1 AND Value IN ("active", "Payload2");
                )", FormatPragma(disableOpt)));

            auto params = TParamsBuilder()
                    .AddParam("$in1")
                        .BeginList()
                            .AddListItem().Int32(1)
                            .AddListItem().Int32(2)
                            .AddListItem().Int32(42)
                        .EndList().Build()
                    .Build();

            auto result = ExecQueryAndTestResult(session, query, params, R"([[[2]]])");
            assertFn(result);
        };

        test(DisableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryComplexKeys/Index/indexImplTable", 2);
            AssertTableReads(result, "/Root/SecondaryComplexKeys", 2);
            UNIT_ASSERT_C(result.GetIssues().Empty(), result.GetIssues().ToString());
        });

        test(EnableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryComplexKeys/Index/indexImplTable", 2);
            AssertTableReads(result, "/Root/SecondaryComplexKeys", 2);
            UNIT_ASSERT_C(result.GetIssues().Empty(), result.GetIssues().ToString());
        });
    }

    Y_UNIT_TEST(SecondaryIndex_ComplexKey_In_And_In) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, std::function<void(const TDataQueryResult&)> assertFn) {
            const TString query = Q1_(Sprintf(R"(
                    PRAGMA AnsiInForEmptyOrNullableItemsCollections;
                    %s
                    DECLARE $in1 AS List<Int32>;
                    DECLARE $in2 AS List<String>;
                    SELECT Key, FROM `/Root/SecondaryComplexKeys` VIEW Index
                        WHERE Fk1 IN $in1 AND Fk2 IN $in2 AND Value IN ("active", "Payload2");
                )", FormatPragma(disableOpt)));

            auto params = TParamsBuilder()
                    .AddParam("$in1")
                        .BeginList()
                            .AddListItem().Int32(1)
                            .AddListItem().Int32(2)
                            .AddListItem().Int32(42)
                        .EndList().Build()
                    .AddParam("$in2")
                        .BeginList()
                            .AddListItem().String("Fk2")
                            .AddListItem().String("qq")
                        .EndList().Build()

                    .Build();

            auto result = ExecQueryAndTestResult(session, query, params, R"([[[2]]])");
            assertFn(result);
        };

        test(DisableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryComplexKeys/Index/indexImplTable", 1);
            AssertTableReads(result, "/Root/SecondaryComplexKeys", 1);
            UNIT_ASSERT_C(result.GetIssues().Empty(), result.GetIssues().ToString());
        });

        test(EnableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryComplexKeys/Index/indexImplTable", 1);
            AssertTableReads(result, "/Root/SecondaryComplexKeys", 1);
            UNIT_ASSERT_C(result.GetIssues().Empty(), result.GetIssues().ToString());
        });
    }

    Y_UNIT_TEST(SecondaryIndex_TupleParameter) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, std::function<void(const TDataQueryResult&)> assertFn) {
            const TString query = Q1_(Sprintf(R"(
                    %s
                    DECLARE $in AS List<Tuple<Int32?, String>>;
                    SELECT Value
                    FROM `/Root/SecondaryComplexKeys` VIEW Index
                    WHERE (Fk1, Fk2) IN $in
                    ORDER BY Value
                )", FormatPragma(disableOpt)));

            auto params = TParamsBuilder().AddParam("$in").BeginList()
                    .AddListItem().BeginTuple().AddElement().OptionalInt32(1).AddElement().String("Fk1").EndTuple()
                    .AddListItem().BeginTuple().AddElement().OptionalInt32(2).AddElement().String("Fk2").EndTuple()
                    .AddListItem().BeginTuple().AddElement().OptionalInt32(42).AddElement().String("Fk5").EndTuple()
                    .AddListItem().BeginTuple().AddElement().OptionalInt32(Nothing()).AddElement().String("FkNull").EndTuple()
                    .EndList().Build().Build();

            auto result = ExecQueryAndTestResult(session, query, params, R"([[["Payload1"]];[["Payload2"]]])");
            assertFn(result);
        };

        test(DisableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryComplexKeys/Index/indexImplTable", 2);
            AssertTableReads(result, "/Root/SecondaryComplexKeys", 2);
        });

        test(EnableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryComplexKeys/Index/indexImplTable", 2);
            AssertTableReads(result, "/Root/SecondaryComplexKeys", 2);
        });
    }

    Y_UNIT_TEST(SecondaryIndex_TupleLiteral) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, std::function<void(const TDataQueryResult&)> assertFn) {
            const TString query = Q1_(Sprintf(R"(
                    %s
                    SELECT Value
                    FROM `/Root/SecondaryComplexKeys` VIEW Index
                    WHERE (Fk1, Fk2) IN AsList((1, "Fk1"), (2, "Fk2"), (42, "Fk5"), (Null, "FkNull"))
                    ORDER BY Value
                )", FormatPragma(disableOpt)));

            auto result = ExecQueryAndTestResult(session, query, R"([[["Payload1"]];[["Payload2"]]])");
            assertFn(result);
        };

        test(DisableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryComplexKeys/Index/indexImplTable", 2);
            AssertTableReads(result, "/Root/SecondaryComplexKeys", 2);
        });

        test(EnableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/SecondaryComplexKeys/Index/indexImplTable", 2);
            AssertTableReads(result, "/Root/SecondaryComplexKeys", 2);
        });
    }

    Y_UNIT_TEST(SecondaryIndex_TupleSelect) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);
        const TString query = Q1_(R"(
                PRAGMA Kikimr.OptDisableSqlInToJoin = "True";
                DECLARE $in AS List<Struct<k: Int32?, v: String>>;
                SELECT Value
                FROM `/Root/SecondaryComplexKeys` VIEW Index
                WHERE (Fk1, Fk2) IN (SELECT (k, v) FROM AS_TABLE($in))
                ORDER BY Value
            )");

        auto params = TParamsBuilder().AddParam("$in").BeginList()
            .AddListItem().BeginStruct().AddMember("k").OptionalInt32(1).AddMember("v").String("Fk1").EndStruct()
            .AddListItem().BeginStruct().AddMember("k").OptionalInt32(2).AddMember("v").String("Fk2").EndStruct()
            .AddListItem().BeginStruct().AddMember("k").OptionalInt32(42).AddMember("v").String("Fk5").EndStruct()
            .AddListItem().BeginStruct().AddMember("k").OptionalInt32(Nothing()).AddMember("v").String("FkNull").EndStruct()
            .EndList().Build().Build();

        auto result = ExecQueryAndTestResult(session, query, params, R"([[["Payload1"]];[["Payload2"]]])");
            
        AssertTableReads(result, "/Root/SecondaryComplexKeys/Index/indexImplTable", 2);
        AssertTableReads(result, "/Root/SecondaryComplexKeys", 2);
    }

    Y_UNIT_TEST(TupleNotOnlyOfKeys) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto test = [&](bool disableOpt, std::function<void(const TDataQueryResult&)> assertFn) {
            auto query = TStringBuilder()
                << Q1_(Sprintf(R"(
                    %s
                    DECLARE $in AS List<Tuple<String, Uint32>>;
                    SELECT Group AS g, Name as n, Amount AS a
                    FROM `/Root/Test`
                    WHERE (Comment, Group) IN $in  -- (not key, key prefix)
                    ORDER BY g, n, a
                )", FormatPragma(disableOpt)));

            auto params = TParamsBuilder().AddParam("$in").BeginList()
                    .AddListItem().BeginTuple()
                        .AddElement().String("None")
                        .AddElement().Uint32(1).EndTuple()
                    .AddListItem().BeginTuple()
                        .AddElement().String("Just Jack")
                        .AddElement().Uint32(1).EndTuple()
                    .AddListItem().BeginTuple()
                        .AddElement().String("Boss")
                        .AddElement().Uint32(4).EndTuple()
                    .AddListItem().BeginTuple()
                        .AddElement().String("Not Potter")
                        .AddElement().Uint32(9).EndTuple()
                    .EndList().Build().Build();

            auto result = ExecQueryAndTestResult(session, query, params,
                    R"([[[1u];["Anna"];[3500u]];
                        [[1u];["Jack"];[100500u]];
                        [[1u];["Paul"];[300u]];
                        [[4u];["Hugo"];[77u]]])");
            assertFn(result);
        };

        test(DisableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/Test", 4);
        });

        test(EnableOpt, [](const TDataQueryResult& result) {
            AssertTableReads(result, "/Root/Test", 4);
        });
    }

    Y_UNIT_TEST(Delete) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        auto query = TStringBuilder()
            << Q1_(R"(
                DECLARE $in AS List<Uint64>;
                DELETE FROM `/Root/KeyValue` WHERE Key IN $in
            )");

        auto params = TParamsBuilder().AddParam("$in").BeginList()
                .AddListItem().Uint64(1)
                .AddListItem().Uint64(2)
                .AddListItem().Uint64(42)
                .EndList().Build().Build();

        NYdb::NTable::TExecDataQuerySettings settings;
        settings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params, settings)
                .ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        AssertTableStats(result, "/Root/KeyValue", {
            .ExpectedReads = 0,
            .ExpectedDeletes = 3,
        });
    }

    Y_UNIT_TEST(InWithCast) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$keys")
                .BeginList()
                .AddListItem().Uint64(1)
                .EndList()
                .Build()
            .Build();

        NYdb::NTable::TExecDataQuerySettings settings;
        settings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $keys AS List<Uint64>;
            SELECT * FROM `/Root/TwoShard` WHERE Key IN $keys
        )"), TTxControl::BeginTx().CommitTx(), params, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        AssertTableStats(result, "/Root/TwoShard", {
            .ExpectedReads = 1,
        });
    }

    Y_UNIT_TEST(PhasesCount) {
        TKikimrSettings serverSettings;
        TKikimrRunner kikimr(serverSettings);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        // simple key
        {
            const auto query = Q1_(R"(
                DECLARE $in AS List<Uint64>;
                SELECT * FROM `/Root/KeyValue` WHERE Key IN $in
            )");

            const auto params = TParamsBuilder()
                .AddParam("$in")
                    .BeginList()
                        .AddListItem().Uint64(1)
                        .AddListItem().Uint64(1)
                        .AddListItem().Uint64(100)
                    .EndList().Build()
                .Build();

            const auto settings = TExecDataQuerySettings()
                .CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params, settings).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[1u];["One"]]])", FormatResultSetYson(result.GetResultSet(0)));

            const Ydb::TableStats::QueryStats stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_EQUAL_C(2, stats.query_phases_size(), stats.DebugString());
        }

        // complex (tuple) key
        {
            const auto query = Q1_(R"(
                DECLARE $in AS List<Tuple<UInt32, String>>;

                SELECT * FROM `/Root/Test` WHERE (Group, Name) IN $in
            )");

            const auto params = TParamsBuilder()
                .AddParam("$in")
                    .BeginList()
                        .AddListItem().BeginTuple()
                            .AddElement().Uint32(1)
                            .AddElement().String("Anna")
                        .EndTuple()
                        .AddListItem().BeginTuple()
                            .AddElement().Uint32(1)
                            .AddElement().String("Anna")
                        .EndTuple()
                        .AddListItem().BeginTuple()
                            .AddElement().Uint32(2)
                            .AddElement().String("Bob")
                        .EndTuple()
                    .EndList().Build()
                .Build();

            const auto settings = TExecDataQuerySettings()
                .CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params, settings).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[3500u];["None"];[1u];["Anna"]]])", FormatResultSetYson(result.GetResultSet(0)));

            const Ydb::TableStats::QueryStats stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_EQUAL_C(2, stats.query_phases_size(), stats.DebugString());
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
