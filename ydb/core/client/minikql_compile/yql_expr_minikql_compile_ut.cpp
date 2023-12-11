#include "yql_expr_minikql.h"
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/testlib/minikql_compile.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

using TTypeInfo = NScheme::TTypeInfo;

namespace {
    const TDuration TIME_LIMIT = TDuration::Seconds(60);

    struct TServices {
        TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
        TScopedAlloc Alloc;
        TTypeEnvironment TypeEnv;
        TMockDbSchemeResolver DbSchemeResolver;
        TVector<THolder<TKeyDesc>> DescList;

        TServices()
            : FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry()))
            , Alloc(__LOCATION__)
            , TypeEnv(Alloc)
        {
            Alloc.Release();
        }

        ~TServices()
        {
            Alloc.Acquire();
        }

        void ExtractKeys(TRuntimeNode pgm) {
            Alloc.Acquire();
            TExploringNodeVisitor explorer;
            explorer.Walk(pgm.GetNode(), TypeEnv);
            DescList = ExtractTableKeys(explorer, TypeEnv);
            Alloc.Release();
        }
    };

    bool TryProgramText2Bin(const TString& programText, TServices& services, TConvertResult& result) {
        auto expr = ParseText(programText);

        auto resFuture = ConvertToMiniKQL(expr, services.FunctionRegistry.Get(),
            &services.TypeEnv, &services.DbSchemeResolver);
        result = resFuture.GetValue(TIME_LIMIT);

        if (!result.Node.GetNode()) {
            return false;
        }

        // Cout << PrintNode(result.Node.GetNode()) << Endl;

        return true;
    }

    TRuntimeNode ProgramText2Bin(const TString& programText, TServices& services) {
        TConvertResult res;
        TryProgramText2Bin(programText, services, res);

        res.Errors.PrintTo(Cerr);
        UNIT_ASSERT(res.Node.GetNode());
        return res.Node;
    }

    void RegisterSampleTables(TServices& services) {
        using TColumn = IDbSchemeResolver::TTableResult::TColumn;
        IDbSchemeResolver::TTableResult table(IDbSchemeResolver::TTableResult::Ok);
        table.Table.TableName = "table1";
        table.Table.ColumnNames = { "key", "value" };
        table.TableId.Reset(new TTableId(1, 2));
        table.KeyColumnCount = 1;
        table.Columns.insert(std::make_pair("key", TColumn{ 34, 0, TTypeInfo(NUdf::TDataType<ui32>::Id), 0, EColumnTypeConstraint::Nullable }));
        table.Columns.insert(std::make_pair("value", TColumn{ 56, -1, TTypeInfo(NUdf::TDataType<char*>::Id), (ui32)EInplaceUpdateMode::Min, EColumnTypeConstraint::Nullable }));
        services.DbSchemeResolver.AddTable(table);

        IDbSchemeResolver::TTableResult table2(IDbSchemeResolver::TTableResult::Ok);
        table2.Table.TableName = "table2";
        table2.Table.ColumnNames = { "key", "value" };
        table2.TableId.Reset(new TTableId(10, 20));
        table2.KeyColumnCount = 1;
        table2.Columns.insert(std::make_pair("key", TColumn{ 340, 0, TTypeInfo(NUdf::TDataType<ui32>::Id), 0, EColumnTypeConstraint::Nullable }));
        table2.Columns.insert(std::make_pair("value", TColumn{ 560, -1, TTypeInfo(NUdf::TDataType<char*>::Id), (ui32)EInplaceUpdateMode::Min, EColumnTypeConstraint::Nullable }));
        services.DbSchemeResolver.AddTable(table2);
    }
}

Y_UNIT_TEST_SUITE(TTestYqlToMiniKQLCompile) {
    Y_UNIT_TEST(CheckResolve) {
        TServices services;
        RegisterSampleTables(services);
        TVector<IDbSchemeResolver::TTable> tablesToResolve;
        IDbSchemeResolver::TTable tableToResolve;
        tableToResolve.TableName = "table1";
        tableToResolve.ColumnNames.insert("value");
        tablesToResolve.push_back(tableToResolve);
        auto resolveRes = services.DbSchemeResolver.ResolveTables(tablesToResolve);
        auto res = resolveRes.GetValue(TIME_LIMIT);
        UNIT_ASSERT_VALUES_EQUAL(res.size(), 1);
        UNIT_ASSERT_EQUAL(res[0].Status, IDbSchemeResolver::TTableResult::Ok);
        UNIT_ASSERT(!!res[0].TableId);
        UNIT_ASSERT(res[0].TableId->HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT_EQUAL(res[0].KeyColumnCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(res[0].Columns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res[0].Columns["value"].Column, 56);
        UNIT_ASSERT_VALUES_EQUAL(res[0].Columns["value"].KeyPosition, -1);
        UNIT_ASSERT_VALUES_EQUAL(res[0].Columns["value"].AllowInplaceMode, (ui32)EInplaceUpdateMode::Min);
        UNIT_ASSERT_VALUES_EQUAL(res[0].Columns["value"].Type.GetTypeId(), (ui32)NUdf::TDataType<char*>::Id);
    }

    Y_UNIT_TEST(OnlyResult) {
        auto programText = R"___(
(
(let pgmReturn (AsList (SetResult 'myRes (String 'abc))))
(return pgmReturn)
)
)___";

        TServices services;
        auto pgm = ProgramText2Bin(programText, services);
        services.ExtractKeys(pgm);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList.size(), 0);
    }

    Y_UNIT_TEST(EraseRow) {
        auto programText = R"___(
(
(let row1 '('('key (Uint32 '23))))
(let row2 '('('key (Just (Uint32 '23)))))
(let row3 '('('key (Nothing (OptionalType (DataType 'Uint32))))))
(let pgmReturn (AsList
    (EraseRow 'table1 row1)
    (EraseRow 'table1 row2)
    (EraseRow 'table1 row3)
))
(return pgmReturn)
)
)___";

        TServices services;
        RegisterSampleTables(services);
        auto pgm = ProgramText2Bin(programText, services);
        services.ExtractKeys(pgm);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList.size(), 3);
        for (ui32 i = 0; i < services.DescList.size(); ++i) {
            UNIT_ASSERT_EQUAL(services.DescList[i]->RowOperation, TKeyDesc::ERowOperation::Erase);
        }
    }

    Y_UNIT_TEST(UpdateRow) {
        auto programText = R"___(
(
(let row '('('key (Uint32 '23))))
(let update1 '('('value (String 'abc))))
(let update2 '('('value (Just (String 'def)))))
(let update3 '('('value (Nothing (OptionalType (DataType 'String))))))
(let update4 '('('value))) # erase of column
(let update5 '('('value 'Min (String 'def)))) # inplace update of column
(let pgmReturn (AsList
    (UpdateRow 'table1 row update1)
    (UpdateRow 'table1 row update2)
    (UpdateRow 'table1 row update3)
    (UpdateRow 'table1 row update4)
    (UpdateRow 'table1 row update5)
))
(return pgmReturn)
))___";

        TServices services;
        RegisterSampleTables(services);
        auto pgm = ProgramText2Bin(programText, services);
        services.ExtractKeys(pgm);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList.size(), 5);
        for (ui32 i = 0; i < services.DescList.size(); ++i) {
            UNIT_ASSERT_EQUAL(services.DescList[i]->RowOperation, TKeyDesc::ERowOperation::Update);
        }
    }

    Y_UNIT_TEST(SelectRow) {
        auto programText = R"___(
(
(let row '('('key (Uint32 '23))))
(let select '('value))
(let pgmReturn (AsList
    (SetResult 'myRes (Map (SelectRow 'table1 row select) (lambda '(x) (Member x 'value))))
))
(return pgmReturn)
)
)___";

        TServices services;
        RegisterSampleTables(services);
        auto pgm = ProgramText2Bin(programText, services);
        services.ExtractKeys(pgm);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList.size(), 1);
        for (ui32 i = 0; i < services.DescList.size(); ++i) {
            UNIT_ASSERT_EQUAL(services.DescList[i]->RowOperation, TKeyDesc::ERowOperation::Read);
        }
    }

    Y_UNIT_TEST(SelectRange) {
        auto programText = R"___(
(
(let range '('IncFrom 'IncTo '('key (Uint32 '23) (Void))))
(let select '('value))
(let options '('('ItemsLimit (Uint64 '2)) '('BytesLimit (Uint64 '1000))))
(let pgmReturn (AsList
    (SetResult 'myRes (Map (Member (SelectRange 'table1 range select options) 'List) (lambda '(x) (Member x 'value))))
))
(return pgmReturn)
)
)___";

        TServices services;
        RegisterSampleTables(services);
        auto pgm = ProgramText2Bin(programText, services);
        services.ExtractKeys(pgm);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList.size(), 1);
        for (ui32 i = 0; i < services.DescList.size(); ++i) {
            UNIT_ASSERT_EQUAL(services.DescList[i]->RowOperation, TKeyDesc::ERowOperation::Read);
        }
    }

    Y_UNIT_TEST(SimpleCrossShardTx) {
        auto programText = R"___(
                (
                (let row '('('key (Uint32 '2))))
                (let select '('value))
                (let selectRes (SelectRow 'table1 row select))
                (let val (FlatMap selectRes (lambda '(x) (Member x 'value))))
                (let row '('('key (Uint32 '2))))
                (let myUpd '(
                    '('value val)
                ))
                (let pgmReturn (AsList
                    (UpdateRow 'table2 row myUpd)
                ))
                    (return pgmReturn)
                )
            )___";

        TServices services;
        RegisterSampleTables(services);
        auto pgm = ProgramText2Bin(programText, services);
        services.ExtractKeys(pgm);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList.size(), 2);
        THashMap<TKeyDesc::ERowOperation, ui32> counters;
        for (ui32 i = 0; i < services.DescList.size(); ++i) {
            ++counters[services.DescList[i]->RowOperation];
        }

        UNIT_ASSERT_VALUES_EQUAL(counters.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters[TKeyDesc::ERowOperation::Read], 1);
        UNIT_ASSERT_VALUES_EQUAL(counters[TKeyDesc::ERowOperation::Update], 1);
    }

    Y_UNIT_TEST(AcquireLocks) {
        auto programText = R"___(
(
(let row '('('key (Uint32 '23))))
(let select '('value))
(let pgmReturn (AsList
    (SetResult 'myRes (Map (SelectRow 'table1 row select) (lambda '(x) (Member x 'value))))
    (AcquireLocks (Uint64 '0))
))
(return pgmReturn)
)
)___";

        TServices services;
        RegisterSampleTables(services);
        auto pgm = ProgramText2Bin(programText, services);
        services.ExtractKeys(pgm);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList.size(), 1);
        for (ui32 i = 0; i < services.DescList.size(); ++i) {
            UNIT_ASSERT_EQUAL(services.DescList[i]->RowOperation, TKeyDesc::ERowOperation::Read);
        }
    }

    Y_UNIT_TEST(StaticMapTypeOf) {
        auto programText = R"___(
(
    (return (AsList
        (SetResult 'x (StaticMap
            (AsStruct
                '('aaa (Just (Uint32 '17)))
                '('bbb (Nothing (OptionalType (DataType 'Utf8))))
            )
            (lambda '(field) (block '(
                (return (Coalesce field (Default (OptionalItemType (TypeOf field)))))
            )))
        ))
    ))
)
)___";

        TServices services;
        RegisterSampleTables(services);
        auto pgm = ProgramText2Bin(programText, services);
        services.ExtractKeys(pgm);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList.size(), 0);
    }

    Y_UNIT_TEST(SelectRangeAtomInRange) {
        auto programText = R"___(
(
    (let range '('key (Void) 'Void))
    (let rows (SelectRange 'table1 '(range) '() '()))
    (return rows)
)
)___";

        TServices services;
        RegisterSampleTables(services);
        TConvertResult result;
        UNIT_ASSERT(!TryProgramText2Bin(programText, services, result));
        UNIT_ASSERT(!result.Errors.Empty());
        UNIT_ASSERT_NO_DIFF(result.Errors.begin()->GetMessage(), "At function: SelectRange");
    }

    Y_UNIT_TEST(Extract) {
        auto programText = R"___(
(
(let range '('IncFrom 'IncTo '('key (Uint32 '23) (Void))))
(let select '('key 'value))
(let options '('('ItemsLimit (Uint64 '2))))
(let tupleList (AsList '((Uint32 '1) (Uint32 '2)) '((Uint32 '3) (Uint32 '4))))
(let pgmReturn (AsList
    (SetResult 'extractRes (Extract (Member (SelectRange 'table1 range select options) 'List) 'value))
    (SetResult 'orderedRes (OrderedExtract tupleList '1))
))
(return pgmReturn)
)
)___";

        TServices services;
        RegisterSampleTables(services);
        auto pgm = ProgramText2Bin(programText, services);
    }
}
} // namespace NYql
