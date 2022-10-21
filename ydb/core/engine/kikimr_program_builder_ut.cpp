#include "kikimr_program_builder.h"

#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/scheme_types/scheme_types_defs.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NMiniKQL {

    using TTypeInfo = NScheme::TTypeInfo;

    TIntrusivePtr<IRandomProvider> CreateRandomProvider() {
        return CreateDeterministicRandomProvider(1);
    }

    TIntrusivePtr<ITimeProvider> CreateTimeProvider() {
        return CreateDeterministicTimeProvider(1);
    }

    TComputationNodeFactory GetLazyListFactory() {
        return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            if (callable.GetType()->GetName() == "LazyList") {
                return new TExternalComputationNode(ctx.Mutables);
            }

            return GetBuiltinFactory()(callable, ctx);
        };
    }

    struct TSetup {
        TSetup()
            : FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry()))
            , RandomProvider(CreateRandomProvider())
            , TimeProvider(CreateTimeProvider())
            , Alloc(__LOCATION__)
            , Env(new TTypeEnvironment(Alloc))
            , PgmBuilder(new TKikimrProgramBuilder(*Env, *FunctionRegistry))
        {}

        TAutoPtr<IComputationGraph> BuildGraph(TRuntimeNode pgm) {
            Explorer.Walk(pgm.GetNode(), *Env);
            TComputationPatternOpts opts(Alloc.Ref(), *Env, GetLazyListFactory(),
                FunctionRegistry.Get(), NUdf::EValidateMode::None,
                NUdf::EValidatePolicy::Exception, "OFF", EGraphPerProcess::Multi);
            Pattern = MakeComputationPattern(Explorer, pgm, {}, opts);
            return Pattern->Clone(opts.ToComputationOptions(*RandomProvider, *TimeProvider));
        }

        TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
        TIntrusivePtr<IRandomProvider> RandomProvider;
        TIntrusivePtr<ITimeProvider> TimeProvider;

        TScopedAlloc Alloc;
        THolder<TTypeEnvironment> Env;
        THolder<TKikimrProgramBuilder> PgmBuilder;

        TExploringNodeVisitor Explorer;
        IComputationPattern::TPtr Pattern;
    };

Y_UNIT_TEST_SUITE(TMiniKQLProgramBuilderTest) {

    void VerifySerialization(TNode* pgm, const TTypeEnvironment& env) {
        TString s = PrintNode(pgm);
        TString serialized = SerializeNode(pgm, env);
        Cout << "Serialized as " << serialized.size() << " bytes" << Endl;
        TNode* pgm2 = DeserializeNode(serialized, env);
        TString s2 = PrintNode(pgm2);
        UNIT_ASSERT_EQUAL(s, s2);
    }

    void VerifyProgram(TNode* pgm, const TTypeEnvironment& env) {
        VerifySerialization(pgm, env);
    }

    Y_UNIT_TEST(TestEraseRowStaticKey) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        TVector<TRuntimeNode> keyColumns;
        keyColumns.push_back(pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42));
        keyColumns.push_back(pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id));
        keyColumns.push_back(pgmBuilder.NewOptional(
            pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("qwe")));
        TVector<TTypeInfo> keyTypes({
            TTypeInfo(NUdf::TDataType<ui32>::Id),
            TTypeInfo(NUdf::TDataType<ui64>::Id),
            TTypeInfo(NUdf::TDataType<char*>::Id)
        });
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.EraseRow(TTableId(1, 2),
            keyTypes,
            keyColumns));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        //Cout << PrintNode(*pgm) << Endl;
        VerifyProgram(pgm, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgm, env);
        TVector<THolder<TKeyDesc>> tableKeys = ExtractTableKeys(explorer, env);
        UNIT_ASSERT_VALUES_EQUAL(tableKeys.size(), 1);
        UNIT_ASSERT(tableKeys[0]->TableId.HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT(tableKeys[0]->RowOperation == TKeyDesc::ERowOperation::Erase);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveFrom);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveTo);
        UNIT_ASSERT(tableKeys[0]->Range.Point);
        UNIT_ASSERT(tableKeys[0]->Range.From.size() == 3);
        UNIT_ASSERT(tableKeys[0]->Range.From[0].Size() == 4);
        UNIT_ASSERT(tableKeys[0]->Range.From[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.From[2].Size() == 3);
        UNIT_ASSERT(tableKeys[0]->Range.To.size() == 3);
        UNIT_ASSERT(tableKeys[0]->Range.To[0].Size() == 4);
        UNIT_ASSERT(tableKeys[0]->Range.To[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.To[2].Size() == 3);
        UNIT_ASSERT(tableKeys[0]->Columns.size() == 0);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes.size() == 3);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[0].GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[1].GetTypeId() == NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[2].GetTypeId() == NUdf::TDataType<char*>::Id);
    }

    Y_UNIT_TEST(TestEraseRowPartialDynamicKey) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        TVector<TRuntimeNode> keyColumns;
        keyColumns.push_back(pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id));
        keyColumns.push_back(pgmBuilder.Add(
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(34),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(12)));
        keyColumns.push_back(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("qwe"));
        TVector<TTypeInfo> keyTypes({
            TTypeInfo(NUdf::TDataType<ui64>::Id),
            TTypeInfo(NUdf::TDataType<ui32>::Id),
            TTypeInfo(NUdf::TDataType<char*>::Id)
        });
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.EraseRow(TTableId(1, 2),
            keyTypes,
            keyColumns));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        //Cout << PrintNode(*pgm) << Endl;
        VerifyProgram(pgm, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgm, env);
        TVector<THolder<TKeyDesc>> tableKeys = ExtractTableKeys(explorer, env);
        UNIT_ASSERT_VALUES_EQUAL(tableKeys.size(), 1);
        UNIT_ASSERT(tableKeys[0]->TableId.HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT(tableKeys[0]->RowOperation == TKeyDesc::ERowOperation::Erase);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveFrom);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveTo);
        UNIT_ASSERT(!tableKeys[0]->Range.Point);
        UNIT_ASSERT(tableKeys[0]->Range.From.size() == 3);
        UNIT_ASSERT(tableKeys[0]->Range.From[0].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.From[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.From[2].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.To.size() == 1);
        UNIT_ASSERT(tableKeys[0]->Range.To[0].IsNull());
        UNIT_ASSERT(tableKeys[0]->Columns.size() == 0);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes.size() == 3);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[0].GetTypeId() == NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[1].GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[2].GetTypeId() == NUdf::TDataType<char*>::Id);
    }

    Y_UNIT_TEST(TestEraseRowDynamicKey) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        TVector<TRuntimeNode> keyColumns;
        keyColumns.push_back(pgmBuilder.Add(
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(34),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(12)));
        keyColumns.push_back(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("qwe"));

        TVector<TTypeInfo> keyTypes({
            TTypeInfo(NUdf::TDataType<ui32>::Id),
            TTypeInfo(NUdf::TDataType<char*>::Id)
        });
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.EraseRow(TTableId(1, 2),
            keyTypes,
            keyColumns));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        //Cout << PrintNode(*pgm) << Endl;
        VerifyProgram(pgm, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgm, env);
        TVector<THolder<TKeyDesc>> tableKeys = ExtractTableKeys(explorer, env);
        UNIT_ASSERT_VALUES_EQUAL(tableKeys.size(), 1);
        UNIT_ASSERT(tableKeys[0]->TableId.HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT(tableKeys[0]->RowOperation == TKeyDesc::ERowOperation::Erase);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveFrom);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveTo);
        UNIT_ASSERT(!tableKeys[0]->Range.Point);
        UNIT_ASSERT(tableKeys[0]->Range.From.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Range.From[0].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.From[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.To.size() == 0);
        UNIT_ASSERT(tableKeys[0]->Columns.size() == 0);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes.size() == 2);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[0].GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[1].GetTypeId() == NUdf::TDataType<char*>::Id);
    }

    Y_UNIT_TEST(TestSelectRow) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        TVector<TRuntimeNode> keyColumns;
        keyColumns.push_back(pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42));
        keyColumns.push_back(pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id));
        keyColumns.push_back(pgmBuilder.NewOptional(
            pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("qwe")));
        TVector<TSelectColumn> columnsToRead;
        columnsToRead.emplace_back("column1", 34, TTypeInfo(NUdf::TDataType<ui32>::Id), EColumnTypeConstraint::Nullable);
        columnsToRead.emplace_back("column2", 56, TTypeInfo(NUdf::TDataType<ui64>::Id), EColumnTypeConstraint::Nullable);
        TVector<TTypeInfo> keyTypes({
            TTypeInfo(NUdf::TDataType<ui32>::Id),
            TTypeInfo(NUdf::TDataType<ui64>::Id),
            TTypeInfo(NUdf::TDataType<char*>::Id)
        });
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.SetResult("myRes",
            pgmBuilder.SelectRow(TTableId(1, 2),
            keyTypes,
            columnsToRead, keyColumns)));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        //Cout << PrintNode(*pgm) << Endl;
        VerifyProgram(pgm, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgm, env);
        TVector<THolder<TKeyDesc>> tableKeys = ExtractTableKeys(explorer, env);
        UNIT_ASSERT_VALUES_EQUAL(tableKeys.size(), 1);
        UNIT_ASSERT(tableKeys[0]->TableId.HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT(tableKeys[0]->RowOperation == TKeyDesc::ERowOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveFrom);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveTo);
        UNIT_ASSERT(tableKeys[0]->Range.Point);
        UNIT_ASSERT(tableKeys[0]->Range.From.size() == 3);
        UNIT_ASSERT(tableKeys[0]->Range.From[0].Size() == 4);
        UNIT_ASSERT(tableKeys[0]->Range.From[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.From[2].Size() == 3);
        UNIT_ASSERT(tableKeys[0]->Range.To.size() == 3);
        UNIT_ASSERT(tableKeys[0]->Range.To[0].Size() == 4);
        UNIT_ASSERT(tableKeys[0]->Range.To[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.To[2].Size() == 3);
        UNIT_ASSERT(tableKeys[0]->Columns.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Column == 34);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[0].ExpectedType.GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Column == 56);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[1].ExpectedType.GetTypeId() == NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes.size() == 3);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[0].GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[1].GetTypeId() == NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[2].GetTypeId() == NUdf::TDataType<char*>::Id);
    }

    Y_UNIT_TEST(TestUpdateRowStaticKey) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        TVector<TRuntimeNode> keyColumns;
        keyColumns.push_back(pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42));
        keyColumns.push_back(pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id));
        keyColumns.push_back(pgmBuilder.NewOptional(
            pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("qwe")));
        auto update = pgmBuilder.GetUpdateRowBuilder();
        update.SetColumn(34, TTypeInfo(NUdf::TDataType<ui32>::Id),
            pgmBuilder.NewOptional(pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(12)));
        update.EraseColumn(56);
        update.InplaceUpdateColumn(78, TTypeInfo(NUdf::TDataType<ui64>::Id),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(1), EInplaceUpdateMode::Sum);
        TVector<TTypeInfo> keyTypes({
            TTypeInfo(NUdf::TDataType<ui32>::Id),
            TTypeInfo(NUdf::TDataType<ui64>::Id),
            TTypeInfo(NUdf::TDataType<char*>::Id)
        });
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.UpdateRow(TTableId(1, 2),
            keyTypes,
            keyColumns, update));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        //Cout << PrintNode(*pgm) << Endl;
        VerifyProgram(pgm, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgm, env);
        TVector<THolder<TKeyDesc>> tableKeys = ExtractTableKeys(explorer, env);
        UNIT_ASSERT_VALUES_EQUAL(tableKeys.size(), 1);
        UNIT_ASSERT(tableKeys[0]->TableId.HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT(tableKeys[0]->RowOperation == TKeyDesc::ERowOperation::Update);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveFrom);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveTo);
        UNIT_ASSERT(tableKeys[0]->Range.Point);
        UNIT_ASSERT(tableKeys[0]->Range.From.size() == 3);
        UNIT_ASSERT(tableKeys[0]->Range.From[0].Size() == 4);
        UNIT_ASSERT(tableKeys[0]->Range.From[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.From[2].Size() == 3);
        UNIT_ASSERT(tableKeys[0]->Range.To.size() == 3);
        UNIT_ASSERT(tableKeys[0]->Range.To[0].Size() == 4);
        UNIT_ASSERT(tableKeys[0]->Range.To[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.To[2].Size() == 3);
        UNIT_ASSERT(tableKeys[0]->Columns.size() == 3);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Column == 34);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Operation == TKeyDesc::EColumnOperation::Set);
        UNIT_ASSERT(tableKeys[0]->Columns[0].ExpectedType.GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Column == 56);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Operation == TKeyDesc::EColumnOperation::Set);
        UNIT_ASSERT(tableKeys[0]->Columns[1].ExpectedType.GetTypeId() == 0);
        UNIT_ASSERT(tableKeys[0]->Columns[2].Column == 78);
        UNIT_ASSERT(tableKeys[0]->Columns[2].Operation == TKeyDesc::EColumnOperation::InplaceUpdate);
        UNIT_ASSERT(tableKeys[0]->Columns[2].ExpectedType.GetTypeId() == NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes.size() == 3);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[0].GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[1].GetTypeId() == NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[2].GetTypeId() == NUdf::TDataType<char*>::Id);
    }

    Y_UNIT_TEST(TestUpdateRowDynamicKey) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        TVector<TRuntimeNode> keyColumns;
        keyColumns.push_back(pgmBuilder.Add(
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(34),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(12)));
        keyColumns.push_back(pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id));
        keyColumns.push_back(pgmBuilder.NewOptional(
            pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("qwe")));
        auto update = pgmBuilder.GetUpdateRowBuilder();
        update.SetColumn(34, TTypeInfo(NUdf::TDataType<ui32>::Id),
            pgmBuilder.NewOptional(pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(12)));
        update.EraseColumn(56);
        update.InplaceUpdateColumn(78, TTypeInfo(NUdf::TDataType<ui64>::Id),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(1), EInplaceUpdateMode::Sum);
        TVector<TTypeInfo> keyTypes({
            TTypeInfo(NUdf::TDataType<ui32>::Id),
            TTypeInfo(NUdf::TDataType<ui64>::Id),
            TTypeInfo(NUdf::TDataType<char*>::Id)
        });
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.UpdateRow(TTableId(1, 2),
            keyTypes,
            keyColumns, update));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        //Cout << PrintNode(*pgm) << Endl;
        VerifyProgram(pgm, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgm, env);
        TVector<THolder<TKeyDesc>> tableKeys = ExtractTableKeys(explorer, env);
        UNIT_ASSERT_VALUES_EQUAL(tableKeys.size(), 1);
        UNIT_ASSERT(tableKeys[0]->TableId.HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT(tableKeys[0]->RowOperation == TKeyDesc::ERowOperation::Update);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveFrom);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveTo);
        UNIT_ASSERT(!tableKeys[0]->Range.Point);
        UNIT_ASSERT(tableKeys[0]->Range.From.size() == 3);
        UNIT_ASSERT(tableKeys[0]->Range.From[0].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.From[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.From[2].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.To.size() == 0);
        UNIT_ASSERT(tableKeys[0]->Columns.size() == 3);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Column == 34);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Operation == TKeyDesc::EColumnOperation::Set);
        UNIT_ASSERT(tableKeys[0]->Columns[0].ExpectedType.GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Column == 56);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Operation == TKeyDesc::EColumnOperation::Set);
        UNIT_ASSERT(tableKeys[0]->Columns[1].ExpectedType.GetTypeId() == 0);
        UNIT_ASSERT(tableKeys[0]->Columns[2].Column == 78);
        UNIT_ASSERT(tableKeys[0]->Columns[2].Operation == TKeyDesc::EColumnOperation::InplaceUpdate);
        UNIT_ASSERT(tableKeys[0]->Columns[2].ExpectedType.GetTypeId() == NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns[2].InplaceUpdateMode == (ui32)EInplaceUpdateMode::Sum);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes.size() == 3);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[0].GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[1].GetTypeId() == NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[2].GetTypeId() == NUdf::TDataType<char*>::Id);
    }

    Y_UNIT_TEST(TestSelectFromInclusiveRange) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        TTableRangeOptions options(pgmBuilder.GetDefaultTableRangeOptions());
        TVector<TRuntimeNode> from;
        from.push_back(pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42));
        from.push_back(pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id));
        options.FromColumns = from;

        TVector<TSelectColumn> columnsToRead;
        columnsToRead.emplace_back("column1", 34, TTypeInfo(NUdf::TDataType<ui32>::Id), EColumnTypeConstraint::Nullable);
        columnsToRead.emplace_back("column2", 56, TTypeInfo(NUdf::TDataType<ui64>::Id), EColumnTypeConstraint::Nullable);
        TVector<TTypeInfo> keyTypes({
            TTypeInfo(NUdf::TDataType<ui32>::Id),
            TTypeInfo(NUdf::TDataType<ui64>::Id)
        });
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.SetResult("myRes",
            pgmBuilder.SelectRange(TTableId(1, 2),
            keyTypes,
            columnsToRead, options)));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        //Cout << PrintNode(*pgm) << Endl;
        VerifyProgram(pgm, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgm, env);
        TVector<THolder<TKeyDesc>> tableKeys = ExtractTableKeys(explorer, env);
        UNIT_ASSERT_VALUES_EQUAL(tableKeys.size(), 1);
        UNIT_ASSERT(tableKeys[0]->TableId.HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT(tableKeys[0]->RowOperation == TKeyDesc::ERowOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveFrom);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveTo);
        UNIT_ASSERT(!tableKeys[0]->Range.Point);
        UNIT_ASSERT(tableKeys[0]->Range.From.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Range.From[0].Size() == 4);
        UNIT_ASSERT(tableKeys[0]->Range.From[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.To.size() == 0);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes.size() == 2);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[0].GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[1].GetTypeId() == NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Column == 34);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[0].ExpectedType.GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Column == 56);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[1].ExpectedType.GetTypeId() == NUdf::TDataType<ui64>::Id);
    }

    Y_UNIT_TEST(TestSelectFromExclusiveRange) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        TTableRangeOptions options(pgmBuilder.GetDefaultTableRangeOptions());
        TVector<TRuntimeNode> from;
        from.push_back(pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42));
        from.push_back(pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id));
        options.FromColumns = from;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::ExcludeInitValue);

        TVector<TSelectColumn> columnsToRead;
        columnsToRead.emplace_back("column1", 34, TTypeInfo(NUdf::TDataType<ui32>::Id), EColumnTypeConstraint::Nullable);
        columnsToRead.emplace_back("column2", 56, TTypeInfo(NUdf::TDataType<ui64>::Id), EColumnTypeConstraint::Nullable);
        TVector<TTypeInfo> keyTypes({
            TTypeInfo(NUdf::TDataType<ui32>::Id),
            TTypeInfo(NUdf::TDataType<ui64>::Id)
        });
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.SetResult("myRes",
            pgmBuilder.SelectRange(TTableId(1, 2),
            keyTypes,
            columnsToRead, options)));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        //Cout << PrintNode(*pgm) << Endl;
        VerifyProgram(pgm, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgm, env);
        TVector<THolder<TKeyDesc>> tableKeys = ExtractTableKeys(explorer, env);
        UNIT_ASSERT_VALUES_EQUAL(tableKeys.size(), 1);
        UNIT_ASSERT(tableKeys[0]->TableId.HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT(tableKeys[0]->RowOperation == TKeyDesc::ERowOperation::Read);
        UNIT_ASSERT(!tableKeys[0]->Range.InclusiveFrom);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveTo);
        UNIT_ASSERT(!tableKeys[0]->Range.Point);
        UNIT_ASSERT(tableKeys[0]->Range.From.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Range.From[0].Size() == 4);
        UNIT_ASSERT(tableKeys[0]->Range.From[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.To.size() == 0);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes.size() == 2);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[0].GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[1].GetTypeId() == NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Column == 34);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[0].ExpectedType.GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Column == 56);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[1].ExpectedType.GetTypeId() == NUdf::TDataType<ui64>::Id);
    }

    Y_UNIT_TEST(TestSelectToInclusiveRange) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        TTableRangeOptions options(pgmBuilder.GetDefaultTableRangeOptions());
        TVector<TRuntimeNode> from;
        from.push_back(pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id));
        TVector<TRuntimeNode> to;
        to.push_back(pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42));
        to.push_back(pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id));
        options.FromColumns = from;
        options.ToColumns = to;

        TVector<TSelectColumn> columnsToRead;
        columnsToRead.emplace_back("column1", 34, TTypeInfo(NUdf::TDataType<ui32>::Id), EColumnTypeConstraint::Nullable);
        columnsToRead.emplace_back("column2", 56, TTypeInfo(NUdf::TDataType<ui64>::Id), EColumnTypeConstraint::Nullable);
        TVector<TTypeInfo> keyTypes({
            TTypeInfo(NUdf::TDataType<ui32>::Id),
            TTypeInfo(NUdf::TDataType<ui64>::Id)
        });
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.SetResult("myRes",
            pgmBuilder.SelectRange(TTableId(1, 2),
            keyTypes,
            columnsToRead, options)));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        //Cout << PrintNode(*pgm) << Endl;
        VerifyProgram(pgm, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgm, env);
        TVector<THolder<TKeyDesc>> tableKeys = ExtractTableKeys(explorer, env);
        UNIT_ASSERT_VALUES_EQUAL(tableKeys.size(), 1);
        UNIT_ASSERT(tableKeys[0]->TableId.HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT(tableKeys[0]->RowOperation == TKeyDesc::ERowOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveFrom);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveTo);
        UNIT_ASSERT(!tableKeys[0]->Range.Point);
        UNIT_ASSERT(tableKeys[0]->Range.From.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Range.From[0].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.From[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.To.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Range.To[0].Size() == 4);
        UNIT_ASSERT(tableKeys[0]->Range.To[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes.size() == 2);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[0].GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[1].GetTypeId() == NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Column == 34);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[0].ExpectedType.GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Column == 56);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[1].ExpectedType.GetTypeId() == NUdf::TDataType<ui64>::Id);
    }

    Y_UNIT_TEST(TestSelectToExclusiveRange) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        TTableRangeOptions options(pgmBuilder.GetDefaultTableRangeOptions());
        TVector<TRuntimeNode> from;
        from.push_back(pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id));
        TVector<TRuntimeNode> to;
        to.push_back(pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42));
        to.push_back(pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id));
        options.FromColumns = from;
        options.ToColumns = to;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::ExcludeTermValue);

        TVector<TSelectColumn> columnsToRead;
        columnsToRead.emplace_back("column1", 34, TTypeInfo(NUdf::TDataType<ui32>::Id), EColumnTypeConstraint::Nullable);
        columnsToRead.emplace_back("column2", 56, TTypeInfo(NUdf::TDataType<ui64>::Id), EColumnTypeConstraint::Nullable);
        TVector<TTypeInfo> keyTypes({
            TTypeInfo(NUdf::TDataType<ui32>::Id),
            TTypeInfo(NUdf::TDataType<ui64>::Id)
        });
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.SetResult("myRes",
            pgmBuilder.SelectRange(TTableId(1, 2),
            keyTypes,
            columnsToRead, options)));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        //Cout << PrintNode(*pgm) << Endl;
        VerifyProgram(pgm, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgm, env);
        TVector<THolder<TKeyDesc>> tableKeys = ExtractTableKeys(explorer, env);
        UNIT_ASSERT_VALUES_EQUAL(tableKeys.size(), 1);
        UNIT_ASSERT(tableKeys[0]->TableId.HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT(tableKeys[0]->RowOperation == TKeyDesc::ERowOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveFrom);
        UNIT_ASSERT(!tableKeys[0]->Range.InclusiveTo);
        UNIT_ASSERT(!tableKeys[0]->Range.Point);
        UNIT_ASSERT(tableKeys[0]->Range.From.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Range.From[0].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.From[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->Range.To.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Range.To[0].Size() == 4);
        UNIT_ASSERT(tableKeys[0]->Range.To[1].IsNull());
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes.size() == 2);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[0].GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[1].GetTypeId() == NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Column == 34);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[0].ExpectedType.GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Column == 56);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[1].ExpectedType.GetTypeId() == NUdf::TDataType<ui64>::Id);
    }

    Y_UNIT_TEST(TestSelectBothFromInclusiveToInclusiveRange) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        TTableRangeOptions options(pgmBuilder.GetDefaultTableRangeOptions());
        TVector<TRuntimeNode> from;
        TVector<TRuntimeNode> to;
        from.push_back(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("a"));
        to.push_back(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("bc"));
        options.FromColumns = from;
        options.ToColumns = to;

        TVector<TSelectColumn> columnsToRead;
        columnsToRead.emplace_back("column1", 34, TTypeInfo(NUdf::TDataType<ui32>::Id), EColumnTypeConstraint::Nullable);
        columnsToRead.emplace_back("column2", 56, TTypeInfo(NUdf::TDataType<ui64>::Id), EColumnTypeConstraint::Nullable);
        TVector<TTypeInfo> keyTypes({ TTypeInfo(NUdf::TDataType<char*>::Id) });
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.SetResult("myRes",
            pgmBuilder.SelectRange(TTableId(1, 2),
            keyTypes,
            columnsToRead, options)));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        //Cout << PrintNode(*pgm) << Endl;
        VerifyProgram(pgm, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgm, env);
        TVector<THolder<TKeyDesc>> tableKeys = ExtractTableKeys(explorer, env);
        UNIT_ASSERT_VALUES_EQUAL(tableKeys.size(), 1);
        UNIT_ASSERT(tableKeys[0]->TableId.HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT(tableKeys[0]->RowOperation == TKeyDesc::ERowOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveFrom);
        UNIT_ASSERT(tableKeys[0]->Range.InclusiveTo);
        UNIT_ASSERT(!tableKeys[0]->Range.Point);
        UNIT_ASSERT(tableKeys[0]->Range.From.size() == 1);
        UNIT_ASSERT(tableKeys[0]->Range.From[0].Size() == 1);
        UNIT_ASSERT(tableKeys[0]->Range.To.size() == 1);
        UNIT_ASSERT(tableKeys[0]->Range.To[0].Size() == 2);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes.size() == 1);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[0].GetTypeId() == NUdf::TDataType<char*>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Column == 34);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[0].ExpectedType.GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Column == 56);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[1].ExpectedType.GetTypeId() == NUdf::TDataType<ui64>::Id);
    }

    Y_UNIT_TEST(TestSelectBothFromExclusiveToExclusiveRange) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        TTableRangeOptions options(pgmBuilder.GetDefaultTableRangeOptions());
        TVector<TRuntimeNode> from;
        TVector<TRuntimeNode> to;
        from.push_back(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("a"));
        to.push_back(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("bc"));
        options.FromColumns = from;
        options.ToColumns = to;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(
            TReadRangeOptions::TFlags::ExcludeInitValue | TReadRangeOptions::TFlags::ExcludeTermValue);

        TVector<TSelectColumn> columnsToRead;
        columnsToRead.emplace_back("column1", 34, TTypeInfo(NUdf::TDataType<ui32>::Id), EColumnTypeConstraint::Nullable);
        columnsToRead.emplace_back("column2", 56, TTypeInfo(NUdf::TDataType<ui64>::Id), EColumnTypeConstraint::Nullable);
        TVector<TTypeInfo> keyTypes({ TTypeInfo(NUdf::TDataType<char*>::Id) });
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.SetResult("myRes",
            pgmBuilder.SelectRange(TTableId(1, 2),
            keyTypes,
            columnsToRead, options)));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        //Cout << PrintNode(*pgm) << Endl;
        VerifyProgram(pgm, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgm, env);
        TVector<THolder<TKeyDesc>> tableKeys = ExtractTableKeys(explorer, env);
        UNIT_ASSERT_VALUES_EQUAL(tableKeys.size(), 1);
        UNIT_ASSERT(tableKeys[0]->TableId.HasSamePath(TTableId(1, 2)));
        UNIT_ASSERT(tableKeys[0]->RowOperation == TKeyDesc::ERowOperation::Read);
        UNIT_ASSERT(!tableKeys[0]->Range.InclusiveFrom);
        UNIT_ASSERT(!tableKeys[0]->Range.InclusiveTo);
        UNIT_ASSERT(!tableKeys[0]->Range.Point);
        UNIT_ASSERT(tableKeys[0]->Range.From.size() == 1);
        UNIT_ASSERT(tableKeys[0]->Range.From[0].Size() == 1);
        UNIT_ASSERT(tableKeys[0]->Range.To.size() == 1);
        UNIT_ASSERT(tableKeys[0]->Range.To[0].Size() == 2);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes.size() == 1);
        UNIT_ASSERT(tableKeys[0]->KeyColumnTypes[0].GetTypeId() == NUdf::TDataType<char*>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns.size() == 2);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Column == 34);
        UNIT_ASSERT(tableKeys[0]->Columns[0].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[0].ExpectedType.GetTypeId() == NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Column == 56);
        UNIT_ASSERT(tableKeys[0]->Columns[1].Operation == TKeyDesc::EColumnOperation::Read);
        UNIT_ASSERT(tableKeys[0]->Columns[1].ExpectedType.GetTypeId() == NUdf::TDataType<ui64>::Id);
    }

    Y_UNIT_TEST(TestAcquireLocks) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.SetResult("locks",
            pgmBuilder.AcquireLocks(pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(0))));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();

        VerifyProgram(pgm, env);
    }

    Y_UNIT_TEST(TestDiagnostics) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        auto pgmReturn = pgmBuilder.NewEmptyListOfVoid();
        pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.SetResult("diag", pgmBuilder.Diagnostics()));
        auto pgm = pgmBuilder.Build(pgmReturn, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();

        VerifyProgram(pgm, env);
    }

    Y_UNIT_TEST(TestInvalidParameterName) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);

        auto paramsBuilder = pgmBuilder.GetParametersBuilder();
        paramsBuilder.Add("Param1", pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(10));

        auto param = pgmBuilder.Parameter("Param2", pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));

        try {
            pgmBuilder.Bind(pgmBuilder.AsList(pgmBuilder.SetResult("Result", param)), paramsBuilder.Build());
        } catch (const yexception& ex) {
            UNIT_ASSERT(TString(ex.what()).EndsWith("Missing value for parameter: Param2"));
            return;
        }

        UNIT_FAIL("Expected exception.");
    }

    Y_UNIT_TEST(TestInvalidParameterType) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);

        auto paramsBuilder = pgmBuilder.GetParametersBuilder();
        paramsBuilder.Add("Param1", pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(10));

        auto param = pgmBuilder.Parameter("Param1", pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));

        try {
            pgmBuilder.Bind(pgmBuilder.AsList(pgmBuilder.SetResult("Result", param)), paramsBuilder.Build());
        } catch (const yexception& ex) {
            UNIT_ASSERT(TString(ex.what()).Contains("Incorrect type for parameter Param1"));
            return;
        }

        UNIT_FAIL("Expected exception.");
    }
}

} // namespace NMiniKQL
} // namespace NKikimr
