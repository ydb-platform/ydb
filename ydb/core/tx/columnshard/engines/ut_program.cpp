#include "index_info.h"
#include "index_logic_logs.h"

#include <ydb/core/tx/columnshard/columnshard__index_scan.h>
#include <ydb/core/tx/columnshard/columnshard_ut_common.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/formats/arrow/converter.h>

#include <ydb/library/yql/core/arrow_kernels/request/request.h>
#include <ydb/library/yql/core/arrow_kernels/registry/registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NOlap;
using namespace NKikimr::NColumnShard;
using namespace NKikimr;
namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

namespace {
    static const std::vector<std::pair<TString, TTypeInfo>> testColumns = {
        {"timestamp", TTypeInfo(NTypeIds::Timestamp) },
        {"uid", TTypeInfo(NTypeIds::Utf8) },
        {"sum", TTypeInfo(NTypeIds::Int32) },
        {"vat", TTypeInfo(NTypeIds::Int32) },
    };

    static const std::vector<std::pair<TString, TTypeInfo>> testKey = {
        {"timestamp", TTypeInfo(NTypeIds::Timestamp) },
        {"uid", TTypeInfo(NTypeIds::Utf8) }
    };
}

Y_UNIT_TEST_SUITE(TestProgram) {

    class TKernelsWrapper {
        TIntrusivePtr<NMiniKQL::IFunctionRegistry> Reg;
        std::unique_ptr<NYql::TKernelRequestBuilder> ReqBuilder;
        public:
            TKernelsWrapper() {
                auto reg = CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry())->Clone();
                NMiniKQL::FillStaticModules(*reg);
                Reg.Reset(reg.Release());
                ReqBuilder = std::make_unique<NYql::TKernelRequestBuilder>(*Reg);
            }

            ui32 Add(NYql::TKernelRequestBuilder::EBinaryOp operation, bool scalar = false) {
                switch (operation) {
                    case NYql::TKernelRequestBuilder::EBinaryOp::Add:
                    {
                        NYql::TExprContext ctx;
                        auto blockInt32Type = ctx.template MakeType<NYql::TBlockExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Int32));
                        return ReqBuilder->AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::Add, blockInt32Type, blockInt32Type, blockInt32Type);
                    }
                    case NYql::TKernelRequestBuilder::EBinaryOp::StartsWith:
                    case NYql::TKernelRequestBuilder::EBinaryOp::EndsWith:
                    {
                        NYql::TExprContext ctx;
                        auto blockStringType = ctx.template MakeType<NYql::TBlockExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Utf8));
                        auto blockBoolType = ctx.template MakeType<NYql::TBlockExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool));
                        if (scalar) {
                            auto scalarStringType = ctx.template MakeType<NYql::TScalarExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::String));
                            return ReqBuilder->AddBinaryOp(operation, blockStringType, scalarStringType, blockBoolType);
                        } else {
                            return ReqBuilder->AddBinaryOp(operation, blockStringType, blockStringType, blockBoolType);
                        }
                    }
                    case NYql::TKernelRequestBuilder::EBinaryOp::StringContains:
                    {
                        NYql::TExprContext ctx;
                        auto blockStringType = ctx.template MakeType<NYql::TBlockExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::String));
                        auto blockBoolType = ctx.template MakeType<NYql::TBlockExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool));
                        return ReqBuilder->AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::StringContains, blockStringType, blockStringType, blockBoolType);
                    }
                    default:
                        Y_FAIL("Not implemented");

                }
            }

            ui32 AddJsonExists(bool isBinaryType = true) {
                NYql::TExprContext ctx;
                auto blockOptJsonType = ctx.template MakeType<NYql::TBlockExprType>(
                    ctx.template MakeType<NYql::TOptionalExprType>(
                    ctx.template MakeType<NYql::TDataExprType>(isBinaryType ? NYql::EDataSlot::JsonDocument : NYql::EDataSlot::Json)));
                auto scalarStringType = ctx.template MakeType<NYql::TScalarExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Utf8));
                auto blockBoolType = ctx.template MakeType<NYql::TBlockExprType>(
                    ctx.template MakeType<NYql::TOptionalExprType>(
                    ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool)));

                return ReqBuilder->JsonExists(blockOptJsonType, scalarStringType, blockBoolType);
            }

            ui32 AddJsonValue(bool isBinaryType = true, NYql::EDataSlot resultType = NYql::EDataSlot::Utf8) {
                NYql::TExprContext ctx;
                auto blockOptJsonType = ctx.template MakeType<NYql::TBlockExprType>(
                    ctx.template MakeType<NYql::TOptionalExprType>(
                    ctx.template MakeType<NYql::TDataExprType>(isBinaryType ? NYql::EDataSlot::JsonDocument : NYql::EDataSlot::Json)));
                auto scalarStringType = ctx.template MakeType<NYql::TScalarExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Utf8));
                auto blockResultType = ctx.template MakeType<NYql::TBlockExprType>(
                    ctx.template MakeType<NYql::TOptionalExprType>(
                    ctx.template MakeType<NYql::TDataExprType>(resultType)));

                return ReqBuilder->JsonValue(blockOptJsonType, scalarStringType, blockResultType);
            }

            TString Serialize() {
                return ReqBuilder->Serialize();
            }
    };

    TString SerializeProgram(const NKikimrSSA::TProgram& programProto) {
        NKikimrSSA::TOlapProgram olapProgramProto;
        {
            TString str;
            Y_PROTOBUF_SUPPRESS_NODISCARD programProto.SerializeToString(&str);
            olapProgramProto.SetProgram(str);
        }
        TString programSerialized;
        Y_PROTOBUF_SUPPRESS_NODISCARD olapProgramProto.SerializeToString(&programSerialized);
        return programSerialized;
    }

    Y_UNIT_TEST(YqlKernel) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetName("sum");
            functionProto->AddArguments()->SetName("vat");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_LENGTH);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetName("0");
        }

        TKernelsWrapper kernels;
        kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::Add);
        programProto.SetKernels(kernels.Serialize());
        const auto programSerialized = SerializeProgram(programProto);

        TProgramContainer program;
        TString errors;
        UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

        TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({{"sum", TTypeInfo(NTypeIds::Int32) }, {"vat", TTypeInfo(NTypeIds::Int32) }}));
        updates.AddRow().Add<int32_t>(1).Add<int32_t>(1);
        updates.AddRow().Add<int32_t>(100).Add<int32_t>(0);

        auto batch = updates.BuildArrow();
        auto res = program.ApplyProgram(batch);
        UNIT_ASSERT_C(res.ok(), res.ToString());

        TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Int32)) }));
        result.AddRow().Add<int32_t>(2);
        result.AddRow().Add<int32_t>(100);

        auto expected = result.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
    }

    Y_UNIT_TEST(YqlKernelStartsWithScalar) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* constantProto = command->MutableAssign()->MutableConstant();
            constantProto->SetBytes("Lorem");
            command->MutableAssign()->MutableColumn()->SetName("prefix");
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetName("string");
            functionProto->AddArguments()->SetName("prefix");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_LENGTH);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetName("0");
        }

        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::StartsWith, true);
            programProto.SetKernels(kernels.Serialize());
            const auto programSerialized = SerializeProgram(programProto);

            TProgramContainer program;
            TString errors;
            UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

            TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({{"string", TTypeInfo(NTypeIds::Utf8) }}));
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.");
            updates.AddRow().Add<std::string>("ipsum dolor sit amet.");

            auto batch = updates.BuildArrow();
            Cerr << batch->ToString() << Endl;
            auto res = program.ApplyProgram(batch);
            UNIT_ASSERT_C(res.ok(), res.ToString());

            TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Uint8)) }));
            result.AddRow().Add<ui8>(1);
            result.AddRow().Add<ui8>(0);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(YqlKernelStartsWith) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetName("string");
            functionProto->AddArguments()->SetName("substring");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_LENGTH);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetName("0");
        }

        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::StartsWith);
            programProto.SetKernels(kernels.Serialize());
            const auto programSerialized = SerializeProgram(programProto);

            TProgramContainer program;
            TString errors;
            UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

            TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({{"string", TTypeInfo(NTypeIds::Utf8) }, {"substring", TTypeInfo(NTypeIds::Utf8) }}));
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.").Add<std::string>("Lorem");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.").Add<std::string>("amet.");

            auto batch = updates.BuildArrow();
            auto res = program.ApplyProgram(batch);
            UNIT_ASSERT_C(res.ok(), res.ToString());

            TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Uint8)) }));
            result.AddRow().Add<ui8>(1);
            result.AddRow().Add<ui8>(0);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }

        // Test with binary data
        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::StringContains);
            programProto.SetKernels(kernels.Serialize());
            const auto programSerialized = SerializeProgram(programProto);

            TProgramContainer program;
            TString errors;
            UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

            TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({{"string", TTypeInfo(NTypeIds::Bytes) }, {"substring", TTypeInfo(NTypeIds::Bytes) }}));
            updates.AddRow().Add<std::string>("Lorem ipsum \xC0 dolor\f sit amet.").Add<std::string>("dolor");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit \amet.").Add<std::string>("amet.");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.").Add<std::string>("\amet.");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit \amet.").Add<std::string>("\amet.");

            auto batch = updates.BuildArrow();
            Cerr << batch->ToString() << Endl;
            auto res = program.ApplyProgram(batch);
            UNIT_ASSERT_C(res.ok(), res.ToString());

            TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Uint8)) }));
            result.AddRow().Add<ui8>(1);
            result.AddRow().Add<ui8>(0);
            result.AddRow().Add<ui8>(0);
            result.AddRow().Add<ui8>(1);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    void JsonExistsImpl(bool isBinaryType) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* constantProto = command->MutableAssign()->MutableConstant();
            constantProto->SetText("$.key");
            command->MutableAssign()->MutableColumn()->SetName("json_path");
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetName("json_data");
            functionProto->AddArguments()->SetName("json_path");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_LENGTH);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetName("0");
        }

        TKernelsWrapper kernels;
        kernels.AddJsonExists(isBinaryType);
        programProto.SetKernels(kernels.Serialize());
        const auto programSerialized = SerializeProgram(programProto);

        TProgramContainer program;
        TString errors;
        UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

        TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({{"json_data", TTypeInfo(isBinaryType ? NTypeIds::JsonDocument : NTypeIds::Json) }}));
        NJson::TJsonValue testJson;
        testJson["key"] = "value";
        updates.AddRow().Add<std::string>(testJson.GetStringRobust());
        updates.AddRow().Add<std::string>(NJson::TJsonValue(NJson::JSON_ARRAY).GetStringRobust());

        auto batch = updates.BuildArrow();
        Cerr << batch->ToString() << Endl;

        if (isBinaryType) {
            THashMap<TString, NScheme::TTypeInfo> cc;
            cc["json_data"] = TTypeInfo(NTypeIds::JsonDocument);
            batch = NArrow::ConvertColumns(batch, cc);
            Cerr << batch->ToString() << Endl;
        }
        auto res = program.ApplyProgram(batch);
        UNIT_ASSERT_C(res.ok(), res.ToString());

        TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Uint8)) }));
        result.AddRow().Add<ui8>(1);
        result.AddRow().Add<ui8>(0);

        auto expected = result.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
    }

    Y_UNIT_TEST(StartsAndEnds) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetName("string");
            functionProto->AddArguments()->SetName("substring");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_STARTS_WITH);
            command->MutableAssign()->MutableColumn()->SetName("start_with");
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(1);
            functionProto->AddArguments()->SetName("string");
            functionProto->AddArguments()->SetName("substring");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_ENDS_WITH);
            command->MutableAssign()->MutableColumn()->SetName("ends_with");
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_SIMPLE_ARROW);
            functionProto->AddArguments()->SetName("start_with");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_CAST_TO_BOOLEAN);
            command->MutableAssign()->MutableColumn()->SetName("start_with_bool");
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_SIMPLE_ARROW);
            functionProto->AddArguments()->SetName("ends_with");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_CAST_TO_BOOLEAN);
            command->MutableAssign()->MutableColumn()->SetName("ends_with_bool");
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_SIMPLE_ARROW);
            functionProto->AddArguments()->SetName("start_with_bool");
            functionProto->AddArguments()->SetName("ends_with_bool");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_BINARY_AND);
            command->MutableAssign()->MutableColumn()->SetName("result");
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetName("result");
        }

        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::StartsWith);
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith);
            programProto.SetKernels(kernels.Serialize());
            const auto programSerialized = SerializeProgram(programProto);

            TProgramContainer program;
            TString errors;
            UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

            TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({{"string", TTypeInfo(NTypeIds::Utf8) }, {"substring", TTypeInfo(NTypeIds::Utf8) }}));
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit Lorem").Add<std::string>("Lorem");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.").Add<std::string>("amet.");

            auto batch = updates.BuildArrow();
            auto res = program.ApplyProgram(batch);
            UNIT_ASSERT_C(res.ok(), res.ToString());

            TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("result", TTypeInfo(NTypeIds::Bool)) }));
            result.AddRow().Add<bool>(true);
            result.AddRow().Add<bool>(false);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }

    }

    Y_UNIT_TEST(JsonExists) {
        JsonExistsImpl(false);
    }

    Y_UNIT_TEST(JsonExistsBinary) {
        JsonExistsImpl(true);
    }

    void JsonValueImpl(bool isBinaryType, NYql::EDataSlot resultType) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* constantProto = command->MutableAssign()->MutableConstant();
            constantProto->SetText("$.key");
            command->MutableAssign()->MutableColumn()->SetName("json_path");
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetName("json_data");
            functionProto->AddArguments()->SetName("json_path");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_LENGTH);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetName("0");
        }

        TKernelsWrapper kernels;
        kernels.AddJsonValue(isBinaryType, resultType);
        programProto.SetKernels(kernels.Serialize());
        const auto programSerialized = SerializeProgram(programProto);

        TProgramContainer program;
        TString errors;
        UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

        TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({{"json_data", TTypeInfo(isBinaryType ? NTypeIds::JsonDocument : NTypeIds::Json) }}));
        {
            NJson::TJsonValue testJson;
            testJson["key"] = "value";
            updates.AddRow().Add<std::string>(testJson.GetStringRobust());
        }
        {
            NJson::TJsonValue testJson;
            testJson["key"] = 10;
            updates.AddRow().Add<std::string>(testJson.GetStringRobust());
        }
        {
            NJson::TJsonValue testJson;
            testJson["key"] = 0.1;
            updates.AddRow().Add<std::string>(testJson.GetStringRobust());
        }
        {
            NJson::TJsonValue testJson;
            testJson["key"] = false;
            updates.AddRow().Add<std::string>(testJson.GetStringRobust());
        }
        {
            NJson::TJsonValue testJson;
            testJson["another"] = "value";
            updates.AddRow().Add<std::string>(testJson.GetStringRobust());
        }
        {
            updates.AddRow().Add<std::string>(NJson::TJsonValue(NJson::JSON_ARRAY).GetStringRobust());
        }

        auto batch = updates.BuildArrow();
        Cerr << batch->ToString() << Endl;

        if (isBinaryType) {
            THashMap<TString, NScheme::TTypeInfo> cc;
            cc["json_data"] = TTypeInfo(NTypeIds::JsonDocument);
            batch = NArrow::ConvertColumns(batch, cc);
            Cerr << batch->ToString() << Endl;
        }

        auto res = program.ApplyProgram(batch);
        UNIT_ASSERT_C(res.ok(), res.ToString());

        Cerr << "Check output for " << resultType << Endl;
        if (resultType == NYql::EDataSlot::Utf8) {
            TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Utf8)) }));

            result.AddRow().Add<std::string>("value");
            result.AddRow().Add<std::string>("10");
            result.AddRow().Add<std::string>("0.1");
            result.AddRow().Add<std::string>("false");
            result.AddRow().AddNull();
            result.AddRow().AddNull();

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        } else if (resultType == NYql::EDataSlot::Bool) {
            TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Uint8)) }));

            result.AddRow().AddNull();
            result.AddRow().AddNull();
            result.AddRow().AddNull();
            result.AddRow().Add<ui8>(0);
            result.AddRow().AddNull();
            result.AddRow().AddNull();

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        } else if (resultType == NYql::EDataSlot::Int64 || resultType == NYql::EDataSlot::Uint64) {
            TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Int64)) }));

            result.AddRow().AddNull();
            result.AddRow().Add<i64>(10);
            result.AddRow().AddNull();
            result.AddRow().AddNull();
            result.AddRow().AddNull();
            result.AddRow().AddNull();

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        } else if (resultType == NYql::EDataSlot::Double || resultType == NYql::EDataSlot::Float) {
            TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Double)) }));

            result.AddRow().AddNull();
            result.AddRow().Add<double>(10);
            result.AddRow().Add<double>(0.1);
            result.AddRow().AddNull();
            result.AddRow().AddNull();
            result.AddRow().AddNull();

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        } else {
            Y_FAIL("Not implemented");
        }
    }

    Y_UNIT_TEST(JsonValue) {
        JsonValueImpl(false, NYql::EDataSlot::Utf8);
        JsonValueImpl(false, NYql::EDataSlot::Bool);
        JsonValueImpl(false, NYql::EDataSlot::Int64);
        JsonValueImpl(false, NYql::EDataSlot::Uint64);
        JsonValueImpl(false, NYql::EDataSlot::Float);
        JsonValueImpl(false, NYql::EDataSlot::Double);
    }

    Y_UNIT_TEST(JsonValueBinary) {
        JsonValueImpl(true, NYql::EDataSlot::Utf8);
        JsonValueImpl(true, NYql::EDataSlot::Bool);
        JsonValueImpl(true, NYql::EDataSlot::Int64);
        JsonValueImpl(true, NYql::EDataSlot::Uint64);
        JsonValueImpl(true, NYql::EDataSlot::Float);
        JsonValueImpl(true, NYql::EDataSlot::Double);
    }

    Y_UNIT_TEST(SimpleFunction) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);;
        TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            auto* funcArg = functionProto->AddArguments();
            funcArg->SetName("uid");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_LENGTH);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetName("0");
        }
        const auto programSerialized = SerializeProgram(programProto);

        TProgramContainer program;
        TString errors;
        UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

        TTableUpdatesBuilder updates(NArrow::MakeArrowSchema( { std::make_pair("uid", TTypeInfo(NTypeIds::Utf8)) }));
        updates.AddRow().Add("aaa");
        updates.AddRow().Add("b");
        updates.AddRow().Add("");

        auto batch = updates.BuildArrow();
        auto res = program.ApplyProgram(batch);
        UNIT_ASSERT_C(res.ok(), res.ToString());

        TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Uint64)) }));
        result.AddRow().Add<uint64_t>(3);
        result.AddRow().Add<uint64_t>(1);
        result.AddRow().Add<uint64_t>(0);

        auto expected = result.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
    }
}
