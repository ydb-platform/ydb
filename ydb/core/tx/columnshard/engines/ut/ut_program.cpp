#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/formats/arrow/program/aggr_common.h>
#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/resolver.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>
#include <ydb/core/tx/columnshard/test_helper/kernels_wrapper.h>
#include <ydb/core/tx/columnshard/test_helper/program_constructor.h>
#include <ydb/core/tx/program/program.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/core/arrow_kernels/registry/registry.h>
#include <yql/essentials/core/arrow_kernels/request/request.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

using namespace NKikimr::NOlap;
using namespace NKikimr::NColumnShard;
using namespace NKikimr::NTxUT;
using namespace NKikimr;
namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

namespace {
static const std::vector<NArrow::NTest::TTestColumn> testColumns = { NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp)),
    NArrow::NTest::TTestColumn("uid", TTypeInfo(NTypeIds::Utf8)), NArrow::NTest::TTestColumn("sum", TTypeInfo(NTypeIds::Int32)),
    NArrow::NTest::TTestColumn("vat", TTypeInfo(NTypeIds::Int32)), NArrow::NTest::TTestColumn("json_string", TTypeInfo(NTypeIds::Json)),
    NArrow::NTest::TTestColumn("json_binary", TTypeInfo(NTypeIds::JsonDocument)),
    NArrow::NTest::TTestColumn("string", TTypeInfo(NTypeIds::Utf8)), NArrow::NTest::TTestColumn("binary", TTypeInfo(NTypeIds::Bytes)),
    NArrow::NTest::TTestColumn("substring", TTypeInfo(NTypeIds::Utf8)), NArrow::NTest::TTestColumn("i16", TTypeInfo(NTypeIds::Int16)),
    NArrow::NTest::TTestColumn("float", TTypeInfo(NTypeIds::Float)) };

static const std::vector<NArrow::NTest::TTestColumn> testKey = { NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp)),
    NArrow::NTest::TTestColumn("uid", TTypeInfo(NTypeIds::Utf8)) };
}   // namespace

Y_UNIT_TEST_SUITE(TestProgram) {
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
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            command->MutableAssign()->MutableColumn()->SetId(15);
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("sum"));
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("vat"));
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetId(15);
        }

        TKernelsWrapper kernels;
        kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::Add);
        programProto.SetKernels(kernels.Serialize());
        const auto programSerialized = SerializeProgram(programProto);

        TProgramContainer program;
        program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized).Validate();

        TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({ { "sum", TTypeInfo(NTypeIds::Int32) }, { "vat", TTypeInfo(NTypeIds::Int32) } }));
        updates.AddRow().Add<int32_t>(1).Add<int32_t>(1);
        updates.AddRow().Add<int32_t>(100).Add<int32_t>(0);
        auto batch = updates.BuildArrow();
        batch = program.ApplyProgram(batch, columnResolver).DetachResult();

        TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("15", TTypeInfo(NTypeIds::Int32)) }));
        result.AddRow().Add<int32_t>(2);
        result.AddRow().Add<int32_t>(100);

        auto expected = result.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
    }

    Y_UNIT_TEST(YqlKernelStartsWithScalar) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* constantProto = command->MutableAssign()->MutableConstant();
            constantProto->SetBytes("Lorem");
            command->MutableAssign()->MutableColumn()->SetId(15);
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            command->MutableAssign()->MutableColumn()->SetId(16);
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("string"));
            functionProto->AddArguments()->SetId(15);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetId(16);
        }

        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::StartsWith, true);
            programProto.SetKernels(kernels.Serialize());
            const auto programSerialized = SerializeProgram(programProto);

            TProgramContainer program;
            program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized)
                .Validate();

            TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({ { "string", TTypeInfo(NTypeIds::Utf8) } }));
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.");
            updates.AddRow().Add<std::string>("ipsum dolor sit amet.");

            auto batch = program.ApplyProgram(updates.BuildArrow(), columnResolver).DetachResult();

            TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("16", TTypeInfo(NTypeIds::Uint8)) }));
            result.AddRow().Add<ui8>(1);
            result.AddRow().Add<ui8>(0);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(YqlKernelEndsWithScalar) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* constantProto = command->MutableAssign()->MutableConstant();
            constantProto->SetBytes("amet.");
            command->MutableAssign()->MutableColumn()->SetId(15);
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("string"));
            functionProto->AddArguments()->SetId(15);
            command->MutableAssign()->MutableColumn()->SetId(16);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            prjectionProto->AddColumns()->SetId(16);
        }

        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, true);
            programProto.SetKernels(kernels.Serialize());
            const auto programSerialized = SerializeProgram(programProto);

            TProgramContainer program;
            program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized)
                .Validate();

            TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({ { "string", TTypeInfo(NTypeIds::Utf8) } }));
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit.");

            auto batch = program.ApplyProgram(updates.BuildArrow(), columnResolver).DetachResult();

            TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("16", TTypeInfo(NTypeIds::Uint8)) }));
            result.AddRow().Add<ui8>(1);
            result.AddRow().Add<ui8>(0);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(YqlKernelStartsWith) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("string"));
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("substring"));
            command->MutableAssign()->MutableColumn()->SetId(15);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            prjectionProto->AddColumns()->SetId(15);
        }

        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::StartsWith);
            programProto.SetKernels(kernels.Serialize());
            const auto programSerialized = SerializeProgram(programProto);

            TProgramContainer program;
            program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized)
                .Validate();

            TTableUpdatesBuilder updates(
                NArrow::MakeArrowSchema({ { "string", TTypeInfo(NTypeIds::Utf8) }, { "substring", TTypeInfo(NTypeIds::Utf8) } }));
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.").Add<std::string>("Lorem");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.").Add<std::string>("amet.");

            auto batch = program.ApplyProgram(updates.BuildArrow(), columnResolver).DetachResult();

            TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("15", TTypeInfo(NTypeIds::Uint8)) }));
            result.AddRow().Add<ui8>(1);
            result.AddRow().Add<ui8>(0);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(YqlKernelEndsWith) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;

        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("string"));
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("substring"));
            command->MutableAssign()->MutableColumn()->SetId(15);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            prjectionProto->AddColumns()->SetId(15);
        }

        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith);
            programProto.SetKernels(kernels.Serialize());
            const auto programSerialized = SerializeProgram(programProto);

            TProgramContainer program;
            program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized)
                .Validate();

            TTableUpdatesBuilder updates(
                NArrow::MakeArrowSchema({ { "string", TTypeInfo(NTypeIds::Utf8) }, { "substring", TTypeInfo(NTypeIds::Utf8) } }));
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.").Add<std::string>("Lorem");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.").Add<std::string>("amet.");

            auto batch = program.ApplyProgram(updates.BuildArrow(), columnResolver).DetachResult();

            TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("15", TTypeInfo(NTypeIds::Uint8)) }));
            result.AddRow().Add<ui8>(0);
            result.AddRow().Add<ui8>(1);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(YqlKernelContains) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;

        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("string"));
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("substring"));
            command->MutableAssign()->MutableColumn()->SetId(15);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            prjectionProto->AddColumns()->SetId(15);
        }

        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::StringContains);
            programProto.SetKernels(kernels.Serialize());
            const auto programSerialized = SerializeProgram(programProto);

            TProgramContainer program;
            program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized)
                .Validate();

            TTableUpdatesBuilder updates(
                NArrow::MakeArrowSchema({ { "string", TTypeInfo(NTypeIds::Bytes) }, { "substring", TTypeInfo(NTypeIds::Bytes) } }));
            updates.AddRow().Add<std::string>("Lorem ipsum \xC0 dolor\f sit amet.").Add<std::string>("dolor");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit \amet.").Add<std::string>("amet.");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.").Add<std::string>("\amet.");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit \amet.").Add<std::string>("\amet.");

            auto batch = program.ApplyProgram(updates.BuildArrow(), columnResolver).DetachResult();

            TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("15", TTypeInfo(NTypeIds::Uint8)) }));
            result.AddRow().Add<ui8>(1);
            result.AddRow().Add<ui8>(0);
            result.AddRow().Add<ui8>(0);
            result.AddRow().Add<ui8>(1);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(YqlKernelEquals) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;

        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("i16"));
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("float"));
            command->MutableAssign()->MutableColumn()->SetId(15);
        }
        {
            auto* command = programProto.AddCommand();
            auto* projectionProto = command->MutableProjection();
            projectionProto->AddColumns()->SetId(15);
        }

        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::Equals);
            programProto.SetKernels(kernels.Serialize());
            const auto programSerialized = SerializeProgram(programProto);

            TProgramContainer program;
            program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized)
                .Validate();

            TTableUpdatesBuilder updates(
                NArrow::MakeArrowSchema({ { "i16", TTypeInfo(NTypeIds::Int16) }, { "float", TTypeInfo(NTypeIds::Float) } }));
            updates.AddRow().Add<i16>(-2).Add<float>(-2.f);
            updates.AddRow().Add<i16>(-1).Add<float>(-1.1f);
            updates.AddRow().Add<i16>(0).Add<float>(0.f);
            updates.AddRow().Add<i16>(1).Add<float>(2.f);
            updates.AddRow().Add<i16>(2).Add<float>(2.f);

            Cerr << program.GetChainVerified()->DebugDOT() << Endl;

            auto batch = program.ApplyProgram(updates.BuildArrow(), columnResolver).DetachResult();

            TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("15", TTypeInfo(NTypeIds::Uint8)) }));
            result.AddRow().Add<ui8>(1);
            result.AddRow().Add<ui8>(0);
            result.AddRow().Add<ui8>(1);
            result.AddRow().Add<ui8>(0);
            result.AddRow().Add<ui8>(1);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(Like) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* constantProto = command->MutableAssign()->MutableConstant();
            constantProto->SetBytes("001");
            command->MutableAssign()->MutableColumn()->SetId(15);   // suffix
        }
        {
            auto* command = programProto.AddCommand();
            auto* constantProto = command->MutableAssign()->MutableConstant();
            constantProto->SetBytes("uid");
            command->MutableAssign()->MutableColumn()->SetId(16);   // prefix
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("string"));
            functionProto->AddArguments()->SetId(16);
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_STARTS_WITH);
            command->MutableAssign()->MutableColumn()->SetId(17);   // starts_with
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(1);
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("string"));
            functionProto->AddArguments()->SetId(15);
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_ENDS_WITH);
            command->MutableAssign()->MutableColumn()->SetId(/*"ends_with"*/ 18);
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_SIMPLE_ARROW);
            functionProto->AddArguments()->SetId(/*"start_with"*/ 17);
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_CAST_TO_BOOLEAN);
            command->MutableAssign()->MutableColumn()->SetId(/* "start_with_bool" */ 19);
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_SIMPLE_ARROW);
            functionProto->AddArguments()->SetId(/*"ends_with"*/ 18);
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_CAST_TO_BOOLEAN);
            command->MutableAssign()->MutableColumn()->SetId(/*"ends_with_bool"*/ 20);
        }
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_SIMPLE_ARROW);
            functionProto->AddArguments()->SetId(/*"start_with_bool"*/ 19);
            functionProto->AddArguments()->SetId(/*"ends_with_bool"*/ 20);
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_BINARY_AND);
            command->MutableAssign()->MutableColumn()->SetId(/*"result"*/ 21);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetId(/*"result"*/ 21);
        }

        {
            TKernelsWrapper kernels;
            UNIT_ASSERT_VALUES_EQUAL(kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::StartsWith, true), 0);
            UNIT_ASSERT_VALUES_EQUAL(kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, true), 1);
            programProto.SetKernels(kernels.Serialize());
            const auto programSerialized = SerializeProgram(programProto);

            TProgramContainer program;
            program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized)
                .Validate();

            TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({ { "string", TTypeInfo(NTypeIds::Utf8) } }));
            updates.AddRow().Add<std::string>("uid_3000001");
            updates.AddRow().Add<std::string>("uid_3000003");

            auto batch = program.ApplyProgram(updates.BuildArrow(), columnResolver).DetachResult();

            auto schema = arrow::schema({ arrow::field("21", arrow::boolean()) });
            TTableUpdatesBuilder result(schema);
            result.AddRow().Add<bool>(true);
            result.AddRow().Add<bool>(false);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(SimpleFunction) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        ;
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("uid"));
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_LENGTH);
            command->MutableAssign()->MutableColumn()->SetId(15);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            prjectionProto->AddColumns()->SetId(15);
        }
        const auto programSerialized = SerializeProgram(programProto);

        TProgramContainer program;
        program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized).Validate();

        TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({ std::make_pair("uid", TTypeInfo(NTypeIds::Utf8)) }));
        updates.AddRow().Add("aaa");
        updates.AddRow().Add("b");
        updates.AddRow().Add("");

        auto batch = program.ApplyProgram(updates.BuildArrow(), columnResolver).DetachResult();

        TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("15", TTypeInfo(NTypeIds::Uint64)) }));
        result.AddRow().Add<uint64_t>(3);
        result.AddRow().Add<uint64_t>(1);
        result.AddRow().Add<uint64_t>(0);

        auto expected = result.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
    }

    Y_UNIT_TEST(NumRowsWithNulls) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        TProgramProtoBuilder protoBuilder;
        const ui32 isNullId =
            protoBuilder.AddOperation(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_IS_NULL,
                { columnResolver.GetColumnIdVerified("uid") });
        protoBuilder.AddFilter(isNullId);
        const ui32 countId = protoBuilder.AddAggregation(NArrow::NSSA::NAggregation::EAggregate::Count, {}, {});
        protoBuilder.AddProjection({ countId });
        const auto programSerialized = SerializeProgram(protoBuilder.GetProto());

        TProgramContainer program;
        program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized).Validate();

        TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({ std::make_pair("uid", TTypeInfo(NTypeIds::Utf8)) }));
        updates.AddRow().Add("a");
        updates.AddRow().AddNull();
        updates.AddRow().Add("bbb");
        updates.AddRow().AddNull();
        updates.AddRow().AddNull();

        auto batch = program.ApplyProgram(updates.BuildArrow(), columnResolver).DetachResult();

        TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("10002", TTypeInfo(NTypeIds::Uint64)) }));
        result.AddRow().Add<uint64_t>(3);

        auto expected = result.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
    }

    Y_UNIT_TEST(CountWithNulls) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        TProgramProtoBuilder protoBuilder;
        const ui32 resId =
            protoBuilder.AddAggregation(NArrow::NSSA::NAggregation::EAggregate::Count, { columnResolver.GetColumnIdVerified("uid") }, {});
        protoBuilder.AddProjection({ resId });
        const auto programSerialized = SerializeProgram(protoBuilder.GetProto());

        TProgramContainer program;
        program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized).Validate();

        TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({ std::make_pair("uid", TTypeInfo(NTypeIds::Utf8)) }));
        updates.AddRow().Add("a");
        updates.AddRow().AddNull();
        updates.AddRow().Add("bbb");
        updates.AddRow().AddNull();
        updates.AddRow().AddNull();

        auto batch = program.ApplyProgram(updates.BuildArrow(), columnResolver).DetachResult();

        TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("10001", TTypeInfo(NTypeIds::Uint64)) }));
        result.AddRow().Add<uint64_t>(2);

        auto expected = result.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
    }

    Y_UNIT_TEST(CountUIDByVAT) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        TProgramProtoBuilder protoBuilder;
        const ui32 resId = protoBuilder.AddAggregation(NArrow::NSSA::NAggregation::EAggregate::Count,
            { columnResolver.GetColumnIdVerified("uid") }, { columnResolver.GetColumnIdVerified("vat") });
        protoBuilder.AddProjection({ resId, columnResolver.GetColumnIdVerified("vat") });
        const auto programSerialized = SerializeProgram(protoBuilder.GetProto());

        TProgramContainer program;
        program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized).Validate();

        TTableUpdatesBuilder updates(
            NArrow::MakeArrowSchema({ std::make_pair("uid", TTypeInfo(NTypeIds::Utf8)), std::make_pair("vat", TTypeInfo(NTypeIds::Int32)) }));
        updates.AddRow().Add("a").Add(1);
        updates.AddRow().AddNull().Add(1);
        updates.AddRow().Add("bbb").Add(1);
        updates.AddRow().Add("a").Add(2);
        updates.AddRow().AddNull().Add(2);
        updates.AddRow().AddNull().Add(3);
        updates.AddRow().AddNull().Add(3);

        auto batch = program.ApplyProgram(updates.BuildArrow(), columnResolver).DetachResult();

        TTableUpdatesBuilder result(
            NArrow::MakeArrowSchema({ std::make_pair("10001", TTypeInfo(NTypeIds::Uint64)), std::make_pair("4", TTypeInfo(NTypeIds::Int32)) }));
        result.AddRow().Add<ui64>(0).Add<i32>(3);
        result.AddRow().Add<ui64>(1).Add<i32>(2);
        result.AddRow().Add<ui64>(2).Add<i32>(1);

        auto expected = result.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
    }

    void JsonExistsImpl(const bool isBinaryType) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* constantProto = command->MutableAssign()->MutableConstant();
            constantProto->SetText("$.key");
            command->MutableAssign()->MutableColumn()->SetId(/*"json_path"*/ 15);
        }
        const TString jsonColName = isBinaryType ? "json_binary" : "json_string";
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified(jsonColName));
            functionProto->AddArguments()->SetId(/*"json_path"*/ 15);
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_LENGTH);
            command->MutableAssign()->MutableColumn()->SetId(16);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetId(16);
        }

        TKernelsWrapper kernels;
        kernels.AddJsonExists(isBinaryType);
        programProto.SetKernels(kernels.Serialize());
        const auto programSerialized = SerializeProgram(programProto);

        TProgramContainer program;
        program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized).Validate();

        TTableUpdatesBuilder updates(
            NArrow::MakeArrowSchema({ { jsonColName, TTypeInfo(isBinaryType ? NTypeIds::JsonDocument : NTypeIds::Json) } }));
        NJson::TJsonValue testJson;
        testJson["key"] = "value";
        updates.AddRow().Add<std::string>(testJson.GetStringRobust());
        updates.AddRow().Add<std::string>(NJson::TJsonValue(NJson::JSON_ARRAY).GetStringRobust());

        auto batch = updates.BuildArrow();
        Cerr << batch->ToString() << Endl;

        if (isBinaryType) {
            THashMap<TString, NScheme::TTypeInfo> cc;
            cc[jsonColName] = TTypeInfo(NTypeIds::JsonDocument);
            auto convertResult = NArrow::ConvertColumns(batch, cc);
            UNIT_ASSERT_C(convertResult.ok(), convertResult.status().ToString());
            batch = *convertResult;
            Cerr << batch->ToString() << Endl;
        }
        batch = program.ApplyProgram(batch, columnResolver).DetachResult();

        TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("16", TTypeInfo(NTypeIds::Uint8)) }));
        result.AddRow().Add<ui8>(1);
        result.AddRow().Add<ui8>(0);

        auto expected = result.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
    }

    Y_UNIT_TEST(JsonExists) {
        JsonExistsImpl(false);
    }

    Y_UNIT_TEST(JsonExistsBinary) {
        JsonExistsImpl(true);
    }

    void JsonValueImpl(bool isBinaryType, NYql::EDataSlot resultType) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* constantProto = command->MutableAssign()->MutableConstant();
            constantProto->SetText("$.key");
            command->MutableAssign()->MutableColumn()->SetId(/*"json_path"*/ 15);
        }
        const TString jsonColName = isBinaryType ? "json_binary" : "json_string";
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            command->MutableAssign()->MutableColumn()->SetId(16);
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified(jsonColName));
            functionProto->AddArguments()->SetId(/*"json_path"*/ 15);
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_LENGTH);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetId(16);
        }

        TKernelsWrapper kernels;
        kernels.AddJsonValue(isBinaryType, resultType);
        programProto.SetKernels(kernels.Serialize());
        const auto programSerialized = SerializeProgram(programProto);

        TProgramContainer program;
        program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized).Validate();

        TTableUpdatesBuilder updates(
            NArrow::MakeArrowSchema({ { jsonColName, TTypeInfo(isBinaryType ? NTypeIds::JsonDocument : NTypeIds::Json) } }));
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
        { updates.AddRow().Add<std::string>(NJson::TJsonValue(NJson::JSON_ARRAY).GetStringRobust()); }

        auto batch = updates.BuildArrow();
        Cerr << batch->ToString() << Endl;

        if (isBinaryType) {
            THashMap<TString, NScheme::TTypeInfo> cc;
            cc[jsonColName] = TTypeInfo(NTypeIds::JsonDocument);
            auto convertResult = NArrow::ConvertColumns(batch, cc);
            UNIT_ASSERT_C(convertResult.ok(), convertResult.status().ToString());
            batch = *convertResult;
            Cerr << batch->ToString() << Endl;
        }

        batch = program.ApplyProgram(batch, columnResolver).DetachResult();

        Cerr << "Check output for " << resultType << Endl;
        if (resultType == NYql::EDataSlot::Utf8) {
            TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("16", TTypeInfo(NTypeIds::Utf8)) }));

            result.AddRow().Add<std::string>("value");
            result.AddRow().Add<std::string>("10");
            result.AddRow().Add<std::string>("0.1");
            result.AddRow().Add<std::string>("false");
            result.AddRow().AddNull();
            result.AddRow().AddNull();

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        } else if (resultType == NYql::EDataSlot::Bool) {
            TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("16", TTypeInfo(NTypeIds::Uint8)) }));

            result.AddRow().AddNull();
            result.AddRow().AddNull();
            result.AddRow().AddNull();
            result.AddRow().Add<ui8>(0);
            result.AddRow().AddNull();
            result.AddRow().AddNull();

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        } else if (resultType == NYql::EDataSlot::Int64 || resultType == NYql::EDataSlot::Uint64) {
            TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("16", TTypeInfo(NTypeIds::Int64)) }));

            result.AddRow().AddNull();
            result.AddRow().Add<i64>(10);
            result.AddRow().AddNull();
            result.AddRow().AddNull();
            result.AddRow().AddNull();
            result.AddRow().AddNull();

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        } else if (resultType == NYql::EDataSlot::Double || resultType == NYql::EDataSlot::Float) {
            TTableUpdatesBuilder result(NArrow::MakeArrowSchema({ std::make_pair("16", TTypeInfo(NTypeIds::Double)) }));

            result.AddRow().AddNull();
            result.AddRow().Add<double>(10);
            result.AddRow().Add<double>(0.1);
            result.AddRow().AddNull();
            result.AddRow().AddNull();
            result.AddRow().AddNull();

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        } else {
            Y_ABORT("Not implemented");
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

    // Reports a fixed ngram size so hint derivation does not need a real bloom index.
    class TFixedNgramResolver: public NReader::NCommon::TIndexColumnResolver {
    public:
        using TIndexColumnResolver::TIndexColumnResolver;
        std::optional<ui32> NgramSize;
        std::optional<ui32> MaxNgramSize;

        std::optional<NArrow::NSSA::TBloomNgramSizes> GetBloomNgramSize(const ui32 /*columnId*/, const bool /*caseSensitive*/) const override {
            if (!NgramSize) {
                return std::nullopt;
            }
            return NArrow::NSSA::TBloomNgramSizes{*NgramSize, MaxNgramSize.value_or(*NgramSize)};
        }
    };

    // PatternHint adds bloom checks next to the original kernel. With no index blobs the checks allow
    // every row, so the kernel result is unchanged. No reported ngram size means no checks are built.
    Y_UNIT_TEST(PatternHintBuildsCheckers) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        TFixedNgramResolver columnResolver(indexInfo);
        columnResolver.NgramSize = 3;

        auto makeProgram = [&](const TString& pattern, const bool caseSensitive, const std::optional<ui32> argumentIndex) {
            NKikimrSSA::TProgram programProto;
            {
                auto* command = programProto.AddCommand();
                command->MutableAssign()->MutableConstant()->SetBytes("Lorem");
                command->MutableAssign()->MutableColumn()->SetId(15);
            }
            {
                auto* command = programProto.AddCommand();
                auto* functionProto = command->MutableAssign()->MutableFunction();
                functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
                functionProto->SetKernelIdx(0);
                functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("string"));
                functionProto->AddArguments()->SetId(15);
                if (argumentIndex) {
                    auto* hint = functionProto->MutablePatternHint();
                    hint->SetArgumentIndex(*argumentIndex);
                    hint->MutableLike()->SetPattern(pattern);
                    hint->SetCaseSensitive(caseSensitive);
                }
                command->MutableAssign()->MutableColumn()->SetId(16);
            }
            {
                auto* command = programProto.AddCommand();
                command->MutableProjection()->AddColumns()->SetId(16);
            }
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::StartsWith, true);
            programProto.SetKernels(kernels.Serialize());
            return programProto;
        };

        auto apply = [&](NKikimrSSA::TProgram programProto, const bool hintsEnabled) {
            TProgramContainer program;
            program.SetPatternIndexHintsEnabled(hintsEnabled);
            program
                .Init(
                    columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, SerializeProgram(programProto))
                .Validate();
            TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({ { "string", TTypeInfo(NTypeIds::Utf8) } }));
            updates.AddRow().Add<std::string>("Lorem ipsum");
            updates.AddRow().Add<std::string>("ipsum");
            auto batch = program.ApplyProgram(updates.BuildArrow(), columnResolver).DetachResult();
            return std::make_pair(program.GetChainVerified()->DebugDOT(), batch);
        };

        auto uint8Pair = [](const std::shared_ptr<arrow::RecordBatch>& batch) {
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 2);
            auto column = std::static_pointer_cast<arrow::UInt8Array>(batch->column(0));
            UNIT_ASSERT(column);
            return std::make_pair(column->Value(0), column->Value(1));
        };

        const auto withHints = apply(makeProgram("%aaa%bbb%", true, 0), true);
        UNIT_ASSERT_C(withHints.first.Contains("PATTERN_MATCH"), withHints.first);
        UNIT_ASSERT_C(withHints.first.Contains("aaa") && withHints.first.Contains("bbb"), withHints.first);
        UNIT_ASSERT_VALUES_EQUAL(uint8Pair(withHints.second), (std::pair<ui8, ui8>(1, 0)));
        const auto checkerCount = [&](const TString& dot) {
            ui32 count = 0;
            for (size_t pos = 0; (pos = dot.find("CheckIndexData", pos)) != TString::npos; pos += 14) {
                ++count;
            }
            return count;
        };
        UNIT_ASSERT_VALUES_EQUAL(checkerCount(withHints.first), 2);

        const auto flagOff = apply(makeProgram("%aaa%bbb%", true, 0), false);
        UNIT_ASSERT_C(!flagOff.first.Contains("PATTERN_MATCH"), flagOff.first);
        UNIT_ASSERT_VALUES_EQUAL(uint8Pair(flagOff.second), (std::pair<ui8, ui8>(1, 0)));

        const auto ignored = apply(makeProgram("%aaa%bbb%", true, 5), true);
        UNIT_ASSERT_C(!ignored.first.Contains("PATTERN_MATCH"), ignored.first);
        UNIT_ASSERT_VALUES_EQUAL(uint8Pair(ignored.second), (std::pair<ui8, ui8>(1, 0)));

        const auto ilike = apply(makeProgram("%Err%Ошибка%", false, 0), true);
        UNIT_ASSERT_C(ilike.first.Contains("PATTERN_MATCH"), ilike.first);
        UNIT_ASSERT_C(ilike.first.Contains("Err"), ilike.first);
        UNIT_ASSERT_C(!ilike.first.Contains("Ошибка"), ilike.first);
        UNIT_ASSERT_VALUES_EQUAL(checkerCount(ilike.first), 1);
        UNIT_ASSERT_VALUES_EQUAL(uint8Pair(ilike.second), (std::pair<ui8, ui8>(1, 0)));

        columnResolver.NgramSize.reset();
        const auto noIndex = apply(makeProgram("%aaa%bbb%", true, 0), true);
        UNIT_ASSERT_C(!noIndex.first.Contains("PATTERN_MATCH"), noIndex.first);
        UNIT_ASSERT_VALUES_EQUAL(uint8Pair(noIndex.second), (std::pair<ui8, ui8>(1, 0)));

        columnResolver.NgramSize = 3;
        const auto noField = apply(makeProgram("%aaa%bbb%", true, std::nullopt), true);
        UNIT_ASSERT_C(!noField.first.Contains("PATTERN_MATCH"), noField.first);
        UNIT_ASSERT_VALUES_EQUAL(uint8Pair(noField.second), (std::pair<ui8, ui8>(1, 0)));

        // EndsWith("b") does not match the stored suffix window, so only the "aaa" check is built.
        const auto shortSuffix = apply(makeProgram("%aaa%b", true, 0), true);
        UNIT_ASSERT_C(shortSuffix.first.Contains("PATTERN_MATCH"), shortSuffix.first);
        UNIT_ASSERT_VALUES_EQUAL(checkerCount(shortSuffix.first), 1);
        UNIT_ASSERT_VALUES_EQUAL(uint8Pair(shortSuffix.second), (std::pair<ui8, ui8>(1, 0)));
    }
}
