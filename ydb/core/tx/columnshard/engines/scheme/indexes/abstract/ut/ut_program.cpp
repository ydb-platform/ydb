#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/resolver.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/coverage.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>
#include <ydb/core/tx/columnshard/test_helper/kernels_wrapper.h>
#include <ydb/core/tx/columnshard/test_helper/program_constructor.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/core/arrow_kernels/request/request.h>

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

Y_UNIT_TEST_SUITE(TestProgramBloomCoverage) {
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
            functionProto->SetYqlOperationId((ui32)NYql::TKernelRequestBuilder::EBinaryOp::EndsWith);
            functionProto->AddArguments()->SetId(columnResolver.GetColumnIdVerified("string"));
            functionProto->AddArguments()->SetId(15);
            command->MutableAssign()->MutableColumn()->SetId(16);
        }
        {
            auto* command = programProto.AddCommand();
            command->MutableFilter()->MutablePredicate()->SetId(16);
        }

        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, true);
            programProto.SetKernels(kernels.Serialize());
            TProgramContainer program;
            program.Init(columnResolver, programProto).Validate();
            auto coverage = NOlap::NIndexes::NRequest::TDataForIndexesCheckers::Build(program);
            AFL_VERIFY(coverage);
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("coverage", coverage->DebugString());
            AFL_VERIFY(coverage->GetBranches().size() == 1)("coverage", coverage->DebugString());
            AFL_VERIFY(coverage->GetBranches().front()->GetEquals().size() == 0)("coverage", coverage->DebugString());
            AFL_VERIFY(coverage->GetBranches().front()->GetLikes().size() == 1)("coverage", coverage->DebugString());
            AFL_VERIFY(coverage->GetBranches().front()->GetLikes().begin()->second.ToString() == "[%amet.];")("coverage", coverage->DebugString());
        }
    }

    Y_UNIT_TEST(OrConditionsSimple0) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        TProgramProtoBuilder builder;
        const auto idLikeString = builder.AddConstant("like_string");
        const auto idEqualString = builder.AddConstant("equals_string");
        const auto idColumn = columnResolver.GetColumnIdVerified("string");
        const auto idEndsWith = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, { idColumn, idLikeString });
        const auto idEquals = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Equals, { idColumn, idEqualString });
        const auto idFilter1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Or, { idEndsWith, idEquals });
        builder.AddFilter(idFilter1);
        {
            TProgramContainer program;
            program.Init(columnResolver, builder.GetProto()).Validate();
            auto coverage = NOlap::NIndexes::NRequest::TDataForIndexesCheckers::Build(program);
            AFL_VERIFY(coverage);
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("coverage", coverage->DebugString());
            AFL_VERIFY(coverage->GetBranches().size() == 2);
            AFL_VERIFY(coverage->GetBranches().front()->GetEquals().size() == 0);
            AFL_VERIFY(coverage->GetBranches().front()->GetLikes().size() == 1);
            AFL_VERIFY(coverage->GetBranches().front()->GetLikes().begin()->second.ToString() == "[%like_string];");

            AFL_VERIFY(coverage->GetBranches().back()->GetEquals().size() == 1);
            AFL_VERIFY(coverage->GetBranches().back()->GetLikes().size() == 0);
            AFL_VERIFY(coverage->GetBranches().back()->GetEquals().begin()->second->ToString() == "equals_string");
        }
    }

    Y_UNIT_TEST(OrConditionsSimple1) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        TProgramProtoBuilder builder;
        const auto idLikeString = builder.AddConstant("like_string");
        const auto idEqualString = builder.AddConstant("equals_string");
        const auto idColumn1 = columnResolver.GetColumnIdVerified("string");
        const auto idEndsWith1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, { idColumn1, idLikeString });
        const auto idEquals1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Equals, { idColumn1, idEqualString });
        const auto idFilter1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Or, { idEndsWith1, idEquals1 });
        builder.AddFilter(idFilter1);
        const auto idColumn2 = columnResolver.GetColumnIdVerified("substring");
        const auto idEndsWith2 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, { idColumn2, idLikeString });
        const auto idEquals2 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Equals, { idColumn2, idEqualString });
        const auto idFilter2 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Or, { idEndsWith2, idEquals2 });
        builder.AddFilter(idFilter2);
        {
            TProgramContainer program;
            program.Init(columnResolver, builder.GetProto()).Validate();
            auto coverage = NOlap::NIndexes::NRequest::TDataForIndexesCheckers::Build(program);
            AFL_VERIFY(coverage);
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("coverage", coverage->DebugString());
            AFL_VERIFY(coverage->GetBranches().size() == 4);
            AFL_VERIFY(coverage->GetBranches()[0]->DebugString() = R"("{"likes":{"9":{"sequences":["%like_string"]},"7":{"sequences":["%like_string"]}}}")");
            AFL_VERIFY(coverage->GetBranches()[1]->DebugString() = R"({"likes":{"7":{"sequences":["%like_string"]}},"equals":{"9":"equals_string"}})");
            AFL_VERIFY(coverage->GetBranches()[2]->DebugString() = R"({"likes":{"9":{"sequences":["%like_string"]}},"equals":{"7":"equals_string"}})");
            AFL_VERIFY(coverage->GetBranches()[3]->DebugString() = R"({"equals":{"9":"equals_string","7":"equals_string"}})");
        }
    }
}
