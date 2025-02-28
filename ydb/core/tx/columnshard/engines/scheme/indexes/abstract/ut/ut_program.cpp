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

        TProgramProtoBuilder builder;
        const ui32 likeStringId = builder.AddConstant("amet.");
        const ui32 filterId = builder.AddOperation(
            NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, { columnResolver.GetColumnIdVerified("string"), likeStringId });
        builder.AddFilter(filterId);

        {
            TProgramContainer program;
            program.Init(columnResolver, builder.FinishProto()).Validate();
            auto coverage = NOlap::NIndexes::NRequest::TDataForIndexesCheckers::Build(program);
            AFL_VERIFY(coverage);
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("coverage", coverage->DebugString());
            AFL_VERIFY(coverage->GetBranches().size() == 1)("coverage", coverage->DebugString());
            AFL_VERIFY(coverage->GetBranches().front()->DebugString() == R"({"likes":{"7":{"sequences":["%amet."]}}})")(
                "coverage", coverage->DebugString());
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
            program.Init(columnResolver, builder.FinishProto()).Validate();
            auto coverage = NOlap::NIndexes::NRequest::TDataForIndexesCheckers::Build(program);
            AFL_VERIFY(coverage);
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("coverage", coverage->DebugString());
            AFL_VERIFY(coverage->GetBranches().size() == 2);
            AFL_VERIFY(coverage->GetBranches().front()->DebugString() == R"({"likes":{"7":{"sequences":["%like_string"]}}})");
            AFL_VERIFY(coverage->GetBranches().back()->DebugString() == R"({"equals":{"7":"equals_string"}})");
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
            program.Init(columnResolver, builder.FinishProto()).Validate();
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

    Y_UNIT_TEST(OrConditionsSimple2) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        TProgramProtoBuilder builder;
        const auto idLikeString = builder.AddConstant("like_string");
        const auto idEqualString = builder.AddConstant("equals_string");
        const auto idColumn1 = columnResolver.GetColumnIdVerified("string");
        const auto idEndsWith1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, { idColumn1, idLikeString });
        const auto idEquals1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Equals, { idColumn1, idEqualString });
        const auto idFilter1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Or, { idEndsWith1, idEquals1 });
        const auto idColumn2 = columnResolver.GetColumnIdVerified("substring");
        const auto idEndsWith2 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, { idColumn2, idLikeString });
        const auto idEquals2 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Equals, { idColumn2, idEqualString });
        const auto idFilter2 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Or, { idEndsWith2, idEquals2 });
        const auto idFilter3 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::And, { idFilter1, idFilter2 });
        builder.AddFilter(idFilter3);
        {
            TProgramContainer program;
            program.Init(columnResolver, builder.FinishProto()).Validate();
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

    Y_UNIT_TEST(OrConditionsSimple3) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        TProgramProtoBuilder builder;
        const auto idLikeString = builder.AddConstant("like_string");
        const auto idEqualString = builder.AddConstant("equals_string");
        const auto idColumn1 = columnResolver.GetColumnIdVerified("string");
        const auto idEndsWith1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, { idColumn1, idLikeString });
        const auto idEquals1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Equals, { idColumn1, idEqualString });
        const auto idFilter1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::And, { idEndsWith1, idEquals1 });
        builder.AddFilter(idFilter1);
        const auto idColumn2 = columnResolver.GetColumnIdVerified("substring");
        const auto idEndsWith2 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, { idColumn2, idLikeString });
        const auto idEquals2 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Equals, { idColumn2, idEqualString });
        const auto idFilter2 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::And, { idEndsWith2, idEquals2 });
        builder.AddFilter(idFilter2);
        {
            TProgramContainer program;
            program.Init(columnResolver, builder.FinishProto()).Validate();
            auto coverage = NOlap::NIndexes::NRequest::TDataForIndexesCheckers::Build(program);
            AFL_VERIFY(coverage);
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("coverage", coverage->DebugString());
            AFL_VERIFY(coverage->GetBranches().size() == 1);
            AFL_VERIFY(coverage->GetBranches()[0]->DebugString() = R"({"likes":{"9":{"sequences":["%like_string"]},"7":{"sequences":["%like_string"]}},"equals":{"9":"equals_string","7":"equals_string"}})");
        }
    }

    Y_UNIT_TEST(JsonSubColumnUsage) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        NReader::NCommon::TIndexColumnResolver columnResolver(indexInfo);

        TProgramProtoBuilder builder;
        const auto idPathString1 = builder.AddConstant("json.path1");
        const auto idPathString2 = builder.AddConstant("json.path2");
        const auto idColumn = columnResolver.GetColumnIdVerified("json_binary");
        const auto idJsonValue1 = builder.AddOperation("JsonValue", { idColumn, idPathString1 });
        const auto idJsonValue2 = builder.AddOperation("JsonValue", { idColumn, idPathString2 });

        const auto idLikeString = builder.AddConstant("like_string");
        const auto idEqualString = builder.AddConstant("equals_string");
        const auto idEndsWith1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, { idJsonValue1, idLikeString });
        const auto idEquals1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::Equals, { idJsonValue2, idEqualString });
        const auto idFilter1 = builder.AddOperation(NYql::TKernelRequestBuilder::EBinaryOp::And, { idEndsWith1, idEquals1 });
        builder.AddFilter(idFilter1);
        {
            TProgramContainer program;
            program.Init(columnResolver, builder.FinishProto()).Validate();
            auto coverage = NOlap::NIndexes::NRequest::TDataForIndexesCheckers::Build(program);
            AFL_VERIFY(coverage);
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("coverage", coverage->DebugString());
            AFL_VERIFY(coverage->GetBranches().size() == 1);
            AFL_VERIFY(coverage->GetBranches().front()->DebugString() == R"({"likes":{"{cId=6;sub=json.path1}":{"sequences":["%like_string"]}},"equals":{"{cId=6;sub=json.path2}":"equals_string"}})");
        }
    }
}
