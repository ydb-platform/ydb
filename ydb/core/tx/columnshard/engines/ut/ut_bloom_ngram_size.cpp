#include <ydb/core/local_indexes/bloom/const.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom_ngramm/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NTest {

namespace {

constexpr ui32 ValueColumnId = 2;

NIndexes::TIndexMetaContainer MakeBloom(const ui32 indexId, const TString& name, const ui32 ngramSize, const bool caseSensitive) {
    NLocalIndex::NBloom::TRequestSettings request;
    request.NGrammSize = ngramSize;
    request.CaseSensitive = caseSensitive;
    request.FalsePositiveProbability = 0.01;
    return NIndexes::TIndexMetaContainer(std::make_shared<NIndexes::NBloomNGramm::TIndexMeta>(indexId, name, IStoragesManager::DefaultStorageId,
        false, ValueColumnId, NIndexes::TReadDataExtractorContainer(std::make_shared<NIndexes::TDefaultDataExtractor>()),
        NIndexes::IBitsStorageConstructor::GetDefault(), request));
}

TIndexInfo MakeIndexInfo() {
    NKikimrSchemeOp::TColumnTableSchema proto;
    const std::vector<NArrow::NTest::TTestColumn> columns = {
        NArrow::NTest::TTestColumn("pk", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)),
        NArrow::NTest::TTestColumn("value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)),
    };
    *proto.MutableColumns()->Add() = columns[0].CreateColumn(1);
    *proto.MutableColumns()->Add() = columns[1].CreateColumn(ValueColumnId);
    proto.AddKeyColumnNames("pk");
    proto.SetVersion(1);
    // Case-sensitive size 3 and case-insensitive size 5. A case-sensitive probe can use both.
    *proto.AddIndexes() = MakeBloom(1001, "ngram_cs", 3, true).SerializeToProto();
    *proto.AddIndexes() = MakeBloom(1002, "ngram_ci", 5, false).SerializeToProto();

    auto indexInfo = TIndexInfo::BuildFromProto(1, proto, TTestStoragesManager::GetInstance(), std::make_shared<TSchemaObjectsCache>());
    UNIT_ASSERT(indexInfo);
    return std::move(*indexInfo);
}

}   // namespace

Y_UNIT_TEST_SUITE(BloomNgramSize) {
    Y_UNIT_TEST(SmallestCompatibleSize) {
        const auto indexInfo = MakeIndexInfo();
        const auto caseSensitive = indexInfo.GetBloomNgramSize(ValueColumnId, true);
        UNIT_ASSERT(caseSensitive);
        UNIT_ASSERT_VALUES_EQUAL(*caseSensitive, 3);
        // ILIKE is not served by the case-sensitive index, so only the size-5 index remains.
        const auto caseInsensitive = indexInfo.GetBloomNgramSize(ValueColumnId, false);
        UNIT_ASSERT(caseInsensitive);
        UNIT_ASSERT_VALUES_EQUAL(*caseInsensitive, 5);
        UNIT_ASSERT(!indexInfo.GetBloomNgramSize(1, true));
    }
}

}   // namespace NKikimr::NOlap::NTest
