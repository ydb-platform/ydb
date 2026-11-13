#include <ydb/core/tx/datashard/build_index/kmeans_helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDataShard::NKMeans {

Y_UNIT_TEST_SUITE(KMeansUploadLayout) {
    Y_UNIT_TEST(PrimaryKeyEmbeddingIsNotDuplicatedInBuildValues) {
        for (const auto state : {NKikimrTxDataShard::UPLOAD_MAIN_TO_BUILD,
                NKikimrTxDataShard::UPLOAD_BUILD_TO_BUILD,
                NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING,
                NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING}) {
            const bool fromBuild = state == NKikimrTxDataShard::UPLOAD_BUILD_TO_BUILD
                || state == NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING;
            const bool toBuild = state == NKikimrTxDataShard::UPLOAD_MAIN_TO_BUILD
                || state == NKikimrTxDataShard::UPLOAD_BUILD_TO_BUILD;
            TUserTable table;
            table.Columns.emplace(1, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "", "key", true));
            table.Columns.emplace(2, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "embedding", true));
            table.Columns.emplace(3, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "payload"));
            table.KeyColumnIds = {1, 2};
            if (fromBuild) {
                table.Columns.emplace(4, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "", NTableIndex::NKMeans::ParentColumn, true));
                table.KeyColumnIds.insert(table.KeyColumnIds.begin(), 4);
            }
            google::protobuf::RepeatedPtrField<TProtoStringType> data;
            *data.Add() = "payload";
            google::protobuf::RepeatedPtrField<TProtoStringType> pk;
            *pk.Add() = "key";
            *pk.Add() = "embedding";
            NTable::TPos embeddingPos = 0, dataPos = 0;
            const auto tags = MakeScanTags(table, "embedding", data, toBuild, embeddingPos, dataPos);
            UNIT_ASSERT_VALUES_EQUAL(tags.at(embeddingPos), 2u);
            UNIT_ASSERT_VALUES_EQUAL(tags.size() - dataPos, 1u);
            const auto types = MakeOutputTypes(table, state, "embedding", data, pk);
            // Parent, key and embedding are key cells; only payload is a value.
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 3u + tags.size() - dataPos);
            UNIT_ASSERT_VALUES_EQUAL(types->at(2).first, "embedding");
            UNIT_ASSERT_VALUES_EQUAL(types->at(3).first, "payload");
        }
    }
}

} // namespace NKikimr::NDataShard::NKMeans
