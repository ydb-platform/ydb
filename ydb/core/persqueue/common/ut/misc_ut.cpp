#include <ydb/core/persqueue/common/blob_refcounter.h>
#include <ydb/core/persqueue/common/metering.h>
#include <ydb/core/persqueue/common/sourceid_info.h>
#include <ydb/core/persqueue/common/write_stats.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/serialized_enum.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TCommonMiscTest) {

Y_UNIT_TEST(BlobKeyTokens) {
    TBlobKeyTokens tokens;
    UNIT_ASSERT_VALUES_EQUAL(tokens.Size(), 0u);

    auto token = std::make_shared<TBlobKeyToken>();
    token->Key = "blob-key";
    token->NeedDelete = false;
    tokens.Append(std::move(token));
    UNIT_ASSERT_VALUES_EQUAL(tokens.Size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(tokens.Tokens[0]->Key, "blob-key");
    UNIT_ASSERT(!tokens.Tokens[0]->NeedDelete);

    TBlobKeyTokenCreator creator = [](const TString& key) {
        return std::make_shared<TBlobKeyToken>(TBlobKeyToken{.Key = key, .NeedDelete = true});
    };
    tokens.Append(creator("k2"));
    UNIT_ASSERT_VALUES_EQUAL(tokens.Size(), 2u);
    UNIT_ASSERT(tokens.Tokens[1]->NeedDelete);
}

Y_UNIT_TEST(MeteringJsonValues) {
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(EMeteringJson::PutEventsV1), 1);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(EMeteringJson::ResourcesReservedV1), 2);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(EMeteringJson::ThroughputV1), 3);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(EMeteringJson::ReadThroughputV1), 4);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(EMeteringJson::StorageV1), 5);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(EMeteringJson::UsedStorageV1), 6);
}

Y_UNIT_TEST(WriteStatsHoldsManagers) {
    TWriteStats stats;
    stats.PerSourceMetrics.resize(2);
    stats.PerSourceMetrics[0].emplace_back("src", 7);
    stats.PartitioningKeysManagers.push_back(
        std::make_unique<TPartitioningKeysManager>(1, TDuration::Seconds(1)));
    UNIT_ASSERT_VALUES_EQUAL(stats.PerSourceMetrics[0].front().second, 7u);
    UNIT_ASSERT(stats.PartitioningKeysManagers[0] != nullptr);
}

Y_UNIT_TEST(SourceIdStateEnumSerialization) {
    for (auto state : GetEnumAllValues<TSourceIdInfo::EState>()) {
        const TString name = ToString(state);
        UNIT_ASSERT(!name.empty());
        TSourceIdInfo::EState parsed = TSourceIdInfo::EState::Unknown;
        UNIT_ASSERT(TryFromString(name, parsed));
        UNIT_ASSERT_EQUAL(parsed, state);
    }
}

} // Y_UNIT_TEST_SUITE(TCommonMiscTest)

} // namespace NKikimr::NPQ
