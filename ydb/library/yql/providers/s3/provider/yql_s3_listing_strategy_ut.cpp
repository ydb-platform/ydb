#include "yql_s3_listing_strategy.cpp"

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

class TMockS3Lister : public NS3Lister::IS3Lister {
public:
    explicit TMockS3Lister(std::vector<NS3Lister::TListResult> batches)
        : Batches(std::move(batches)) { }

    TFuture<NS3Lister::TListResult> Next() override {
        Y_ENSURE(!Consumed);
        Consumed = true;
        return MakeFuture(Batches[Step]);
    }
    bool HasNext() override {
        Y_ENSURE(Consumed);
        ++Step;
        Consumed = false;
        return static_cast<ui16>(Step) < Batches.size();
    }

private:
    std::vector<NS3Lister::TListResult> Batches;
    i32 Step = -1;
    bool Consumed = true;
};

class TMockS3ExceptionLister : public NS3Lister::IS3Lister {
public:
    explicit TMockS3ExceptionLister(TString exceptionMessage)
        : ExceptionMessage(std::move(exceptionMessage)) { }

    TFuture<NS3Lister::TListResult> Next() override {
        Y_ENSURE(!Consumed);
        Consumed = true;
        auto promise = NewPromise<NS3Lister::TListResult>();
        promise.SetException(ExceptionMessage);
        return promise;
    }
    bool HasNext() override { return !Consumed; }

private:
    TString ExceptionMessage;
    bool Consumed = false;
};

Y_UNIT_TEST_SUITE(TCollectingS3ListingStrategyTests) {

void UnitAssertListResultEquals(
    const NS3Lister::TListResult& expected, const NS3Lister::TListResult& actual) {
    UNIT_ASSERT_VALUES_EQUAL(expected.index(), actual.index());
    if (std::holds_alternative<TListError>(expected)) {
        const auto& expectedError = std::get<TListError>(expected);
        const auto& actualError = std::get<TListError>(actual);
        UNIT_ASSERT_VALUES_EQUAL(expectedError.Type, actualError.Type);
        UNIT_ASSERT_VALUES_EQUAL(expectedError.Issues.Size(), actualError.Issues.Size());
        UNIT_ASSERT_VALUES_EQUAL(
            expectedError.Issues.ToOneLineString(), actualError.Issues.ToOneLineString());
        return;
    }

    const auto& expectedEntries = std::get<NS3Lister::TListEntries>(expected);
    const auto& actualEntries = std::get<NS3Lister::TListEntries>(actual);

    UNIT_ASSERT_VALUES_EQUAL(expectedEntries.Objects.size(), actualEntries.Objects.size());
    for (size_t i = 0; i < expectedEntries.Objects.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(
            expectedEntries.Objects[i].Path, actualEntries.Objects[i].Path);
        UNIT_ASSERT_VALUES_EQUAL(
            expectedEntries.Objects[i].Size, actualEntries.Objects[i].Size);
        UNIT_ASSERT_VALUES_EQUAL(
            expectedEntries.Objects[i].MatchedGlobs.size(),
            actualEntries.Objects[i].MatchedGlobs.size());
        for (size_t j = 0; j < expectedEntries.Objects[i].MatchedGlobs.size(); ++j) {
            UNIT_ASSERT_VALUES_EQUAL(
                expectedEntries.Objects[i].MatchedGlobs[j],
                actualEntries.Objects[i].MatchedGlobs[j]);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(
        expectedEntries.Directories.size(), actualEntries.Directories.size());
    for (size_t i = 0; i < expectedEntries.Directories.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(
            expectedEntries.Directories[i].Path, actualEntries.Directories[i].Path);
        UNIT_ASSERT_VALUES_EQUAL(
            expectedEntries.Directories[i].MatchedGlobs.size(),
            actualEntries.Directories[i].MatchedGlobs.size());
        for (size_t j = 0; j < expectedEntries.Directories[i].MatchedGlobs.size(); ++j) {
            UNIT_ASSERT_VALUES_EQUAL(
                expectedEntries.Directories[i].MatchedGlobs[j],
                actualEntries.Directories[i].MatchedGlobs[j]);
        }
    }
}

Y_UNIT_TEST(IfNoIssuesOccursShouldReturnCollectedPaths) {
    auto strategy = TCollectingS3ListingStrategy{
        [](const NS3Lister::TListingRequest& listingRequest, TS3ListingOptions options) {
            UNIT_ASSERT_VALUES_EQUAL(listingRequest.Prefix, "TEST_INPUT");
            UNIT_ASSERT_VALUES_EQUAL(options.IsPartitionedDataset, false);
            UNIT_ASSERT_VALUES_EQUAL(options.IsConcurrentListing, false);
            return MakeFuture(std::static_pointer_cast<NS3Lister::IS3Lister>(
                std::make_shared<TMockS3Lister>(std::vector<NS3Lister::TListResult>{
                    NS3Lister::TListEntries{
                        .Objects =
                            std::vector<NS3Lister::TObjectListEntry>{NS3Lister::TObjectListEntry{
                                .Path = "a/a",
                                .Size = 10,
                            }}},
                    NS3Lister::TListEntries{
                        .Objects = std::vector<NS3Lister::TObjectListEntry>{
                            NS3Lister::TObjectListEntry{
                                .Path = "a/b",
                                .Size = 15,
                            }}}})));
        },
        "TTest"};

    auto actualResultFuture = strategy.List(
        NS3Lister::TListingRequest{.Prefix = "TEST_INPUT"}, TS3ListingOptions{.MaxResultSet = 10});
    auto expectedResult = NS3Lister::TListResult{NS3Lister::TListEntries{
        .Objects = std::vector<NS3Lister::TObjectListEntry>{
            NS3Lister::TObjectListEntry{
                .Path = "a/a",
                .Size = 10,
            },
            NS3Lister::TObjectListEntry{
                .Path = "a/b",
                .Size = 15,
            }}}};
    const auto& actualResult = actualResultFuture.GetValue();
    UnitAssertListResultEquals(expectedResult, actualResult);
}

Y_UNIT_TEST(IfThereAreMoreRecordsThanSpecifiedByLimitShouldReturnError) {
    auto strategy = TCollectingS3ListingStrategy{
        [](const NS3Lister::TListingRequest& listingRequest, TS3ListingOptions options) {
            UNIT_ASSERT_VALUES_EQUAL(listingRequest.Prefix, "TEST_INPUT");
            UNIT_ASSERT_VALUES_EQUAL(options.IsPartitionedDataset, false);
            UNIT_ASSERT_VALUES_EQUAL(options.IsConcurrentListing, false);
            return MakeFuture(std::static_pointer_cast<NS3Lister::IS3Lister>(
                std::make_shared<TMockS3Lister>(std::vector<NS3Lister::TListResult>{
                    NS3Lister::TListEntries{
                        .Objects =
                            std::vector<NS3Lister::TObjectListEntry>{NS3Lister::TObjectListEntry{
                                .Path = "a/a",
                                .Size = 10,
                            }}},
                    NS3Lister::TListEntries{
                        .Objects = std::vector<NS3Lister::TObjectListEntry>{
                            NS3Lister::TObjectListEntry{
                                .Path = "a/b",
                                .Size = 15,
                            }}}})));
        },
        "TTest"};

    auto actualResultFuture = strategy.List(
        NS3Lister::TListingRequest{.Prefix = "TEST_INPUT"}, TS3ListingOptions{.MaxResultSet = 1});
    auto expectedResult = NS3Lister::TListResult{MakeLimitExceededError("TTest", 1, 2, 0)};
    const auto& actualResult = actualResultFuture.GetValue();
    UnitAssertListResultEquals(expectedResult, actualResult);
}

Y_UNIT_TEST(IfAnyIterationReturnIssueThanWholeStrategyShouldReturnIt) {
    auto strategy = TCollectingS3ListingStrategy{
        [](const NS3Lister::TListingRequest& listingRequest, TS3ListingOptions options) {
            UNIT_ASSERT_VALUES_EQUAL(listingRequest.Prefix, "TEST_INPUT");
            UNIT_ASSERT_VALUES_EQUAL(options.IsPartitionedDataset, false);
            UNIT_ASSERT_VALUES_EQUAL(options.IsConcurrentListing, false);
            return MakeFuture(std::static_pointer_cast<NS3Lister::IS3Lister>(
                std::make_shared<TMockS3Lister>(std::vector<NS3Lister::TListResult>{
                    NS3Lister::TListEntries{
                        .Objects =
                            std::vector<NS3Lister::TObjectListEntry>{NS3Lister::TObjectListEntry{
                                .Path = "a/a",
                                .Size = 10,
                            }}},
                    MakeGenericError("TEST_ISSUE")})));
        },
        "TTest"};

    auto actualResultFuture = strategy.List(
        NS3Lister::TListingRequest{.Prefix = "TEST_INPUT"}, TS3ListingOptions{.MaxResultSet = 1});
    auto expectedResult = NS3Lister::TListResult{MakeGenericError("TEST_ISSUE")};
    const auto& actualResult = actualResultFuture.GetValue();
    UnitAssertListResultEquals(expectedResult, actualResult);
}

Y_UNIT_TEST(IfExceptionIsReturnedFromIteratorThanItShouldCovertItToIssue) {
    auto strategy = TCollectingS3ListingStrategy{
        [](const NS3Lister::TListingRequest& listingRequest, TS3ListingOptions options) {
            UNIT_ASSERT_VALUES_EQUAL(listingRequest.Prefix, "TEST_INPUT");
            UNIT_ASSERT_VALUES_EQUAL(options.IsPartitionedDataset, false);
            UNIT_ASSERT_VALUES_EQUAL(options.IsConcurrentListing, false);
            return MakeFuture(std::static_pointer_cast<NS3Lister::IS3Lister>(
                std::make_shared<TMockS3ExceptionLister>("EXCEPTION MESSAGE")));
        },
        "TTest"};

    auto actualResultFuture = strategy.List(
        NS3Lister::TListingRequest{.Prefix = "TEST_INPUT"}, TS3ListingOptions{.MaxResultSet = 10});
    UNIT_ASSERT(actualResultFuture.HasValue());
    auto expectedResult = NS3Lister::TListResult{MakeGenericError("EXCEPTION MESSAGE")};
    const auto& actualResult = actualResultFuture.GetValue();
    UnitAssertListResultEquals(expectedResult, actualResult);
}

} // Y_UNIT_TEST_SUITE(TPathTests2)

} // namespace NYql
