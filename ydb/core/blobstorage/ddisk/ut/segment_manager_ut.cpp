#include <ydb/core/blobstorage/ddisk/segment_manager.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <tuple>
#include <vector>

#include <util/generic/ylimits.h>


namespace NKikimr::NDDisk {

namespace {

using TSegment = TSegmentManager::TSegment;
using TOutdatedRequest = TSegmentManager::TOutdatedRequest;

std::vector<TSegment> SortSegments(std::vector<TSegment> segments) {
    std::sort(segments.begin(), segments.end());
    return segments;
}

std::vector<std::tuple<ui64, ui64>> NormalizeOutdated(const std::vector<TOutdatedRequest>& outdated) {
    std::vector<std::tuple<ui64, ui64>> normalized;
    normalized.reserve(outdated.size());
    for (const auto& item : outdated) {
        normalized.emplace_back(item.SyncIndex, item.RequestId);
    }
    std::sort(normalized.begin(), normalized.end());
    return normalized;
}

void AssertSegmentsEqual(std::vector<TSegment> actual, std::vector<TSegment> expected) {
    actual = SortSegments(std::move(actual));
    expected = SortSegments(std::move(expected));

    UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(actual[i]), std::get<0>(expected[i]));
        UNIT_ASSERT_VALUES_EQUAL(std::get<1>(actual[i]), std::get<1>(expected[i]));
    }
}

void AssertOutdatedEqual(const std::vector<TOutdatedRequest>& actual,
        std::vector<std::tuple<ui64, ui64>> expected) {
    auto normalized = NormalizeOutdated(actual);
    std::sort(expected.begin(), expected.end());

    UNIT_ASSERT_VALUES_EQUAL(normalized.size(), expected.size());
    for (size_t i = 0; i < normalized.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(normalized[i]), std::get<0>(expected[i]));
        UNIT_ASSERT_VALUES_EQUAL(std::get<1>(normalized[i]), std::get<1>(expected[i]));
    }
}

void AssertPop(TSegmentManager& manager, ui64 requestId, ui64 expectedSyncId, std::vector<TSegment> expectedSegments) {
    std::vector<TSegment> actualSegments;
    ui64 actualSyncId = manager.GetSync(requestId);
    manager.PopRequest(requestId, &actualSegments);

    UNIT_ASSERT_VALUES_EQUAL(actualSyncId, expectedSyncId);
    AssertSegmentsEqual(std::move(actualSegments), std::move(expectedSegments));
}

} // namespace

Y_UNIT_TEST_SUITE(TSegmentManagerTest) {
    Y_UNIT_TEST(PopUnknownRequestReturnsMaxSyncId) {
        TSegmentManager manager;
        std::vector<TSegment> segments;
        ui64 syncId = manager.GetSync(42);

        manager.PopRequest(42, &segments);

        UNIT_ASSERT(segments.empty());
        UNIT_ASSERT_VALUES_EQUAL(syncId, Max<ui64>());
    }

    Y_UNIT_TEST(PushPopSingleRequest) {
        TSegmentManager manager;
        std::vector<TOutdatedRequest> outdated;
        ui64 requestId = 0;

        manager.PushRequest(1, 10, TSegment{10, 20}, &requestId, &outdated);

        UNIT_ASSERT(outdated.empty());
        AssertPop(manager, requestId, 10, std::vector<TSegment>{{10, 20}});
    }

    Y_UNIT_TEST(AdjacentRangesDoNotOverlap) {
        TSegmentManager manager;
        std::vector<TOutdatedRequest> outdated;
        ui64 requestId1 = 0;
        ui64 requestId2 = 0;

        manager.PushRequest(1, 100, TSegment{0, 10}, &requestId1, &outdated);
        UNIT_ASSERT(outdated.empty());

        outdated.clear();
        manager.PushRequest(1, 101, TSegment{10, 20}, &requestId2, &outdated);
        UNIT_ASSERT(outdated.empty());

        AssertPop(manager, requestId1, 100, std::vector<TSegment>{{0, 10}});
        AssertPop(manager, requestId2, 101, std::vector<TSegment>{{10, 20}});
    }

    Y_UNIT_TEST(MiddleOverlapSplitsExistingRequest) {
        TSegmentManager manager;
        std::vector<TOutdatedRequest> outdated;
        ui64 requestId1 = 0;
        ui64 requestId2 = 0;

        manager.PushRequest(1, 10, TSegment{10, 20}, &requestId1, &outdated);
        UNIT_ASSERT(outdated.empty());

        outdated.clear();
        manager.PushRequest(1, 11, TSegment{12, 14}, &requestId2, &outdated);
        UNIT_ASSERT(outdated.empty());

        AssertPop(manager, requestId1, 10, std::vector<TSegment>{{10, 12}, {14, 20}});
        AssertPop(manager, requestId2, 11, std::vector<TSegment>{{12, 14}});
    }

    Y_UNIT_TEST(FullCoverProducesOutdatedAndEmptyPop) {
        TSegmentManager manager;
        std::vector<TOutdatedRequest> outdated;
        ui64 requestId1 = 0;
        ui64 requestId2 = 0;

        manager.PushRequest(1, 10, TSegment{10, 20}, &requestId1, &outdated);
        UNIT_ASSERT(outdated.empty());

        outdated.clear();
        manager.PushRequest(1, 11, TSegment{0, 30}, &requestId2, &outdated);

        AssertOutdatedEqual(outdated, {{10, requestId1}});

        AssertPop(manager, requestId1, Max<ui64>(), std::vector<TSegment>{});
        AssertPop(manager, requestId2, 11, std::vector<TSegment>{{0, 30}});
    }

    Y_UNIT_TEST(OverlapAcrossSeveralRequestsTrimsSidesAndOutdatesMiddle) {
        TSegmentManager manager;
        std::vector<TOutdatedRequest> outdated;
        ui64 requestId1 = 0;
        ui64 requestId2 = 0;
        ui64 requestId3 = 0;
        ui64 requestId4 = 0;
        ui64 requestIdOtherVChunk = 0;

        manager.PushRequest(1, 101, TSegment{0, 10}, &requestId1, &outdated);
        UNIT_ASSERT(outdated.empty());

        outdated.clear();
        manager.PushRequest(1, 102, TSegment{10, 20}, &requestId2, &outdated);
        UNIT_ASSERT(outdated.empty());

        outdated.clear();
        manager.PushRequest(1, 103, TSegment{20, 30}, &requestId3, &outdated);
        UNIT_ASSERT(outdated.empty());

        outdated.clear();
        manager.PushRequest(2, 201, TSegment{10, 20}, &requestIdOtherVChunk, &outdated);
        UNIT_ASSERT(outdated.empty());

        outdated.clear();
        manager.PushRequest(1, 104, TSegment{5, 25}, &requestId4, &outdated);

        AssertOutdatedEqual(outdated, {{102, requestId2}});

        AssertPop(manager, requestId1, 101, std::vector<TSegment>{{0, 5}});
        AssertPop(manager, requestId2, Max<ui64>(), std::vector<TSegment>{});
        AssertPop(manager, requestId3, 103, std::vector<TSegment>{{25, 30}});
        AssertPop(manager, requestId4, 104, std::vector<TSegment>{{5, 25}});
        AssertPop(manager, requestIdOtherVChunk, 201, std::vector<TSegment>{{10, 20}});
    }

    Y_UNIT_TEST(CoverSeveralRequestsCollectsAllOutdated) {
        TSegmentManager manager;
        std::vector<TOutdatedRequest> outdated;
        ui64 requestId1 = 0;
        ui64 requestId2 = 0;
        ui64 requestId3 = 0;
        ui64 requestId4 = 0;

        manager.PushRequest(1, 11, TSegment{0, 4}, &requestId1, &outdated);
        UNIT_ASSERT(outdated.empty());

        outdated.clear();
        manager.PushRequest(1, 12, TSegment{4, 8}, &requestId2, &outdated);
        UNIT_ASSERT(outdated.empty());

        outdated.clear();
        manager.PushRequest(1, 13, TSegment{8, 12}, &requestId3, &outdated);
        UNIT_ASSERT(outdated.empty());

        outdated.clear();
        manager.PushRequest(1, 14, TSegment{0, 12}, &requestId4, &outdated);

        AssertOutdatedEqual(outdated, {
            {11, requestId1},
            {12, requestId2},
            {13, requestId3},
        });

        AssertPop(manager, requestId1, Max<ui64>(), std::vector<TSegment>{});
        AssertPop(manager, requestId2, Max<ui64>(), std::vector<TSegment>{});
        AssertPop(manager, requestId3, Max<ui64>(), std::vector<TSegment>{});
        AssertPop(manager, requestId4, 14, std::vector<TSegment>{{0, 12}});
    }
}

} // namespace NKikimr::NDDisk
