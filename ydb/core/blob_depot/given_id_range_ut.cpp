#include "types.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NBlobDepot;

ui32 GenerateRandomValue(ui32 min, ui32 max) {
    return min + RandomNumber(max - min + 1);
}

TGivenIdRange GenerateRandomRange(ui32 maxItems) {
    TGivenIdRange res;

    for (ui32 issuePos = 0; issuePos != maxItems; ) {
        const bool value = RandomNumber(2u);
        const ui32 numItems = GenerateRandomValue(1, maxItems - issuePos);
        if (value) {
            res.IssueNewRange(issuePos, issuePos + numItems);
        }
        issuePos += numItems;
    }

    res.CheckConsistency();
    return res;
}

TGivenIdRange RangeFromArray(const std::vector<bool>& array) {
    TGivenIdRange res;

    std::optional<ui32> sequenceBegin;
    for (ui32 i = 0; i < array.size(); ++i) {
        if (array[i]) {
            if (!sequenceBegin) {
                sequenceBegin = i;
            }
        } else {
            if (sequenceBegin) {
                res.IssueNewRange(*sequenceBegin, i);
            }
            sequenceBegin.reset();
        }
    }
    if (sequenceBegin) {
        res.IssueNewRange(*sequenceBegin, array.size());
    }

    res.CheckConsistency();
    UNIT_ASSERT_EQUAL(res.ToDebugArray(array.size()), array);
    return res;
}

Y_UNIT_TEST_SUITE(GivenIdRange) {
    Y_UNIT_TEST(IssueNewRange) {
        for (ui32 i = 0; i < 1000; ++i) {
            GenerateRandomRange(GenerateRandomValue(1, 1000));
        }
    }

    Y_UNIT_TEST(Trim) {
        for (ui32 i = 0; i < 1000; ++i) {
            const ui32 maxItems = GenerateRandomValue(1, 1000);
            TGivenIdRange range = GenerateRandomRange(maxItems);
            TGivenIdRange originalRange(range);
            const ui32 validSince = GenerateRandomValue(0, maxItems);
            TGivenIdRange trimmed = range.Trim(validSince);
            trimmed.CheckConsistency();
            std::vector<bool> originalRangeArr = originalRange.ToDebugArray(maxItems);
            std::vector<bool> rangeArr = range.ToDebugArray(maxItems);
            std::vector<bool> trimmedArr = trimmed.ToDebugArray(maxItems);
            for (ui32 i = 0; i < maxItems; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(originalRangeArr[i], rangeArr[i] + trimmedArr[i]);
                if (i < validSince) {
                    UNIT_ASSERT(!rangeArr[i]);
                } else {
                    UNIT_ASSERT(!trimmedArr[i]);
                }
            }
        }
    }

    Y_UNIT_TEST(Subtract) {
        for (ui32 i = 0; i < 10000; ++i) {
            const ui32 maxItems = GenerateRandomValue(1, 1000);
            TGivenIdRange range = GenerateRandomRange(maxItems);
            std::vector<bool> rangeArr = range.ToDebugArray(maxItems);
            std::vector<bool> subtractArr = GenerateRandomRange(maxItems).ToDebugArray(maxItems);
            std::vector<bool> resArr = rangeArr;
            for (ui32 i = 0; i < rangeArr.size(); ++i) {
                if (subtractArr[i]) {
                    if (rangeArr[i]) {
                        resArr[i] = false;
                    } else {
                        subtractArr[i] = false;
                    }
                }
            }
            TGivenIdRange subtract = RangeFromArray(subtractArr);
            range.Subtract(subtract);
            range.CheckConsistency();
            UNIT_ASSERT_EQUAL(range.ToDebugArray(maxItems), resArr);
        }
    }

    Y_UNIT_TEST(Points) {
        for (ui32 i = 0; i < 100; ++i) {
            const ui32 maxItems = GenerateRandomValue(1, 1000);
            std::vector<bool> items(maxItems, false);
            TGivenIdRange range;

            for (ui32 j = 0; j < 1000; ++j) {
                const ui32 index = RandomNumber(maxItems);
                if (items[index]) {
                    range.RemovePoint(index);
                } else {
                    range.AddPoint(index);
                }
                items[index] = !items[index];
                range.CheckConsistency();
                UNIT_ASSERT_EQUAL(range.ToDebugArray(maxItems), items);
            }

            for (ui32 i = 0; i < maxItems; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(range.GetPoint(i), items[i]);
            }

            const size_t index = std::find(items.begin(), items.end(), true) - items.begin();
            if (index != items.size()) {
                UNIT_ASSERT(!range.IsEmpty());
                UNIT_ASSERT_VALUES_EQUAL(range.GetMinimumValue(), index);
            } else {
                UNIT_ASSERT(range.IsEmpty());
            }
        }
    }

    Y_UNIT_TEST(Runs) {
        for (ui32 i = 0; i < 100; ++i) {
            const ui32 maxItems = GenerateRandomValue(1, 1000);
            std::vector<bool> items(maxItems, false);
            TGivenIdRange range;

            for (ui32 j = 0; j < 100; ++j) {
                const ui32 index = RandomNumber(maxItems);
                const bool value = items[index];
                ui32 maxRunLen = 0;
                while (index + maxRunLen < maxItems && items[index + maxRunLen] == value) {
                    ++maxRunLen;
                }
                const ui32 runLen = GenerateRandomValue(1, maxRunLen);
                std::fill(items.begin() + index, items.begin() + index + runLen, !value);
                if (value) {
                    for (ui32 i = 0; i < runLen; ++i) {
                        range.RemovePoint(index + i);
                    }
                } else {
                    range.IssueNewRange(index, index + runLen);
                }
                range.CheckConsistency();
                UNIT_ASSERT_EQUAL(range.ToDebugArray(maxItems), items);
            }
        }
    }

    Y_UNIT_TEST(Allocate) {
        for (ui32 i = 0; i < 100; ++i) {
            const ui32 maxItems = GenerateRandomValue(1, 1000);
            TGivenIdRange range = GenerateRandomRange(maxItems);
            std::vector<bool> items = range.ToDebugArray(maxItems);
            while (!range.IsEmpty()) {
                const ui32 index = range.Allocate();
                UNIT_ASSERT_EQUAL(items.begin() + index, std::find(items.begin(), items.end(), true));
                items[index] = false;
                range.CheckConsistency();
                UNIT_ASSERT_EQUAL(range.ToDebugArray(maxItems), items);
            }
        }
    }
}
