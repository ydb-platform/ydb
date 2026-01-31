#include "range_allocator.h"

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore::NLoadTest {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TRangeAllocator::TRangeAllocator(const NProto::TRangeTest& rangeTest)
    : LoadType(rangeTest.GetLoadType())
    , Range(TBlockRange64::MakeClosedInterval(
          rangeTest.GetStart(),
          rangeTest.GetEnd()))
    , CurrentBlock(Range.Start)
{
    SetupSubRanges(rangeTest);
    SetupRequestSizes(rangeTest);
}

TBlockRange64 TRangeAllocator::AllocateRange()
{
    auto sample = RandomNumber<double>();

    auto requestSize = LowerBound(
        RequestSizes.begin(),
        RequestSizes.end(),
        sample,
        [] (const TRequestSize& a, const double b) {
            return a.Cdf < b;
        }
    );

    Y_ABORT_UNLESS(requestSize != RequestSizes.end());

    auto size = requestSize->MinSize
        + RandomNumber(requestSize->MaxSize - requestSize->MinSize + 1);

    if (LoadType == NProto::LOAD_TYPE_SEQUENTIAL) {
        if (CurrentBlock > Range.End) {
            CurrentBlock = Range.Start;
        }

        auto range = TBlockRange64::WithLength(CurrentBlock, size);
        if (range.End > Range.End) {
            range = TBlockRange64::MakeClosedInterval(range.Start, Range.End);
        }

        CurrentBlock = range.End + 1;
        return range;
    } else {
        auto subRange = LowerBound(
            SubRanges.begin(),
            SubRanges.end(),
            sample,
            [] (const TSubRange& a, const double b) {
                return a.Cdf < b;
            }
        );

        Y_ABORT_UNLESS(subRange != SubRanges.end());

        auto pos = subRange->Range.Start;
        if (LoadType == NProto::LOAD_TYPE_RANDOM) {
            pos += RandomNumber(subRange->Range.Size());
        } else {
            Y_ABORT_UNLESS(LoadType == NProto::LOAD_TYPE_ZIPF_RANDOM);
            pos += pow(subRange->Range.Size(), RandomNumber<double>()) - 1;
        }

        return TBlockRange64::WithLength(pos, size);
    }
}

void TRangeAllocator::SetupSubRanges(const NProto::TRangeTest& rangeTest)
{
    double cdf = 0;
    ui64 start = Range.Start;

    if (rangeTest.SubRangesSize()) {
        for (ui32 i = 0; i < rangeTest.SubRangesSize(); ++i) {
            const auto& r = rangeTest.GetSubRanges(i);
            cdf += r.GetProbability();
            SubRanges.push_back({
                cdf,
                TBlockRange64::WithLength(start, r.GetLength())
            });
            start += r.GetLength();
        }

        Y_ENSURE(cdf >= 1, "invalid probabilities");
    } else {
        SubRanges.push_back({1, Range});
    }

    for (const auto& subRange: SubRanges) {
        Y_ENSURE(subRange.Range.Start <= subRange.Range.End, "bad range");
    }
}

void TRangeAllocator::SetupRequestSizes(const NProto::TRangeTest& rangeTest)
{
    double cdf = 0;

    if (rangeTest.RequestSizesSize()) {
        for (ui32 i = 0; i < rangeTest.RequestSizesSize(); ++i) {
            const auto& r = rangeTest.GetRequestSizes(i);
            cdf += r.GetProbability();
            RequestSizes.push_back({cdf, r.GetMinSize(), r.GetMaxSize()});
        }
    }

    if (cdf < 1) {
        RequestSizes.push_back({
            1,
            Max<ui32>(1, rangeTest.GetMinRequestSize()),
            rangeTest.GetMaxRequestSize()
        });
    }

    for (const auto& requestSize: RequestSizes) {
        Y_ENSURE(requestSize.MinSize <= requestSize.MaxSize, "bad size");
    }
}

}   // namespace NCloud::NBlockStore::NLoadTest
