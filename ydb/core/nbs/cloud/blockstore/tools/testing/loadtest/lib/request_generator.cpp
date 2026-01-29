#include "request_generator.h"

#include "range_allocator.h"
#include "range_map.h"

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NLoadTest {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TArtificialRequestGenerator final
    : public IRequestGenerator
{
private:
    TLog Log;
    NProto::TRangeTest RangeTest;
    TRangeMap BlocksRange;
    TRangeAllocator RangeAllocator;

    TVector<std::pair<ui64, EBlockStoreRequest>> Rates;
    ui64 TotalRate = 0;
    ui64 SentRequestCount = 0;

public:
    TArtificialRequestGenerator(
            ILoggingServicePtr logging,
            NProto::TRangeTest range)
        : RangeTest(std::move(range))
        , BlocksRange(TBlockRange64::MakeClosedInterval(
              RangeTest.GetStart(),
              RangeTest.GetEnd()))
        , RangeAllocator(RangeTest)
    {
        Log = logging->CreateLog(Describe());

        SetupRequestWeights();
    }

    bool Next(TRequest* request) override;
    void Complete(TBlockRange64 blockRange) override;
    TString Describe() const override;
    bool HasMoreRequests() const override;

private:
    TMaybe<TBlockRange64> AllocateRange();
    void SetupRequestWeights();
    EBlockStoreRequest ChooseRequest() const;
};

////////////////////////////////////////////////////////////////////////////////

TString TArtificialRequestGenerator::Describe() const
{
    return TStringBuilder()
        << "Range[" << RangeTest.GetStart()
        << ',' << RangeTest.GetEnd() << ']';
}

bool TArtificialRequestGenerator::HasMoreRequests() const
{
    return !RangeTest.GetRequestsCount()
        || RangeTest.GetRequestsCount() - SentRequestCount;
}

bool TArtificialRequestGenerator::Next(TRequest* request)
{
    auto blockRange = AllocateRange();
    if (!blockRange.Defined()) {
        STORAGE_WARN(
            "No free blocks found: (%lu) have = %s",
            BlocksRange.Size(),
            BlocksRange.DumpRanges().data());
        return false;
    }

    STORAGE_TRACE(
        "BlockAllocated: (%lu) allocated = %s left = %s",
        BlocksRange.Size(),
        DescribeRange(*blockRange).data(),
        BlocksRange.DumpRanges().data());

    request->RequestType = ChooseRequest();
    request->BlockRange = *blockRange;

    ++SentRequestCount;

    return true;
}

void TArtificialRequestGenerator::Complete(TBlockRange64 blockRange)
{
    STORAGE_TRACE(
        "Block Deallocated: (%lu) %s \n%s",
        BlocksRange.Size(),
        DescribeRange(blockRange).data(),
        BlocksRange.DumpRanges().data());

    if (RangeTest.GetAllowIntersectingRanges()) {
        return;
    }

    BlocksRange.PutBlock(blockRange);
}

void TArtificialRequestGenerator::SetupRequestWeights()
{
    if (RangeTest.GetWriteRate()) {
        TotalRate += RangeTest.GetWriteRate();
        Rates.emplace_back(TotalRate, EBlockStoreRequest::WriteBlocks);
    }

    if (RangeTest.GetReadRate()) {
        TotalRate += RangeTest.GetReadRate();
        Rates.emplace_back(TotalRate, EBlockStoreRequest::ReadBlocks);
    }

    if (RangeTest.GetZeroRate()) {
        TotalRate += RangeTest.GetZeroRate();
        Rates.emplace_back(TotalRate, EBlockStoreRequest::ZeroBlocks);
    }
}

EBlockStoreRequest TArtificialRequestGenerator::ChooseRequest() const
{
    auto it = LowerBound(
        Rates.begin(),
        Rates.end(),
        RandomNumber(TotalRate),
        [] (const auto& a, const auto& b) { return a.first < b; });

    auto offset = std::distance(Rates.begin(), it);
    return Rates[offset].second;
}

TMaybe<TBlockRange64> TArtificialRequestGenerator::AllocateRange()
{
    const auto allocatedRange = RangeAllocator.AllocateRange();
    if (RangeTest.GetAllowIntersectingRanges()) {
        return allocatedRange;
    }

    return BlocksRange.GetBlock(
        allocatedRange,
        RangeTest.GetLoadType() == NProto::LOAD_TYPE_SEQUENTIAL);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateArtificialRequestGenerator(
    ILoggingServicePtr loggingService,
    NProto::TRangeTest range)
{
    return std::make_shared<TArtificialRequestGenerator>(
        std::move(loggingService),
        std::move(range));
}

}   // namespace NCloud::NBlockStore::NLoadTest
