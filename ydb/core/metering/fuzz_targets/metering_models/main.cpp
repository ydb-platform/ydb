#include <ydb/core/metering/bill_record.h>
#include <ydb/core/metering/stream_ru_calculator.h>
#include <ydb/core/metering/time_grid.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/array_ref.h>
#include <util/system/yassert.h>

namespace {

using namespace NKikimr;

constexpr ui32 MaxOps = 128;
constexpr ui64 MaxPayload = 1ull << 20;

constexpr ui32 Periods[] = {
    1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60,
    120, 180, 240, 300, 360, 450, 600, 720, 900, 1200, 1800, 3600
};

void CheckTimeGrid(FuzzedDataProvider& provider) {
    const ui32 periodSeconds = Periods[provider.ConsumeIntegralInRange<size_t>(0, std::size(Periods) - 1)];
    TTimeGrid grid(TDuration::Seconds(periodSeconds));

    const ui64 nowSeconds = provider.ConsumeIntegralInRange<ui64>(0, 7 * 24 * 3600);
    const auto now = TInstant::Seconds(nowSeconds);
    const auto slot = grid.Get(now);
    Y_ABORT_UNLESS(slot.Start <= now);
    Y_ABORT_UNLESS(slot.End >= now);
    Y_ABORT_UNLESS(slot.End - slot.Start == TDuration::Seconds(periodSeconds - 1));
    Y_ABORT_UNLESS(grid.GetNext(slot).Start == slot.Start + grid.Period);
    Y_ABORT_UNLESS(grid.GetNext(slot).End == slot.End + grid.Period);
    Y_ABORT_UNLESS(grid.GetPrev(slot).Start == slot.Start - grid.Period);
    Y_ABORT_UNLESS(grid.GetPrev(slot).End == slot.End - grid.Period);
}

void CheckRuCalculator(FuzzedDataProvider& provider) {
    const ui64 blockSize = provider.ConsumeIntegralInRange<ui64>(1, 1 << 16);
    NKikimr::NMetering::TStreamRequestUnitsCalculator calculator(blockSize);
    ui64 modelRemainder = blockSize;

    const ui32 ops = provider.ConsumeIntegralInRange<ui32>(0, MaxOps);
    for (ui32 i = 0; i < ops; ++i) {
        const ui64 payload = provider.ConsumeIntegralInRange<ui64>(0, MaxPayload);
        ui64 expected = 0;
        if (payload) {
            ui64 remainingPayload = payload;
            if (remainingPayload > modelRemainder) {
                remainingPayload -= modelRemainder;
                const ui64 blocks = remainingPayload / blockSize;
                remainingPayload -= blockSize * blocks;
                modelRemainder = blockSize - remainingPayload;
                expected = blocks + ui64(bool(remainingPayload));
            } else {
                modelRemainder -= remainingPayload;
            }
        }

        const ui64 consumed = calculator.CalcConsumption(payload);
        Y_ABORT_UNLESS(consumed == expected);
        Y_ABORT_UNLESS(calculator.GetRemainder() <= blockSize);
        Y_ABORT_UNLESS(calculator.GetRemainder() == modelRemainder);
    }
}

void CheckBillRecord(FuzzedDataProvider& provider) {
    const auto start = TInstant::Seconds(provider.ConsumeIntegralInRange<ui64>(0, 1'000'000));
    const auto finish = start + TDuration::Seconds(provider.ConsumeIntegralInRange<ui64>(0, 3600));
    const ui64 quantity = provider.ConsumeIntegralInRange<ui64>(0, 1ull << 32);

    TBillRecord record;
    record.Id("id")
        .CloudId("cloud")
        .FolderId("folder")
        .ResourceId("resource")
        .SourceWt(finish)
        .Usage(TBillRecord::RequestUnits(quantity, start, finish));

    const TString json = record.ToString();
    NJson::TJsonValue parsed;
    Y_ABORT_UNLESS(NJson::ReadJsonTree(json, &parsed));
    Y_ABORT_UNLESS(parsed.IsMap());
    Y_ABORT_UNLESS(parsed["usage"].IsMap());
    Y_ABORT_UNLESS(parsed["usage"]["quantity"].GetUInteger() == quantity);
    Y_ABORT_UNLESS(parsed["usage"]["start"].GetUInteger() == start.Seconds());
    Y_ABORT_UNLESS(parsed["usage"]["finish"].GetUInteger() == finish.Seconds());
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    CheckTimeGrid(provider);
    CheckRuCalculator(provider);
    CheckBillRecord(provider);
    return 0;
}
