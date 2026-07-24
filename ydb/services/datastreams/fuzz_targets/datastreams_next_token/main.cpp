#include <ydb/services/datastreams/next_token.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace {

TString ConsumeString(FuzzedDataProvider& fdp, size_t maxLen = 128) {
    return TString(fdp.ConsumeRandomLengthString(maxLen));
}

ui64 ConsumeTimestamp(FuzzedDataProvider& fdp, ui64 now) {
    const i64 skew = fdp.ConsumeIntegralInRange<i64>(
        -static_cast<i64>(2 * NKikimr::NDataStreams::V1::TNextToken::LIFETIME_MS),
        static_cast<i64>(2 * NKikimr::NDataStreams::V1::TNextToken::LIFETIME_MS));
    if (skew < 0) {
        const ui64 delta = static_cast<ui64>(-skew);
        return delta > now ? 0 : now - delta;
    }
    return now + static_cast<ui64>(skew);
}

void TouchParsedToken(const NKikimr::NDataStreams::V1::TNextToken& token) {
    (void)token.IsValid();
    (void)token.IsExpired();
    (void)token.GetAlreadyRead();
    (void)token.GetMaxResults();
    (void)token.GetCreationTimestamp();
    (void)token.GetStreamArn();
    (void)token.GetStreamName();
    (void)token.Serialize();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    const TString rawToken = ConsumeString(fdp, 512);
    try {
        NKikimr::NDataStreams::V1::TNextToken parsed(rawToken);
        TouchParsedToken(parsed);

        const TString serialized = parsed.Serialize();
        NKikimr::NDataStreams::V1::TNextToken reparsed(serialized);
        TouchParsedToken(reparsed);
    } catch (...) {
    }

    const ui64 now = TInstant::Now().MilliSeconds();
    const TString streamArn = ConsumeString(fdp);
    const ui32 alreadyRead = fdp.ConsumeIntegral<ui32>();
    const ui32 maxResults = fdp.ConsumeIntegral<ui32>();
    const ui64 creationTimestamp = ConsumeTimestamp(fdp, now);

    try {
        NKikimr::NDataStreams::V1::TNextToken built(
            streamArn,
            alreadyRead,
            maxResults,
            creationTimestamp);
        TouchParsedToken(built);
        (void)built.IsAlive(now);

        const TString serialized = built.Serialize();
        NKikimr::NDataStreams::V1::TNextToken roundtrip(serialized);
        TouchParsedToken(roundtrip);
        (void)roundtrip.IsAlive(now);
    } catch (...) {
    }

    return 0;
}
