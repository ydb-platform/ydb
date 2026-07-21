#include <ydb/services/datastreams/shard_iterator.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace {

TString ConsumeString(FuzzedDataProvider& fdp, size_t maxLen = 128) {
    return TString(fdp.ConsumeRandomLengthString(maxLen));
}

void TouchIterator(const NKikimr::NDataStreams::V1::TShardIterator& iterator) {
    (void)iterator.IsValid();
    (void)iterator.IsExpired();
    (void)iterator.GetStreamName();
    (void)iterator.GetStreamArn();
    (void)iterator.GetShardId();
    (void)iterator.GetReadTimestamp();
    (void)iterator.GetSequenceNumber();
    (void)iterator.GetKind();
    (void)iterator.IsCdcTopic();
    (void)iterator.Serialize();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    const TString rawIterator = ConsumeString(fdp, 512);
    try {
        NKikimr::NDataStreams::V1::TShardIterator parsed(rawIterator);
        TouchIterator(parsed);

        const TString serialized = parsed.Serialize();
        NKikimr::NDataStreams::V1::TShardIterator reparsed(serialized);
        TouchIterator(reparsed);
    } catch (...) {
    }

    const TString streamName = ConsumeString(fdp);
    const TString streamArn = ConsumeString(fdp);
    const ui32 shardId = fdp.ConsumeIntegral<ui32>();
    const ui64 readTimestamp = fdp.ConsumeIntegral<ui64>();
    const ui64 sequenceNumber = fdp.ConsumeIntegral<ui64>();
    const bool isCdc = fdp.ConsumeBool();

    try {
        auto built = isCdc
            ? NKikimr::NDataStreams::V1::TShardIterator::Cdc(
                streamName,
                streamArn,
                shardId,
                readTimestamp,
                sequenceNumber)
            : NKikimr::NDataStreams::V1::TShardIterator::Common(
                streamName,
                streamArn,
                shardId,
                readTimestamp,
                sequenceNumber);

        TouchIterator(built);

        built.SetReadTimestamp(fdp.ConsumeIntegral<ui64>());
        built.SetSequenceNumber(fdp.ConsumeIntegral<ui64>());
        TouchIterator(built);
        (void)built.IsAlive(TInstant::Now().MilliSeconds());

        NKikimr::NDataStreams::V1::TShardIterator copied(built);
        TouchIterator(copied);
    } catch (...) {
    }

    return 0;
}
