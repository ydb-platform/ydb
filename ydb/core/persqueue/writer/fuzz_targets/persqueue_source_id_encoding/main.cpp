#include <ydb/core/persqueue/writer/source_id_encoding.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

namespace {

TString ConsumeString(FuzzedDataProvider& fdp, size_t maxLen = 256) {
    return TString(fdp.ConsumeRandomLengthString(maxLen));
}

void TouchEncodedSourceId(const NKikimr::NPQ::NSourceIdEncoding::TEncodedSourceId& encoded) {
    (void)encoded.OriginalSourceId;
    (void)encoded.EscapedSourceId;
    (void)encoded.Hash;
    (void)encoded.KeysHash;
    (void)encoded.Generation;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    const TString topic = ConsumeString(fdp);
    const TString sourceId = ConsumeString(fdp);
    const TString candidate = ConsumeString(fdp);

    try {
        const TString encoded = NKikimr::NPQ::NSourceIdEncoding::Encode(sourceId);
        (void)NKikimr::NPQ::NSourceIdEncoding::IsValidEncoded(encoded);
        (void)NKikimr::NPQ::NSourceIdEncoding::Decode(encoded);
    } catch (...) {
    }

    try {
        if (!candidate.empty()) {
            (void)NKikimr::NPQ::NSourceIdEncoding::IsValidEncoded(candidate);
            (void)NKikimr::NPQ::NSourceIdEncoding::Decode(candidate);
        }
    } catch (...) {
    }

    try {
        auto encoded = NKikimr::NPQ::NSourceIdEncoding::EncodeSrcId(
            topic,
            sourceId,
            NKikimr::NPQ::ESourceIdTableGeneration::SrcIdMeta2);
        TouchEncodedSourceId(encoded);
    } catch (...) {
    }

    try {
        auto encoded = NKikimr::NPQ::NSourceIdEncoding::EncodeSrcId(
            topic,
            sourceId,
            NKikimr::NPQ::ESourceIdTableGeneration::PartitionMapping);
        TouchEncodedSourceId(encoded);
    } catch (...) {
    }

    return 0;
}
