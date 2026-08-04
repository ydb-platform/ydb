// Fuzzer for TDiscoveryConverter (federation-mode topic discovery).
// TTopicNamesConverterFactory::MakeDiscoveryConverter parses a raw topic path
// string and dc/database identifiers arriving over gRPC TopicService.
// It builds internal path representations, performs path component splitting
// and various Y_VERIFY_S / Y_ABORT_UNLESS assertions on parsed results.
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <util/generic/string.h>
#include <cstring>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size == 0) return 0;

    // Reuse 4 fields split by NUL bytes: topic | dc | database | pqPrefix
    TString parts[4];
    const uint8_t* p = data;
    const uint8_t* end = data + size;
    for (int i = 0; i < 4; ++i) {
        const uint8_t* nul = static_cast<const uint8_t*>(std::memchr(p, 0, end - p));
        size_t len = nul ? static_cast<size_t>(nul - p) : static_cast<size_t>(end - p);
        parts[i] = TString(reinterpret_cast<const char*>(p), len);
        p += len + (nul ? 1 : 0);
        if (p >= end) break;
    }

    const TString& topic    = parts[0];
    const TString& dc       = parts[1];
    const TString& pqPrefix = parts[2];
    const TString& database = parts[3];

    try {
        // First-class citizen mode (noDcMode=true)
        NPersQueue::TTopicNamesConverterFactory factory(/*noDcMode=*/true, pqPrefix, /*localDc=*/"");
        auto conv = factory.MakeDiscoveryConverter(topic, /*isInLocalDc=*/Nothing(), dc, database);
        if (conv) {
            (void)conv->IsValid();
            (void)conv->GetPrimaryPath();
        }
    } catch (...) {}

    try {
        // Federation mode (noDcMode=false)
        NPersQueue::TTopicNamesConverterFactory factory(/*noDcMode=*/false, pqPrefix, /*localDc=*/dc);
        auto conv = factory.MakeDiscoveryConverter(topic, /*isInLocalDc=*/TMaybe<bool>(true), dc, database);
        if (conv) {
            (void)conv->IsValid();
            (void)conv->GetPrimaryPath();
        }
    } catch (...) {}

    return 0;
}
