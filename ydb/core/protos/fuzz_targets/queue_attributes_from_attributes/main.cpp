#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>
#include <ydb/core/ymq/base/queue_attributes.h>
#include <ydb/core/protos/config.pb.h>
#include <util/generic/hash.h>

using namespace NKikimr::NSQS;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    try {
        NKikimrConfig::TSqsConfig config;
        THashMap<TString, TString> attributes;

        const size_t items = provider.ConsumeIntegralInRange<size_t>(0, 8);
        for (size_t i = 0; i < items; ++i) {
            attributes[provider.ConsumeRandomLengthString(24)] = provider.ConsumeRandomLengthString(48);
        }

        bool isFifoQueue = provider.ConsumeBool();
        bool clamp = true;
        TQueueAttributes queueAttrs = TQueueAttributes::FromAttributesAndConfig(attributes, config, isFifoQueue, clamp);
        (void)queueAttrs;
    } catch (...) {
    }

    return 0;
}
