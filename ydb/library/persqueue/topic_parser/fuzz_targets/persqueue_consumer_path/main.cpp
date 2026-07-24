#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace {

TString ConsumeString(FuzzedDataProvider& fdp, size_t maxLen = 256) {
    TString value(fdp.ConsumeRandomLengthString(maxLen));
    TString filtered;
    filtered.reserve(value.size());
    for (const char ch : value) {
        if (ch != '\0') {
            filtered.push_back(ch);
        }
    }
    return filtered;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    const TString consumer = ConsumeString(fdp);
    const TString database = ConsumeString(fdp);
    const TString topicPath = ConsumeString(fdp);

    NKikimrPQ::TPQConfig firstClassConfig;
    firstClassConfig.SetTopicsAreFirstClassCitizen(true);
    firstClassConfig.SetEnabled(true);

    NKikimrPQ::TPQConfig legacyConfig;
    legacyConfig.SetTopicsAreFirstClassCitizen(false);
    legacyConfig.SetEnabled(true);

    try {
        const TString legacyNew = NPersQueue::ConvertNewConsumerName(consumer, legacyConfig);
        const TString legacyOld = NPersQueue::ConvertOldConsumerName(legacyNew, legacyConfig);
        const TString legacyPath = NPersQueue::MakeConsumerPath(legacyNew);

        (void)legacyOld;
        (void)NPersQueue::StripLeadSlash(legacyPath);
        (void)NPersQueue::NormalizeFullPath(legacyPath);
    } catch (...) {
    }

    try {
        const TString firstClassNew = NPersQueue::ConvertNewConsumerName(consumer, firstClassConfig);
        const TString firstClassOld = NPersQueue::ConvertOldConsumerName(consumer, firstClassConfig);
        const TString rawPath = NPersQueue::MakeConsumerPath(consumer);

        (void)firstClassNew;
        (void)firstClassOld;
        (void)rawPath;
    } catch (...) {
    }

    try {
        TMaybe<TString> maybeDb;
        if (!database.empty()) {
            maybeDb = database;
        }
        (void)NPersQueue::GetFullTopicPath(maybeDb, topicPath);
    } catch (...) {
    }

    return 0;
}
