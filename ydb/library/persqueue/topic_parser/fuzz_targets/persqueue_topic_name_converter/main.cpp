#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace {

TString ConsumeString(FuzzedDataProvider& fdp, size_t maxLen = 128) {
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

TString NormalizePath(TString value) {
    if (value.empty()) {
        return value;
    }

    if (!value.StartsWith("/")) {
        value = TString("/") + value;
    }

    while (value.size() > 1 && value.back() == '/') {
        value.pop_back();
    }

    return value;
}

TString SanitizePathComponent(const TString& value, TStringBuf fallback) {
    TString filtered;
    filtered.reserve(value.size());
    for (const char ch : value) {
        if (ch != '/' && ch != '\0') {
            filtered.push_back(ch);
        }
    }

    if (filtered.empty()) {
        return TString(fallback);
    }

    return filtered;
}

TMaybe<bool> ConsumeMaybeBool(FuzzedDataProvider& fdp) {
    switch (fdp.ConsumeIntegralInRange<int>(0, 2)) {
        case 1:
            return true;
        case 2:
            return false;
        default:
            return Nothing();
    }
}

NKikimrPQ::TPQTabletConfig BuildTabletConfig(FuzzedDataProvider& fdp) {
    NKikimrPQ::TPQTabletConfig config;

    TString topicName = ConsumeString(fdp);
    TString topicPath = NormalizePath(ConsumeString(fdp));
    if (topicName.empty() && topicPath.empty()) {
        topicPath = "/topic";
    }
    if (topicName.empty() && topicPath.find('/') == TString::npos) {
        topicPath = TString("/") + topicPath + "/topic";
    }
    if (topicPath.empty()) {
        topicPath = topicName.StartsWith("/") ? topicName : TString("/") + topicName;
    }
    topicPath = NormalizePath(topicPath);
    if (topicPath == "/") {
        topicPath = "/topic";
    }

    TString databasePath = NormalizePath(ConsumeString(fdp));
    if (!databasePath.empty() && topicPath == databasePath) {
        topicPath += "/topic";
    }

    config.SetTopicName(topicName);
    config.SetTopicPath(topicPath);
    config.SetTopic(ConsumeString(fdp));
    config.SetFederationAccount(ConsumeString(fdp));
    config.SetDC(ConsumeString(fdp));
    config.SetLocalDC(fdp.ConsumeBool());
    config.SetYdbDatabasePath(databasePath);

    return config;
}

void NormalizeFederatedTabletConfig(NKikimrPQ::TPQTabletConfig& config) {
    const TString account = SanitizePathComponent(config.GetFederationAccount(), "account");
    const TString topicComponent = SanitizePathComponent(
        !config.GetTopicName().empty() ? config.GetTopicName() : config.GetTopic(),
        "topic");

    config.SetFederationAccount(account);
    config.SetTopicName(topicComponent);
    config.SetTopicPath(TString("/") + account + "/" + topicComponent);
}

void TouchDiscoveryConverter(
    const NPersQueue::TDiscoveryConverterPtr& converter) {
    if (!converter) {
        return;
    }

    (void)converter->IsValid();
    (void)converter->GetReason();
    (void)converter->GetPrintableString();
    (void)converter->GetOriginalTopic();

    if (converter->IsValid()) {
        (void)converter->GetPrimaryPath();
        (void)converter->GetOriginalPath();
    }
}

void TouchTopicConverter(const NPersQueue::TTopicConverterPtr& converter) {
    if (!converter) {
        return;
    }

    (void)converter->IsValid();
    (void)converter->GetReason();
    (void)converter->GetPrintableString();

    if (!converter->IsValid()) {
        return;
    }

    (void)converter->GetPrimaryPath();
    (void)converter->GetOriginalPath();
    (void)converter->GetTopicForSrcId();
    (void)converter->GetTopicForSrcIdHash();
    (void)converter->GetFederationPath();
    (void)converter->GetFederationPathWithDC();
    (void)converter->GetModernName();
    (void)converter->GetCluster();
    (void)converter->GetAccount();

    if (!converter->IsFirstClass()) {
        (void)converter->GetClientsideName();
        (void)converter->GetShortClientsideName();
        (void)converter->GetShortLegacyName();
        (void)converter->GetLegacyProducer();
        (void)converter->GetLegacyLogtype();
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    const TString pqRoot = ConsumeString(fdp);
    const TString localDc = ConsumeString(fdp);
    const TString topic = ConsumeString(fdp);
    const TString dc = ConsumeString(fdp);
    const TString database = ConsumeString(fdp);
    const bool noDcMode = fdp.ConsumeBool();
    const TMaybe<bool> isInLocalDc = ConsumeMaybeBool(fdp);
    const TMaybe<bool> isLocalDc = ConsumeMaybeBool(fdp);

    try {
        NPersQueue::TTopicNamesConverterFactory factory(noDcMode, pqRoot, localDc, isLocalDc);
        auto converter = factory.MakeDiscoveryConverter(topic, isInLocalDc, dc, database);
        TouchDiscoveryConverter(converter);
    } catch (...) {
    }

    NKikimrPQ::TPQConfig pqConfig;
    pqConfig.SetTopicsAreFirstClassCitizen(fdp.ConsumeBool());
    pqConfig.SetEnabled(fdp.ConsumeBool());
    pqConfig.SetRoot(pqRoot);
    pqConfig.SetTestDatabaseRoot(ConsumeString(fdp));

    try {
        NPersQueue::TTopicNamesConverterFactory factory(pqConfig, localDc, isLocalDc);
        auto discovery = factory.MakeDiscoveryConverter(topic, isInLocalDc, dc, database);
        TouchDiscoveryConverter(discovery);

        auto tabletConfig = BuildTabletConfig(fdp);
        NPersQueue::TTopicConverterPtr topicConverter;
        if (pqConfig.GetTopicsAreFirstClassCitizen()) {
            topicConverter = factory.MakeTopicConverter(tabletConfig);
        } else {
            NormalizeFederatedTabletConfig(tabletConfig);
            tabletConfig.SetYdbDatabasePath("");
            topicConverter = NPersQueue::TTopicNameConverter::ForFederation("/pq", tabletConfig, "");
        }
        TouchTopicConverter(topicConverter);
    } catch (...) {
    }

    return 0;
}
