#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NPersQueue {

struct TPQLabelsInfo {
    TVector<std::pair<TString, TString>> Labels;
    TVector<TString> AggrNames;
};

struct TTopicCounterNames {
    TString Account;
    TString LegacyProducer;
    TString ShortClientsideName;
    TString FederationPath;
    TString ClientsideName;
    TString Cluster;
};

} // namespace NPersQueue
