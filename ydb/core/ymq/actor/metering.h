#pragma once
#include "defs.h"

#include <ydb/core/ymq/base/action.h>
#include <ydb/core/util/address_classifier.h>
#include <ydb/core/ymq/base/processed_request_attributes.h>

#include <ydb/core/protos/config.pb.h>

#include <library/cpp/scheme/scheme.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>

namespace NKikimr::NSQS {

struct TMeteringCounters;

NSc::TValue CreateMeteringBillingRecord(
    const TString& folderId,
    const TString& resourceId,
    const TString& schema,
    const TString& fqdn,
    const TInstant& now,
    const ui64 quantity,
    const TString& unit,
    const NSc::TValue& tags,
    const NSc::TValue& labels = {}
);

class TProcessedRequestsAggregator {
public:
    static const THashMap<TString, TString> LabelTransformation;
public:
    TProcessedRequestsAggregator(const NKikimrConfig::TSqsConfig& config);

    // Do not rename enum members in the enums below (SQS-22)
    enum ETrafficType {
        ingress = 0,
        egress
    };

    enum ENetworkClass {
        inet = 0,
        cloud,
        yandex,
        unknown
    };

    enum EQueueType {
        std = 0,
        fifo,
        other
    };

    struct TReportedTrafficKey {
        TString ResourceId;
        ETrafficType TrafficType;
        TString NetworkClassLabel;
        NSc::TValue Labels;

        bool operator<(const TReportedTrafficKey& rhs) const;
    };

    struct TReportedRequestsKey {
        TString ResourceId;
        EQueueType QueueType;
        NSc::TValue Labels;

        bool operator<(const TReportedRequestsKey& rhs) const;
    };

    TString ClassifyNetwork(const TString& address) const;

    ui64 CountBlocks(const ui64 bytes, const ui64 blockSize) const;

    bool Add(const TProcessedRequestAttributes& attrs);

    TVector<NSc::TValue> DumpReportedTrafficAsJsonArray(const TString& fqdn, const TInstant& now) const;
    TVector<NSc::TValue> DumpReportedRequestsAsJsonArray(const TString& fqdn, const TInstant& now) const;

    void ResetReportsStorage();

    void UpdateNetClassifier(NAddressClassifier::TLabeledAddressClassifier::TConstPtr classifier, TMaybe<TInstant> netDataUpdateTimestamp);

private:
    static TString ClassifyNetwork(NAddressClassifier::TLabeledAddressClassifier::TConstPtr classifier, const TString& address);

    void InitAddressClassifier(const NKikimrConfig::TSqsConfig& config, TVector<TString>&& labels);

private:
    bool NetClassifierOnly;

    THashMap<TString, TMap<TReportedTrafficKey, ui64>> ReportedTraffic;
    THashMap<TString, TMap<TReportedRequestsKey, ui64>> ReportedRequests;
    NAddressClassifier::TLabeledAddressClassifier::TConstPtr AddressClassifier;
    NAddressClassifier::TLabeledAddressClassifier::TConstPtr NetClassifier;

    TIntrusivePtr<TMeteringCounters> Counters;
};

} // namespace NKikimr::NSQS
