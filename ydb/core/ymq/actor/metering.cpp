#include "metering.h"

#include "cfg.h"
#include "serviceid.h"
#include "proxy_actor.h"

#include <ydb/core/mind/address_classification/net_classifier.h>

#include <ydb/core/ymq/base/action.h>
#include <ydb/core/ymq/base/counters.h>
#include <ydb/core/ymq/base/processed_request_attributes.h>

#include <library/cpp/logger/global/global.h>
#include <library/cpp/logger/record.h>

#include <util/generic/guid.h>
#include <util/generic/serialized_enum.h>
#include <util/string/builder.h>
#include <util/system/hostname.h>

#include <tuple>

namespace NKikimr::NSQS {

using namespace NAddressClassifier;

const THashMap<TString, TString> TProcessedRequestsAggregator::LabelTransformation = {
    {"yacloud", ToString(ENetworkClass::cloud)}
};

TProcessedRequestsAggregator::TProcessedRequestsAggregator(const NKikimrConfig::TSqsConfig& config)
    : NetClassifierOnly(config.GetMeteringByNetClassifierOnly())
{
    auto enumValues = GetEnumNames<ENetworkClass>();
    TVector<TString> labels(enumValues.size());
    for (auto enumItem : enumValues) {
        Y_ABORT_UNLESS(enumItem.first < labels.size());
        labels[enumItem.first] = enumItem.second;
    }
    Counters = new TMeteringCounters(config, GetSqsServiceCounters(AppData()->Counters, "metering"), labels);
    if (!NetClassifierOnly) {
        InitAddressClassifier(config, std::move(labels));
    }
}

bool TProcessedRequestsAggregator::TReportedTrafficKey::operator<(const TReportedTrafficKey& rhs) const {
    return std::tie(ResourceId, TrafficType, NetworkClassLabel) <
        std::tie(rhs.ResourceId, rhs.TrafficType, rhs.NetworkClassLabel);
}

bool TProcessedRequestsAggregator::TReportedRequestsKey::operator<(const TReportedRequestsKey& rhs) const {
    return std::tie(ResourceId, QueueType) <
        std::tie(rhs.ResourceId, rhs.QueueType);
}

TString TProcessedRequestsAggregator::ClassifyNetwork(NAddressClassifier::TLabeledAddressClassifier::TConstPtr classifier, const TString& address)  {
    if (classifier) {
        auto result = classifier->ClassifyAddress(address);
        if (!result) {
            return ToString(ENetworkClass::inet);
        }
        auto it = LabelTransformation.find(result.GetRef());
        if (it != LabelTransformation.end()) {
            return it->second;
        }
        return result.GetRef();
    }
    return ToString(ENetworkClass::unknown);
}

TString TProcessedRequestsAggregator::ClassifyNetwork(const TString& address) const {
    TString netClassifierResult = ClassifyNetwork(NetClassifier, address);

    TString result;
    if (NetClassifierOnly) {
        result = std::move(netClassifierResult);
    } else {
        result = ClassifyNetwork(AddressClassifier, address);
        INC_COUNTER(Counters, IdleClassifierRequestsResults[netClassifierResult]);
    }
    INC_COUNTER(Counters, ClassifierRequestsResults[result]);
    return result;
}

ui64 TProcessedRequestsAggregator::CountBlocks(const ui64 bytes, const ui64 blockSize) const {
    return (bytes - 1) / blockSize + 1;
}

bool TProcessedRequestsAggregator::Add(const TProcessedRequestAttributes& attrs) {
    if (attrs.HttpStatusCode >= 500 || !attrs.FolderId || !attrs.SourceAddress) {
        return false; // do not log with insufficient attributes
    }

    TString resourceId = attrs.ResourceId;
    EQueueType queueType = attrs.IsFifo ? EQueueType::fifo : EQueueType::std;
    if (!IsActionForQueue(attrs.Action) || attrs.Action == EAction::GetQueueUrl) {
        queueType = EQueueType::other;
        resourceId = "";
    }
    const auto networkClassLabel = ClassifyNetwork(attrs.SourceAddress);

    Y_ABORT_UNLESS(attrs.RequestSizeInBytes);
    Y_ABORT_UNLESS(attrs.ResponseSizeInBytes);

    ReportedTraffic[attrs.FolderId][{resourceId, ETrafficType::ingress, networkClassLabel, attrs.QueueTags}] += attrs.RequestSizeInBytes;
    ReportedTraffic[attrs.FolderId][{resourceId, ETrafficType::egress, networkClassLabel, attrs.QueueTags}] += attrs.ResponseSizeInBytes;

    static const ui64 defaultRequestBlockSize = 64 * 1024; // SQS-22
    ReportedRequests[attrs.FolderId][{resourceId, queueType, attrs.QueueTags}] += CountBlocks(attrs.RequestSizeInBytes + attrs.ResponseSizeInBytes,
                                                                             defaultRequestBlockSize);

    return true;
}

NSc::TValue CreateMeteringBillingRecord(
    const TString& folderId,
    const TString& resourceId,
    const TString& schema,
    const TString& fqdn,
    const TInstant& now,
    const ui64 quantity,
    const TString& unit,
    const NSc::TValue& tags,
    const NSc::TValue& labels
) {
    const TString& billingRecordVersion = "v1";
    const ui64 utcSeconds = now.Seconds();

    NSc::TValue record;
    record["folder_id"] = folderId;
    record["id"] = CreateGuidAsString();
    if (resourceId) {
        record["resource_id"] = resourceId;
    }
    record["schema"] = schema;
    record["source_id"] = fqdn;
    record["source_wt"] = utcSeconds;

    auto& usage = record["usage"].SetDict();
    usage["start"] = utcSeconds;
    usage["finish"] = utcSeconds;
    usage["type"] = "delta";
    usage["unit"] = unit;
    usage["quantity"] = quantity;

    record["tags"] = tags;      // Billing record tags
    if (!labels.DictEmpty()) {
        record["labels"] = labels;  // Queue tags
    }

    record["version"] = billingRecordVersion;

    return record;
}

TVector<NSc::TValue> TProcessedRequestsAggregator::DumpReportedTrafficAsJsonArray(const TString& fqdn, const TInstant& now) const {
    TVector<NSc::TValue> result;

    for (const auto& perFolderData : ReportedTraffic) {
        const TString& folderId = perFolderData.first;
        for (const auto& [trafficKey, trafficValue] : perFolderData.second) {
            NSc::TValue tags;
            tags.SetDict();
            tags["type"] = trafficKey.NetworkClassLabel;
            tags["direction"] = ToString(trafficKey.TrafficType);

            result.push_back(CreateMeteringBillingRecord(folderId,
                                                 trafficKey.ResourceId,
                                                 "ymq.traffic.v1",
                                                 fqdn,
                                                 now,
                                                 trafficValue,
                                                 "byte",
                                                 tags,
                                                 trafficKey.Labels));
        }
    }

    return result;
}

TVector<NSc::TValue> TProcessedRequestsAggregator::DumpReportedRequestsAsJsonArray(const TString& fqdn, const TInstant& now) const {
    TVector<NSc::TValue> result;

    for (const auto& perFolderData : ReportedRequests) {
        const TString& folderId = perFolderData.first;
        for (const auto& [requestsKey, requestsValue] : perFolderData.second) {
            NSc::TValue tags;
            tags.SetDict();
            tags["queue_type"] = ToString(requestsKey.QueueType);

            result.push_back(CreateMeteringBillingRecord(folderId,
                                                 requestsKey.ResourceId,
                                                 "ymq.requests.v1",
                                                 fqdn,
                                                 now,
                                                 requestsValue,
                                                 "request",
                                                 tags,
                                                 requestsKey.Labels));
        }
    }

    return result;
}

void TProcessedRequestsAggregator::ResetReportsStorage() {
    ReportedTraffic.clear();
    ReportedRequests.clear();
}

void TProcessedRequestsAggregator::UpdateNetClassifier(NAddressClassifier::TLabeledAddressClassifier::TConstPtr classifier, TMaybe<TInstant> /*netDataUpdateTimestamp*/) {
    NetClassifier = classifier;
}

void TProcessedRequestsAggregator::InitAddressClassifier(const NKikimrConfig::TSqsConfig& config, TVector<TString>&& labels) {
    TAddressClassifier addressClassifier;
    for (size_t i = 0; i < config.MeteringCloudNetCidrSize(); ++i) {
        addressClassifier.AddNetByCidrAndLabel(config.GetMeteringCloudNetCidr(i) , ENetworkClass::cloud);
    }

    for (size_t i = 0; i < config.MeteringYandexNetCidrSize(); ++i) {
        addressClassifier.AddNetByCidrAndLabel(config.GetMeteringYandexNetCidr(i) , ENetworkClass::yandex);
    }

    AddressClassifier = TLabeledAddressClassifier::MakeLabeledAddressClassifier(std::move(addressClassifier), std::move(labels));
}

class TMeteringActor
    : public TActorBootstrapped<TMeteringActor>
{
public:
    TMeteringActor()
        : HostFQDN(FQDNHostName())
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::Work);

        ctx.Send(NNetClassifier::MakeNetClassifierID(), new NNetClassifier::TEvNetClassifier::TEvSubscribe);

        Aggregator = MakeHolder<TProcessedRequestsAggregator>(Cfg());

        FlushProcessedRequestsAttributes();
    }

    void WriteLogRecord(const NSc::TValue& record, TStringBuilder& records) const {
        records << record.ToJsonSafe() << "\n";
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_METERING_ACTOR;
    }

    void FlushProcessedRequestsAttributes() {
        Y_ABORT_UNLESS(Aggregator);

        Schedule(TDuration::MilliSeconds(Cfg().GetMeteringFlushingIntervalMs()), new TEvWakeup());

        const auto reportedTrafficRecords = Aggregator->DumpReportedTrafficAsJsonArray(HostFQDN, TActivationContext::Now());
        const auto reportedRequestsRecords = Aggregator->DumpReportedRequestsAsJsonArray(HostFQDN, TActivationContext::Now());

        if (Cfg().GetMeteringLogFilePath()) {
            TStringBuilder records;

            for (const auto& trafficRecord : reportedTrafficRecords) {
                WriteLogRecord(trafficRecord, records);
            }
            for (const auto& requestRecord : reportedRequestsRecords) {
                WriteLogRecord(requestRecord, records);
            }

            if (const TString packedRecords = records) {
                TLoggerOperator<TGlobalLog>::Log().Write(packedRecords.Data(), packedRecords.Size());
            }
        }

        Aggregator->ResetReportsStorage();
    }

    void HandleWakeup(TEvWakeup::TPtr&) {
        FlushProcessedRequestsAttributes();
    }

    void HandleReportProcessedRequestAttributes(TSqsEvents::TEvReportProcessedRequestAttributes::TPtr& ev) {
        Y_ABORT_UNLESS(Aggregator);

        Aggregator->Add(ev->Get()->Data);
    }

    STATEFN(Work) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup, HandleWakeup);
            hFunc(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate, HandleNetClassifierUpdate);
            hFunc(TSqsEvents::TEvReportProcessedRequestAttributes, HandleReportProcessedRequestAttributes);
        }
    }

private:
    void HandleNetClassifierUpdate(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev) {
        Aggregator->UpdateNetClassifier(ev->Get()->Classifier, ev->Get()->NetDataUpdateTimestamp);
    }

private:
    const TString HostFQDN;
    THolder<TProcessedRequestsAggregator> Aggregator;
};

IActor* CreateSqsMeteringService() { return new TMeteringActor(); }

} // namespace NKikimr::NSQS
