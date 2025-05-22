#include <ydb/core/ymq/actor/serviceid.h>

#include <ydb/core/ymq/http/types.h>
#include <ydb/core/ymq/actor/events.h>
#include <ydb/core/ymq/actor/metering.h>

#include <ydb/core/mind/address_classification/net_classifier.h>
#include <ydb/core/testlib/test_client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/logger/global/global.h>

#include <util/generic/serialized_enum.h>
#include <util/stream/file.h>

namespace NKikimr::NSQS {

using namespace Tests;

bool IsSameRecords(const NSc::TValue& a, const NSc::TValue& b) {
    auto cmp = [&](const TString& key, const TString& subkey = "") {
        auto& va = a.Get(key);
        auto& vb = b.Get(key);
        if (subkey) {
            return NSc::TValue::Equal(va.Get(subkey), vb.Get(subkey));
        }
        return NSc::TValue::Equal(va, vb);
    };
    return cmp("folder_id") && cmp("resource_id") && cmp("schema") && cmp("version")
        && cmp("usage", "type") && cmp("usage", "unit") && cmp("usage", "quantity")
        && cmp("tags", "direction") && cmp("tags", "type");
}

TVector<NSc::TValue>::const_iterator MatchBillingRecord(const TVector<NSc::TValue>& records, const NSc::TValue& expectedRecord) {
    for (auto it = records.begin(); it != records.end(); ++it) {
        if (IsSameRecords(*it, expectedRecord)) {
            return it;
        }
    }
    return records.end();
}

void CheckBillingRecord(const TVector<NSc::TValue>& records, const TVector<NSc::TValue>& expectedRecords) {
    UNIT_ASSERT_VALUES_EQUAL(records.size(), expectedRecords.size());
    TVector<NSc::TValue> unmatchedRecords = records;
    for (auto& record : expectedRecords) {
        auto it = MatchBillingRecord(unmatchedRecords, record);
        UNIT_ASSERT(unmatchedRecords.end() != it);
        unmatchedRecords.erase(it);
    }
}

TVector<NSc::TValue> LoadBillingRecords(const TString& filepath) {
    TString data = TFileInput(filepath).ReadAll();
    auto rawRecords = SplitString(data, "\n");
    TVector<NSc::TValue> records;
    for (auto& record : rawRecords) {
        records.push_back(NSc::TValue::FromJson(record));
    }
    return records;
}

void AddRequestToQueue(
    TList<THolder<TSqsEvents::TEvReportProcessedRequestAttributes>>& requests,
    const TString sourceAddress,
    ui32 requestSizeInBytes,
    ui32 responseSizeInBytes,
    int statusCode,
    const TString& folderId = "",
    const TString& resourceId = "",
    bool isFifo = false
) {
    auto request = MakeHolder<TSqsEvents::TEvReportProcessedRequestAttributes>();
    TProcessedRequestAttributes& requestAttributes = request->Data;
    requestAttributes.HttpStatusCode = statusCode;
    requestAttributes.IsFifo = isFifo;
    requestAttributes.FolderId = folderId;
    requestAttributes.RequestSizeInBytes = requestSizeInBytes;
    requestAttributes.ResponseSizeInBytes = responseSizeInBytes;
    requestAttributes.SourceAddress = sourceAddress;
    requestAttributes.ResourceId = resourceId;
    requestAttributes.Action = EAction::ModifyPermissions;

    requests.push_back(std::move(request));
}

auto AddToExpectedRecords(TVector<NSc::TValue>& expectedRecords, const TProcessedRequestAttributes& attrs, TProcessedRequestsAggregator::ENetworkClass networkClass) {
    {
        NSc::TValue tags;
        tags.SetDict();
        tags["type"] = ToString(networkClass);
        tags["direction"] = ToString(TProcessedRequestsAggregator::ETrafficType::ingress);
        expectedRecords.push_back(CreateMeteringBillingRecord(
            attrs.FolderId, attrs.ResourceId,
            "ymq.traffic.v1", "" /*fqdn*/, TInstant::Now(), attrs.RequestSizeInBytes, "byte", tags,
            attrs.QueueTags
        ));
    }
    {
        NSc::TValue tags;
        tags.SetDict();
        tags["type"] = ToString(networkClass);
        tags["direction"] = ToString(TProcessedRequestsAggregator::ETrafficType::egress);
        expectedRecords.push_back(CreateMeteringBillingRecord(
            attrs.FolderId, attrs.ResourceId,
            "ymq.traffic.v1", "" /*fqdn*/, TInstant::Now(), attrs.ResponseSizeInBytes, "byte", tags,
            attrs.QueueTags
        ));
    }
    {
        NSc::TValue tags;
        tags.SetDict();
        tags["queue_type"] = ToString(TProcessedRequestsAggregator::EQueueType::other);
        expectedRecords.push_back(CreateMeteringBillingRecord(
            attrs.FolderId, attrs.ResourceId,
            "ymq.requests.v1", "" /*fqdn*/, TInstant::Now(), attrs.RequestSizeInBytes, "request", tags,
            attrs.QueueTags
        ));
    }
}

Y_UNIT_TEST_SUITE(Metering) {
    Y_UNIT_TEST(BillingRecords) {
        TServerSettings serverSettings(0);
        TServer server = TServer(serverSettings, true);
        TTestActorRuntime* runtime = server.GetRuntime();

        auto& sqsConfig = runtime->GetAppData().SqsConfig;

        sqsConfig.SetMeteringFlushingIntervalMs(100);
        sqsConfig.SetMeteringLogFilePath("sqs_meterig.log");
        TFsPath(sqsConfig.GetMeteringLogFilePath()).DeleteIfExists();

        sqsConfig.AddMeteringCloudNetCidr("5.45.196.0/24");
        sqsConfig.AddMeteringCloudNetCidr("2a0d:d6c0::/29");
        sqsConfig.AddMeteringYandexNetCidr("127.0.0.0/8");
        sqsConfig.AddMeteringYandexNetCidr("5.45.217.0/24");

        DoInitGlobalLog(CreateOwningThreadedLogBackend(sqsConfig.GetMeteringLogFilePath(), 0));

        runtime->RegisterService(
            MakeSqsMeteringServiceID(),
            runtime->Register(NSQS::CreateSqsMeteringService())
        );

        TList<THolder<TSqsEvents::TEvReportProcessedRequestAttributes>> requests;
        TVector<NSc::TValue> expectedRecords;



        AddRequestToQueue(requests, "5.45.196.0", 1, 2, 200, "folder1");
        AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::cloud);
        AddRequestToQueue(requests, "5.45.196.0", 10, 20, 500, "folder1");
        AddRequestToQueue(requests, "5.45.196.0", 100, 200, 200);

        AddRequestToQueue(requests, "5.45.217.0", 1, 2, 200, "folder2");
        AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::yandex);
        AddRequestToQueue(requests, "5.45.217.0", 10, 20, 500, "folder2");
        AddRequestToQueue(requests, "5.45.217.0", 100, 200, 200);

        AddRequestToQueue(requests, "5.45.215.192", 1, 2, 200, "folder3");
        AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::inet);
        AddRequestToQueue(requests, "5.45.215.192", 10, 20, 500, "folder3");
        AddRequestToQueue(requests, "5.45.215.192", 100, 200, 200);

        for (auto& req : requests) {
            runtime->Send(new IEventHandle(
                MakeSqsMeteringServiceID(), runtime->AllocateEdgeActor(), req.Release()
            ));
        }

        Sleep(TDuration::Seconds(5));
        TVector<NSc::TValue> records = LoadBillingRecords(sqsConfig.GetMeteringLogFilePath());
        CheckBillingRecord(records, expectedRecords);
    }

    Y_UNIT_TEST(MockedNetClassifierOnly) {
        TServerSettings serverSettings(0);
        TServer server = TServer(serverSettings, true);
        TTestActorRuntime* runtime = server.GetRuntime();

        auto& sqsConfig = runtime->GetAppData().SqsConfig;

        sqsConfig.SetMeteringByNetClassifierOnly(true);
        sqsConfig.SetMeteringFlushingIntervalMs(100);
        sqsConfig.SetMeteringLogFilePath("sqs_meterig.log");
        TFsPath(sqsConfig.GetMeteringLogFilePath()).DeleteIfExists();

        sqsConfig.AddMeteringCloudNetCidr("5.45.196.0/24");
        sqsConfig.AddMeteringYandexNetCidr("5.45.217.0/24");

        DoInitGlobalLog(CreateOwningThreadedLogBackend(sqsConfig.GetMeteringLogFilePath(), 0));

        runtime->RegisterService(
            MakeSqsMeteringServiceID(),
            runtime->Register(NSQS::CreateSqsMeteringService())
        );

        TList<THolder<TSqsEvents::TEvReportProcessedRequestAttributes>> requests;
        TVector<NSc::TValue> expectedRecords;

        {
            AddRequestToQueue(requests, "5.45.196.0", 1, 2, 200, "folder1");
            AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::unknown);
            AddRequestToQueue(requests, "5.45.196.0", 10, 20, 500, "folder1");
            AddRequestToQueue(requests, "5.45.196.0", 100, 200, 200);

            AddRequestToQueue(requests, "5.45.217.0", 1, 2, 200, "folder2");
            AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::unknown);
            AddRequestToQueue(requests, "5.45.217.0", 10, 20, 500, "folder2");
            AddRequestToQueue(requests, "5.45.217.0", 100, 200, 200);

            AddRequestToQueue(requests, "5.45.215.192", 1, 2, 200, "folder3");
            AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::unknown);
            AddRequestToQueue(requests, "5.45.215.192", 10, 20, 500, "folder3");
            AddRequestToQueue(requests, "5.45.215.192", 100, 200, 200);
        }

        for (auto& req : requests) {
            runtime->Send(new IEventHandle(
                MakeSqsMeteringServiceID(), runtime->AllocateEdgeActor(), req.Release()
            ));
        }

        Sleep(TDuration::Seconds(5));
        TVector<NSc::TValue> records = LoadBillingRecords(sqsConfig.GetMeteringLogFilePath());
        CheckBillingRecord(records, expectedRecords);

        {
            NAddressClassifier::TAddressClassifier rawClassifier;
            rawClassifier.AddNetByCidrAndLabel("127.0.0.0/8" , TProcessedRequestsAggregator::ENetworkClass::cloud);
            rawClassifier.AddNetByCidrAndLabel("2a0d:d6c0::/29" , TProcessedRequestsAggregator::ENetworkClass::yandex);

            auto enumValues = GetEnumNames<TProcessedRequestsAggregator::ENetworkClass>();
            TVector<TString> labels(enumValues.size());
            for (auto enumItem : enumValues) {
                Y_ABORT_UNLESS(enumItem.first < labels.size());
                labels[enumItem.first] = enumItem.second;
            }
            auto classifier = NAddressClassifier::TLabeledAddressClassifier::MakeLabeledAddressClassifier(std::move(rawClassifier), std::move(labels));


            auto classifierUpdateRequest = MakeHolder<NNetClassifier::TEvNetClassifier::TEvClassifierUpdate>();
            classifierUpdateRequest->Classifier = classifier;
            classifierUpdateRequest->NetDataUpdateTimestamp = TInstant::Now();
            runtime->Send(new IEventHandle(
                MakeSqsMeteringServiceID(), runtime->AllocateEdgeActor(), classifierUpdateRequest.Release()
            ));
        }
        Sleep(TDuration::MilliSeconds(100));
        {
            requests.clear();
            AddRequestToQueue(requests, "5.45.196.0", 1, 2, 200, "folder4");
            AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::inet);
            AddRequestToQueue(requests, "5.45.196.0", 10, 20, 500, "folder4");
            AddRequestToQueue(requests, "5.45.196.0", 100, 200, 200);

            AddRequestToQueue(requests, "5.45.217.0", 1, 2, 200, "folder5");
            AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::inet);
            AddRequestToQueue(requests, "5.45.217.0", 10, 20, 500, "folder5");
            AddRequestToQueue(requests, "5.45.217.0", 100, 200, 200);

            AddRequestToQueue(requests, "5.45.215.192", 1, 2, 200, "folder6");
            AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::inet);
            AddRequestToQueue(requests, "5.45.215.192", 10, 20, 500, "folder6");
            AddRequestToQueue(requests, "5.45.215.192", 100, 200, 200);

            AddRequestToQueue(requests, "127.255.255.255", 1, 2, 200, "folder7");
            AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::cloud);
            AddRequestToQueue(requests, "127.255.255.255", 10, 20, 500, "folder7");
            AddRequestToQueue(requests, "127.255.255.255", 100, 200, 200);

            AddRequestToQueue(requests, "2a0d:d6c0:bbbb:bbbb:bbbb:bbbb:bbbb:bbbb", 1, 2, 200, "folder8");
            AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::yandex);
            AddRequestToQueue(requests, "2a0d:d6c0:bbbb:bbbb:bbbb:bbbb:bbbb:bbbb", 10, 20, 500, "folder8");
            AddRequestToQueue(requests, "2a0d:d6c0:bbbb:bbbb:bbbb:bbbb:bbbb:bbbb", 100, 200, 200);
        }
        for (auto& req : requests) {
            runtime->Send(new IEventHandle(
                MakeSqsMeteringServiceID(), runtime->AllocateEdgeActor(), req.Release()
            ));
        }

        Sleep(TDuration::Seconds(5));
        records = LoadBillingRecords(sqsConfig.GetMeteringLogFilePath());
        CheckBillingRecord(records, expectedRecords);
    }

    Y_UNIT_TEST(MockedNetClassifierLabelTransformation) {
        TServerSettings serverSettings(0);
        TServer server = TServer(serverSettings, true);
        TTestActorRuntime* runtime = server.GetRuntime();

        auto& sqsConfig = runtime->GetAppData().SqsConfig;

        sqsConfig.SetMeteringByNetClassifierOnly(true);
        sqsConfig.SetMeteringFlushingIntervalMs(100);
        sqsConfig.SetMeteringLogFilePath("sqs_meterig.log");
        TFsPath(sqsConfig.GetMeteringLogFilePath()).DeleteIfExists();

        DoInitGlobalLog(CreateOwningThreadedLogBackend(sqsConfig.GetMeteringLogFilePath(), 0));

        runtime->RegisterService(
            MakeSqsMeteringServiceID(),
            runtime->Register(NSQS::CreateSqsMeteringService())
        );
        Sleep(TDuration::MilliSeconds(500)); // waiting to override classifier from real NetClassifier service
        {
            NAddressClassifier::TAddressClassifier rawClassifier;
            TVector<std::pair<TString, TString>> cidrWithLabel = {
                {"127.0.0.0/8", ToString(TProcessedRequestsAggregator::ENetworkClass::yandex)},
                {"2a0d:d6c0::/29", ToString(TProcessedRequestsAggregator::ENetworkClass::cloud)},
                {"5.45.196.0/24", "yacloud"}
            };
            TVector<TString> labels(cidrWithLabel.size());
            for (ui32 index = 0; index < cidrWithLabel.size(); ++index) {
                auto& [cidr, label] = cidrWithLabel[index];
                rawClassifier.AddNetByCidrAndLabel(cidr , index);
                labels[index] = label;
            }
            auto classifier = NAddressClassifier::TLabeledAddressClassifier::MakeLabeledAddressClassifier(std::move(rawClassifier), std::move(labels));

            auto classifierUpdateRequest = MakeHolder<NNetClassifier::TEvNetClassifier::TEvClassifierUpdate>();
            classifierUpdateRequest->Classifier = classifier;
            classifierUpdateRequest->NetDataUpdateTimestamp = TInstant::Now();
            runtime->Send(new IEventHandle(
                MakeSqsMeteringServiceID(), runtime->AllocateEdgeActor(), classifierUpdateRequest.Release()
            ));
        }
        Sleep(TDuration::MilliSeconds(100));
        TList<THolder<TSqsEvents::TEvReportProcessedRequestAttributes>> requests;
        TVector<NSc::TValue> expectedRecords;
        {
            AddRequestToQueue(requests, "127.255.255.255", 1, 2, 200, "folder1");
            AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::yandex);

            AddRequestToQueue(requests, "2a0d:d6c0:bbbb:bbbb:bbbb:bbbb:bbbb:bbbb", 1, 2, 200, "folder2");
            AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::cloud);

            AddRequestToQueue(requests, "5.45.196.0", 1, 2, 200, "folder3");
            AddToExpectedRecords(expectedRecords, requests.back()->Data, TProcessedRequestsAggregator::ENetworkClass::cloud);
        }
        for (auto& req : requests) {
            runtime->Send(new IEventHandle(
                MakeSqsMeteringServiceID(), runtime->AllocateEdgeActor(), req.Release()
            ));
        }

        Sleep(TDuration::Seconds(5));
        auto records = LoadBillingRecords(sqsConfig.GetMeteringLogFilePath());
        CheckBillingRecord(records, expectedRecords);
    }
}

} // NKikimr::NSQS
