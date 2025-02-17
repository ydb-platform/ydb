#include "s3_storage_config.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>

#include <ydb/library/actors/core/log.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>

#include <aws/core/Aws.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NWrappers;
using namespace Aws::S3::Model;

namespace {

Aws::SDKOptions Options;

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    Aws::InitAPI(Options);
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
    Aws::ShutdownAPI(Options);
}

}

class TS3MockTest: public NUnitTest::TTestBase {
    using TS3Mock = NWrappers::NTestHelpers::TS3Mock;

    static auto MakeClientConfig(ui16 port) {
        Aws::Client::ClientConfiguration config;

        config.endpointOverride = Sprintf("localhost:%hu", port);
        config.verifySSL = false;
        config.connectTimeoutMs = 10000;
        config.maxConnections = 5;
        config.scheme = Aws::Http::Scheme::HTTP;

        return config;
    }

public:
    void SetUp() override {
        Y_ABORT_UNLESS(!Port.Defined());
        Port = PortManager.GetPort();

        S3Mock = MakeHolder<TS3Mock>(TS3Mock::TSettings(*Port));
        Y_ABORT_UNLESS(S3Mock->Start());

        Runtime = MakeHolder<TTestBasicRuntime>();
        Runtime->Initialize(TAppPrepare().Unwrap());
        Runtime->SetLogPriority(NKikimrServices::S3_WRAPPER, NLog::PRI_DEBUG);
        NWrappers::IExternalStorageConfig::TPtr config = std::make_shared<NExternalStorage::TS3ExternalStorageConfig>(Aws::Auth::AWSCredentials(), MakeClientConfig(*Port), "TEST");
        Wrapper = Runtime->Register(NWrappers::CreateS3Wrapper(config->ConstructStorageOperator()));
    }

    void TearDown() override {
        Runtime.Reset();
        S3Mock.Reset();
    }

    ui16 GetPort() const {
        UNIT_ASSERT(Port.Defined());
        return *Port;
    }

    template <typename TEvResponse>
    auto Send(IEventBase* ev) {
        if (!Edge) {
            Edge = Runtime->AllocateEdgeActor();
        }

        Runtime->Send(new IEventHandle(Wrapper, *Edge, ev));
        return Runtime->GrabEdgeEvent<TEvResponse>(*Edge);
    }

private:
    TPortManager PortManager;
    TMaybe<ui16> Port;
    THolder<TS3Mock> S3Mock;
    THolder<TTestBasicRuntime> Runtime;
    TActorId Wrapper;
    TMaybe<TActorId> Edge;

}; // TS3MockTest

class TS3WrapperTests: public TS3MockTest {
    auto PutObject(const TString& key, TString&& body) {
        auto request = PutObjectRequest().WithBucket("").WithKey(key);
        auto response = Send<NExternalStorage::TEvPutObjectResponse>(
            new NExternalStorage::TEvPutObjectRequest(request, std::move(body)));

        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto HeadObject(const TString& key) {
        auto request = HeadObjectRequest().WithBucket("").WithKey(key);
        auto response = Send<NExternalStorage::TEvHeadObjectResponse>(
            new NExternalStorage::TEvHeadObjectRequest(request));

        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto GetObject(const TString& key, ui64 bodySize) {
        auto request = GetObjectRequest()
            .WithBucket("")
            .WithKey(key)
            .WithRange(Sprintf("bytes=0-%" PRIu64, bodySize - 1));
        auto response = Send<NExternalStorage::TEvGetObjectResponse>(
            new NExternalStorage::TEvGetObjectRequest(request));

        UNIT_ASSERT(response->Get());
        return std::make_pair(response->Get()->Result, response->Get()->Body);
    }

    auto CreateMultipartUpload(const TString& key) {
        auto request = CreateMultipartUploadRequest().WithBucket("").WithKey(key);
        auto response = Send<NExternalStorage::TEvCreateMultipartUploadResponse>(
            new NExternalStorage::TEvCreateMultipartUploadRequest(request));

        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto CompleteMultipartUpload(const TString& key, const TString& uploadId, const TVector<CompletedPart>& parts) {
        auto request = CompleteMultipartUploadRequest()
            .WithBucket("")
            .WithKey(key)
            .WithUploadId(uploadId)
            .WithMultipartUpload(CompletedMultipartUpload().WithParts(parts));
        auto response = Send<NExternalStorage::TEvCompleteMultipartUploadResponse>(
            new NExternalStorage::TEvCompleteMultipartUploadRequest(request));

        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto AbortMultipartUpload(const TString& key, const TString& uploadId) {
        auto request = AbortMultipartUploadRequest()
            .WithBucket("")
            .WithKey(key)
            .WithUploadId(uploadId);
        auto response = Send<NExternalStorage::TEvAbortMultipartUploadResponse>(
            new NExternalStorage::TEvAbortMultipartUploadRequest(request));

        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto UploadPart(const TString& key, const TString& uploadId, int partNumber, TString&& body) {
        auto request = UploadPartRequest()
            .WithBucket("")
            .WithKey(key)
            .WithUploadId(uploadId)
            .WithPartNumber(partNumber);
        auto response = Send<NExternalStorage::TEvUploadPartResponse>(
            new NExternalStorage::TEvUploadPartRequest(request, std::move(body)));

        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto UploadPartCopy(const TString& sourceBucket, const TString& copySource, const TString& key, const TString& uploadId, int partNumber, int fromBytes, int toBytes) {
        auto request = UploadPartCopyRequest()
            .WithBucket("")
            .WithCopySource(TString("/") + sourceBucket + "/" + copySource)
            .WithKey(key)
            .WithCopySourceRange(TString("bytes=") + ToString(fromBytes) + "-" + ToString(toBytes))
            .WithUploadId(uploadId)
            .WithPartNumber(partNumber);
        auto response = Send<NExternalStorage::TEvUploadPartCopyResponse>(
            new NExternalStorage::TEvUploadPartCopyRequest(request));

        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

public:
    void PutObject() {
        auto result = PutObject("key", "body");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
    }

    void HeadObject() {
        const TString body = "body";

        {
            auto result = PutObject("key", TString(body));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        {
            auto result = HeadObject("key");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().GetETag(), MD5::Data(body));
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().GetContentLength(), body.size());
        }
    }

    void GetObject() {
        const TString body = "body";

        {
            auto result = PutObject("key", TString(body));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        {
            auto [result, actualBody] = GetObject("key", body.size());
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(actualBody, body);
        }
    }

    void MultipartUpload() {
        const TString body = "body";
        TString uploadId;
        TVector<CompletedPart> parts;

        {
            auto result = CreateMultipartUpload("key");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            uploadId = result.GetResult().GetUploadId();
        }

        {
            auto result = UploadPart("key", uploadId, 1, TString(body));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            parts.push_back(CompletedPart().WithPartNumber(1).WithETag(result.GetResult().GetETag()));
        }

        {
            auto result = CompleteMultipartUpload("key", uploadId, parts);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        {
            auto [result, actualBody] = GetObject("key", body.size());
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(actualBody, body);
        }
    }

    void AbortMultipartUpload() {
        TString uploadId;

        {
            auto result = CreateMultipartUpload("key");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            uploadId = result.GetResult().GetUploadId();
        }

        {
            auto result = AbortMultipartUpload("key", uploadId);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        {
            auto result = HeadObject("key");
            UNIT_ASSERT(!result.IsSuccess());
        }
    }

    void HeadUnknownObject() {
        auto result = HeadObject("key");
        UNIT_ASSERT(!result.IsSuccess());
    }

    void GetUnknownObject() {
        auto [result, actualBody] = GetObject("key", 4);
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(actualBody, "");
    }

    void UploadUnknownPart() {
        auto result = UploadPart("key", "uploadId", 1, "body");
        UNIT_ASSERT(!result.IsSuccess());
    }

    void CompleteUnknownUpload() {
        auto result = CompleteMultipartUpload("key", "uploadId", {
            CompletedPart().WithPartNumber(1).WithETag("ETag"),
        });
        UNIT_ASSERT(!result.IsSuccess());
    }

    void AbortUnknownUpload() {
        auto result = AbortMultipartUpload("key", "uploadId");
        UNIT_ASSERT(!result.IsSuccess());
    }

    void CopyPartUpload() {
        const TString body = "body";

        {
            auto result = PutObject("key", TString(body));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        TString uploadId;
        TVector<CompletedPart> parts;

        {
            auto result = CreateMultipartUpload("key1");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            uploadId = result.GetResult().GetUploadId();
        }

        {
            auto result = UploadPartCopy("TEST", "key", "key1", uploadId, 1, 1, 2);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            parts.push_back(CompletedPart().WithPartNumber(1).WithETag(result.GetResult().GetCopyPartResult().GetETag()));
        }

        {
            auto result = CompleteMultipartUpload("key1", uploadId, parts);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        {
            auto [result, actualBody] = GetObject("key1", 2);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(actualBody, body.substr(1, 2));
        }
    }

private:
    UNIT_TEST_SUITE(TS3WrapperTests);
    UNIT_TEST(PutObject);
    UNIT_TEST(HeadObject);
    UNIT_TEST(GetObject);
    UNIT_TEST(MultipartUpload);
    UNIT_TEST(AbortMultipartUpload);
    UNIT_TEST(HeadUnknownObject);
    UNIT_TEST(GetUnknownObject);
    UNIT_TEST(UploadUnknownPart);
    UNIT_TEST(CompleteUnknownUpload);
    UNIT_TEST(AbortUnknownUpload);
    UNIT_TEST(CopyPartUpload);
    UNIT_TEST_SUITE_END();

}; // TS3WrapperTests

UNIT_TEST_SUITE_REGISTRATION(TS3WrapperTests);
