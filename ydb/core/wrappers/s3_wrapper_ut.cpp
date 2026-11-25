#include "s3_storage_config.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/library/aws_init/aws.h>

#include <ydb/library/actors/core/log.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/string/printf.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NWrappers;
using namespace Aws::S3::Model;

namespace {

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    NKikimr::InitAwsAPI();
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
    NKikimr::ShutdownAwsAPI();
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

        TS3Mock::TSettings settings(*Port);
        settings.ErrorResponse = [](TStringBuf method, TStringBuf path) -> TString {
            if (method == "GET" && path.EndsWith("/short-range")) {
                return "HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 0-1/2\r\n\r\nab";
            }
            if (method == "GET" && path.EndsWith("/oversized-range")) {
                return "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nabcdefgh";
            }
            return {};
        };
        S3Mock = MakeHolder<TS3Mock>(settings);
        Y_ABORT_UNLESS(S3Mock->Start());

        Runtime = MakeHolder<TTestBasicRuntime>();
        Runtime->Initialize(TAppPrepare().Unwrap());
        Runtime->SetLogPriority(NKikimrServices::S3_WRAPPER, NLog::PRI_DEBUG);
        NWrappers::IExternalStorageConfig::TPtr config = std::make_shared<NExternalStorage::TS3ExternalStorageConfig>(Aws::Auth::AWSCredentials(), MakeClientConfig(*Port), "TEST", nullptr);
        Wrapper = Runtime->Register(NWrappers::CreateStorageWrapper(config->ConstructStorageOperator()));
    }

    void TearDown() override {
        Runtime.Reset();
        S3Mock.Reset();
        Port = Nothing();
        Edge = Nothing();
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

    auto ListObjects(const TString& prefix) {
        auto request = ListObjectsRequest().WithBucket("").WithPrefix(prefix);
        auto response = Send<NExternalStorage::TEvListObjectsResponse>(
            new NExternalStorage::TEvListObjectsRequest(request));

        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto DeleteObjects(const TString& firstKey, const TString& secondKey) {
        auto request = DeleteObjectsRequest().WithBucket("").WithDelete(Aws::S3::Model::Delete()
            .AddObjects(ObjectIdentifier().WithKey(firstKey))
            .AddObjects(ObjectIdentifier().WithKey(secondKey)));
        auto response = Send<NExternalStorage::TEvDeleteObjectsResponse>(
            new NExternalStorage::TEvDeleteObjectsRequest(request));

        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    void CheckNormalizedKeys(const TString& rawPrefix, const TString& prefix) {
        const TString firstKey = prefix + "1/a/b/1";
        const TString secondKey = prefix + "1/c/d/2";
        const TString body = "body";

        // An object written by the old point-request implementation must remain readable.
        const auto oldPut = PutObject(rawPrefix + "1/a/b/1", TString(body));
        UNIT_ASSERT_C(oldPut.IsSuccess(), oldPut.GetError().GetMessage());
        const auto newPut = PutObject(secondKey, TString(body));
        UNIT_ASSERT_C(newPut.IsSuccess(), newPut.GetError().GetMessage());

        const auto [get, bytes] = GetObject(firstKey, body.size());
        UNIT_ASSERT_C(get.IsSuccess(), get.GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(bytes, body);

        const auto list = ListObjects(prefix);
        UNIT_ASSERT_C(list.IsSuccess(), list.GetError().GetMessage());
        const auto& contents = list.GetResult().GetContents();
        UNIT_ASSERT_VALUES_EQUAL(contents.size(), 2);
        THashSet<TString> listed;
        for (const auto& item : contents) {
            listed.emplace(item.GetKey());
        }
        UNIT_ASSERT(listed.contains(firstKey));
        UNIT_ASSERT(listed.contains(secondKey));

        const auto deleted = DeleteObjects(firstKey, secondKey);
        UNIT_ASSERT_C(deleted.IsSuccess(), deleted.GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(deleted.GetResult().GetDeleted().size(), 2);
        const auto after = ListObjects(prefix);
        UNIT_ASSERT_C(after.IsSuccess(), after.GetError().GetMessage());
        UNIT_ASSERT(after.GetResult().GetContents().empty());
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

    void NormalizesEmptyKeySegments() {
        CheckNormalizedKeys("/vg/", "vg/");
        CheckNormalizedKeys("archive/team//vg/", "archive/team/vg/");
    }

    void RejectsInvalidRangeResponseLength() {
        for (const char *key : {"short-range", "oversized-range"}) {
            auto [result, body] = GetObject(key, 4);
            UNIT_ASSERT_C(!result.IsSuccess(), key);
            UNIT_ASSERT_VALUES_EQUAL(TString(result.GetError().GetExceptionName()), "InvalidRangeResponse");
            UNIT_ASSERT(result.GetError().ShouldRetry());
            UNIT_ASSERT(body.empty());
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
    UNIT_TEST(NormalizesEmptyKeySegments);
    UNIT_TEST(RejectsInvalidRangeResponseLength);
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
