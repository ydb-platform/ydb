#include "fs_storage_config.h"
#include "s3_wrapper.h"

#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/wrappers/events/common.h>
#include <ydb/core/wrappers/events/get_object.h>
#include <ydb/core/wrappers/events/object_exists.h>

#include <ydb/library/actors/core/log.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/file.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NWrappers;
using namespace NKikimr::NWrappers::NExternalStorage;
using namespace Aws::S3::Model;

class TFsStorageTestBase : public NUnitTest::TTestBase {
protected:
    void SetUp() override {
        TempDir = MakeHolder<TTempDir>();

        Runtime = MakeHolder<TTestBasicRuntime>();
        Runtime->Initialize(TAppPrepare().Unwrap());
        Runtime->SetLogPriority(NKikimrServices::FS_WRAPPER, NLog::PRI_DEBUG);

        NKikimrSchemeOp::TFSSettings settings;
        settings.SetBasePath(TempDir->Name());
        auto config = std::make_shared<TFsExternalStorageConfig>(settings);

        Wrapper = Runtime->Register(NWrappers::CreateStorageWrapper(config->ConstructStorageOperator()));
        Edge = Runtime->AllocateEdgeActor();
    }

    void TearDown() override {
        Runtime.Reset();
        TempDir.Reset();
    }

    TString KeyPath(const TString& name) const {
        return (TFsPath(TempDir->Name()) / name).GetPath();
    }

    template <typename TEvResponse>
    auto Send(IEventBase* ev) {
        Runtime->Send(new IEventHandle(Wrapper, Edge, ev));
        return Runtime->GrabEdgeEvent<TEvResponse>(Edge);
    }

    auto PutObject(const TString& key, TString body) {
        auto request = PutObjectRequest().WithKey(key);
        auto response = Send<TEvPutObjectResponse>(
            new TEvPutObjectRequest(request, std::move(body)));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto GetObject(const TString& key, const TString& range = "") {
        auto request = GetObjectRequest().WithKey(key);
        if (!range.empty()) {
            request.SetRange(range.c_str());
        }
        auto response = Send<TEvGetObjectResponse>(
            new TEvGetObjectRequest(request));
        UNIT_ASSERT(response->Get());
        return std::make_pair(response->Get()->Result, response->Get()->Body);
    }

    auto HeadObject(const TString& key) {
        auto request = HeadObjectRequest().WithKey(key);
        auto response = Send<TEvHeadObjectResponse>(
            new TEvHeadObjectRequest(request));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto CreateMultipartUpload(const TString& key) {
        auto request = CreateMultipartUploadRequest().WithKey(key);
        auto response = Send<TEvCreateMultipartUploadResponse>(
            new TEvCreateMultipartUploadRequest(request));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto UploadPart(const TString& key, const TString& uploadId, int partNumber, TString body) {
        auto request = UploadPartRequest()
            .WithKey(key)
            .WithUploadId(uploadId.c_str())
            .WithPartNumber(partNumber);
        auto response = Send<TEvUploadPartResponse>(
            new TEvUploadPartRequest(request, std::move(body)));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto CompleteMultipartUpload(const TString& key, const TString& uploadId) {
        auto request = CompleteMultipartUploadRequest()
            .WithKey(key)
            .WithUploadId(uploadId.c_str());
        auto response = Send<TEvCompleteMultipartUploadResponse>(
            new TEvCompleteMultipartUploadRequest(request));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto AbortMultipartUpload(const TString& key, const TString& uploadId) {
        auto request = AbortMultipartUploadRequest()
            .WithKey(key)
            .WithUploadId(uploadId.c_str());
        auto response = Send<TEvAbortMultipartUploadResponse>(
            new TEvAbortMultipartUploadRequest(request));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto DeleteObject(const TString& key) {
        auto request = DeleteObjectRequest().WithKey(key);
        auto response = Send<TEvDeleteObjectResponse>(
            new TEvDeleteObjectRequest(request));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto ListObjects(const TString& prefix = "") {
        auto request = ListObjectsRequest();
        if (!prefix.empty()) {
            request.SetPrefix(prefix.c_str());
        }
        auto response = Send<TEvListObjectsResponse>(
            new TEvListObjectsRequest(request));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto CheckObjectExists(const TString& key) {
        auto request = HeadObjectRequest().WithKey(key);
        auto response = Send<TEvCheckObjectExistsResponse>(
            new TEvCheckObjectExistsRequest(request));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto UploadPartCopy(const TString& key, const TString& uploadId, int partNumber, const TString& copySource) {
        auto request = UploadPartCopyRequest()
            .WithKey(key)
            .WithUploadId(uploadId.c_str())
            .WithPartNumber(partNumber)
            .WithCopySource(copySource.c_str());
        auto response = Send<TEvUploadPartCopyResponse>(
            new TEvUploadPartCopyRequest(request));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

protected:
    THolder<TTempDir> TempDir;
    THolder<TTestBasicRuntime> Runtime;
    TActorId Wrapper;
    TActorId Edge;
};

class TFsStorageTests : public TFsStorageTestBase {
    UNIT_TEST_SUITE(TFsStorageTests);

    UNIT_TEST(PutObjectDoesNotTruncateLockedFile);
    UNIT_TEST(PutObjectCreatesIntermediateDirectories);
    UNIT_TEST(PutObjectOverwritesExistingFile);
    UNIT_TEST(GetObjectReturnsCorrectBytes);
    UNIT_TEST(GetObjectWithRangeReturnsCorrectSlice);
    UNIT_TEST(GetObjectForNonExistentFileReturnsError);
    UNIT_TEST(GetObjectForEmptyFileReturnsEmptyBody);
    UNIT_TEST(GetObjectWithStartGreaterThanEndReturnsError);
    UNIT_TEST(HeadObjectReturnsCorrectContentLength);
    UNIT_TEST(HeadObjectForNonExistentFileReturnsNoSuchKey);
    UNIT_TEST(MultipartUploadFullCycleCreatesCorrectFile);
    UNIT_TEST(AbortMultipartUploadDeletesIncompleteFile);
    UNIT_TEST(DeleteObjectReturnsNotImplementedError);
    UNIT_TEST(ListObjectsReturnsNotImplementedError);
    UNIT_TEST(CheckObjectExistsReturnsNotImplementedError);
    UNIT_TEST(UploadPartCopyReturnsNotImplementedError);
    UNIT_TEST(ConcurrentMultipartUploadSessionsForSameKey);
    UNIT_TEST_SUITE_END();

public:
    void PutObjectDoesNotTruncateLockedFile() {
        const TString key = KeyPath("data.csv");
        const TVector<TString> originalContent = {"generation1.0", "generation1.1"};
        const TString newContent      = "generation2";

        {
            TFileOutput f(key);
            f.Write(originalContent[0]);
        }

        TFile externalLock(key, OpenExisting | RdWr);
        externalLock.Flock(LOCK_EX | LOCK_NB);

        {
            auto result = PutObject(key, newContent);
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT(result.GetError().ShouldRetry());

            TFileInput f(key);
            const TString actualContent = f.ReadAll();
            UNIT_ASSERT_VALUES_EQUAL(actualContent, originalContent[0]);
        }

        {
            TFileOutput f(key);
            f.Write(originalContent[1]);
        }

        externalLock.Flock(LOCK_UN);
        externalLock.Close();

        {
            auto result = PutObject(key, newContent);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());

            TFileInput f(key);
            UNIT_ASSERT_VALUES_EQUAL(f.ReadAll(), newContent);
        }
    }

    void PutObjectCreatesIntermediateDirectories() {
        const TString key = KeyPath("nested/path/to/file.txt");
        const TString content = "nested content";

        auto result = PutObject(key, content);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());

        UNIT_ASSERT(TFsPath(key).Exists());
        TFileInput f(key);
        UNIT_ASSERT_VALUES_EQUAL(f.ReadAll(), content);
    }

    void PutObjectOverwritesExistingFile() {
        const TString key = KeyPath("overwrite_test.txt");
        const TString originalContent = "original";
        const TString newContent = "new content";

        {
            auto result = PutObject(key, originalContent);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        {
            auto result = PutObject(key, newContent);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        TFileInput f(key);
        UNIT_ASSERT_VALUES_EQUAL(f.ReadAll(), newContent);
    }

    void GetObjectReturnsCorrectBytes() {
        const TString key = KeyPath("get_test.txt");
        const TString content = "test content for get";

        {
            auto result = PutObject(key, content);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        auto [result, body] = GetObject(key);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(body, content);
    }

    void GetObjectWithRangeReturnsCorrectSlice() {
        const TString key = KeyPath("range_test.txt");
        const TString content = "0123456789";

        {
            auto result = PutObject(key, content);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        {
            auto [result, body] = GetObject(key, "bytes=2-5");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(body, "2345");
        }

        {
            auto [result, body] = GetObject(key, "bytes=0-2");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(body, "012");
        }

        {
            auto [result, body] = GetObject(key, "bytes=7-9");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(body, "789");
        }
    }

    void GetObjectForNonExistentFileReturnsError() {
        const TString key = KeyPath("non_existent_file.txt");

        auto [result, body] = GetObject(key);
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(body, "");
    }

    void GetObjectForEmptyFileReturnsEmptyBody() {
        const TString key = KeyPath("empty_file.txt");

        {
            auto result = PutObject(key, "");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        auto [result, body] = GetObject(key);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(body, "");
    }

    void GetObjectWithStartGreaterThanEndReturnsError() {
        const TString key = KeyPath("range_error_test.txt");
        const TString content = "0123456789";

        {
            auto result = PutObject(key, content);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        auto [result, body] = GetObject(key, "bytes=5-2");
        UNIT_ASSERT(!result.IsSuccess());
    }

    void HeadObjectReturnsCorrectContentLength() {
        const TString key = KeyPath("head_test.txt");
        const TString content = "test content";

        {
            auto result = PutObject(key, content);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        auto result = HeadObject(key);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResult().GetContentLength(), static_cast<i64>(content.size()));
    }

    void HeadObjectForNonExistentFileReturnsNoSuchKey() {
        const TString key = KeyPath("non_existent_head.txt");

        auto result = HeadObject(key);
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL((int)result.GetError().GetErrorType(), (int)Aws::S3::S3Errors::NO_SUCH_KEY);
    }

    void MultipartUploadFullCycleCreatesCorrectFile() {
        const TString key = KeyPath("mpu_test.txt");
        const TString part1 = "part1_content_";
        const TString part2 = "part2_content_";
        const TString part3 = "part3_content";
        const TString expectedContent = part1 + part2 + part3;
        const TString incompleteKey = key + ".incomplete";

        TString uploadId;

        {
            auto result = CreateMultipartUpload(key);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            uploadId = result.GetResult().GetUploadId();
            UNIT_ASSERT(!uploadId.empty());
        }

        {
            auto result = UploadPart(key, uploadId, 1, part1);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        UNIT_ASSERT(TFsPath(incompleteKey).Exists());
        UNIT_ASSERT(!TFsPath(key).Exists());

        {
            auto result = UploadPart(key, uploadId, 2, part2);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        UNIT_ASSERT(TFsPath(incompleteKey).Exists());
        UNIT_ASSERT(!TFsPath(key).Exists());

        {
            auto result = UploadPart(key, uploadId, 3, part3);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        UNIT_ASSERT(TFsPath(incompleteKey).Exists());
        UNIT_ASSERT(!TFsPath(key).Exists());

        {
            auto result = CompleteMultipartUpload(key, uploadId);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        UNIT_ASSERT(TFsPath(key).Exists());
        UNIT_ASSERT(!TFsPath(incompleteKey).Exists());
        TFileInput f(key);
        UNIT_ASSERT_VALUES_EQUAL(f.ReadAll(), expectedContent);
    }

    void AbortMultipartUploadDeletesIncompleteFile() {
        const TString key = KeyPath("abort_mpu_test.txt");
        const TString incompleteKey = key + ".incomplete";

        TString uploadId;

        {
            auto result = CreateMultipartUpload(key);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            uploadId = result.GetResult().GetUploadId();
        }

        {
            auto result = UploadPart(key, uploadId, 1, "some data");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        UNIT_ASSERT(TFsPath(incompleteKey).Exists());

        {
            auto result = AbortMultipartUpload(key, uploadId);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        UNIT_ASSERT(!TFsPath(incompleteKey).Exists());
        UNIT_ASSERT(!TFsPath(key).Exists());
    }

    void DeleteObjectReturnsNotImplementedError() {
        const TString key = KeyPath("delete_test.txt");

        auto result = DeleteObject(key);
        UNIT_ASSERT(!result.IsSuccess());
    }

    void ListObjectsReturnsNotImplementedError() {
        auto result = ListObjects();
        UNIT_ASSERT(!result.IsSuccess());
    }

    void CheckObjectExistsReturnsNotImplementedError() {
        const TString key = KeyPath("check_exists_test.txt");

        auto result = CheckObjectExists(key);
        UNIT_ASSERT(!result.IsSuccess());
    }

    void UploadPartCopyReturnsNotImplementedError() {
        const TString key = KeyPath("upload_part_copy_test.txt");

        auto result = UploadPartCopy(key, "fake_upload_id", 1, "source_key");
        UNIT_ASSERT(!result.IsSuccess());
    }

    void ConcurrentMultipartUploadSessionsForSameKey() {
        const TString key = KeyPath("concurrent_mpu_test.txt");
        const TString incompleteKey = key + ".incomplete";
        const TString part1Content = "first_session_data";
        const TString part2Content = "second_session_data";

        TString uploadId1;
        {
            auto result = CreateMultipartUpload(key);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            uploadId1 = result.GetResult().GetUploadId();
            UNIT_ASSERT(!uploadId1.empty());
        }

        {
            auto result = UploadPart(key, uploadId1, 1, part1Content);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        UNIT_ASSERT(TFsPath(incompleteKey).Exists());

        {
            auto result = CreateMultipartUpload(key);
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT(result.GetError().ShouldRetry());
        }

        {
            auto result = CompleteMultipartUpload(key, uploadId1);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        UNIT_ASSERT(!TFsPath(incompleteKey).Exists());
        UNIT_ASSERT(TFsPath(key).Exists());

        TString uploadId2;
        {
            auto result = CreateMultipartUpload(key);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
            uploadId2 = result.GetResult().GetUploadId();
            UNIT_ASSERT(!uploadId2.empty());
        }

        {
            auto result = UploadPart(key, uploadId2, 1, part2Content);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        {
            auto result = CompleteMultipartUpload(key, uploadId2);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetError().GetMessage());
        }

        UNIT_ASSERT(TFsPath(key).Exists());
        UNIT_ASSERT(!TFsPath(incompleteKey).Exists());

        TFileInput f(key);
        UNIT_ASSERT_VALUES_EQUAL(f.ReadAll(), part2Content);

        TVector<TString> files;
        TFsPath(TempDir->Name()).ListNames(files);
        int finalFileCount = 0;
        for (const auto& file : files) {
            if (file.find("concurrent_mpu_test") != TString::npos) {
                ++finalFileCount;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(finalFileCount, 1);
    }
};

UNIT_TEST_SUITE_REGISTRATION(TFsStorageTests);
