#include "fs_storage_config.h"
#include "s3_wrapper.h"

#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/wrappers/events/common.h>
#include <ydb/core/wrappers/events/get_object.h>

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

// ---------------------------------------------------------------------------
// Base test fixture
// ---------------------------------------------------------------------------

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

        Wrapper = Runtime->Register(NWrappers::CreateS3Wrapper(config->ConstructStorageOperator()));
        Edge = Runtime->AllocateEdgeActor();
    }

    void TearDown() override {
        // Do NOT send explicit Poison here: Runtime.Reset() destroys all actors
        // via the actor system cleanup path where TlsActivationContext is null,
        // so TFsExternalStorage::Shutdown() exits early (the null-check guards it).
        // Sending Poison first and then immediately calling Reset() races with the
        // runtime destructor, causing Shutdown() to Send() to an actor that is
        // concurrently being destroyed → SIGSEGV.
        Runtime.Reset();
        TempDir.Reset();
    }

    // Absolute path inside the temp directory that is used as the S3 "key"
    // (fs_storage uses the key directly as a filesystem path).
    TString KeyPath(const TString& name) const {
        return (TFsPath(TempDir->Name()) / name).GetPath();
    }

    template <typename TEvResponse>
    auto Send(IEventBase* ev) {
        Runtime->Send(new IEventHandle(Wrapper, Edge, ev));
        return Runtime->GrabEdgeEvent<TEvResponse>(Edge);
    }

    // --- request helpers ---

    auto PutObject(const TString& key, TString body) {
        auto request = PutObjectRequest().WithKey(key);
        auto response = Send<TEvPutObjectResponse>(
            new TEvPutObjectRequest(request, std::move(body)));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto HeadObject(const TString& key) {
        auto request = HeadObjectRequest().WithKey(key);
        auto response = Send<TEvHeadObjectResponse>(
            new TEvHeadObjectRequest(request));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    // Returns {Result, Body}
    auto GetObject(const TString& key) {
        auto request = GetObjectRequest().WithKey(key);
        auto response = Send<TEvGetObjectResponse>(
            new TEvGetObjectRequest(request));
        UNIT_ASSERT(response->Get());
        return std::make_pair(response->Get()->Result, response->Get()->Body);
    }

    auto GetObjectRange(const TString& key, ui64 from, ui64 to) {
        auto request = GetObjectRequest()
            .WithKey(key)
            .WithRange(TStringBuilder() << "bytes=" << from << "-" << to);
        auto response = Send<TEvGetObjectResponse>(
            new TEvGetObjectRequest(request));
        UNIT_ASSERT(response->Get());
        return std::make_pair(response->Get()->Result, response->Get()->Body);
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
            .WithUploadId(uploadId)
            .WithPartNumber(partNumber);
        auto response = Send<TEvUploadPartResponse>(
            new TEvUploadPartRequest(request, std::move(body)));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto CompleteMultipartUpload(const TString& key, const TString& uploadId,
                                 const TVector<CompletedPart>& parts)
    {
        auto request = CompleteMultipartUploadRequest()
            .WithKey(key)
            .WithUploadId(uploadId)
            .WithMultipartUpload(CompletedMultipartUpload().WithParts(parts));
        auto response = Send<TEvCompleteMultipartUploadResponse>(
            new TEvCompleteMultipartUploadRequest(request));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

    auto AbortMultipartUpload(const TString& key, const TString& uploadId) {
        auto request = AbortMultipartUploadRequest()
            .WithKey(key)
            .WithUploadId(uploadId);
        auto response = Send<TEvAbortMultipartUploadResponse>(
            new TEvAbortMultipartUploadRequest(request));
        UNIT_ASSERT(response->Get());
        return response->Get()->Result;
    }

protected:
    THolder<TTempDir> TempDir;
    THolder<TTestBasicRuntime> Runtime;
    TActorId Wrapper;
    TActorId Edge;
};

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

class TFsStorageTests : public TFsStorageTestBase {
    UNIT_TEST_SUITE(TFsStorageTests);

    // Group 2 – race / lock tests
    UNIT_TEST(PutObjectDoesNotTruncateLockedFile);
    UNIT_TEST(PutObjectReturnsRetryableErrorWhenFileLocked);
    UNIT_TEST(MultipartUploadReturnsRetryableErrorWhenFileLocked);

    UNIT_TEST_SUITE_END();

public:
    // -----------------------------------------------------------------------
    // Group 2 – race / lock tests
    // -----------------------------------------------------------------------

    // TC-2.1: BUG-1 — PutObject must NOT truncate a file before acquiring the lock.
    //
    // Scenario: "generation 1" holds the file open with LOCK_EX (simulates a DataShard
    // that is still writing).  "Generation 2" sends a PutObject to the same key.
    //
    // Before the fix (CreateAlways | WrOnly): the file was truncated to zero at open
    // time, BEFORE flock — so generation 1's data was silently destroyed.
    //
    // After the fix (OpenAlways | WrOnly | ForAppend + Flock + Resize(0)): the file
    // is not touched until the lock is held, so generation 1's data survives.
    void PutObjectDoesNotTruncateLockedFile() {
        const TString key = KeyPath("data.csv");
        const TString originalContent = "generation1_data_long_enough_to_notice_truncation";
        const TString newContent      = "generation2_short";

        // Step 1: write "generation1" content directly (simulates a completed previous write)
        {
            TFileOutput f(key);
            f.Write(originalContent);
        }

        // Step 2: acquire an external exclusive lock (simulates a DataShard generation
        // that still holds the file open while exporting)
        TFile externalLock(key, OpenExisting | RdWr);
        externalLock.Flock(LOCK_EX | LOCK_NB);

        // Step 3-4: PutObject from generation 2 — must fail with a retryable error
        auto result = PutObject(key, newContent);
        UNIT_ASSERT_C(!result.IsSuccess(),
            "Expected PutObject to fail while file is locked by external holder");
        UNIT_ASSERT_C(result.GetError().ShouldRetry(),
            "Expected retryable EWOULDBLOCK error, got: " << result.GetError().GetMessage());

        // Step 5 — KEY CHECK: file content must NOT have been truncated.
        // Before the fix PutObject used CreateAlways which truncates at open time
        // (before flock), destroying generation 1's data even though the lock
        // was never granted.
        {
            TFileInput f(key);
            const TString actualContent = f.ReadAll();
            UNIT_ASSERT_VALUES_EQUAL_C(
                actualContent, originalContent,
                "BUG-1 regression: file was truncated before lock was acquired!"
                " actual_size=" << actualContent.size()
                    << " expected_size=" << originalContent.size());
        }

        // Step 6: release the external lock
        externalLock.Flock(LOCK_UN);
        externalLock.Close();

        // Step 7: retry PutObject — must now succeed
        auto result2 = PutObject(key, newContent);
        UNIT_ASSERT_C(result2.IsSuccess(), result2.GetError().GetMessage());

        // Step 8: verify the new content is written correctly
        {
            TFileInput f(key);
            UNIT_ASSERT_VALUES_EQUAL(f.ReadAll(), newContent);
        }
    }

    // TC-2.2: error code returned when file is locked must be retryable
    void PutObjectReturnsRetryableErrorWhenFileLocked() {
        const TString key = KeyPath("locked.csv");

        // Create the file first so flock can open it
        {
            TFileOutput f(key);
            f.Write("existing");
        }

        TFile externalLock(key, OpenExisting | RdWr);
        externalLock.Flock(LOCK_EX | LOCK_NB);

        auto result = PutObject(key, "new_data");
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_C(result.GetError().ShouldRetry(),
            "Lock contention must produce a retryable error so the caller backs off and retries");

        externalLock.Flock(LOCK_UN);
        externalLock.Close();
    }

    // TC-2.3: CreateMultipartUpload must also return a retryable error when the
    // .incomplete file is already locked (e.g. previous generation still writing)
    void MultipartUploadReturnsRetryableErrorWhenFileLocked() {
        const TString key            = KeyPath("mpu.csv");
        const TString incompletePath = key + ".incomplete";

        // Pre-create .incomplete and lock it
        {
            TFileOutput f(incompletePath);
            f.Write("in_progress_data");
        }
        TFile externalLock(incompletePath, OpenExisting | RdWr);
        externalLock.Flock(LOCK_EX | LOCK_NB);

        auto result = CreateMultipartUpload(key);
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_C(result.GetError().ShouldRetry(),
            "Lock contention on .incomplete must produce a retryable error");

        externalLock.Flock(LOCK_UN);
        externalLock.Close();

        // After releasing the lock a new MPU session must succeed
        {
            auto result2 = CreateMultipartUpload(key);
            UNIT_ASSERT_C(result2.IsSuccess(), result2.GetError().GetMessage());

            const TString uploadId = result2.GetResult().GetUploadId().c_str();
            TVector<CompletedPart> parts;

            auto up = UploadPart(key, uploadId, 1, "data");
            UNIT_ASSERT_C(up.IsSuccess(), up.GetError().GetMessage());
            parts.push_back(CompletedPart().WithPartNumber(1).WithETag(up.GetResult().GetETag()));

            auto cmpl = CompleteMultipartUpload(key, uploadId, parts);
            UNIT_ASSERT_C(cmpl.IsSuccess(), cmpl.GetError().GetMessage());
        }

        auto [getResult, body] = GetObject(key);
        UNIT_ASSERT_C(getResult.IsSuccess(), getResult.GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(body, "data");
    }
};

UNIT_TEST_SUITE_REGISTRATION(TFsStorageTests);
