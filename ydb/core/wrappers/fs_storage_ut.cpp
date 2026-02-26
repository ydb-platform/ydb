#include "fs_storage_config.h"
#include "s3_wrapper.h"

#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/wrappers/events/common.h>

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

protected:
    THolder<TTempDir> TempDir;
    THolder<TTestBasicRuntime> Runtime;
    TActorId Wrapper;
    TActorId Edge;
};

class TFsStorageTests : public TFsStorageTestBase {
    UNIT_TEST_SUITE(TFsStorageTests);

    UNIT_TEST(PutObjectDoesNotTruncateLockedFile);
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
};

UNIT_TEST_SUITE_REGISTRATION(TFsStorageTests);
