#include <ydb/core/blobstorage/group_selector/group_selector.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_http_request.h>
#include <ydb/core/mon/mon_impl.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

#include <thread>

using namespace NKikimr;
using namespace NActors;

namespace {

struct TRequest : THttpRequestMock {
    HTTP_METHOD Method = HTTP_METHOD_GET;

    HTTP_METHOD GetMethod() const override {
        return Method;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(BlobStorageGroupSelector) {
    Y_UNIT_TEST(MissingFileKeepsOriginalBehavior) {
        TTempDir dir;
        const TString path = dir.Path().Child("missing.conf").GetPath();
        TBlobStorageGroupSelector selector(path);
        UNIT_ASSERT(!selector.Matches(123, "test"));
        UNIT_ASSERT_VALUES_EQUAL(selector.GetRules(), "");
        TString error;
        UNIT_ASSERT(selector.AddRules("*", &error));
        UNIT_ASSERT(selector.Matches(123, ""));
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), "*\n");
        UNIT_ASSERT(TBlobStorageGroupSelector(path).Matches(123, ""));
    }

    Y_UNIT_TEST(StartupRulesAndPersistedAdditions) {
        TTempDir dir;
        const TString path = dir.Path().Child("rules.conf").GetPath();
        const TString initial = " # comment\r\n g00042 \r\n g0\ng4294967295\npemergency-\n\n";
        {
            TFileOutput file(path);
            file << initial;
        }
        TBlobStorageGroupSelector selector(path);
        UNIT_ASSERT(selector.Matches(0, ""));
        UNIT_ASSERT(selector.Matches(42, "other"));
        UNIT_ASSERT(selector.Matches(Max<ui32>(), "other"));
        UNIT_ASSERT(!selector.Matches(43, "other"));
        UNIT_ASSERT(selector.Matches(43, "emergency-"));
        UNIT_ASSERT(selector.Matches(43, "emergency-ssd"));
        UNIT_ASSERT(!selector.Matches(43, "tenant-emergency-ssd"));
        UNIT_ASSERT(!selector.Matches(43, "Emergency-ssd"));
        UNIT_ASSERT(!selector.Matches(43, "emergency"));
        UNIT_ASSERT(!selector.Matches(43, ""));
        TString error;
        UNIT_ASSERT(selector.AddRules("g42\ng43\npextra-", &error));
        UNIT_ASSERT_VALUES_EQUAL(selector.GetRules(), "g0\ng42\ng43\ng4294967295\npemergency-\npextra-\n");
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), selector.GetRules());
        TBlobStorageGroupSelector restarted(path);
        UNIT_ASSERT(restarted.Matches(43, "extra-ssd"));
        UNIT_ASSERT(restarted.Matches(42, "other"));
    }

    Y_UNIT_TEST(StartupFileErrorsAreRejected) {
        TTempDir dir;
        const TString path = dir.Path().Child("rules.conf").GetPath();
        {
            TFileOutput file(path);
            file << "g42\np\n";
        }
        UNIT_ASSERT_EXCEPTION(TBlobStorageGroupSelector{path}, yexception);
        UNIT_ASSERT_EXCEPTION(TBlobStorageGroupSelector{dir.Path().GetPath()}, TFileError);
    }

    Y_UNIT_TEST(InvalidAdditionIsAtomic) {
        TTempDir dir;
        const TString path = dir.Path().Child("rules.conf").GetPath();
        TBlobStorageGroupSelector selector(path);
        TString error;
        UNIT_ASSERT(selector.AddRules("g42", &error));
        for (const TString invalid : {"g", "g-1", "g4294967296", "g1x", "p", "**", "x123"}) {
            UNIT_ASSERT(!selector.AddRules(TString("g43\n") + invalid, &error));
            UNIT_ASSERT(!error.empty());
            UNIT_ASSERT_VALUES_EQUAL(selector.GetRules(), "g42\n");
            UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), "g42\n");
            UNIT_ASSERT(!selector.Matches(43, "other"));
        }
        UNIT_ASSERT(selector.AddRules("*", &error));
        UNIT_ASSERT(error.empty());
        UNIT_ASSERT(selector.Matches(1, ""));
    }

    Y_UNIT_TEST(FailedUpdatePreservesLastLoadedRules) {
        TTempDir dir;
        const TFsPath path = dir.Path().Child("rules.conf");
        const TFsPath backup = dir.Path().Child("original.conf");
        TBlobStorageGroupSelector selector(path.GetPath());
        TString error;
        UNIT_ASSERT(selector.AddRules("g42", &error));
        path.RenameTo(backup);
        path.MkDir();
        UNIT_ASSERT(!selector.AddRules("g43", &error));
        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT_VALUES_EQUAL(selector.GetRules(), "g42\n");
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(backup.GetPath()).ReadAll(), "g42\n");
        path.DeleteIfExists();
        backup.RenameTo(path);
        UNIT_ASSERT(selector.AddRules("g43", &error));
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path.GetPath()).ReadAll(), "g42\ng43\n");
    }

#if defined(_linux_)
    Y_UNIT_TEST(SaveFailurePreservesFileAndCache) {
        TTempDir dir;
        const TString path = dir.Path().Child("rules.conf").GetPath();
        {
            TFileOutput output(path);
            output << "g42\n";
        }
        TFile file(path, OpenExisting | RdOnly);
        // /proc exposes the readable file but cannot create a sibling temporary file, even as root.
        TBlobStorageGroupSelector selector(TStringBuilder() << "/proc/self/fd/" << file.GetHandle());
        TString error;
        UNIT_ASSERT(!selector.AddRules("g43", &error));
        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT_VALUES_EQUAL(selector.GetRules(), "g42\n");
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), "g42\n");
    }
#endif

    Y_UNIT_TEST(ConcurrentReadersAndAdditions) {
        TTempDir dir;
        const TString path = dir.Path().Child("rules.conf").GetPath();
        TBlobStorageGroupSelector selector(path);
        std::vector<std::thread> threads;
        for (ui32 i = 0; i < 4; ++i) {
            threads.emplace_back([&, i] {
                TString error;
                for (ui32 j = 0; j < 10; ++j) {
                    UNIT_ASSERT(selector.AddRules(TStringBuilder() << 'g' << i * 10 + j, &error));
                    UNIT_ASSERT(selector.Matches(i * 10 + j, ""));
                    UNIT_ASSERT(!selector.GetRules().empty());
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        TBlobStorageGroupSelector restarted(path);
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), selector.GetRules());
        for (ui32 i = 0; i < 40; ++i) {
            UNIT_ASSERT(selector.Matches(i, ""));
            UNIT_ASSERT(restarted.Matches(i, ""));
        }
    }

    Y_UNIT_TEST(MonitoringActorShowsAndAddsRules) {
        TTempDir dir;
        const TString path = dir.Path().Child("rules.conf").GetPath();
        TMon mon(TMon::TConfig{});
        auto* index = mon.RegisterIndexPage("actors", "Actors");
        TTestActorSystem runtime(1);
        runtime.SetupNodeSubSystems = [&](ui32, TActorSystemSetup* setup) {
            setup->RegisterSubSystem(std::make_unique<TBlobStorageGroupSelector>(path, &mon));
        };
        runtime.Start();
        runtime.Sim([&] { return !index->FindPage("blobstorage_group_selector"); });
        auto* page = dynamic_cast<TActorMonPage*>(index->FindPage("blobstorage_group_selector"));
        UNIT_ASSERT(page);
        auto* selector = runtime.GetNode(1)->ActorSystem->GetSubSystem<TBlobStorageGroupSelector>();
        UNIT_ASSERT(selector);
        auto request = [&](HTTP_METHOD method, const TString& rules) {
            TRequest http;
            http.Method = method;
            http.CgiParameters.InsertUnescaped("action", "add");
            http.CgiParameters.InsertUnescaped("rules", rules);
            NMonitoring::TMonService2HttpRequest monRequest(nullptr, &http, nullptr, nullptr, "", nullptr);
            const TActorId edge = runtime.AllocateEdgeActor(1);
            runtime.Send(new IEventHandle(page->TargetActorId, edge, new NMon::TEvHttpInfo(monRequest), 0, 123), 1);
            const auto reply = runtime.WaitForEdgeActorEvent<NMon::TEvHttpInfoRes>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, 123);
            return reply->Get()->Answer;
        };
        UNIT_ASSERT(request(HTTP_METHOD_GET, "*").Contains("(empty)"));
        UNIT_ASSERT(!selector->Matches(1, "test"));
        UNIT_ASSERT(!TFsPath(path).Exists());
        const TString html = request(HTTP_METHOD_POST, "g42\np<script>");
        UNIT_ASSERT(selector->Matches(42, ""));
        UNIT_ASSERT(selector->Matches(1, "<script>pool"));
        UNIT_ASSERT(html.Contains("&lt;script&gt;"));
        UNIT_ASSERT(!html.Contains("p<script>"));
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), selector->GetRules());
        UNIT_ASSERT(TBlobStorageGroupSelector(path).Matches(42, ""));
        {
            TFileOutput file(path);
            file << "g43\npexternal-\n";
        }
        UNIT_ASSERT(request(HTTP_METHOD_GET, "").Contains("g43"));
        UNIT_ASSERT(selector->Matches(43, ""));
        UNIT_ASSERT(selector->Matches(1, "external-ssd"));
        UNIT_ASSERT(!selector->Matches(42, ""));
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), "g43\npexternal-\n");
        UNIT_ASSERT(request(HTTP_METHOD_POST, "g44\np").Contains("Invalid selection rule"));
        UNIT_ASSERT(!selector->Matches(44, ""));
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), "g43\npexternal-\n");
        {
            TFileOutput file(path);
            file << "g45\n";
        }
        UNIT_ASSERT(request(HTTP_METHOD_POST, "g44").Contains("Rules saved"));
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), "g44\ng45\n");
        {
            TFileOutput file(path);
            file << "p\n";
        }
        UNIT_ASSERT(request(HTTP_METHOD_GET, "").Contains("Could not read rules file"));
        UNIT_ASSERT(selector->Matches(44, ""));
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), "p\n");
        TFsPath(path).DeleteIfExists();
        UNIT_ASSERT(request(HTTP_METHOD_GET, "").Contains("(empty)"));
        UNIT_ASSERT(!selector->Matches(44, ""));
    }
}
