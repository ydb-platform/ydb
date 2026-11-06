#include <ydb/apps/bridge_skipper/runtime/state_file.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

#include <limits>
#include <sys/stat.h>
#include <unistd.h>

namespace NBridgeSkipper {
namespace {

TString StatePath(const TTempDir& dir) {
    return TStringBuilder() << dir.Name() << "/state.json";
}

TString TemporaryPath(const TTempDir& dir) {
    return TStringBuilder() << dir.Name() << "/state.json.tmp";
}

TPersistentState WithPending() {
    TPersistentState state;
    state.ClusterId = "cluster";
    state.GenerationHighWater = std::numeric_limits<ui64>::max();
    state.PendingMutation = TPendingMutation{42, {{"", EPileState::Primary}, {"dc-b", EPileState::Disconnected}}};
    return state;
}

} // namespace

Y_UNIT_TEST_SUITE(TSkipperStateFile) {
    Y_UNIT_TEST(CreatesDirectoryAndInitialState) {
        TTempDir parent;
        const TString directory = TStringBuilder() << parent.Name() << "/new-state";
        const auto file = TStateFile::Open(directory, "cluster");
        UNIT_ASSERT_VALUES_EQUAL(file->GetState().ClusterId, "cluster");
        UNIT_ASSERT_VALUES_EQUAL(file->GetState().GenerationHighWater, 0);
        UNIT_ASSERT(!file->GetState().PendingMutation);
        struct stat info;
        UNIT_ASSERT_VALUES_EQUAL(stat((directory + "/state.json").c_str(), &info), 0);
    }

    Y_UNIT_TEST(RoundTripPreservesPendingAndFullWidthGeneration) {
        TTempDir dir;
        const auto expected = WithPending();
        {
            const auto file = TStateFile::Open(dir.Name(), "cluster");
            UNIT_ASSERT_VALUES_EQUAL(file->Write(expected), "");
            UNIT_ASSERT(file->GetState() == expected);
        }
        const auto reopened = TStateFile::Open(dir.Name(), "cluster");
        UNIT_ASSERT(reopened->GetState() == expected);
        const TString json = TFileInput(StatePath(dir)).ReadAll();
        UNIT_ASSERT(json.Contains("\"18446744073709551615\""));
        UNIT_ASSERT(json.Contains("\"expected_generation\":\"42\""));
    }

    Y_UNIT_TEST(ClusterBindingAndExclusiveLockAreEnforced) {
        TTempDir dir;
        auto file = TStateFile::Open(dir.Name(), "cluster");
        UNIT_ASSERT_EXCEPTION_CONTAINS(TStateFile::Open(dir.Name(), "cluster"), yexception, "locked");
        file.reset();
        UNIT_ASSERT_EXCEPTION_CONTAINS(TStateFile::Open(dir.Name(), "other"), yexception, "cluster_id");
        UNIT_ASSERT_NO_EXCEPTION(TStateFile::Open(dir.Name(), "cluster"));
    }

    Y_UNIT_TEST(ClearPendingPreservesClusterAndHighWaterAcrossRestart) {
        TTempDir dir;
        auto expected = WithPending();
        {
            const auto file = TStateFile::Open(dir.Name(), "cluster");
            UNIT_ASSERT_VALUES_EQUAL(file->Write(expected), "");
            UNIT_ASSERT_VALUES_EQUAL(file->ClearPending(), "");
            expected.PendingMutation.reset();
            UNIT_ASSERT(file->GetState() == expected);
            UNIT_ASSERT_VALUES_EQUAL(file->ClearPending(), "");
        }
        UNIT_ASSERT(TStateFile::Open(dir.Name(), "cluster")->GetState() == expected);
    }

    Y_UNIT_TEST(CorruptStateFailsClosedWithoutReplacingIt) {
        for (const TString& corrupt : {
                TString(), TString("{bad json"), TString("{}"),
                TString(R"({"cluster_id":"cluster","generation_high_water":1,"pending_mutation":null})"),
                TString(R"({"cluster_id":"cluster","generation_high_water":"-1","pending_mutation":null})"),
                TString(R"({"cluster_id":"cluster","generation_high_water":"18446744073709551616","pending_mutation":null})"),
                TString(R"({"cluster_id":"cluster","generation_high_water":"1","pending_mutation":null,"extra":true})"),
                TString(R"({"cluster_id":"cluster","generation_high_water":"1","pending_mutation":{"expected_generation":"1","update":{}}})"),
                TString(R"({"cluster_id":"cluster","generation_high_water":"1","pending_mutation":{"expected_generation":"1","update":{"a":"DISCONNECTED"},"first_sent_at":"2026-01-01T00:00:00Z"}})"),
                TString(R"({"cluster_id":"cluster","generation_high_water":"1","pending_mutation":{"expected_generation":1,"update":{"a":"DISCONNECTED"}}})"),
                TString(R"({"cluster_id":"cluster","generation_high_water":"1","pending_mutation":[]})"),
                TString(1024 * 1024 + 1, ' '),
                TString(R"({"cluster_id":"cluster","generation_high_water":"1","pending_mutation":{"expected_generation":"1","update":{"a":"FUTURE"}}})")}) {
            TTempDir dir;
            TFileOutput(StatePath(dir)).Write(corrupt);
            UNIT_ASSERT_EXCEPTION(TStateFile::Open(dir.Name(), "cluster"), yexception);
            UNIT_ASSERT_VALUES_EQUAL(TFileInput(StatePath(dir)).ReadAll(), corrupt);
        }
    }

    Y_UNIT_TEST(PendingGenerationAboveHighWaterIsCorrupt) {
        TTempDir dir;
        const TString corrupt = R"({"cluster_id":"cluster","generation_high_water":"7","pending_mutation":{"expected_generation":"8","update":{"a":"DISCONNECTED"}}})";
        TFileOutput(StatePath(dir)).Write(corrupt);
        UNIT_ASSERT_EXCEPTION_CONTAINS(TStateFile::Open(dir.Name(), "cluster"), yexception, "exceeds generation_high_water");
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(StatePath(dir)).ReadAll(), corrupt);
    }

    Y_UNIT_TEST(WriteFailurePreservesOldFileAndMemory) {
        // A directory at the temporary path fails the write deterministically,
        // also for privileged runners that ignore permission bits.
        TTempDir dir;
        const auto file = TStateFile::Open(dir.Name(), "cluster");
        const auto before = file->GetState();
        const TString bytes = TFileInput(StatePath(dir)).ReadAll();
        UNIT_ASSERT_VALUES_EQUAL(mkdir(TemporaryPath(dir).c_str(), 0700), 0);
        UNIT_ASSERT(!file->Write(WithPending()).empty());
        UNIT_ASSERT(file->GetState() == before);
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(StatePath(dir)).ReadAll(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(rmdir(TemporaryPath(dir).c_str()), 0);
        UNIT_ASSERT_VALUES_EQUAL(file->Write(WithPending()), "");
        UNIT_ASSERT(file->GetState() == WithPending());
    }
}

} // namespace NBridgeSkipper
