#include "ut_helpers.h"

#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/yaml_config/yaml_config_helpers.h>

namespace NKikimr::NConsole {
namespace {

using namespace NUT;

TTenantTestConfig ConsoleTestConfig() {
    TTenantTestConfig config;
    config.Domains = {{DOMAIN1_NAME, SCHEME_SHARD1_ID, {}}};
    config.HiveId = HIVE_ID;
    config.FakeTenantSlotBroker = true;
    config.FakeSchemeShard = false;
    config.CreateConsole = true;
    TTenantTestConfig::TNodeConfig node;
    node.TenantPoolConfig.StaticSlots = {{DOMAIN1_NAME, {1, 1, 1}}};
    node.TenantPoolConfig.NodeType = "type1";
    config.Nodes = {node};
    config.DataCenterCount = 1;
    return config;
}

TString MakeYamlConfig(ui64 version, TStringBuf value) {
    return TStringBuilder() << "metadata:\n"
        << "  kind: MainConfig\n"
        << "  cluster: \"\"\n"
        << "  version: " << version << "\n"
        << "config:\n"
        << "  log_config:\n"
        << "    cluster_name: " << value << "\n";
}

struct TSession {
    TActorId Sender;
    TActorId Pipe;
};

TSession CreateSession(TTenantTestRuntime& runtime, TActorId sender = {}) {
    if (!sender) {
        sender = runtime.AllocateEdgeActor();
    }
    return {sender, runtime.ConnectToPipe(MakeConsoleID(), sender, 0, GetPipeConfigWithRetries())};
}

auto Propose(TTenantTestRuntime& runtime, const TSession& session, bool distconf) {
    auto request = MakeHolder<TEvBlobStorage::TEvControllerProposeConfigRequest>();
    request->Record.SetDistconf(distconf);
    runtime.SendToPipe(session.Pipe, session.Sender, request.Release());
    return runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerProposeConfigResponse>(session.Sender);
}

void SendCommit(TTenantTestRuntime& runtime, const TSession& session, const TString& yaml, ui64 cookie = 0) {
    auto request = MakeHolder<TEvBlobStorage::TEvControllerConsoleCommitRequest>();
    request->Record.SetYAML(yaml);
    runtime.SendToPipe(session.Pipe, session.Sender, request.Release(), 0, cookie);
}

auto GrabCommit(TTenantTestRuntime& runtime, const TSession& session, ui64 cookie = 0) {
    auto response = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConsoleCommitResponse>(session.Sender);
    UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
    return response;
}

TActorId Subscribe(TTenantTestRuntime& runtime) {
    const auto subscriber = runtime.AllocateEdgeActor();
    auto request = MakeHolder<TEvConsole::TEvConfigSubscriptionRequest>();
    request->Record.SetGeneration(1);
    request->Record.MutableOptions()->SetNodeId(runtime.GetNodeId());
    request->Record.SetServeYaml(true);
    request->Record.SetYamlApiVersion(1);
    request->Record.AddConfigItemKinds(NKikimrConsole::TConfigItem::LogConfigItem);
    runtime.SendToPipe(MakeConsoleID(), subscriber, request.Release(), 0, GetPipeConfigWithRetries());
    return subscriber;
}

void CheckNotification(TTenantTestRuntime& runtime, const TActorId& subscriber, const TString& expected) {
    auto notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(subscriber);
    UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetMainYamlConfig(), expected);
}

void CheckStoredConfig(TTenantTestRuntime& runtime, const TString& expected) {
    TString stored;
    FetchMainConfigFromConsole(runtime, stored);
    UNIT_ASSERT_VALUES_EQUAL(stored, expected);
}

void TestDelayedCommit(bool distconf, bool sameSender, bool replaceSession) {
    TTenantTestRuntime runtime(ConsoleTestConfig());
    const auto oldSession = CreateSession(runtime);
    Propose(runtime, oldSession, distconf);

    const auto subscriber = Subscribe(runtime);
    CheckNotification(runtime, subscriber, {});

    const auto oldConfig = MakeYamlConfig(0, "old-session");
    const auto newConfig = MakeYamlConfig(0, "new-session");
    const auto expected = NYamlConfig::UpgradeMainConfigVersion(newConfig);
    auto notifications = runtime.AddObserver<TEvConsole::TEvConfigSubscriptionNotification>([&](auto& ev) {
        if (ev->Get()->Record.HasMainYamlConfig()) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetMainYamlConfig(), expected);
        }
    });
    TBlockEvents<TEvConsole::TEvSetYamlConfigRequest> delayedCommit(runtime,
        [&](const auto& ev) {
            return ev->Get()->Record.GetRequest().config() == oldConfig;
        });

    SendCommit(runtime, oldSession, oldConfig, 123);
    runtime.WaitFor("old commit accepted and its YAML write delayed", [&] { return !delayedCommit.empty(); });

    auto newSession = oldSession;
    if (replaceSession) {
        newSession = CreateSession(runtime, sameSender ? oldSession.Sender : TActorId());
        Propose(runtime, newSession, distconf);
    }
    SendCommit(runtime, newSession, newConfig, 456);
    auto committed = GrabCommit(runtime, newSession, 456);
    UNIT_ASSERT_C(committed->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvControllerConsoleCommitResponse::Committed,
        committed->Get()->Record.ShortDebugString());
    CheckStoredConfig(runtime, expected);
    CheckNotification(runtime, subscriber, expected);

    delayedCommit.Stop().Unblock();
    auto stale = GrabCommit(runtime, oldSession, 123);
    CheckStoredConfig(runtime, expected);
    CheckNotification(runtime, Subscribe(runtime), expected);
    UNIT_ASSERT_C(stale->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvControllerConsoleCommitResponse::SessionMismatch,
        stale->Get()->Record.ShortDebugString());

    GracefulRestartTablet(runtime, MakeConsoleID(), runtime.AllocateEdgeActor());
    CheckStoredConfig(runtime, expected);
}

}

Y_UNIT_TEST_SUITE(TConsoleHandshakeTest) {
    Y_UNIT_TEST(DelayedCommitRejectedAfterSessionReplacement) {
        for (bool distconf : {false, true}) {
            for (bool sameSender : {false, true}) {
                TestDelayedCommit(distconf, sameSender, true);
            }
        }
    }

    Y_UNIT_TEST(DelayedCommitRejectedAfterNewerCommitInSameSession) {
        for (bool distconf : {false, true}) {
            TestDelayedCommit(distconf, true, false);
        }
    }

    Y_UNIT_TEST(QueuedCommitRejectedAfterSessionReplacement) {
        for (bool distconf : {false, true}) {
            TTenantTestRuntime runtime(ConsoleTestConfig());
            const auto oldSession = CreateSession(runtime);
            Propose(runtime, oldSession, distconf);

            runtime.SendToConsole(new TEvConsole::TEvGetConfigRequest());
            auto current = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetConfigResponse>(runtime.Sender);
            auto config = current->Get()->Record.GetConfig();
            config.MutableConfigsConfig()->MutableValidationOptions()->SetMaxConfigChecksPerModification(12345);

            TBlockEvents<TEvBlobStorage::TEvPut> blockedCommits(runtime, [](const auto& ev) {
                return ev->Get()->Id.TabletID() == MakeConsoleID() && ev->Get()->Id.Channel() == 0;
            });
            runtime.SendToConsole(new TEvConsole::TEvSetConfigRequest(config));
            runtime.WaitFor("Console transaction awaiting commit", [&] { return !blockedCommits.empty(); });

            const auto oldConfig = MakeYamlConfig(0, "queued-old-session");
            bool setRequestObserved = false;
            auto observer = runtime.AddObserver<TEvConsole::TEvSetYamlConfigRequest>([&](auto& ev) {
                if (ev->Get()->Record.GetRequest().config() == oldConfig) {
                    setRequestObserved = true;
                }
            });
            SendCommit(runtime, oldSession, oldConfig);
            runtime.WaitFor("old commit transaction queued", [&] { return setRequestObserved; });
            const auto newSession = CreateSession(runtime);
            Propose(runtime, newSession, distconf);

            blockedCommits.Stop().Unblock();
            auto setConfig = runtime.GrabEdgeEventRethrow<TEvConsole::TEvSetConfigResponse>(runtime.Sender);
            UNIT_ASSERT_VALUES_EQUAL(setConfig->Get()->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            auto stale = GrabCommit(runtime, oldSession);
            UNIT_ASSERT_C(stale->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvControllerConsoleCommitResponse::SessionMismatch,
                stale->Get()->Record.ShortDebugString());

            const auto newConfig = MakeYamlConfig(0, "new-session");
            SendCommit(runtime, newSession, newConfig);
            auto committed = GrabCommit(runtime, newSession);
            UNIT_ASSERT_C(committed->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvControllerConsoleCommitResponse::Committed,
                committed->Get()->Record.ShortDebugString());
            CheckStoredConfig(runtime, NYamlConfig::UpgradeMainConfigVersion(newConfig));
        }
    }

    Y_UNIT_TEST(IndependentClientSessionDoesNotInvalidateCommit) {
        for (bool distconf : {false, true}) {
            TTenantTestRuntime runtime(ConsoleTestConfig());
            const auto session = CreateSession(runtime);
            auto initial = Propose(runtime, session, distconf);
            UNIT_ASSERT_C(initial->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvControllerProposeConfigResponse::ReverseCommit,
                initial->Get()->Record.ShortDebugString());

            const auto config = MakeYamlConfig(42, "recovered-config");
            const auto expected = NYamlConfig::UpgradeMainConfigVersion(MakeYamlConfig(0, "recovered-config"));
            TBlockEvents<TEvConsole::TEvSetYamlConfigRequest> delayedCommit(runtime,
                [&](const auto& ev) { return ev->Get()->Record.GetRequest().config() == config; });
            SendCommit(runtime, session, config);
            runtime.WaitFor("commit YAML write delayed", [&] { return !delayedCommit.empty(); });

            const auto independentSession = CreateSession(runtime);
            Propose(runtime, independentSession, !distconf);
            delayedCommit.Stop().Unblock();
            auto committed = GrabCommit(runtime, session);
            UNIT_ASSERT_C(committed->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvControllerConsoleCommitResponse::Committed,
                committed->Get()->Record.ShortDebugString());
            CheckStoredConfig(runtime, expected);

            auto request = MakeHolder<TEvBlobStorage::TEvControllerProposeConfigRequest>();
            request->Record.SetDistconf(distconf);
            request->Record.SetConfigVersion(0);
            request->Record.SetConfigHash(NYaml::GetConfigHash(expected));
            runtime.SendToPipe(session.Pipe, session.Sender, request.Release());
            auto proposal = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerProposeConfigResponse>(session.Sender);
            UNIT_ASSERT_C(proposal->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNotNeeded,
                proposal->Get()->Record.ShortDebugString());

            auto reverse = Propose(runtime, session, distconf);
            UNIT_ASSERT_VALUES_EQUAL(reverse->Get()->Record.GetConsoleConfigVersion(), 1);
            UNIT_ASSERT_VALUES_EQUAL(reverse->Get()->Record.GetYAML(), expected);
        }
    }
}

}
