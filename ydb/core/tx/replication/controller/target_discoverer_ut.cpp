#include "private_events.h"
#include "target_discoverer.h"
#include "target_table.h"
#include "target_transfer.h"

#include <ydb/core/protos/metrics_config.pb.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/core/tx/replication/ut_helpers/test_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/string/printf.h>

namespace NKikimr::NReplication::NController {

NKikimrReplication::TReplicationConfig CreateConfig(const TVector<std::pair<TString, TString>>& paths) {
    NKikimrReplication::TReplicationConfig config;

    auto& specific = *config.MutableSpecific();
    for (const auto& [src, dst] : paths) {
        auto& t = *specific.AddTargets();
        t.SetSrcPath(src);
        t.SetDstPath(dst);
    }

    return config;
}

NKikimrReplication::TReplicationConfig CreateTransferConfig(const std::tuple<TString, TString, TString>& path) {
    NKikimrReplication::TReplicationConfig config;

    const auto& [src, dst, lambda] = path;
    auto& specific = *config.MutableTransferSpecific();
    auto& t = *specific.MutableTarget();
    t.SetSrcPath(src);
    t.SetDstPath(dst);
    t.SetTransformLambda(lambda);

    return config;
}

Y_UNIT_TEST_SUITE(TargetDiscoverer) {
    using namespace NTestHelpers;

    TTestTableDescription DummyTable() {
        return TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Uint32"},
            },
            .ReplicationConfig = Nothing(),
        };
    }

    TTestTopicDescription DummyTopic() {
        return TTestTopicDescription{
            .Name = "Topic",
        };
    }

    Y_UNIT_TEST(Basic) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetSrcPath(), "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetDstPath(), "/Root/Replicated/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Kind, TReplication::ETargetKind::Table);
    }

    Y_UNIT_TEST(IndexedTable) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTableWithIndex("/Root", *MakeTableDescription(DummyTable()),
             "Index", TVector<TString>{"value"}, NKikimrSchemeOp::EIndexTypeGlobal);

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(1).Config->GetSrcPath(), "/Root/Table/Index");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(1).Config->GetDstPath(), "/Root/Replicated/Table/Index/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(1).Kind, TReplication::ETargetKind::IndexTable);
    }

    Y_UNIT_TEST(Transfer) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTopic("/Root", *MakeTopicDescription(DummyTopic()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateTransferConfig(std::tuple<TString, TString, TString>{
                "/Root/Topic", "/Root/Replicated/Table", "lambda body"
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetSrcPath(), "/Root/Topic");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetDstPath(), "/Root/Replicated/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Kind, TReplication::ETargetKind::Transfer);

        auto p = std::dynamic_pointer_cast<const TTargetTransfer::TTransferConfig>(toAdd.at(0).Config);
        UNIT_ASSERT(p);
        UNIT_ASSERT_VALUES_EQUAL(p->GetTransformLambda(), "lambda body");
    }

    Y_UNIT_TEST(MissingTransferSourceDnsNameRecoversAfterRetryBudget) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        const auto created = env.RunCall([&] {
            return env.CreateTopic("/Root", *MakeTopicDescription(DummyTopic()));
        });
        UNIT_ASSERT_VALUES_EQUAL(created, NMsgBusProxy::MSTATUS_OK);

        TActorId discoverer;
        TVector<TInstant> attempts;
        auto observer = runtime.AddObserver<TEvYdbProxy::TEvDescribePathRequest>(
            [&](TEvYdbProxy::TEvDescribePathRequest::TPtr& ev) {
                if (ev->Sender != discoverer || ev->GetRecipientRewrite() != env.GetYdbProxy()
                    || std::get<0>(ev->Get()->GetArgs()) != "/Root/Topic")
                {
                    return;
                }

                attempts.push_back(runtime.GetCurrentTime());
                if (attempts.size() > 8) {
                    return;
                }

                NYdb::NIssue::TIssues issues;
                issues.AddIssue("GRpc error: (14): DNS resolution failed: Domain name not found");
                runtime.Send(new IEventHandle(ev->Sender, env.GetYdbProxy(),
                    new TEvYdbProxy::TEvDescribePathResponse(
                        NYdb::TStatus(NYdb::EStatus::TRANSPORT_UNAVAILABLE, std::move(issues)),
                        NYdb::NScheme::TSchemeEntry()),
                    0, ev->Cookie));
                ev.Reset();
            });

        discoverer = runtime.Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateTransferConfig(std::tuple<TString, TString, TString>{
                "/Root/Topic", "/Root/Replicated/Table", "lambda body"
            })));
        runtime.EnableScheduleForActor(discoverer);

        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(
            env.GetSender(), TDuration::Minutes(3));
        UNIT_ASSERT(result);
        UNIT_ASSERT_C(result->Get()->IsSuccess(),
            "Recoverable transfer source DNS failure must not exhaust the discovery retry budget");
        UNIT_ASSERT_VALUES_EQUAL(attempts.size(), 9);
        for (size_t i = 1; i < attempts.size(); ++i) {
            UNIT_ASSERT(attempts[i] > attempts[i - 1]);
        }

        const auto& targets = result->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(targets.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(targets.at(0).Kind, TReplication::ETargetKind::Transfer);
        UNIT_ASSERT_VALUES_EQUAL(targets.at(0).Config->GetSrcPath(), "/Root/Topic");
        UNIT_ASSERT_VALUES_EQUAL(targets.at(0).Config->GetDstPath(), "/Root/Replicated/Table");
        const auto config = std::dynamic_pointer_cast<const TTargetTransfer::TTransferConfig>(targets.at(0).Config);
        UNIT_ASSERT(config);
        UNIT_ASSERT_VALUES_EQUAL(config->GetTransformLambda(), "lambda body");
    }

    Y_UNIT_TEST(Negative) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root/Table", "/Root/ReplicatedTable"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(!ev->Get()->IsSuccess());

        const auto& failed = ev->Get()->Failed;
        UNIT_ASSERT_VALUES_EQUAL(failed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).SrcPath, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).Error.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(Dirs) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.MkDir("/Root", "Dir");
        env.CreateTable("/Root/Dir", *MakeTableDescription(DummyTable()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetSrcPath(), "/Root/Dir/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetDstPath(), "/Root/Replicated/Dir/Table");
    }

    Y_UNIT_TEST(SystemObjects) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));
        env.MkDir("/Root", "export-100500");
        env.CreateTable("/Root/export-100500", *MakeTableDescription(DummyTable()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetSrcPath(), "/Root/Table");
    }

    Y_UNIT_TEST(InvalidCredentials) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));

        // create aux proxy
        NKikimrReplication::TStaticCredentials staticCreds;
        staticCreds.SetUser("user");
        staticCreds.SetPassword("password");
        const auto ydbProxy = env.GetRuntime().Register(CreateYdbProxy(
            env.GetEndpoint(), env.GetDatabase(), false /* ssl */, "" /* cert */, staticCreds));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, ydbProxy,
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(!ev->Get()->IsSuccess());

        const auto& failed = ev->Get()->Failed;
        UNIT_ASSERT_VALUES_EQUAL(failed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).Error.GetStatus(), NYdb::EStatus::CLIENT_UNAUTHENTICATED);
    }

    Y_UNIT_TEST(MissingSourceDnsNameRecoversAfterRetryBudget) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));
        auto otherTable = DummyTable();
        otherTable.Name = "OtherTable";
        env.CreateTable("/Root", *MakeTableDescription(otherTable));

        TActorId discoverer;
        TVector<TInstant> attempts;
        auto observer = runtime.AddObserver<TEvYdbProxy::TEvDescribePathRequest>(
            [&](TEvYdbProxy::TEvDescribePathRequest::TPtr& ev) {
                if (ev->Sender != discoverer || ev->GetRecipientRewrite() != env.GetYdbProxy()
                    || std::get<0>(ev->Get()->GetArgs()) != "/Root/Table")
                {
                    return;
                }

                // Keep the nonzero cookie so the other source can finish while
                // this source endpoint is temporarily absent from DNS.
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 1);
                attempts.push_back(runtime.GetCurrentTime());
                if (attempts.size() > 8) {
                    return;
                }

                NYdb::NIssue::TIssues issues;
                issues.AddIssue("GRpc error: (14): DNS resolution failed: C-ares status is not ARES_SUCCESS qtype=AAAA: Domain name not found");
                runtime.Send(new IEventHandle(ev->Sender, env.GetYdbProxy(),
                    new TEvYdbProxy::TEvDescribePathResponse(
                        NYdb::TStatus(NYdb::EStatus::TRANSPORT_UNAVAILABLE, std::move(issues)),
                        NYdb::NScheme::TSchemeEntry()),
                    0, ev->Cookie));
                ev.Reset();
            });

        discoverer = runtime.Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root/OtherTable", "/Root/OtherReplica"},
                {"/Root/Table", "/Root/Replica"},
            })));
        runtime.EnableScheduleForActor(discoverer);

        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(
            env.GetSender(), TDuration::Minutes(3));
        UNIT_ASSERT(result);
        UNIT_ASSERT_C(result->Get()->IsSuccess(),
            "Recoverable source DNS failure must not exhaust the discovery retry budget");
        UNIT_ASSERT_VALUES_EQUAL(attempts.size(), 9);
        for (size_t i = 1; i < attempts.size(); ++i) {
            UNIT_ASSERT(attempts[i] > attempts[i - 1]);
        }

        const auto& targets = result->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(targets.size(), 2);
        UNIT_ASSERT(AnyOf(targets, [](const auto& target) {
            return target.Config->GetSrcPath() == "/Root/OtherTable"
                && target.Config->GetDstPath() == "/Root/OtherReplica";
        }));
        UNIT_ASSERT(AnyOf(targets, [](const auto& target) {
            return target.Config->GetSrcPath() == "/Root/Table"
                && target.Config->GetDstPath() == "/Root/Replica";
        }));
    }

    Y_UNIT_TEST(DropCancelsEndpointRetriesBeforeTargetDiscovery) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));
        NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(env.GetEndpoint()).DiscoveryMode(NYdb::EDiscoveryMode::Async).Database(env.GetDatabase()));
        const auto sessionResult = env.RunCall([&] { return client.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();

        ui64 requests = 0;
        auto observer = runtime.AddObserver<TEvYdbProxy::TEvDescribePathRequest>(
            [&](TEvYdbProxy::TEvDescribePathRequest::TPtr& ev) {
                if (std::get<0>(ev->Get()->GetArgs()) != "/Root/Table") {
                    return;
                }
                ++requests;
                runtime.EnableScheduleForActor(ev->Sender);
                NYdb::NIssue::TIssues issues;
                issues.AddIssue("Domain name not found");
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new TEvYdbProxy::TEvDescribePathResponse(
                        NYdb::TStatus(NYdb::EStatus::TRANSPORT_UNAVAILABLE, std::move(issues)),
                        NYdb::NScheme::TSchemeEntry()), 0, ev->Cookie));
                ev.Reset();
            });
        const auto created = env.RunCall([&] {
            return session.ExecuteSchemeQuery(Sprintf(R"(
                CREATE ASYNC REPLICATION `/Root/replication`
                FOR `/Root/Table` AS `/Root/Replica`
                WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN = "root@builtin");
            )", env.GetEndpoint().c_str())).GetValueSync();
        });
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        runtime.WaitFor("source discovery waiting for endpoint recovery", [&] { return requests > 0; }, TDuration::Seconds(30));

        const auto dropped = env.RunCall([&] {
            return session.ExecuteSchemeQuery("DROP ASYNC REPLICATION `/Root/replication` CASCADE;").GetValueSync();
        });
        UNIT_ASSERT_C(dropped.IsSuccess(), dropped.GetIssues().ToString());
        const auto requestsAfterDrop = requests;
        const auto edge = runtime.AllocateEdgeActor();
        runtime.Schedule(new IEventHandle(edge, env.GetSender(), new TEvents::TEvWakeup()), TDuration::Minutes(3));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge, TDuration::Minutes(4)));
        UNIT_ASSERT_VALUES_EQUAL(requests, requestsAfterDrop);

        const auto description = env.Send<TEvYdbProxy::TEvDescribePathResponse>(env.GetYdbProxy(),
            new TEvYdbProxy::TEvDescribePathRequest("/Root/replication", {}));
        UNIT_ASSERT_VALUES_EQUAL(description->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
    }

    void TerminalSourceDiscoveryError(NYdb::EStatus statusCode, const TString& message) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        TActorId discoverer;
        size_t attempts = 0;
        auto observer = runtime.AddObserver<TEvYdbProxy::TEvDescribePathRequest>(
            [&](TEvYdbProxy::TEvDescribePathRequest::TPtr& ev) {
                if (ev->Sender != discoverer || ev->GetRecipientRewrite() != env.GetYdbProxy()) {
                    return;
                }

                ++attempts;
                NYdb::NIssue::TIssues issues;
                issues.AddIssue(message);
                runtime.Send(new IEventHandle(ev->Sender, env.GetYdbProxy(),
                    new TEvYdbProxy::TEvDescribePathResponse(
                        NYdb::TStatus(statusCode, std::move(issues)), NYdb::NScheme::TSchemeEntry()),
                    0, ev->Cookie));
                ev.Reset();
            });

        discoverer = runtime.Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root/Table", "/Root/Replica"},
            })));
        runtime.EnableScheduleForActor(discoverer);

        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(
            env.GetSender(), TDuration::Minutes(1));
        UNIT_ASSERT(result);
        UNIT_ASSERT(!result->Get()->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
        const auto& failed = result->Get()->Failed;
        UNIT_ASSERT_VALUES_EQUAL(failed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed[0].SrcPath, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(failed[0].Error.GetStatus(), statusCode);
        UNIT_ASSERT_VALUES_EQUAL(failed[0].Error.GetIssues().Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed[0].Error.GetIssues().begin()->GetMessage(), message);
    }

    Y_UNIT_TEST(MalformedSourceDomainRemainsTerminal) {
        TerminalSourceDiscoveryError(NYdb::EStatus::TRANSPORT_UNAVAILABLE,
            "GRpc error: (14): DNS resolution failed: Misformatted domain name");
    }

    Y_UNIT_TEST(SourceAuthenticationFailureRemainsTerminal) {
        TerminalSourceDiscoveryError(NYdb::EStatus::CLIENT_UNAUTHENTICATED, "Invalid source credentials");
    }

    Y_UNIT_TEST(SourceAuthorizationFailureRemainsTerminal) {
        TerminalSourceDiscoveryError(NYdb::EStatus::UNAUTHORIZED, "Source access denied");
    }

    Y_UNIT_TEST(InvalidSourceSettingsRemainTerminal) {
        TerminalSourceDiscoveryError(NYdb::EStatus::BAD_REQUEST, "Invalid source settings");
    }

    Y_UNIT_TEST(RetryableError) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        // create aux proxy
        const auto ydbProxy = env.GetRuntime().AllocateEdgeActor();

        auto discoveryActorId = env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, ydbProxy,
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        for (size_t i = 0; i <= 5; ++i) {
            NYdb::NIssue::TIssues issues;
            issues.AddIssue(TStringBuilder() << "iteration-" << i);
            NYdb::TStatus status(NYdb::EStatus::UNAVAILABLE, std::move(issues));
            env.SendAsync(discoveryActorId, new TEvYdbProxy::TEvDescribePathResponse(std::move(status), NYdb::NScheme::TSchemeEntry()));

            env.SendAsync(discoveryActorId, new TEvents::TEvWakeup());
        }

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(!ev->Get()->IsSuccess());

        const auto& failed = ev->Get()->Failed;
        UNIT_ASSERT_VALUES_EQUAL(failed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).Error.GetStatus(), NYdb::EStatus::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).Error.GetIssues().ToOneLineString(), "{ <main>: Error: iteration-5 }");
    }
}

}
