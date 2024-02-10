#include "ydb_common_ut.h"

#include <ydb/public/sdk/cpp/client/ydb_coordination/coordination.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace Tests;
using namespace NYdb;
using namespace NThreading;

using NYdb::NCoordination::TResult;
using NYdb::NCoordination::TSessionSettings;
using NYdb::NCoordination::TAcquireSemaphoreSettings;
using NYdb::NCoordination::TDescribeSemaphoreSettings;

using NYdb::NCoordination::EConsistencyMode;
using NYdb::NCoordination::ESessionState;
using NYdb::NCoordination::EConnectionState;

using NYdb::NScheme::TPermissions;
using NYdb::NScheme::TModifyPermissionsSettings;

namespace {

struct TClientContext {
    TString Location;
    NYdb::TDriver Connection;
    NYdb::NCoordination::TClient Client;
    NYdb::NScheme::TSchemeClient SchemeClient;

    template <typename Config>
    TClientContext(
            TBasicKikimrWithGrpcAndRootSchema<Config>& server,
            const TString& token = { },
            bool secure = false)
        : Location(TStringBuilder() << "localhost:" << server.GetPort())
        , Connection(CreateParams(Location, token, secure))
        , Client(Connection)
        , SchemeClient(Connection)
    { }

    static TDriverConfig CreateParams(
            const TString& endpoint,
            const TString& token,
            bool secure)
    {
        TDriverConfig params;
        params.SetEndpoint(endpoint);
        if (token) {
            params.SetAuthToken(token);
        }
        if (secure) {
            params.UseSecureConnection(NYdbSslTestData::CaCrt);
        }
        return params;
    }
};

template<class T>
class TSimpleQueue : public TThrRefBase {
private:
    TMutex Lock;
    TCondVar NotEmpty;
    TDeque<T> Events;

public:
    T Next() {
        with_lock (Lock) {
            while (Events.empty()) {
                NotEmpty.WaitI(Lock);
            }
            T event = std::move(Events.front());
            Events.pop_front();
            return event;
        }
    }

    void Enqueue(T event) {
        with_lock (Lock) {
            Events.emplace_back(std::move(event));
            NotEmpty.Signal();
        }
    }
};

class TStatusDescription {
public:
    TStatusDescription(const TStatus& status)
        : Status(status)
    { }

    friend IOutputStream& operator<<(IOutputStream& out, const TStatusDescription& desc) {
        out << desc.Status.GetStatus();
        const auto& issues = desc.Status.GetIssues();
        if (issues) {
            out << ":\n";
            issues.PrintTo(out);
        }
        return out;
    }

private:
    const TStatus& Status;
};

template<class T>
T ExpectSuccess(TFuture<TResult<T>>&& future) {
    auto result = future.ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "Unexpected failure: " << TStatusDescription(result));
    return result.ExtractResult();
}

void ExpectSuccess(TAsyncStatus&& future) {
    auto result = future.ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "Unexpected failure: " << TStatusDescription(result));
}

template<class T>
void ExpectError(TFuture<T>&& future, EStatus status) {
    auto result = future.ExtractValueSync();
    UNIT_ASSERT_C(!result.IsSuccess(), "Unexpected success");
    UNIT_ASSERT_C(result.GetStatus() == status, "Unexpected failure: " << TStatusDescription(result));
}

}

Y_UNIT_TEST_SUITE(TGRpcNewCoordinationClient) {
    Y_UNIT_TEST(CheckUnauthorized) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server, "bad@builtin");

        auto result = context.Client.CreateNode("/Root/node1").ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::UNAUTHORIZED, TStatusDescription(result));
        bool issueFound = false;
        for (auto& issue : result.GetIssues()) {
            NYql::WalkThroughIssues(issue, false, [&issueFound](const NYql::TIssue& issue, ui16){
                if (issue.GetCode() == NKikimrIssues::TIssuesIds::ACCESS_DENIED) {
                    issueFound = true;
                }
            });
        }
        UNIT_ASSERT_C(issueFound, TStatusDescription(result));
    }

    Y_UNIT_TEST(CreateDropDescribe) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectSuccess(context.Client.CreateNode("/Root/node1"));

        ExpectSuccess(
            context.Client
                .CreateNode(
                    "/Root/node2",
                    NYdb::NCoordination::TCreateNodeSettings()
                        .SelfCheckPeriod(TDuration::MilliSeconds(1234))));

        auto desc1 = context.SchemeClient.DescribePath("/Root/node1").ExtractValueSync().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(desc1.Type, NYdb::NScheme::ESchemeEntryType::CoordinationNode);

        auto desc2 = context.SchemeClient.DescribePath("/Root/node2").ExtractValueSync().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(desc2.Type, NYdb::NScheme::ESchemeEntryType::CoordinationNode);

        ExpectSuccess(context.Client.DropNode("/Root/node1"));

        UNIT_ASSERT_VALUES_EQUAL(
            context.SchemeClient.DescribePath("/Root/node1").ExtractValueSync().GetStatus(),
            EStatus::SCHEME_ERROR);

        UNIT_ASSERT_VALUES_EQUAL(
            context.SchemeClient.DescribePath("/Root/node2").ExtractValueSync().GetStatus(),
            EStatus::SUCCESS);

        auto result = ExpectSuccess(context.Client.DescribeNode("/Root/node2"));
        UNIT_ASSERT_VALUES_EQUAL(
            result.GetSelfCheckPeriod(),
            TDuration::MilliSeconds(1234));
    }

    Y_UNIT_TEST(CreateAlter) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectSuccess(
            context.Client
                .CreateNode(
                    "/Root/node1",
                    NYdb::NCoordination::TCreateNodeSettings()
                        .SelfCheckPeriod(TDuration::MilliSeconds(1234))));

        auto desc1 = ExpectSuccess(context.Client.DescribeNode("/Root/node1"));
        UNIT_ASSERT_VALUES_EQUAL(
            desc1.GetSelfCheckPeriod(),
            TDuration::MilliSeconds(1234));

        ExpectSuccess(
            context.Client
                .AlterNode(
                    "/Root/node1",
                    NYdb::NCoordination::TAlterNodeSettings()
                        .SessionGracePeriod(TDuration::MilliSeconds(5678))));

        auto desc2 = ExpectSuccess(context.Client.DescribeNode("/Root/node1"));
        UNIT_ASSERT_VALUES_EQUAL(
            desc2.GetSelfCheckPeriod(),
            TDuration::MilliSeconds(1234));
        UNIT_ASSERT_VALUES_EQUAL(
            desc2.GetSessionGracePeriod(),
            TDuration::MilliSeconds(5678));

        ExpectSuccess(
            context.Client
                .AlterNode(
                    "/Root/node1",
                    NYdb::NCoordination::TAlterNodeSettings()
                        .SelfCheckPeriod(TDuration::MilliSeconds(2345))));

        auto desc3 = ExpectSuccess(context.Client.DescribeNode("/Root/node1"));
        UNIT_ASSERT_VALUES_EQUAL(
            desc3.GetSelfCheckPeriod(),
            TDuration::MilliSeconds(2345));
        UNIT_ASSERT_VALUES_EQUAL(
            desc2.GetSessionGracePeriod(),
            TDuration::MilliSeconds(5678));

        ExpectSuccess(
            context.Client
                .AlterNode(
                    "/Root/node1",
                    NYdb::NCoordination::TAlterNodeSettings()
                        .ReadConsistencyMode(EConsistencyMode::STRICT_MODE)));

        auto desc4 = ExpectSuccess(context.Client.DescribeNode("/Root/node1"));
        UNIT_ASSERT_VALUES_EQUAL(
            desc4.GetReadConsistencyMode(),
            EConsistencyMode::STRICT_MODE);
    }

    Y_UNIT_TEST(NodeNotFound) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectError(
            context.Client.StartSession("/Root/node1"),
            EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(BasicMethods) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectSuccess(context.Client.CreateNode("/Root/node1"));

        auto session = ExpectSuccess(context.Client.StartSession("/Root/node1"));
        ExpectSuccess(session.CreateSemaphore("Sem1", 5));
        auto desc = ExpectSuccess(session.DescribeSemaphore("Sem1"));
        UNIT_ASSERT_VALUES_EQUAL(desc.GetName(), "Sem1");
        UNIT_ASSERT_VALUES_EQUAL(desc.GetLimit(), 5);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetData(), "");
        ExpectSuccess(session.UpdateSemaphore("Sem1", "foobar"));
        desc = ExpectSuccess(session.DescribeSemaphore("Sem1"));
        UNIT_ASSERT_VALUES_EQUAL(desc.GetData(), "foobar");
        ExpectSuccess(session.DeleteSemaphore("Sem1"));
        ExpectError(
            session.DescribeSemaphore("Sem1"),
            EStatus::NOT_FOUND);
    }


    Y_UNIT_TEST(SessionMethods) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectSuccess(context.Client.CreateNode("/Root/node1"));

        {
            auto session = ExpectSuccess(context.Client.StartSession("/Root/node1"));

            // Concurrent ping operations
            auto ping1 = session.Ping();
            auto ping2 = session.Ping();
            ExpectSuccess(std::move(ping1));
            ExpectSuccess(std::move(ping2));

            // Create and acquire some semaphores
            ExpectSuccess(session.CreateSemaphore("Sem1", 5));
            UNIT_ASSERT(ExpectSuccess(
                session.AcquireSemaphore("Sem1",
                    TAcquireSemaphoreSettings()
                        .Count(3)
                        .Data("foobar"))));
            UNIT_ASSERT(ExpectSuccess(
                session.AcquireSemaphore("Lock1",
                    TAcquireSemaphoreSettings()
                        .Shared()
                        .Ephemeral())));

            // Test initial description of Sem1
            {
                auto desc = ExpectSuccess(
                    session.DescribeSemaphore("Sem1",
                        TDescribeSemaphoreSettings().IncludeOwners()));
                UNIT_ASSERT_VALUES_EQUAL(desc.GetName(), "Sem1");
                UNIT_ASSERT_VALUES_EQUAL(desc.GetCount(), 3);
                UNIT_ASSERT_VALUES_EQUAL(desc.GetLimit(), 5);
                UNIT_ASSERT_VALUES_EQUAL(desc.GetData(), "");
                UNIT_ASSERT_VALUES_EQUAL(desc.GetOwners().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(desc.GetOwners()[0].GetSessionId(), session.GetSessionId());
                UNIT_ASSERT_VALUES_EQUAL(desc.GetOwners()[0].GetCount(), 3);
                UNIT_ASSERT_VALUES_EQUAL(desc.GetOwners()[0].GetData(), "foobar");
            }

            // Test initial description of Lock1
            {
                auto desc = ExpectSuccess(
                    session.DescribeSemaphore("Lock1",
                        TDescribeSemaphoreSettings().IncludeOwners()));
                UNIT_ASSERT_VALUES_EQUAL(desc.GetOwners().size(), 1);
            }

            UNIT_ASSERT(ExpectSuccess(session.ReleaseSemaphore("Sem1")));

            // Test Sem1 description after the release
            {
                auto desc = ExpectSuccess(
                        session.DescribeSemaphore("Sem1",
                            TDescribeSemaphoreSettings().IncludeOwners()));
                UNIT_ASSERT_VALUES_EQUAL(desc.GetOwners().size(), 0);
            }

            // Close session gracefully
            ExpectSuccess(session.Close());
        }

        // Ephemeral semaphores are deleted with the last session
        {
            auto session = ExpectSuccess(context.Client.StartSession("/Root/node1"));
            ExpectError(
                session.DescribeSemaphore("Lock1"),
                EStatus::NOT_FOUND);
        }
    }

    Y_UNIT_TEST(MultipleSessionsSemaphores) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectSuccess(context.Client.CreateNode("/Root/node1"));

        {
            auto session1 = ExpectSuccess(
                context.Client.StartSession("/Root/node1",
                    TSessionSettings().Timeout(TDuration::Seconds(30))));
            ExpectSuccess(session1.CreateSemaphore("Sem1", 5));
            UNIT_ASSERT(ExpectSuccess(
                session1.AcquireSemaphore("Sem1",
                    TAcquireSemaphoreSettings().Count(1))));

            auto session2 = ExpectSuccess(
                context.Client.StartSession("/Root/node1",
                    TSessionSettings().Timeout(TDuration::Seconds(30))));

            // Cannot acquire 5 units when only 4 are available
            UNIT_ASSERT(!ExpectSuccess(
                session2.AcquireSemaphore("Sem1",
                    TAcquireSemaphoreSettings()
                        .Count(5)
                        .Timeout(TDuration::MilliSeconds(100)))));

            // Cannot release semaphore owned by another session
            UNIT_ASSERT(!ExpectSuccess(session2.ReleaseSemaphore("Sem1")));

            // Asynchronously closes the first session
            session1 = { };

            // Acquire should succeed before session timeout
            UNIT_ASSERT(ExpectSuccess(
                session2.AcquireSemaphore("Sem1",
                    TAcquireSemaphoreSettings()
                        .Count(5)
                        .Timeout(TDuration::MilliSeconds(5000)))));
        }
    }

    Y_UNIT_TEST(SessionSemaphoreInfiniteTimeout) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectSuccess(context.Client.CreateNode("/Root/node1"));

        {
            auto session1 = ExpectSuccess(
                context.Client.StartSession("/Root/node1",
                    TSessionSettings().Timeout(TDuration::Seconds(30))));
            ExpectSuccess(session1.CreateSemaphore("Sem1", 5));
            UNIT_ASSERT(ExpectSuccess(
                session1.AcquireSemaphore("Sem1",
                    TAcquireSemaphoreSettings().Count(1))));

            auto session2 = ExpectSuccess(
                context.Client.StartSession("/Root/node1",
                    TSessionSettings().Timeout(TDuration::Seconds(30))));
            auto future = session2.AcquireSemaphore("Sem1",
                TAcquireSemaphoreSettings().Count(5).Timeout(TDuration::Max()));
            UNIT_ASSERT(ExpectSuccess(session1.ReleaseSemaphore("Sem1")));
            UNIT_ASSERT(ExpectSuccess(std::move(future)));
            UNIT_ASSERT(ExpectSuccess(session2.ReleaseSemaphore("Sem1")));
        }
    }

    Y_UNIT_TEST(SessionDescribeWatchData) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectSuccess(context.Client.CreateNode("/Root/node1"));

        {
            auto session = ExpectSuccess(
                context.Client.StartSession("/Root/node1",
                    TSessionSettings().Timeout(TDuration::Seconds(30))));
            ExpectSuccess(session.CreateSemaphore("Sem1", 5));

            TPromise<bool> changed = NewPromise<bool>();
            auto changedLambda = [=](bool triggered) mutable {
                changed.SetValue(triggered);
            };

            auto desc1 = ExpectSuccess(
                session.DescribeSemaphore("Sem1",
                    TDescribeSemaphoreSettings()
                        .OnChanged(changedLambda)
                        .WatchData()));
            UNIT_ASSERT_VALUES_EQUAL(desc1.GetCount(), 0);

            // Acquire shouldn't cause changed to be set
            UNIT_ASSERT(ExpectSuccess(
                session.AcquireSemaphore("Sem1",
                    TAcquireSemaphoreSettings().Count(1))));
            UNIT_ASSERT_VALUES_EQUAL(changed.GetFuture().Wait(TDuration::MilliSeconds(50)), false);

            // Update should cause changed to be set
            ExpectSuccess(session.UpdateSemaphore("Sem1", "some data"));
            UNIT_ASSERT(changed.GetFuture().GetValueSync());

            auto desc2 = ExpectSuccess(session.DescribeSemaphore("Sem1"));
            UNIT_ASSERT_VALUES_EQUAL(desc2.GetCount(), 1);
        }
    }

    Y_UNIT_TEST(SessionDescribeWatchOwners) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectSuccess(context.Client.CreateNode("/Root/node1"));

        {
            auto session = ExpectSuccess(
                context.Client.StartSession("/Root/node1",
                    TSessionSettings().Timeout(TDuration::Seconds(30))));
            ExpectSuccess(session.CreateSemaphore("Sem1", 5));

            TPromise<bool> changed = NewPromise<bool>();
            auto changedLambda = [=](bool triggered) mutable {
                changed.SetValue(triggered);
            };

            auto desc1 = ExpectSuccess(
                session.DescribeSemaphore("Sem1",
                    TDescribeSemaphoreSettings()
                        .OnChanged(changedLambda)
                        .WatchOwners()));
            UNIT_ASSERT_VALUES_EQUAL(desc1.GetCount(), 0);

            // Update shouldn't cause changed to be set
            ExpectSuccess(session.UpdateSemaphore("Sem1", "some data"));
            UNIT_ASSERT_VALUES_EQUAL(changed.GetFuture().Wait(TDuration::MilliSeconds(50)), false);

            // Acquire should cause changed to be set
            UNIT_ASSERT(ExpectSuccess(
                session.AcquireSemaphore("Sem1",
                    TAcquireSemaphoreSettings().Count(1))));
            UNIT_ASSERT(changed.GetFuture().GetValueSync());

            auto desc2 = ExpectSuccess(session.DescribeSemaphore("Sem1"));
            UNIT_ASSERT_VALUES_EQUAL(desc2.GetCount(), 1);
        }
    }

    Y_UNIT_TEST(SessionDescribeWatchReplace) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectSuccess(context.Client.CreateNode("/Root/node1"));

        {
            auto session = ExpectSuccess(
                context.Client.StartSession("/Root/node1",
                    TSessionSettings().Timeout(TDuration::Seconds(30))));
            ExpectSuccess(session.CreateSemaphore("Sem1", 5));

            TPromise<bool> changed1 = NewPromise<bool>();
            auto desc1 = ExpectSuccess(
                session.DescribeSemaphore("Sem1",
                    TDescribeSemaphoreSettings()
                        .OnChanged([=](bool triggered) mutable { changed1.SetValue(triggered); })
                        .WatchOwners()));

            TPromise<bool> changed2 = NewPromise<bool>();
            auto desc2 = ExpectSuccess(
                session.DescribeSemaphore("Sem1",
                    TDescribeSemaphoreSettings()
                        .OnChanged([=](bool triggered) mutable { changed2.SetValue(triggered); })
                        .WatchOwners()));

            // Setting new watch should call the first callback with triggered == false
            UNIT_ASSERT(!changed1.GetFuture().GetValueSync());

            // Deleting semaphore should trigger the second callback
            ExpectSuccess(session.DeleteSemaphore("Sem1"));
            UNIT_ASSERT(changed2.GetFuture().GetValueSync());
        }
    }

    Y_UNIT_TEST(SessionCreateUpdateDeleteSemaphore) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectSuccess(context.Client.CreateNode("/Root/node1"));

        {
            auto session = ExpectSuccess(
                context.Client.StartSession("/Root/node1",
                    TSessionSettings().Timeout(TDuration::Seconds(30))));
            ExpectSuccess(session.CreateSemaphore("Sem1", 5));
            ExpectSuccess(session.UpdateSemaphore("Sem1", "some data"));
            ExpectSuccess(session.DeleteSemaphore("Sem1"));
        }
    }

    Y_UNIT_TEST(SessionAcquireAcceptedCallback) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectSuccess(context.Client.CreateNode("/Root/node1"));

        {
            auto session1 = ExpectSuccess(
                context.Client.StartSession("/Root/node1",
                    TSessionSettings().Timeout(TDuration::Seconds(30))));
            auto session2 = ExpectSuccess(
                context.Client.StartSession("/Root/node1",
                    TSessionSettings().Timeout(TDuration::Seconds(30))));
            ExpectSuccess(session1.CreateSemaphore("Sem1", 5));

            // OnAccepted callback is not called when acquire is immediate
            TPromise<void> notAccepted = NewPromise();
            auto notAcceptedLambda = [=]() mutable {
                notAccepted.SetValue();
            };
            UNIT_ASSERT(ExpectSuccess(
                session1.AcquireSemaphore("Sem1",
                    TAcquireSemaphoreSettings()
                        .Count(3)
                        .OnAccepted(notAcceptedLambda))));
            UNIT_ASSERT_VALUES_EQUAL(notAccepted.HasValue(), false);

            // OnAccepted callback is called when acquire is queued
            TPromise<void> accepted = NewPromise();
            auto acceptedLambda = [=]() mutable {
                accepted.SetValue();
            };
            auto acquireFuture = session2.AcquireSemaphore("Sem1",
                TAcquireSemaphoreSettings()
                    .Count(3)
                    .Timeout(TDuration::Seconds(30))
                    .OnAccepted(acceptedLambda));
            accepted.GetFuture().GetValueSync();

            // When the first acquire is released it should succeed
            UNIT_ASSERT(ExpectSuccess(session1.ReleaseSemaphore("Sem1")));
            UNIT_ASSERT(ExpectSuccess(std::move(acquireFuture)));
        }
    }

    Y_UNIT_TEST(SessionReconnectReattach) {
        TKikimrWithGrpcAndRootSchema server;
        TClientContext context(server);

        ExpectSuccess(context.Client.CreateNode("/Root/node1"));

        {
            auto states = MakeIntrusive<TSimpleQueue<ESessionState>>();
            auto session = ExpectSuccess(
                context.Client.StartSession("/Root/node1",
                    TSessionSettings()
                        .Timeout(TDuration::Seconds(30))
                        .OnStateChanged([=](auto state) { states->Enqueue(state); })));
            UNIT_ASSERT_VALUES_EQUAL(states->Next(), ESessionState::ATTACHED);

            ExpectSuccess(session.CreateSemaphore("Sem1", 5));
            UNIT_ASSERT(ExpectSuccess(
                session.AcquireSemaphore("Sem1",
                    TAcquireSemaphoreSettings().Count(1))));

            // Reconnect should succeed and cause disconnect/connect/attach
            ExpectSuccess(session.Reconnect());
            UNIT_ASSERT_VALUES_EQUAL(states->Next(), ESessionState::DETACHED);
            UNIT_ASSERT_VALUES_EQUAL(states->Next(), ESessionState::ATTACHED);

            // Close should succeed and move session to expired state
            ExpectSuccess(session.Close());
            UNIT_ASSERT_VALUES_EQUAL(states->Next(), ESessionState::EXPIRED);
            UNIT_ASSERT_VALUES_EQUAL(session.GetConnectionState(), EConnectionState::STOPPED);
        }
    }
}

Y_UNIT_TEST_SUITE(TGRpcNewCoordinationClientAuth) {
    struct WithSslAndAuth : TKikimrTestSettings {
        static constexpr bool SSL = true;
        static constexpr bool AUTH = true;
    };

    using TKikimrWithGrpcAndRootSchema = NYdb::TBasicKikimrWithGrpcAndRootSchema<WithSslAndAuth>;

    Y_UNIT_TEST(OwnersAndPermissions) {
        TKikimrWithGrpcAndRootSchema server;

        // Use root and allow users write permissions
        {
            TClientContext context(server, "root@builtin", true);

            ExpectSuccess(context.SchemeClient.ModifyPermissions("/Root",
                TModifyPermissionsSettings()
                    .AddGrantPermissions(TPermissions(
                        "user_a@builtin",
                        { "ydb.deprecated.create_table" }))
                    .AddGrantPermissions(TPermissions(
                        "user_b@builtin",
                        { "ydb.deprecated.create_table" }))
                ));

            TClient::RefreshPathCache(server.Server_->GetRuntime(), "/Root");
        }

        // Make some requests with user_a
        {
            TClientContext context(server, "user_a@builtin", true);
            ExpectSuccess(context.Client.CreateNode("/Root/node1"));
            ExpectSuccess(context.Client.CreateNode("/Root/node2"));
            ExpectSuccess(context.Client.CreateNode("/Root/node3"));
            ExpectSuccess(context.Client.CreateNode("/Root/node4"));

            auto desc1 = context.SchemeClient.DescribePath("/Root/node1").ExtractValueSync().GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(desc1.Type, NYdb::NScheme::ESchemeEntryType::CoordinationNode);
            UNIT_ASSERT_VALUES_EQUAL(desc1.Owner, "user_a@builtin");

            for (int idx = 1; idx <= 4; ++idx) {
                TString path = TStringBuilder() << "/Root/node" << idx;
                auto session = ExpectSuccess(context.Client.StartSession(path));
                ExpectSuccess(session.CreateSemaphore("/foo/bar", 5, TStringBuilder() << "Hello" << idx));
            }

            TVector<TString> allPermissions = {
                "ydb.deprecated.select_row",
                "ydb.deprecated.update_row",
                "ydb.deprecated.erase_row",
            };

            for (int idx = 2; idx <= 4; ++idx) {
                TString path = TStringBuilder() << "/Root/node" << idx;
                TVector<TString> permissions(allPermissions.begin(), allPermissions.begin() + (idx - 1));
                ExpectSuccess(context.SchemeClient.ModifyPermissions(path,
                    TModifyPermissionsSettings()
                        .AddGrantPermissions(TPermissions(
                            "user_b@builtin",
                            std::move(permissions)))));

                TClient::RefreshPathCache(server.Server_->GetRuntime(), path);
            }
        }

        // Make some requests with user_b
        {
            TClientContext context(server, "user_b@builtin", true);

            // We don't have any permissions with node1
            ExpectError(
                context.Client.StartSession("/Root/node1"),
                EStatus::UNAUTHORIZED);

            // We have limited read permissions with node2
            {
                auto session = ExpectSuccess(context.Client.StartSession("/Root/node2"));

                auto desc = ExpectSuccess(session.DescribeSemaphore("/foo/bar"));
                UNIT_ASSERT_VALUES_EQUAL(desc.GetData(), "Hello2");

                ExpectError(
                    session.CreateSemaphore("/foo/baz", 5, "Hello5"),
                    EStatus::UNAUTHORIZED);

                ExpectError(
                    session.UpdateSemaphore("/foo/baz", "Hello5"),
                    EStatus::UNAUTHORIZED);

                ExpectError(
                    session.DeleteSemaphore("/foo/baz"),
                    EStatus::UNAUTHORIZED);

                ExpectError(
                    session.AcquireSemaphore("/foo/bar",
                        TAcquireSemaphoreSettings()
                            .Count(1)),
                    EStatus::UNAUTHORIZED);

                ExpectError(
                    session.ReleaseSemaphore("/foo/bar"),
                    EStatus::UNAUTHORIZED);
            }

            // We have write permissions with node3
            {
                auto session = ExpectSuccess(context.Client.StartSession("/Root/node3"));

                ExpectSuccess(session.CreateSemaphore("/foo/baz", 5, "Hello5"));

                ExpectSuccess(session.UpdateSemaphore("/foo/baz", "Hello6"));

                ExpectError(
                    session.DeleteSemaphore("/foo/baz"),
                    EStatus::UNAUTHORIZED);

                ExpectSuccess(
                    session.AcquireSemaphore("/foo/bar",
                        TAcquireSemaphoreSettings()
                            .Count(1)));

                ExpectSuccess(session.ReleaseSemaphore("/foo/bar"));
            }

            // We have erase permissions with node4
            {
                auto session = ExpectSuccess(context.Client.StartSession("/Root/node4"));

                ExpectSuccess(session.DeleteSemaphore("/foo/bar"));

                ExpectError(
                    session.DeleteSemaphore("/foo/baz"),
                    EStatus::NOT_FOUND);
            }
        }
    }
}

}
