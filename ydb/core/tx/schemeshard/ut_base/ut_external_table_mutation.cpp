#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/string/builder.h>

using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardExternalTableMutation) {
    Y_UNIT_TEST(CreateTicketSurvivesRebootAndFencesDelayedCreate) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableReplaceIfExistsForExternalEntities(true).RunFakeConfigDispatcher(true));
        ui64 txId = 100;
        TestCreateExternalDataSource(runtime, txId, "/MyRoot", R"(
            Name: "ExternalDataSource"
            SourceType: "ObjectStorage"
            Location: "https://s3.cloud.net/my_bucket"
            Auth { None {} }
        )");
        env.TestWaitNotification(runtime, txId);

        const TString path = "/MyRoot/ExternalTable";
        const TString schema = R"(
            Name: "ExternalTable"
            SourceType: "General"
            DataSourcePath: "/MyRoot/ExternalDataSource"
            Location: "/"
            Columns { Name: "key" Type: "Uint64" }
        )";
        runtime.GetAppData().AdministrationAllowedSIDs.clear();
        const auto rootToken = NACLib::TUserToken("root@builtin", {}).SerializeAsString();
        NKikimrExternalSources::TGeneral format;
        (*format.mutable_attributes())["format"] = "json_each_row";
        NKikimrExternalSources::TGeneral compression;
        (*compression.mutable_attributes())["compression"] = "gzip";

        const auto makeMutation = [&](TExternalTableMutation::EAction action, const TString& token) {
            auto* request = new TEvTx(++txId, TTestTxConfig::SchemeShard);
            request->Record.SetUserToken(token);
            request->Record.SetOwner(BUILTIN_ACL_BASIC_OWNER);
            auto* tx = request->Record.AddTransaction();
            tx->SetOperationType(ESchemeOpExternalTableMutation);
            auto* mutation = tx->MutableExternalTableMutation();
            mutation->SetAction(action);
            mutation->SetWorkingDir("/MyRoot");
            mutation->SetName("ExternalTable");
            return request;
        };
        const auto send = [&](TEvTx* request, NKikimrScheme::EStatus expected) {
            AsyncSend(runtime, TTestTxConfig::SchemeShard, request);
            TestModificationResult(runtime, txId, expected);
        };
        const auto create = [&](const TString& key, NKikimrScheme::EStatus expected,
                                const TString& location = "/", bool reverseAttributes = false) {
            auto* request = makeMutation(TExternalTableMutation::CREATE, rootToken);
            auto* mutation = request->Record.MutableTransaction(0)->MutableExternalTableMutation();
            mutation->SetKey(key);
            auto* definition = mutation->MutableDefinition();
            definition->SetSourceType("General");
            definition->SetDataSourcePath("/MyRoot/ExternalDataSource");
            definition->SetLocation(location);
            auto* column = definition->AddColumns();
            column->SetName("key");
            column->SetType("Uint64");
            definition->SetContent(reverseAttributes
                ? compression.SerializeAsString() + format.SerializeAsString()
                : format.SerializeAsString() + compression.SerializeAsString());
            send(request, expected);
        };
        const auto finish = [&](TExternalTableMutation::EAction action, const TString& key,
                                const TString& token, NKikimrScheme::EStatus expected) {
            auto* request = makeMutation(action, token);
            request->Record.MutableTransaction(0)->MutableExternalTableMutation()->SetKey(key);
            send(request, expected);
        };
        const auto abort = [&](const TString& key, const TString& token, NKikimrScheme::EStatus expected) {
            finish(TExternalTableMutation::ABORT, key, token, expected);
        };
        abort("first", rootToken, NKikimrScheme::StatusAccessDenied);
        runtime.GetAppData().AdministrationAllowedSIDs = {"root@builtin", "other-admin@builtin"};
        abort("closed", NACLib::TUserToken("ordinary@builtin", {}).SerializeAsString(), NKikimrScheme::StatusAccessDenied);
        auto* malformed = makeMutation(TExternalTableMutation::ABORT, rootToken);
        auto* tx = malformed->Record.MutableTransaction(0);
        tx->SetWorkingDir("/MyRoot");
        tx->MutableExternalTableMutation()->SetKey("first");
        send(malformed, NKikimrScheme::StatusInvalidParameter);
        abort("closed", rootToken, NKikimrScheme::StatusSuccess);

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        create("closed", NKikimrScheme::StatusPreconditionFailed);
        TestLs(runtime, path, false, NLs::PathNotExist);

        create("first", NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, txId);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        create("first", NKikimrScheme::StatusSuccess, "/", true);
        create("first", NKikimrScheme::StatusPreconditionFailed, "/other");
        create("second", NKikimrScheme::StatusSchemeError);
        abort("first", NACLib::TUserToken("other-admin@builtin", {}).SerializeAsString(), NKikimrScheme::StatusSuccess);
        TestDropExternalTable(runtime, ++txId, "/MyRoot", "ExternalTable", {{NKikimrScheme::StatusPreconditionFailed}});
        TestCreateExternalTableOrReplace(runtime, ++txId, "/MyRoot", schema, {{NKikimrScheme::StatusPreconditionFailed}});
        TestLs(runtime, path, false, NLs::PathExist);

        const auto describe = DescribePath(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(describe.GetPathDescription().GetSelf().GetOwner(), BUILTIN_ACL_BASIC_OWNER);
        const auto& table = describe.GetPathDescription().GetExternalTableDescription();
        abort("first", rootToken, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, txId);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        create("first", NKikimrScheme::StatusPreconditionFailed);
        create("second", NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, txId);
        abort("first", rootToken, NKikimrScheme::StatusSuccess);

        auto* request = makeMutation(TExternalTableMutation::GUARDED_DROP, rootToken);
        auto* guard = request->Record.MutableTransaction(0)->MutableExternalTableMutation()->MutableExpectedTable();
        *guard->MutablePathId() = table.GetPathId();
        guard->SetVersion(table.GetVersion());
        send(request, NKikimrScheme::StatusPreconditionFailed);
        TestLs(runtime, path, false, NLs::PathExist);

        finish(TExternalTableMutation::COMMIT, "second", rootToken, NKikimrScheme::StatusSuccess);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        abort("second", rootToken, NKikimrScheme::StatusPreconditionFailed);
        TestDropExternalTable(runtime, ++txId, "/MyRoot", "ExternalTable");
        env.TestWaitNotification(runtime, txId);
        create("second", NKikimrScheme::StatusSuccess, "/", true);
        TestLs(runtime, path, false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(LeasedCreateTicketsSurviveExpiryUntilRelease) {
        TTestBasicRuntime runtime;
        runtime.SetScheduledEventFilter([](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline) {
            return event->GetTypeRewrite() != NSchemeShard::TEvPrivate::EvCleanupExternalTableCreateTickets &&
                   TTestActorRuntime::DefaultScheduledFilterFunc(runtime, event, delay, deadline);
        });
        TTestEnv env(runtime, TTestEnvOptions().RunFakeConfigDispatcher(true));
        ui64 txId = 100;
        TestCreateExternalDataSource(runtime, txId, "/MyRoot", R"(
            Name: "ExternalDataSource"
            SourceType: "ObjectStorage"
            Location: "https://s3.cloud.net/my_bucket"
            Auth { None {} }
        )");
        env.TestWaitNotification(runtime, txId);
        runtime.GetAppData().AdministrationAllowedSIDs = { "root@builtin" };
        const auto rootToken = NACLib::TUserToken("root@builtin", {}).SerializeAsString();
        const auto schemeShard = TTestTxConfig::SchemeShard;
        const auto root = DescribePath(runtime, "/MyRoot");
        const auto& domain = root.GetPathDescription().GetDomainDescription().GetDomainKey();
        UNIT_ASSERT(domain.GetSchemeShard() && domain.GetPathId());
        const TInstant deadline = runtime.GetCurrentTime() + TDuration::Minutes(5);
        const TString key = TStringBuilder() << "fqv1:" << deadline.MilliSeconds() << ":first";
        const auto send = [&](TExternalTableMutation::EAction action, const TString& operationKey, NKikimrScheme::EStatus expected,
                              ui64 expectedSchemeShard, bool correctDomain = true) {
            auto* request = new TEvTx(++txId, schemeShard);
            request->Record.SetUserToken(rootToken);
            request->Record.SetOwner(BUILTIN_ACL_BASIC_OWNER);
            auto* tx = request->Record.AddTransaction();
            tx->SetOperationType(ESchemeOpExternalTableMutation);
            auto* mutation = tx->MutableExternalTableMutation();
            mutation->SetAction(action);
            mutation->SetWorkingDir("/MyRoot");
            mutation->SetName("LeasedTable");
            mutation->SetKey(operationKey);
            mutation->SetExpectedSchemeShardId(expectedSchemeShard);
            auto* expectedDomain = mutation->MutableExpectedDomainPathId();
            expectedDomain->SetOwnerId(domain.GetSchemeShard());
            expectedDomain->SetLocalId(correctDomain ? domain.GetPathId() : domain.GetPathId() + 1);
            if (action == TExternalTableMutation::CREATE_LEASED) {
                auto* definition = mutation->MutableDefinition();
                definition->SetSourceType("General");
                definition->SetDataSourcePath("/MyRoot/ExternalDataSource");
                definition->SetLocation("/");
                auto* column = definition->AddColumns();
                column->SetName("key");
                column->SetType("Uint64");
                NKikimrExternalSources::TGeneral content;
                (*content.mutable_attributes())["format"] = "json_each_row";
                definition->SetContent(content.SerializeAsString());
            }
            AsyncSend(runtime, schemeShard, request);
            TestModificationResult(runtime, txId, expected);
        };
        const auto create = [&](const TString& operationKey, NKikimrScheme::EStatus expected) {
            send(TExternalTableMutation::CREATE_LEASED, operationKey, expected, schemeShard);
        };

        send(TExternalTableMutation::CREATE_LEASED, key, NKikimrScheme::StatusPreconditionFailed, schemeShard + 1);
        send(TExternalTableMutation::CREATE_LEASED, key, NKikimrScheme::StatusPreconditionFailed, schemeShard, false);
        create(key, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, txId);
        send(TExternalTableMutation::RELEASE, key, NKikimrScheme::StatusPreconditionFailed, schemeShard);
        runtime.UpdateCurrentTime(deadline + TDuration::Seconds(1));
        RebootTablet(runtime, schemeShard, runtime.AllocateEdgeActor());
        create(key, NKikimrScheme::StatusSuccess);
        send(TExternalTableMutation::COMMIT_LEASED, key, NKikimrScheme::StatusSuccess, schemeShard);
        send(TExternalTableMutation::RELEASE, key, NKikimrScheme::StatusSuccess, schemeShard);
        runtime.SimulateSleep(TDuration::Seconds(1));
        create(key, NKikimrScheme::StatusPreconditionFailed);
        TestDropExternalTable(runtime, ++txId, "/MyRoot", "LeasedTable");
        env.TestWaitNotification(runtime, txId);

        RebootTablet(runtime, schemeShard, runtime.AllocateEdgeActor());
        runtime.UpdateCurrentTime(deadline - TDuration::Minutes(1), true);
        create(key, NKikimrScheme::StatusPreconditionFailed);
        send(TExternalTableMutation::RELEASE, key, NKikimrScheme::StatusSuccess, schemeShard);
        TestLs(runtime, "/MyRoot/LeasedTable", false, NLs::PathNotExist);

        const TInstant absentDeadline = deadline + TDuration::Minutes(2);
        const TString absentKey = TStringBuilder() << "fqv1:" << absentDeadline.MilliSeconds() << ":absent";
        runtime.UpdateCurrentTime(absentDeadline + TDuration::Seconds(1));
        send(TExternalTableMutation::ABORT_LEASED, absentKey, NKikimrScheme::StatusSuccess, schemeShard);
        RebootTablet(runtime, schemeShard, runtime.AllocateEdgeActor());
        runtime.UpdateCurrentTime(absentDeadline - TDuration::Minutes(1), true);
        create(absentKey, NKikimrScheme::StatusPreconditionFailed);
        send(TExternalTableMutation::RELEASE, absentKey, NKikimrScheme::StatusSuccess, schemeShard);
    }
}
