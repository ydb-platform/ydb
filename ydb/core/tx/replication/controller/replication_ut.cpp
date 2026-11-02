#include "replication.h"
#include "target_table.h"
#include "util.h"

#include <ydb/core/protos/replication.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(Replication) {
    Y_UNIT_TEST(SourceRetryClassification) {
        struct TCase {
            NYdb::EStatus Status;
            TString Issue;
            bool Retry;
        };
        for (const auto& test : TVector<TCase>{
            {NYdb::EStatus::TRANSPORT_UNAVAILABLE, "Domain name not found", true},
            {NYdb::EStatus::TRANSPORT_UNAVAILABLE, "connection refused", true},
            {NYdb::EStatus::TRANSPORT_UNAVAILABLE, "Misformatted domain name", false},
            {NYdb::EStatus::TRANSPORT_UNAVAILABLE, "Client TLS credentials validation failed", false},
            {NYdb::EStatus::CLIENT_UNAUTHENTICATED, "expired credentials", false},
            {NYdb::EStatus::UNAUTHORIZED, "permission denied", false},
            {NYdb::EStatus::BAD_REQUEST, "invalid endpoint settings", false},
            {NYdb::EStatus::SCHEME_ERROR, "incompatible object", false},
        }) {
            NYdb::NIssue::TIssues issues;
            issues.AddIssue(test.Issue);
            UNIT_ASSERT_VALUES_EQUAL_C(IsRetryableError(NYdb::TStatus(test.Status, std::move(issues))),
                test.Retry, test.Issue);
        }
    }

    Y_UNIT_TEST(CredentialReadiness) {
        const auto ready = [](const NKikimrReplication::TConnectionParams& params) {
            NKikimrReplication::TReplicationConfig config;
            config.MutableSrcConnectionParams()->CopyFrom(params);
            return MakeIntrusive<TReplication>(ui64(1), TPathId(1, 2), config, "/Root")->IsCredentialsReady();
        };

        NKikimrReplication::TConnectionParams params;
        UNIT_ASSERT(ready(params)); // Local source does not need credentials.
        params.SetEndpoint("localhost:2135");
        UNIT_ASSERT(!ready(params));

        auto* token = params.MutableOAuthToken();
        token->SetTokenSecretName("token");
        UNIT_ASSERT(!ready(params));
        token->SetToken(""); // Explicit anonymous access is already resolved.
        UNIT_ASSERT(ready(params));
        token->SetToken("resolved-token");
        UNIT_ASSERT(ready(params));

        auto* password = params.MutableStaticCredentials();
        password->SetUser("user");
        password->SetPasswordSecretName("password");
        UNIT_ASSERT(!ready(params));
        password->SetPassword("");
        UNIT_ASSERT(ready(params));
        password->SetPassword("resolved-password");
        UNIT_ASSERT(ready(params));

        auto* iam = params.MutableIamCredentials();
        iam->MutableInitialToken()->SetTokenSecretName("initial-token");
        UNIT_ASSERT(!ready(params));
        iam->MutableInitialToken()->SetToken("resolved-initial-token");
        UNIT_ASSERT(!ready(params)); // Resource discovery must finish too.
        iam->SetResourceId("resource-id");
        UNIT_ASSERT(ready(params));

        params.ClearEndpoint();
        iam->ClearResourceId();
        UNIT_ASSERT(ready(params));
    }

    Y_UNIT_TEST(ResourceId) {
        const ui64 id = 1;
        const auto pathId = TPathId(1, 2);
        const auto resourceId = "resource-id";
        NKikimrReplication::TReplicationConfig config;

        // init with resource id
        config.MutableSrcConnectionParams()->MutableIamCredentials()->SetResourceId(resourceId);
        auto replication = MakeIntrusive<TReplication>(id, pathId, std::move(config), "/Root/db");
        UNIT_ASSERT_VALUES_EQUAL(replication->GetConfig().GetSrcConnectionParams().GetIamCredentials().GetResourceId(), resourceId);

        // try to change resource id
        config.MutableSrcConnectionParams()->MutableIamCredentials()->SetResourceId("");
        replication->SetConfig(std::move(config));
        UNIT_ASSERT_VALUES_EQUAL(replication->GetConfig().GetSrcConnectionParams().GetIamCredentials().GetResourceId(), resourceId);

        // clear iam credentials
        config.MutableSrcConnectionParams()->ClearIamCredentials();
        replication->SetConfig(std::move(config));
        UNIT_ASSERT_VALUES_EQUAL(replication->GetConfig().GetSrcConnectionParams().GetIamCredentials().GetResourceId(), "");

        // set resource id back
        config.MutableSrcConnectionParams()->MutableIamCredentials()->SetResourceId(resourceId);
        replication->SetConfig(std::move(config));
        UNIT_ASSERT_VALUES_EQUAL(replication->GetConfig().GetSrcConnectionParams().GetIamCredentials().GetResourceId(), resourceId);

        // clear entire connection params
        config.ClearSrcConnectionParams();
        replication->SetConfig(std::move(config));
        UNIT_ASSERT_VALUES_EQUAL(replication->GetConfig().GetSrcConnectionParams().GetIamCredentials().GetResourceId(), "");
    }

    Y_UNIT_TEST(InitialScanIsImmutable) {
        NKikimrReplication::TReplicationConfig config;
        config.SetInitialScan(false);
        auto replication = MakeIntrusive<TReplication>(ui64(1), TPathId(1, 2), config, "/Root/db");

        config.ClearInitialScan();
        replication->SetConfig(std::move(config));
        UNIT_ASSERT(!replication->GetConfig().GetInitialScan());
    }

    struct TLagSetup {
        TReplication::TPtr Replication = MakeIntrusive<TReplication>(ui64(1), TPathId(1, 2),
            NKikimrReplication::TReplicationConfig(), "/Root/db");
        TReplication::ITarget* Target;
        TReplication::ITarget* Other;

        TLagSetup() {
            const auto add = [&](const TString& path) {
                const auto id = Replication->AddTarget(TReplication::ETargetKind::Table,
                    std::make_shared<TTargetTable::TTableConfig>(path, path + "_replica"));
                return Replication->FindTarget(id);
            };
            Target = add("/Root/table");
            Other = add("/Root/other");
            Target->AddWorker(1);
            Target->AddWorker(2);
            Other->AddWorker(1);
            Target->UpdateLag(1, TDuration::Seconds(5));
            Target->UpdateLag(2, TDuration::Seconds(5));
            Other->UpdateLag(1, TDuration::Seconds(2));
        }
    };

    Y_UNIT_TEST(WorkerLagPropagatesMaximumChanges) {
        TLagSetup s;
        UNIT_ASSERT(s.Replication->GetLag() == TDuration::Seconds(5));
        s.Target->UpdateLag(1, TDuration::Seconds(10));
        UNIT_ASSERT(s.Target->GetLag() == TDuration::Seconds(10));
        UNIT_ASSERT(s.Replication->GetLag() == TDuration::Seconds(10));
        s.Target->UpdateLag(1, TDuration::Seconds(1));
        UNIT_ASSERT(s.Replication->GetLag() == TDuration::Seconds(5));
    }

    Y_UNIT_TEST(InvalidWorkerLagCannotBeMasked) {
        TLagSetup s;
        s.Target->InvalidateLag(1);
        s.Target->InvalidateLag(1); // Repeated invalidation is harmless.
        UNIT_ASSERT(!s.Target->GetLag());
        UNIT_ASSERT(!s.Replication->GetLag());
        UNIT_ASSERT(s.Other->GetLag() == TDuration::Seconds(2));
        s.Target->UpdateLag(2, TDuration::Seconds(20));
        UNIT_ASSERT(!s.Replication->GetLag());
        s.Target->UpdateLag(1, TDuration::Seconds(1));
        UNIT_ASSERT(s.Replication->GetLag() == TDuration::Seconds(20));
    }

    Y_UNIT_TEST(AddingWorkerInvalidatesLagUntilReportOrRemoval) {
        TLagSetup s;
        s.Target->AddWorker(1); // Existing workers keep their state.
        UNIT_ASSERT(s.Replication->GetLag() == TDuration::Seconds(5));
        s.Target->AddWorker(3);
        UNIT_ASSERT(!s.Replication->GetLag());
        s.Target->RemoveWorker(3);
        UNIT_ASSERT(s.Replication->GetLag() == TDuration::Seconds(5));
        s.Target->AddWorker(3);
        s.Target->UpdateLag(3, TDuration::Seconds(9));
        UNIT_ASSERT(s.Replication->GetLag() == TDuration::Seconds(9));
    }

    Y_UNIT_TEST(RemovingWorkerClearsLagAndInvalidState) {
        TLagSetup s;
        s.Target->UpdateLag(1, TDuration::Seconds(20));
        s.Target->RemoveWorker(1);
        UNIT_ASSERT(s.Replication->GetLag() == TDuration::Seconds(5));
        s.Target->AddWorker(1);
        s.Target->UpdateLag(1, TDuration::Seconds(20));
        s.Target->InvalidateLag(1);
        s.Target->RemoveWorker(1);
        UNIT_ASSERT(s.Replication->GetLag() == TDuration::Seconds(5));
        s.Target->RemoveWorker(1);
        s.Target->RemoveWorker(2);
        UNIT_ASSERT(!s.Target->GetLag());
        UNIT_ASSERT(!s.Replication->GetLag());
        s.Replication->RemoveTarget(s.Target->GetId());
        UNIT_ASSERT(s.Replication->GetLag() == TDuration::Seconds(2));
    }

    Y_UNIT_TEST(RemoveTargetClearsReportedLag) {
        auto replication = MakeIntrusive<TReplication>(ui64(1), TPathId(1, 2),
            NKikimrReplication::TReplicationConfig(), "/Root/db");
        const auto base = replication->AddTarget(TReplication::ETargetKind::Table,
            std::make_shared<TTargetTable::TTableConfig>("/Root/table", "/Root/replica"));
        const auto index = replication->AddTarget(TReplication::ETargetKind::IndexTable,
            std::make_shared<TTargetIndexTable::TIndexTableConfig>("/Root/table/by_value", "/Root/replica/by_value"));
        const auto otherIndex = replication->AddTarget(TReplication::ETargetKind::IndexTable,
            std::make_shared<TTargetIndexTable::TIndexTableConfig>("/Root/table/other", "/Root/replica/other"));
        replication->UpdateLag(base, TDuration::Seconds(1));
        replication->UpdateLag(index, TDuration::Seconds(20));
        replication->UpdateLag(otherIndex, TDuration::Seconds(20));
        UNIT_ASSERT(replication->GetLag());
        UNIT_ASSERT_VALUES_EQUAL(*replication->GetLag(), TDuration::Seconds(20));

        replication->RemoveTarget(index);
        UNIT_ASSERT(replication->GetLag());
        UNIT_ASSERT_VALUES_EQUAL(*replication->GetLag(), TDuration::Seconds(20));
        replication->RemoveTarget(otherIndex);
        UNIT_ASSERT(replication->GetLag());
        UNIT_ASSERT_VALUES_EQUAL(*replication->GetLag(), TDuration::Seconds(1));

        replication->UpdateLag(index, TDuration::Seconds(30));
        replication->RemoveTarget(index);
        UNIT_ASSERT(replication->GetLag());
        UNIT_ASSERT_VALUES_EQUAL(*replication->GetLag(), TDuration::Seconds(1));
        replication->RemoveTarget(base);
        UNIT_ASSERT(!replication->GetLag());
    }

    Y_UNIT_TEST(RemoveTargetClearsPendingLag) {
        auto replication = MakeIntrusive<TReplication>(ui64(1), TPathId(1, 2),
            NKikimrReplication::TReplicationConfig(), "/Root/db");
        const auto base = replication->AddTarget(TReplication::ETargetKind::Table,
            std::make_shared<TTargetTable::TTableConfig>("/Root/table", "/Root/replica"));
        const auto index = replication->AddTarget(TReplication::ETargetKind::IndexTable,
            std::make_shared<TTargetIndexTable::TIndexTableConfig>("/Root/table/by_value", "/Root/replica/by_value"));
        replication->UpdateLag(base, TDuration::Seconds(1));
        UNIT_ASSERT(!replication->GetLag());

        replication->RemoveTarget(index);
        UNIT_ASSERT(replication->GetLag());
        UNIT_ASSERT_VALUES_EQUAL(*replication->GetLag(), TDuration::Seconds(1));
        replication->UpdateLag(base, TDuration::Seconds(2));
        UNIT_ASSERT(replication->GetLag());
        UNIT_ASSERT_VALUES_EQUAL(*replication->GetLag(), TDuration::Seconds(2));
    }
}

}
