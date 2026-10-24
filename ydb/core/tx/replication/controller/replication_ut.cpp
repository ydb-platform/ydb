#include "replication.h"
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
}

}
