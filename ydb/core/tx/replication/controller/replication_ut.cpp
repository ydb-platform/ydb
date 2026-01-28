#include "replication.h"

#include <ydb/core/protos/replication.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(Replication) {
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
