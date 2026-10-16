#include "replication.h"
#include "target_table.h"

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

    Y_UNIT_TEST(SkipInitialScanIsImmutable) {
        NKikimrReplication::TReplicationConfig config;
        config.SetSkipInitialScan(true);
        auto replication = MakeIntrusive<TReplication>(ui64(1), TPathId(1, 2), config, "/Root/db");

        config.ClearSkipInitialScan();
        replication->SetConfig(std::move(config));
        UNIT_ASSERT(replication->GetConfig().GetSkipInitialScan());
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
