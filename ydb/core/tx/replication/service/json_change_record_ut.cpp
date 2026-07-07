#include "json_change_record.h"

#include <ydb/core/protos/tx_datashard.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/aclib/user_context.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(JsonChangeRecord) {
    Y_UNIT_TEST(DataChange) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"({"key":[1], "update":{"value":100500}})")
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcDataChange);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 0);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 0);
    }

    Y_UNIT_TEST(DataChangeVersion) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"({"key":[1], "update":{"value":100500}, "ts":[10,20]})")
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);
    }

    Y_UNIT_TEST(Heartbeat) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"({"resolved":[10,20]})")
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);
    }

    Y_UNIT_TEST(SerializeThrowsOnUnknownColumnInsteadOfCrashing) {
        // Simulates a source-table column rename racing with an in-flight CDC record: the
        // record was produced under the old column name ("value_old"), but by the time it
        // reaches Serialize() the destination's resolved schema only knows the new name
        // ("value_new"). This must be a catchable exception, not a process-crashing abort,
        // so the owning actor can leave gracefully and let the worker re-resolve.
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        schema->ValueColumns.emplace("value_new", TLightweightSchema::TColumn{
            .Tag = 2,
            .Type = NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
        });

        auto record = TChangeRecordBuilder()
            .WithBody(R"({"key":[1], "update":{"value_old":100500}})")
            .WithSchema(schema)
            .Build();

        TMemoryPool pool(256);
        NKikimrTxDataShard::TEvApplyReplicationChanges_TChange out;
        UNIT_ASSERT_EXCEPTION(record->Serialize(out, pool), TUnknownColumnException);
    }

    Y_UNIT_TEST(SerializeSucceedsWhenColumnNameMatches) {
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        schema->ValueColumns.emplace("value", TLightweightSchema::TColumn{
            .Tag = 2,
            .Type = NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
        });

        auto record = TChangeRecordBuilder()
            .WithBody(R"({"key":[1], "update":{"value":100500}})")
            .WithSchema(schema)
            .Build();

        TMemoryPool pool(256);
        NKikimrTxDataShard::TEvApplyReplicationChanges_TChange out;
        record->Serialize(out, pool);
        UNIT_ASSERT(out.HasUpsert());
    }
}

}
