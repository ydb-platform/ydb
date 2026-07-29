#include "table_description.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace {

struct TEvConvertColumnTableResult : NActors::TEventLocal<TEvConvertColumnTableResult, NActors::TEvents::ES_PRIVATE + 301> {
    bool Ok = false;
    TString Error;
    NKikimrSchemeOp::TColumnTableDescription TableDesc;
};

class TConvertColumnTableActor : public NActors::TActorBootstrapped<TConvertColumnTableActor> {
private:
    const NActors::TActorId ReplyTo;

public:
    explicit TConvertColumnTableActor(const NActors::TActorId& replyTo)
        : ReplyTo(replyTo)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        Ydb::Table::CreateTableRequest req;

        {
            auto* timestamp = req.add_columns();
            timestamp->set_name("timestamp");
            timestamp->mutable_type()->set_type_id(Ydb::Type::TIMESTAMP);
            timestamp->set_not_null(true);
        }
        {
            auto* resourceId = req.add_columns();
            resourceId->set_name("resource_id");
            resourceId->mutable_type()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto* uid = req.add_columns();
            uid->set_name("uid");
            uid->mutable_type()->set_type_id(Ydb::Type::UTF8);
            uid->set_not_null(true);
        }
        {
            auto* payload = req.add_columns();
            payload->set_name("payload");
            payload->mutable_type()->set_type_id(Ydb::Type::UTF8);
        }

        req.add_primary_key("timestamp");
        req.add_primary_key("uid");

        {
            auto* bloomIndex = req.add_indexes();
            bloomIndex->set_name("idx_bloom");
            bloomIndex->add_index_columns("resource_id");
            bloomIndex->mutable_local_bloom_filter_index()->set_false_positive_probability(0.01);
        }
        {
            auto* minMaxIndex = req.add_indexes();
            minMaxIndex->set_name("idx_minmax");
            minMaxIndex->add_index_columns("payload");
            minMaxIndex->mutable_local_min_max_index();
        }

        NKikimrSchemeOp::TModifyScheme modifyScheme;
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS;
        TString error;
        const bool ok = FillColumnTableDescription(modifyScheme, req, status, error);

        auto* result = new TEvConvertColumnTableResult;
        result->Ok = ok;
        result->Error = error;
        if (ok) {
            result->TableDesc = modifyScheme.GetCreateColumnTable();
        }

        ctx.Send(ReplyTo, result);
        Die(ctx);
    }
};

NKikimrSchemeOp::TColumnTableDescription ConvertColumnTableWithIndexes() {
    NActors::TTestBasicRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    runtime.GetAppData().FeatureFlags.SetEnableLocalBloomFilterIndex(true);
    runtime.GetAppData().FeatureFlags.SetEnableLocalMinMaxIndex(true);

    const auto edge = runtime.AllocateEdgeActor();
    runtime.Register(new TConvertColumnTableActor(edge), 0, runtime.GetAppData().SystemPoolId);
    const auto ev = runtime.GrabEdgeEvent<TEvConvertColumnTableResult>(edge);
    UNIT_ASSERT_C(ev->Get()->Ok, ev->Get()->Error);
    return ev->Get()->TableDesc;
}

} // namespace

Y_UNIT_TEST_SUITE(ConvertColumnTableIndexEntityIds) {
    Y_UNIT_TEST(CreateRequestAssignsIndexIdsAfterColumnIds) {
        const auto tableDesc = ConvertColumnTableWithIndexes();
        const auto& schema = tableDesc.GetSchema();

        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns().size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetIndexes().size(), 2u);

        const ui32 columnCount = static_cast<ui32>(schema.GetColumns().size());
        THashSet<ui32> columnIds;
        for (ui32 i = 1; i <= columnCount; ++i) {
            columnIds.insert(i);
        }

        for (const auto& index : schema.GetIndexes()) {
            UNIT_ASSERT(index.HasId());
            UNIT_ASSERT_C(!columnIds.contains(index.GetId()),
                "index '" << index.GetName() << "' id " << index.GetId() << " overlaps with a column id");
            UNIT_ASSERT_C(index.GetId() > columnCount,
                "index '" << index.GetName() << "' id " << index.GetId()
                          << " must be greater than column count " << columnCount);
        }

        bool foundBloom = false;
        bool foundMinMax = false;
        for (const auto& index : schema.GetIndexes()) {
            if (index.GetName() == "idx_bloom") {
                foundBloom = true;
                UNIT_ASSERT_VALUES_EQUAL(index.GetId(), 5u);
                UNIT_ASSERT(index.HasBloomFilter());
                UNIT_ASSERT_VALUES_EQUAL(index.GetBloomFilter().GetColumnIds().size(), 1u);
                UNIT_ASSERT_VALUES_EQUAL(index.GetBloomFilter().GetColumnIds(0), 2u);
            }
            if (index.GetName() == "idx_minmax") {
                foundMinMax = true;
                UNIT_ASSERT_VALUES_EQUAL(index.GetId(), 6u);
                UNIT_ASSERT(index.HasMinMaxIndex());
                UNIT_ASSERT_VALUES_EQUAL(index.GetMinMaxIndex().GetColumnId(), 4u);
            }
        }
        UNIT_ASSERT(foundBloom);
        UNIT_ASSERT(foundMinMax);
    }
}

} // namespace NKikimr
