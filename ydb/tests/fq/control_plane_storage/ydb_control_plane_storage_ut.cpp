#include "ydb_test_bootstrap.h"

namespace NFq {

using namespace NActors;
using namespace NKikimr;

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageTest) {
    Y_UNIT_TEST(ShouldCreateTable)
    {
        TTestBootstrap bootstrap{"ShouldCreateTable"};

        {
            auto description = bootstrap.WaitTable("queries");
            UNIT_ASSERT(description);
        }

        {
            auto description = bootstrap.WaitTable("pending_small");
            UNIT_ASSERT(description);
        }

        {
            auto description = bootstrap.WaitTable("connections");
            UNIT_ASSERT(description);
        }

        {
            auto description = bootstrap.WaitTable("bindings");
            UNIT_ASSERT(description);
        }

        {
            auto description = bootstrap.WaitTable("idempotency_keys");
            UNIT_ASSERT(description);
        }

        {
            auto description = bootstrap.WaitTable("result_sets");
            UNIT_ASSERT(description);
        }

        {
            auto description = bootstrap.WaitTable("jobs");
            UNIT_ASSERT(description);
        }

        {
            auto description = bootstrap.WaitTable("nodes");
            UNIT_ASSERT(description);
        }

        {
            auto description = bootstrap.WaitTable("quotas");
            UNIT_ASSERT(description);
        }

        {
            auto description = bootstrap.WaitTable("tenants");
            UNIT_ASSERT(description);
        }

        {
            auto description = bootstrap.WaitTable("tenant_acks");
            UNIT_ASSERT(description);
        }

        {
            auto description = bootstrap.WaitTable("mappings");
            UNIT_ASSERT(description);
        }
    }
}

} // NFq
