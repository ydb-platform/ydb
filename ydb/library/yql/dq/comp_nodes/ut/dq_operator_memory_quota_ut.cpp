#include <ydb/library/yql/dq/comp_nodes/operator_memory_quota/dq_operator_memory_quota.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/thread.h>

using namespace NYql::NDq;

namespace {

struct TFakeQuota : public IDqOperatorMemoryQuota {
    bool RequestExtraMemory(ui64, bool) override {
        return true;
    }

    i64 GetMemoryAvailability() const override {
        return 0;
    }

    void TryShrinkMemory() override {
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TDqOperatorMemoryQuotaScopeTest) {

    Y_UNIT_TEST(Unbound) {
        UNIT_ASSERT(GetDqOperatorMemoryQuota() == nullptr);
    }

    Y_UNIT_TEST(BindAndRestore) {
        TFakeQuota a;
        {
            TDqOperatorMemoryQuotaScope scope(&a);
            UNIT_ASSERT_EQUAL(GetDqOperatorMemoryQuota(), &a);
        }
        UNIT_ASSERT(GetDqOperatorMemoryQuota() == nullptr);
    }

    Y_UNIT_TEST(Nested) {
        TFakeQuota a;
        TFakeQuota b;
        TDqOperatorMemoryQuotaScope outer(&a);
        {
            TDqOperatorMemoryQuotaScope inner(&b);
            UNIT_ASSERT_EQUAL(GetDqOperatorMemoryQuota(), &b);
        }
        UNIT_ASSERT_EQUAL(GetDqOperatorMemoryQuota(), &a);
    }

    Y_UNIT_TEST(NullScope) {
        TFakeQuota a;
        TDqOperatorMemoryQuotaScope outer(&a);
        {
            TDqOperatorMemoryQuotaScope inner(nullptr);
            UNIT_ASSERT(GetDqOperatorMemoryQuota() == nullptr);
        }
        UNIT_ASSERT_EQUAL(GetDqOperatorMemoryQuota(), &a);
    }

    Y_UNIT_TEST(ThreadIsolation) {
        TFakeQuota a;
        TFakeQuota b;
        TDqOperatorMemoryQuotaScope scope(&a);

        IDqOperatorMemoryQuota* seenBefore = &a;
        IDqOperatorMemoryQuota* seenInside = nullptr;
        TThread thread([&]() {
            seenBefore = GetDqOperatorMemoryQuota();
            TDqOperatorMemoryQuotaScope other(&b);
            seenInside = GetDqOperatorMemoryQuota();
        });
        thread.Start();
        thread.Join();

        UNIT_ASSERT(seenBefore == nullptr);
        UNIT_ASSERT_EQUAL(seenInside, &b);
        UNIT_ASSERT_EQUAL(GetDqOperatorMemoryQuota(), &a);
    }
}
