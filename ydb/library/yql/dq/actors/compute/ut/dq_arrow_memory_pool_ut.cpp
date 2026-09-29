#include <ydb/library/yql/dq/actors/compute/dq_arrow_memory_pool.h>

#include <library/cpp/testing/unittest/registar.h>

#include <arrow/buffer.h>

#include <atomic>
#include <thread>

namespace NYql::NDq {

namespace {

class TTestQuota : public IMemoryQuotaManager {
public:
    explicit TTestQuota(ui64 limit = std::numeric_limits<ui64>::max())
        : Limit(limit)
    {}

    bool AllocateQuota(ui64 memorySize, bool isOptional) override {
        UNIT_ASSERT(!isOptional);
        if (Quota.load() + memorySize > Limit) {
            ++Refused;
            return false;
        }
        Quota.fetch_add(memorySize);
        return true;
    }

    void FreeQuota(ui64 memorySize) override {
        UNIT_ASSERT(Quota.load() >= memorySize);
        Quota.fetch_sub(memorySize);
    }

    ui64 GetCurrentQuota() const override {
        return Quota.load();
    }

    ui64 GetMaxMemorySize() const override {
        return Limit;
    }

    i64 GetMemoryAvailability() const override {
        return Limit - Quota.load();
    }

    TString MemoryConsumptionDetails() const override {
        return "test quota";
    }

    const ui64 Limit;
    std::atomic<ui64> Quota = 0;
    ui64 Refused = 0;
};

} // namespace

Y_UNIT_TEST_SUITE(TDqArrowMemoryPoolTest) {

Y_UNIT_TEST(UnboundIsNotCharged) {
    auto* pool = GetDqArrowMemoryPool();
    const auto before = pool->bytes_allocated();
    UNIT_ASSERT(!GetArrowMemoryQuota());

    uint8_t* ptr = nullptr;
    UNIT_ASSERT(pool->Allocate(1000, &ptr).ok());
    UNIT_ASSERT(ptr);
    UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(ptr) % 64, 0);
    std::memset(ptr, 1, 1000);
    UNIT_ASSERT_VALUES_EQUAL(pool->bytes_allocated(), before + 1000);
    pool->Free(ptr, 1000);
    UNIT_ASSERT_VALUES_EQUAL(pool->bytes_allocated(), before);
}

Y_UNIT_TEST(BoundIsCharged) {
    auto quota = std::make_shared<TTestQuota>();
    auto* pool = GetDqArrowMemoryPool();

    uint8_t* ptr = nullptr;
    {
        TArrowMemoryQuotaScope scope(quota);
        UNIT_ASSERT_EQUAL(GetArrowMemoryQuota(), quota.get());
        UNIT_ASSERT(pool->Allocate(1000, &ptr).ok());
    }
    UNIT_ASSERT(!GetArrowMemoryQuota());
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 1000);

    // freed out of the scope: the buffer keeps its manager
    pool->Free(ptr, 1000);
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 0);
}

Y_UNIT_TEST(FreedOnAnotherThread) {
    auto quota = std::make_shared<TTestQuota>();
    std::shared_ptr<arrow::Buffer> buffer;
    {
        TArrowMemoryQuotaScope scope(quota);
        auto result = arrow::AllocateBuffer(4096, GetDqArrowMemoryPool());
        UNIT_ASSERT(result.ok());
        buffer = std::move(*result);
    }
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 4096);

    std::weak_ptr<TTestQuota> weak = quota;
    quota.reset();
    // the buffer holds the manager alive
    UNIT_ASSERT(!weak.expired());

    std::thread([buffer = std::move(buffer)]() mutable {
        buffer.reset();
    }).join();
    UNIT_ASSERT(weak.expired());
}

Y_UNIT_TEST(ReallocateChargesOwnManager) {
    auto quota = std::make_shared<TTestQuota>();
    auto other = std::make_shared<TTestQuota>();
    auto* pool = GetDqArrowMemoryPool();

    uint8_t* ptr = nullptr;
    {
        TArrowMemoryQuotaScope scope(quota);
        UNIT_ASSERT(pool->Allocate(100, &ptr).ok());
    }
    std::memset(ptr, 7, 100);

    TArrowMemoryQuotaScope scope(other);
    UNIT_ASSERT(pool->Reallocate(100, 300, &ptr).ok());
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 300);
    UNIT_ASSERT_VALUES_EQUAL(other->GetCurrentQuota(), 0);
    for (int i = 0; i < 100; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(ptr[i], 7);
    }

    UNIT_ASSERT(pool->Reallocate(300, 50, &ptr).ok());
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 50);
    for (int i = 0; i < 50; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(ptr[i], 7);
    }

    pool->Free(ptr, 50);
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 0);
    UNIT_ASSERT_VALUES_EQUAL(other->GetCurrentQuota(), 0);
}

Y_UNIT_TEST(RefusalIsOutOfMemory) {
    auto quota = std::make_shared<TTestQuota>(1000);
    auto* pool = GetDqArrowMemoryPool();
    const auto before = pool->bytes_allocated();
    TArrowMemoryQuotaScope scope(quota);

    uint8_t* ptr = nullptr;
    auto status = pool->Allocate(2000, &ptr);
    UNIT_ASSERT(status.IsOutOfMemory());
    UNIT_ASSERT_STRING_CONTAINS(status.ToString(), "test quota");
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 0);

    UNIT_ASSERT(pool->Allocate(600, &ptr).ok());
    uint8_t* kept = ptr;
    UNIT_ASSERT(pool->Reallocate(600, 1200, &ptr).IsOutOfMemory());
    // the buffer is intact after a refused growth
    UNIT_ASSERT_EQUAL(ptr, kept);
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 600);

    auto result = arrow::AllocateBuffer(1000, pool);
    UNIT_ASSERT(result.status().IsOutOfMemory());

    pool->Free(ptr, 600);
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 0);
    UNIT_ASSERT_VALUES_EQUAL(pool->bytes_allocated(), before);
    UNIT_ASSERT_VALUES_EQUAL(quota->Refused, 3);
}

Y_UNIT_TEST(NestedScopes) {
    auto outer = std::make_shared<TTestQuota>();
    auto inner = std::make_shared<TTestQuota>();
    {
        TArrowMemoryQuotaScope outerScope(outer);
        {
            TArrowMemoryQuotaScope innerScope(inner);
            UNIT_ASSERT_EQUAL(GetArrowMemoryQuota(), inner.get());
            {
                TArrowMemoryQuotaScope unbound(nullptr);
                UNIT_ASSERT(!GetArrowMemoryQuota());
            }
            UNIT_ASSERT_EQUAL(GetArrowMemoryQuota(), inner.get());
        }
        UNIT_ASSERT_EQUAL(GetArrowMemoryQuota(), outer.get());
    }
    UNIT_ASSERT(!GetArrowMemoryQuota());
}

Y_UNIT_TEST(ZeroSize) {
    auto quota = std::make_shared<TTestQuota>();
    auto* pool = GetDqArrowMemoryPool();
    TArrowMemoryQuotaScope scope(quota);

    uint8_t* ptr = nullptr;
    UNIT_ASSERT(pool->Allocate(0, &ptr).ok());
    UNIT_ASSERT(ptr);
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 0);

    // grows from the zero area under the current binding
    UNIT_ASSERT(pool->Reallocate(0, 128, &ptr).ok());
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 128);

    UNIT_ASSERT(pool->Reallocate(128, 0, &ptr).ok());
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 0);
    pool->Free(ptr, 0);
}

Y_UNIT_TEST(ResizableBuffer) {
    auto quota = std::make_shared<TTestQuota>();
    {
        TArrowMemoryQuotaScope scope(quota);
        auto result = arrow::AllocateResizableBuffer(10, GetDqArrowMemoryPool());
        UNIT_ASSERT(result.ok());
        std::shared_ptr<arrow::ResizableBuffer> buffer = std::move(*result);
        UNIT_ASSERT(buffer->Resize(10000).ok());
        UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), buffer->capacity());
        UNIT_ASSERT(buffer->Resize(10, /* shrink_to_fit */ true).ok());
        UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), buffer->capacity());
    }
    UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 0);
}

} // Y_UNIT_TEST_SUITE

} // namespace NYql::NDq
