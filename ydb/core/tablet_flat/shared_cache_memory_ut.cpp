#include "shared_cache_memory.h"

#include <ydb/library/actors/util/shared_data_backtracing_owner.h>
#include <library/cpp/testing/unittest/registar.h>

#include <thread>
#include <type_traits>

namespace NKikimr::NSharedCache {

static_assert(std::is_nothrow_move_constructible_v<TMetadataAccounting>);

namespace {

    class TDescriptor final : public NPageCollection::IPageCollection {
    public:
        TDescriptor(TSharedData raw, ui64 fixedBytes, ui32 id = 1, std::atomic<ui32>* destroyed = nullptr)
            : Id_(77, 1, id)
            , Raw_(std::move(raw))
            , FixedBytes_(fixedBytes)
            , Destroyed_(destroyed)
        {}

        ~TDescriptor() override {
            if (Destroyed_) {
                Destroyed_->fetch_add(1, std::memory_order_relaxed);
            }
        }

        const TLogoBlobID& Label() const noexcept override {
            return Id_;
        }

        ui32 Total() const noexcept override {
            return 0;
        }

        NPageCollection::TInfo Page(ui32) const override {
            return {};
        }

        NPageCollection::TBorder Bounds(ui32) const override {
            return {};
        }

        NPageCollection::TBorder Bounds(const NPageCollection::TPageLocation&) const override {
            return {};
        }

        NPageCollection::TGlobId Glob(ui32) const override {
            return {};
        }

        bool Verify(ui32, TArrayRef<const char>) const override {
            return false;
        }

        bool Verify(const NPageCollection::TPageLocation&, TArrayRef<const char>) const override {
            return false;
        }

        size_t BackingSize() const noexcept override {
            return 0;
        }

        NPageCollection::TPageLocation GetLocation(ui32) const override {
            return {};
        }

        TAtomicSharedPtr<const NPageCollection::TMeta> GetSharedMeta() const noexcept override {
            return {};
        }

        i64 GetMetadataMemoryUsage() const noexcept override {
            return FixedBytes_ + Raw_.GetAllocationSize();
        }

        const TSharedData* GetMetadataBuffer() const noexcept override {
            return Raw_.data() ? &Raw_ : nullptr;
        }

    private:
        const TLogoBlobID Id_;
        const TSharedData Raw_;
        const ui64 FixedBytes_;
        std::atomic<ui32>* const Destroyed_;
    };

} // namespace

Y_UNIT_TEST_SUITE(TSharedCacheMetadataAccounting) {
    Y_UNIT_TEST(EmptyBufferHasNoAllocationCharge) {
        TMetadataAccounting accounting;
        accounting.AccountBuffer({});
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(BuiltinBuffersShareOneOwnerPerCacheWithoutWrappers) {
        TMetadataAccounting accounting;
        auto first = TSharedData::Uninitialized(64);
        auto second = TSharedData::Uninitialized(128);
        const auto owner = [](const TSharedData& buffer) {
            const auto* header = reinterpret_cast<const TSharedData::THeader*>(buffer.data() - TSharedData::HeaderSize);
            return header->Owner.load(std::memory_order_acquire);
        };
        UNIT_ASSERT(owner(first) == nullptr);
        UNIT_ASSERT(owner(second) == nullptr);
        accounting.AccountBuffer(first);
        accounting.AccountBuffer(second);
        UNIT_ASSERT(owner(first));
        UNIT_ASSERT(owner(first) == owner(second));
        const auto* sharedOwner = owner(first);
        accounting.AccountBuffer(first);
        UNIT_ASSERT(owner(first) == sharedOwner);
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), first.GetAllocationSize() + second.GetAllocationSize());
        first = {};
        second = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(BufferOutlivesAllAccountingFacades) {
        TSharedData raw;
        {
            TMetadataAccounting accounting;
            raw = TSharedData::Uninitialized(64);
            accounting.AccountBuffer(raw);
        }
        auto caller = raw;
        caller.TrimBack(16);
        UNIT_ASSERT_VALUES_EQUAL(caller.GetAllocationSize(), 64 + TSharedData::OverheadSize);
        raw = {};
        caller = {};
    }

    Y_UNIT_TEST(TracingAfterAdoptionStillFindsTheSharedAccount) {
        TMetadataAccounting accounting;
        auto raw = TSharedData::Uninitialized(64);
        const ui64 bytes = raw.GetAllocationSize();
        accounting.AccountBuffer(raw);
        TBackTracingOwner::FakeOwner(raw);
        accounting.AccountBuffer(raw);
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), bytes);
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(ExistingTracingOwnerUsesTheFallbackWithoutDoubleCharging) {
        TMetadataAccounting accounting;
        auto raw = TSharedData::Uninitialized(64);
        const ui64 bytes = raw.GetAllocationSize();
        TBackTracingOwner::FakeOwner(raw);
        accounting.AccountBuffer(raw);
        accounting.AccountBuffer(raw);
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), bytes);
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(CopiesAndTrimsShareOneAllocation) {
        TMetadataAccounting accounting;
        auto raw = TSharedData::Uninitialized(128);
        const ui64 bytes = raw.GetAllocationSize();
        accounting.AccountBuffer(raw);
        auto copy = raw;
        copy.TrimBack(16);
        accounting.AccountBuffer(copy);
        auto retained = std::move(raw);
        copy = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), bytes);
        retained = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(CallerOwnershipRemainsChargedUntilFinalFree) {
        TMetadataAccounting accounting;
        TSharedData caller;
        ui64 bytes = 0;
        {
            auto cacheBuffer = TSharedData::Uninitialized(64);
            bytes = cacheBuffer.GetAllocationSize();
            accounting.AccountBuffer(cacheBuffer);
            caller = cacheBuffer;
        }
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), bytes);
        caller = {};
    }

    Y_UNIT_TEST(SharedRawAndDescriptorStorageHaveSeparateFinalOwners) {
        TMetadataAccounting accounting;
        auto raw = TSharedData::Uninitialized(64);
        const ui64 bytes = raw.GetAllocationSize();
        auto first = MakeIntrusiveConst<TDescriptor>(raw, ui64{ 12 }, ui32{ 1 });
        auto second = MakeIntrusiveConst<TDescriptor>(raw, ui64{ 20 }, ui32{ 2 });
        accounting.AccountBuffer(raw);
        accounting.AccountMetadata(*first);
        accounting.AccountMetadata(*first);
        accounting.AccountMetadata(*second);
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), bytes + 32);
        first.Reset();
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), bytes + 20);
        second.Reset();
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), bytes);
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(DescriptorLifetimeSurvivesRegistrationScope) {
        TMetadataAccounting accounting;
        std::atomic<ui32> destroyed = 0;
        TIntrusiveConstPtr<TDescriptor> caller;
        {
            auto descriptor = MakeIntrusiveConst<TDescriptor>(TSharedData{}, ui64{ 12 }, ui32{ 1 }, &destroyed);
            accounting.AccountMetadata(*descriptor);
            caller = descriptor;
        }
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 12);
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 0);
        caller.Reset();
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
    }

    Y_UNIT_TEST(DistinctAllocationsStayDistinct) {
        TMetadataAccounting accounting;
        auto first = TSharedData::Uninitialized(64);
        auto second = TSharedData::Uninitialized(64);
        accounting.AccountBuffer(first);
        accounting.AccountBuffer(second);
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), first.GetAllocationSize() + second.GetAllocationSize());
        first = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), second.GetAllocationSize());
        second = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(SharedFacadeKeepsTheAccountAlive) {
        TMetadataAccounting observer;
        auto raw = TSharedData::Uninitialized(64);
        {
            TMetadataAccounting original;
            observer = original.Share();
            original.AccountBuffer(raw);
        }
        UNIT_ASSERT_VALUES_EQUAL(observer.GetMemoryUsage(), raw.GetAllocationSize());
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(observer.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(AccountsKeepIndependentAttribution) {
        TMetadataAccounting first;
        TMetadataAccounting second;
        auto raw = TSharedData::Uninitialized(64);
        first.AccountBuffer(raw);
        second.AccountBuffer(raw);
        UNIT_ASSERT_VALUES_EQUAL(first.GetMemoryUsage(), raw.GetAllocationSize());
        UNIT_ASSERT_VALUES_EQUAL(second.GetMemoryUsage(), raw.GetAllocationSize());
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(first.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(second.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(ConcurrentRegistrationChargesEachObjectOnce) {
        TMetadataAccounting accounting;
        auto raw = TSharedData::Uninitialized(128);
        auto descriptor = MakeIntrusiveConst<TDescriptor>(raw, ui64{ 12 });
        std::atomic<bool> start = false;
        TVector<std::thread> threads;
        for (ui32 index = 0; index < 6; ++index) {
            threads.emplace_back([&] {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (ui32 iteration = 0; iteration < 200; ++iteration) {
                    accounting.AccountBuffer(raw);
                    accounting.AccountMetadata(*descriptor);
                }
            });
        }
        start.store(true, std::memory_order_release);
        for (auto& thread : threads) {
            thread.join();
        }
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), raw.GetAllocationSize() + 12);
        descriptor.Reset();
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
    }
}

} // namespace NKikimr::NSharedCache
