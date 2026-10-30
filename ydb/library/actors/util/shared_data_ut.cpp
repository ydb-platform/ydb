#include "shared_data.h"
#include "shared_data_backtracing_owner.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/deque.h>
#include <util/system/sys_alloc.h>

#include <array>
#include <thread>

namespace NActors {

    Y_UNIT_TEST_SUITE(TSharedDataTest) {

        Y_UNIT_TEST(BasicBehavior) {
            auto data = TSharedData::Copy("Hello", 5);
            UNIT_ASSERT(data.IsPrivate());
            UNIT_ASSERT(!data.IsShared());
            UNIT_ASSERT_VALUES_EQUAL(data.size(), 5u);
            UNIT_ASSERT_VALUES_EQUAL(data.end() - data.begin(), 5u);
            UNIT_ASSERT_VALUES_EQUAL(data.mutable_end() - data.mutable_begin(), 5u);
            UNIT_ASSERT(data.begin() == data.data());
            UNIT_ASSERT(data.mutable_data() == data.data());
            UNIT_ASSERT(data.mutable_begin() == data.mutable_data());

            UNIT_ASSERT_VALUES_EQUAL(data.ToString(), TString("Hello"));
            UNIT_ASSERT_VALUES_EQUAL(::memcmp(data.data(), "Hello", 5), 0);

            auto link = data;
            UNIT_ASSERT(!link.IsPrivate());
            UNIT_ASSERT(!data.IsPrivate());
            UNIT_ASSERT(link.IsShared());
            UNIT_ASSERT(data.IsShared());
            UNIT_ASSERT(link.data() == data.data());
            UNIT_ASSERT(link.size() == data.size());

            link = { };
            UNIT_ASSERT(link.IsPrivate());
            UNIT_ASSERT(data.IsPrivate());
            UNIT_ASSERT(!link.IsShared());
            UNIT_ASSERT(!data.IsShared());

            UNIT_ASSERT_VALUES_EQUAL(TString(TStringBuf(data)), TString("Hello"));
            UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice()), TString("Hello"));
            UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice(1)), TString("ello"));
            UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice(1, 3)), TString("ell"));
            UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice(1, 100)), TString("ello"));
            UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice(0, 4)), TString("Hell"));

            link = data;
            UNIT_ASSERT(link.data() == data.data());
            UNIT_ASSERT_VALUES_UNEQUAL(link.Detach(), data.data());
            UNIT_ASSERT_EQUAL(data.size(), link.size());
            UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice()), TString(link.Slice()));
        }

        Y_UNIT_TEST(AllocationSizeSurvivesTrimAndSharing) {
            auto data = TSharedData::Uninitialized(1024);
            auto copy = data;
            data.TrimBack(16);
            UNIT_ASSERT_VALUES_EQUAL(data.size(), 16u);
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 1024u + TSharedData::OverheadSize);
            UNIT_ASSERT_VALUES_EQUAL(copy.GetAllocationSize(), data.GetAllocationSize());
            data.TrimBack(0);
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(copy.GetAllocationSize(), 1024u + TSharedData::OverheadSize);
        }

        Y_UNIT_TEST(TrimBehavior) {
            auto data = TSharedData::Uninitialized(42);

            UNIT_ASSERT_VALUES_EQUAL(data.size(), 42u);
            UNIT_ASSERT(data.data() != nullptr);

            // Trim to non-zero does not change addresses
            const char* ptr1 = data.data();
            data.TrimBack(31);
            const char* ptr2 = data.data();

            UNIT_ASSERT_VALUES_EQUAL(data.size(), 31u);
            UNIT_ASSERT(ptr1 == ptr2);

            // Trim to zero releases underlying data
            data.TrimBack(0);

            UNIT_ASSERT_VALUES_EQUAL(data.size(), 0u);
            UNIT_ASSERT(data.data() == nullptr);
        }

        class TCustomOwner : public TSharedData::IOwner {
            using THeader = TSharedData::THeader;

        public:
            TSharedData Allocate(size_t size) {
                char* raw = reinterpret_cast<char*>(y_allocate(sizeof(THeader) + size));
                THeader* header = reinterpret_cast<THeader*>(raw);
                new (header) THeader(this);
                char* data = raw + sizeof(THeader);
                Y_ABORT_UNLESS(Allocated_.emplace(data, sizeof(THeader) + size).second);
                return TSharedData::AttachUnsafe(data, size);
            }

            size_t GetAllocationSize(const char* data) const noexcept override {
                const auto it = Allocated_.find(data);
                Y_ABORT_UNLESS(it != Allocated_.end());
                return it->second;
            }

            void Deallocate(char* data) noexcept override {
                Y_ABORT_UNLESS(Allocated_.erase(data) > 0);
                char* raw = data - sizeof(THeader);
                THeader* header = reinterpret_cast<THeader*>(raw);
                header->~THeader();
                y_deallocate(raw);
                Deallocated_.push_back(data);
            }

            char* NextDeallocated() {
                char* result = nullptr;
                if (Deallocated_) {
                    result = Deallocated_.front();
                    Deallocated_.pop_front();
                }
                return result;
            }

        private:
            THashMap<const void*, size_t> Allocated_;
            TDeque<char*> Deallocated_;
        };

        struct TAllocationTag {
            explicit TAllocationTag(std::atomic<size_t>& created, std::atomic<size_t>& destroyed)
                : Destroyed(destroyed)
            {
                ++created;
            }

            ~TAllocationTag() {
                ++Destroyed;
            }

            std::atomic<size_t>& Destroyed;
        };

        Y_UNIT_TEST(AllocationTagFollowsFinalRelease) {
            std::atomic<size_t> created{ 0 };
            std::atomic<size_t> destroyed{ 0 };
            auto data = TSharedData::Uninitialized(128);
            auto copy = data;
            auto& tag = data.GetOrCreateAllocationTag<TAllocationTag>(created, destroyed);
            UNIT_ASSERT(copy.FindAllocationTag<TAllocationTag>() == &tag);
            copy.TrimBack(64);
            UNIT_ASSERT_VALUES_EQUAL(copy.GetAllocationSize(), 128 + TSharedData::OverheadSize);
            UNIT_ASSERT(&copy.GetOrCreateAllocationTag<TAllocationTag>(created, destroyed) == &tag);
            UNIT_ASSERT_VALUES_EQUAL(created.load(), 1);
            data = {};
            UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 0);
            copy = {};
            UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
        }

        Y_UNIT_TEST(AllocationTagPreservesCustomOwner) {
            std::atomic<size_t> created{ 0 };
            std::atomic<size_t> destroyed{ 0 };
            TCustomOwner owner;
            auto data = owner.Allocate(42);
            const auto* ptr = data.data();
            data.GetOrCreateAllocationTag<TAllocationTag>(created, destroyed);
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 42 + TSharedData::HeaderSize);
            data = {};
            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);
            UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
        }

        Y_UNIT_TEST(AllocationTagComposesWithTracingOwner) {
            for (bool traceFirst : { false, true }) {
                std::atomic<size_t> created{ 0 };
                std::atomic<size_t> destroyed{ 0 };
                auto data = TSharedData::Uninitialized(42);
                if (traceFirst) {
                    TBackTracingOwner::FakeOwner(data);
                }
                auto& tag = data.GetOrCreateAllocationTag<TAllocationTag>(created, destroyed);
                if (!traceFirst) {
                    TBackTracingOwner::FakeOwner(data);
                }
                UNIT_ASSERT(data.FindAllocationTag<TAllocationTag>() == &tag);
                UNIT_ASSERT(data.FindAllocationTag<TBackTracingOwner>());
                UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 42 + TSharedData::OverheadSize);
                data = {};
                UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
            }
        }

        Y_UNIT_TEST(ConcurrentAllocationTagInstallation) {
            std::atomic<size_t> created{ 0 };
            std::atomic<size_t> destroyed{ 0 };
            std::atomic<bool> start{ false };
            auto data = TSharedData::Uninitialized(42);
            std::array<TAllocationTag*, 8> tags;
            std::array<std::thread, 8> threads;
            for (size_t i = 0; i < threads.size(); ++i) {
                threads[i] = std::thread([&, i, copy = data] {
                    while (!start.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    if (i % 2 == 0) {
                        TBackTracingOwner::FakeOwner(copy);
                    }
                    tags[i] = &copy.GetOrCreateAllocationTag<TAllocationTag>(created, destroyed);
                });
            }
            start.store(true, std::memory_order_release);
            for (auto& thread : threads) {
                thread.join();
            }
            for (auto* tag : tags) {
                UNIT_ASSERT(tag == tags[0]);
            }
            UNIT_ASSERT(data.FindAllocationTag<TBackTracingOwner>());
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 42 + TSharedData::OverheadSize);
            UNIT_ASSERT_VALUES_EQUAL(created.load() - destroyed.load(), 1);
            data = {};
            UNIT_ASSERT_VALUES_EQUAL(created.load(), destroyed.load());
        }

        Y_UNIT_TEST(CustomOwnerAllocationSizeSurvivesTrim) {
            TCustomOwner owner;
            auto data = owner.Allocate(42);
            auto copy = data;
            data.TrimBack(7);
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 42u + TSharedData::HeaderSize);
            UNIT_ASSERT_VALUES_EQUAL(copy.GetAllocationSize(), data.GetAllocationSize());
        }

        Y_UNIT_TEST(TracingOwnerAllocationSizeSurvivesTrim) {
            auto data = TSharedData::AttachUnsafe(TBackTracingOwner::Allocate(42), 42);
            data.TrimBack(7);
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 42u + TSharedData::OverheadSize);
        }

        Y_UNIT_TEST(TracingOwnerDelegatesAllocationSize) {
            TCustomOwner owner;
            auto data = owner.Allocate(42);
            TBackTracingOwner::FakeOwner(data);
            data.TrimBack(7);
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 42u + TSharedData::HeaderSize);
        }

        Y_UNIT_TEST(CustomOwner) {
            TCustomOwner owner;
            const char* ptr;

            // Test destructor releases data
            {
                auto data = owner.Allocate(42);
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 42u);
                ptr = data.data();
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

            // Test assignment releases data
            {
                auto data = owner.Allocate(42);
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 42u);
                ptr = data.data();
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
                data = { };
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

            // Test copies keep references correctly
            {
                auto data = owner.Allocate(42);
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 42u);
                ptr = data.data();
                auto copy = data;
                UNIT_ASSERT_VALUES_EQUAL(copy.size(), 42u);
                UNIT_ASSERT(copy.data() == ptr);
                data = { };
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 0u);
                UNIT_ASSERT(data.data() == nullptr);
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

            // Test assignment releases correct data
            {
                auto data1 = owner.Allocate(42);
                UNIT_ASSERT_VALUES_EQUAL(data1.size(), 42u);
                auto data2 = owner.Allocate(31);
                UNIT_ASSERT_VALUES_EQUAL(data2.size(), 31u);
                ptr = data1.data();
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
                data1 = data2;
                UNIT_ASSERT(owner.NextDeallocated() == ptr);
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
                ptr = data2.data();
                UNIT_ASSERT_VALUES_EQUAL(data1.size(), 31u);
                UNIT_ASSERT(data1.data() == ptr);
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

            // Test moves don't produce dangling references
            {
                auto data = owner.Allocate(42);
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 42u);
                ptr = data.data();
                auto moved = std::move(data);
                UNIT_ASSERT_VALUES_EQUAL(moved.size(), 42u);
                UNIT_ASSERT(moved.data() == ptr);
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 0u);
                UNIT_ASSERT(data.data() == nullptr);
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

            // Test Detach copies correctly and doesn't affect owned data
            {
                auto data = owner.Allocate(42);
                ptr = data.data();
                auto disowned = data;
                disowned.Detach();
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

        }

    }

}
