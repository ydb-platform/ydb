#include "shared_data.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/deque.h>
#include <util/system/sys_alloc.h>

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
                Y_ABORT_UNLESS(Allocated_.insert(data).second);
                return TSharedData::AttachUnsafe(data, size);
            }

            void Deallocate(char* data) noexcept {
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
            THashSet<void*> Allocated_;
            TDeque<char*> Deallocated_;
        };

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
