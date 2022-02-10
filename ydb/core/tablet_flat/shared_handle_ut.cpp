#include "shared_handle.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TPageHandleTest) {
    class TTestHandle : public TSharedPageHandle {
    public:
        explicit TTestHandle(TSharedData data) {
            Initialize(std::move(data));
        }
    };

    Y_UNIT_TEST(Uninitialized) {
        TSharedPageHandle handle;
        UNIT_ASSERT(!handle.IsInitialized());
    }

    Y_UNIT_TEST(NormalUse) {
        const char* phello = "hello";
        TTestHandle handle(TSharedData::Copy(phello, 5));

        UNIT_ASSERT_VALUES_EQUAL(handle.UseCount(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(handle.PinCount(), 0u);
        UNIT_ASSERT(!handle.IsGarbage());
        UNIT_ASSERT(!handle.IsDropped());

        UNIT_ASSERT(handle.Use());
        UNIT_ASSERT_VALUES_EQUAL(handle.UseCount(), 2u);

        auto data = handle.Pin();
        UNIT_ASSERT_VALUES_EQUAL(handle.PinCount(), 1u);
        UNIT_ASSERT(data.Slice() == phello);

        const char* pfoobar = "foobar";
        UNIT_ASSERT(!handle.TryMove(TSharedData::Copy(pfoobar, 6)));
        handle.UnPin();
        UNIT_ASSERT_VALUES_EQUAL(handle.PinCount(), 0u);
        UNIT_ASSERT(handle.TryMove(TSharedData::Copy(pfoobar, 6)));

        data = handle.Pin();
        UNIT_ASSERT_VALUES_EQUAL(handle.PinCount(), 1u);
        UNIT_ASSERT(data.Slice() == pfoobar);

        // The last UnUse will mark handle for garbage collection
        UNIT_ASSERT(!handle.UnUse());
        UNIT_ASSERT(handle.UnUse());
        UNIT_ASSERT(handle.IsGarbage());
        UNIT_ASSERT(!handle.IsDropped());

        // Use data before drop -> garbage collection fails
        UNIT_ASSERT(handle.Use());
        UNIT_ASSERT(!handle.TryDrop());
        UNIT_ASSERT(!handle.IsGarbage());
        UNIT_ASSERT(!handle.IsDropped());

        // Drop data before use -> garbage collection succeeds
        UNIT_ASSERT(handle.UnUse());
        auto dropped = handle.TryDrop();
        UNIT_ASSERT(dropped && dropped.data() == data.data());
        UNIT_ASSERT(handle.IsGarbage());
        UNIT_ASSERT(handle.IsDropped());
        UNIT_ASSERT_VALUES_EQUAL(handle.UseCount(), 0u);

        // Dropped data cannot be used again
        UNIT_ASSERT(!handle.Use());
        UNIT_ASSERT_VALUES_EQUAL(handle.UseCount(), 0u);

        // Data is dropped, however pin is still active, new pins are ok
        UNIT_ASSERT_VALUES_EQUAL(handle.PinCount(), 1u);
        auto pinned = handle.Pin();
        UNIT_ASSERT(pinned.data() == data.data());
        UNIT_ASSERT_VALUES_EQUAL(handle.PinCount(), 2u);

        // When the last pin goes out of scope data is freed
        handle.UnPin();
        handle.UnPin();
        UNIT_ASSERT_VALUES_EQUAL(handle.PinCount(), 0u);
    }

    Y_UNIT_TEST(HandleRef) {
        auto handle = MakeIntrusive<TTestHandle>(TSharedData::Copy("hello", 5));
        auto gc = MakeIntrusive<TSharedPageGCList>();
        UNIT_ASSERT_VALUES_EQUAL(handle->UseCount(), 1u);

        {
            TSharedPageRef ref1(handle, gc);
            UNIT_ASSERT(!ref1.IsUsed());
            UNIT_ASSERT(ref1.Use());
            UNIT_ASSERT(ref1.IsUsed());
            UNIT_ASSERT_VALUES_EQUAL(handle->UseCount(), 2u);

            // Make sure now only refs are using the handle
            UNIT_ASSERT(!handle->UnUse());
            UNIT_ASSERT_VALUES_EQUAL(handle->UseCount(), 1u);

            // Test copy construction
            TSharedPageRef ref2 = ref1;
            UNIT_ASSERT(ref2.IsUsed());
            UNIT_ASSERT_VALUES_EQUAL(handle->UseCount(), 2u);

            // Test move construction
            TSharedPageRef ref3 = std::move(ref2);
            UNIT_ASSERT(!ref2 && !ref2.IsUsed());
            UNIT_ASSERT(ref3 && ref3.IsUsed());
            UNIT_ASSERT_VALUES_EQUAL(handle->UseCount(), 2u);

            // Test copy assignment
            TSharedPageRef ref4;
            ref4 = ref3;
            UNIT_ASSERT_VALUES_EQUAL(handle->UseCount(), 3u);

            // Test move assignment
            TSharedPageRef ref5;
            ref5 = std::move(ref3);
            UNIT_ASSERT_VALUES_EQUAL(handle->UseCount(), 3u);

            // Use/UnUse on empty ref shouldn't do anything
            UNIT_ASSERT(!ref2.UnUse());
            UNIT_ASSERT_VALUES_EQUAL(handle->UseCount(), 3u);
            UNIT_ASSERT(!ref2.Use());
            UNIT_ASSERT_VALUES_EQUAL(handle->UseCount(), 3u);

            // UnUse on live ref should decrement use count once
            UNIT_ASSERT(!ref5.UnUse());
            UNIT_ASSERT(!ref5.UnUse());
            UNIT_ASSERT(!ref5.UnUse());
            UNIT_ASSERT(!ref5.IsUsed());
            UNIT_ASSERT_VALUES_EQUAL(handle->UseCount(), 2u);

            // Use on live ref should increment use count once
            UNIT_ASSERT(ref5.Use());
            UNIT_ASSERT(ref5.Use());
            UNIT_ASSERT(ref5.Use());
            UNIT_ASSERT(ref5.IsUsed());
            UNIT_ASSERT_VALUES_EQUAL(handle->UseCount(), 3u);
        }

        // The last ref out of scope should add handle to gc list
        UNIT_ASSERT_VALUES_EQUAL(handle->UseCount(), 0u);
        UNIT_ASSERT(gc->PopGC() == handle);
        UNIT_ASSERT(gc->PopGC() == nullptr);
    }

    Y_UNIT_TEST(PinnedRef) {
        auto handle = MakeIntrusive<TTestHandle>(TSharedData::Copy("hello", 5));
        auto gc = MakeIntrusive<TSharedPageGCList>();
        auto ref = TSharedPageRef::MakeUsed(handle, gc);
        UNIT_ASSERT(ref.IsUsed());

        {
            TPinnedPageRef ref1(ref);
            UNIT_ASSERT(ref1);
            UNIT_ASSERT_VALUES_EQUAL(ref1->Slice(), TStringBuf("hello"));
            UNIT_ASSERT_VALUES_EQUAL(handle->PinCount(), 1u);

            // Copy constructor
            TPinnedPageRef ref2 = ref1;
            UNIT_ASSERT(ref1);
            UNIT_ASSERT(ref2);
            UNIT_ASSERT_VALUES_EQUAL(ref2->Slice(), TStringBuf("hello"));
            UNIT_ASSERT_VALUES_EQUAL(handle->PinCount(), 2u);

            // Move constructor
            TPinnedPageRef ref3 = std::move(ref2);
            UNIT_ASSERT(!ref2);
            UNIT_ASSERT(ref3);
            UNIT_ASSERT_VALUES_EQUAL(ref3->Slice(), TStringBuf("hello"));
            UNIT_ASSERT_VALUES_EQUAL(handle->PinCount(), 2u);

            // Copy assignment
            TPinnedPageRef ref4;
            ref4 = ref3;
            UNIT_ASSERT(ref3);
            UNIT_ASSERT(ref4);
            UNIT_ASSERT_VALUES_EQUAL(ref4->Slice(), TStringBuf("hello"));
            UNIT_ASSERT_VALUES_EQUAL(handle->PinCount(), 3u);

            // Move assignment
            TPinnedPageRef ref5;
            ref5 = std::move(ref4);
            UNIT_ASSERT(!ref4);
            UNIT_ASSERT(ref5);
            UNIT_ASSERT_VALUES_EQUAL(ref5->Slice(), TStringBuf("hello"));
            UNIT_ASSERT_VALUES_EQUAL(handle->PinCount(), 3u);
        }

        UNIT_ASSERT_VALUES_EQUAL(handle->PinCount(), 0u);
    }

    Y_UNIT_TEST(PinnedRefPure) {
        TPinnedPageRef ref1(TSharedData::Copy("hello", 5));
        UNIT_ASSERT(ref1);
        UNIT_ASSERT_VALUES_EQUAL(ref1->Slice(), TStringBuf("hello"));

        // Copy constructor
        TPinnedPageRef ref2 = ref1;
        UNIT_ASSERT(ref1);
        UNIT_ASSERT(ref2);
        UNIT_ASSERT_VALUES_EQUAL(ref2->Slice(), TStringBuf("hello"));

        // Move constructor
        TPinnedPageRef ref3 = std::move(ref2);
        UNIT_ASSERT(!ref2);
        UNIT_ASSERT(ref3);
        UNIT_ASSERT_VALUES_EQUAL(ref3->Slice(), TStringBuf("hello"));

        // Copy assignment
        TPinnedPageRef ref4;
        ref4 = ref3;
        UNIT_ASSERT(ref3);
        UNIT_ASSERT(ref4);
        UNIT_ASSERT_VALUES_EQUAL(ref4->Slice(), TStringBuf("hello"));

        // Move assignment
        TPinnedPageRef ref5;
        ref5 = std::move(ref4);
        UNIT_ASSERT(!ref4);
        UNIT_ASSERT(ref5);
        UNIT_ASSERT_VALUES_EQUAL(ref5->Slice(), TStringBuf("hello"));
    }
}

}
