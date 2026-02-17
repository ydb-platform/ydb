#include "checked_deref_ptr.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

namespace {
struct TTestStruct {
    int Value = 42;

    int GetValue() const {
        return Value;
    }

    void SetValue(int val) {
        Value = val;
    }
};
} // namespace

Y_UNIT_TEST_SUITE(TCheckedDerefPtrTests) {

Y_UNIT_TEST(DefaultConstructor) {
    TCheckedDerefPtr<TTestStruct> ptr;
    UNIT_ASSERT_EQUAL(ptr.Get(), nullptr);
    UNIT_ASSERT(!ptr);
}

Y_UNIT_TEST(ConstructorFromNullptr) {
    TCheckedDerefPtr<TTestStruct> ptr(nullptr);
    UNIT_ASSERT_EQUAL(ptr.Get(), nullptr);
    UNIT_ASSERT(!ptr);
}

Y_UNIT_TEST(ConstructorFromValidPointer) {
    TTestStruct obj;
    TCheckedDerefPtr<TTestStruct> ptr(&obj);
    UNIT_ASSERT_EQUAL(ptr.Get(), &obj);
    UNIT_ASSERT(ptr);
}

Y_UNIT_TEST(CopyConstructor) {
    TTestStruct obj;
    TCheckedDerefPtr<TTestStruct> ptr1(&obj);
    TCheckedDerefPtr<TTestStruct> ptr2(ptr1);
    UNIT_ASSERT_EQUAL(ptr1.Get(), ptr2.Get());
    UNIT_ASSERT_EQUAL(ptr2.Get(), &obj);
}

Y_UNIT_TEST(MoveConstructor) {
    TTestStruct obj;
    TCheckedDerefPtr<TTestStruct> ptr1(&obj);
    TCheckedDerefPtr<TTestStruct> ptr2(std::move(ptr1));
    UNIT_ASSERT_EQUAL(ptr2.Get(), &obj);
}

Y_UNIT_TEST(AssignmentFromPointer) {
    TTestStruct obj;
    TCheckedDerefPtr<TTestStruct> ptr;
    ptr = &obj;
    UNIT_ASSERT_EQUAL(ptr.Get(), &obj);
    UNIT_ASSERT(ptr);
}

Y_UNIT_TEST(AssignmentFromNullptr) {
    TTestStruct obj;
    TCheckedDerefPtr<TTestStruct> ptr(&obj);
    ptr = nullptr;
    UNIT_ASSERT_EQUAL(ptr.Get(), nullptr);
    UNIT_ASSERT(!ptr);
}

Y_UNIT_TEST(CopyAssignment) {
    TTestStruct obj;
    TCheckedDerefPtr<TTestStruct> ptr1(&obj);
    TCheckedDerefPtr<TTestStruct> ptr2;
    ptr2 = ptr1;
    UNIT_ASSERT_EQUAL(ptr1.Get(), ptr2.Get());
    UNIT_ASSERT_EQUAL(ptr2.Get(), &obj);
}

Y_UNIT_TEST(MoveAssignment) {
    TTestStruct obj;
    TCheckedDerefPtr<TTestStruct> ptr1(&obj);
    TCheckedDerefPtr<TTestStruct> ptr2;
    ptr2 = std::move(ptr1);
    UNIT_ASSERT_EQUAL(ptr2.Get(), &obj);
}

Y_UNIT_TEST(DereferenceValidPointer) {
    TTestStruct obj;
    obj.Value = 100;
    TCheckedDerefPtr<TTestStruct> ptr(&obj);
    UNIT_ASSERT_EQUAL((*ptr).Value, 100);
    UNIT_ASSERT_EQUAL(ptr->Value, 100);
}

Y_UNIT_TEST(DereferenceNullPointerThrows) {
    TCheckedDerefPtr<TTestStruct> ptr;
    UNIT_ASSERT_EXCEPTION_CONTAINS(*ptr, TYqlPanic, "Attempt to dereference null pointer");
}

Y_UNIT_TEST(MemberAccessNullPointerThrows) {
    TCheckedDerefPtr<TTestStruct> ptr;
    UNIT_ASSERT_EXCEPTION_CONTAINS(ptr->Value, TYqlPanic, "Attempt to access member through null pointer");
}

Y_UNIT_TEST(MemberAccessValidPointer) {
    TTestStruct obj;
    TCheckedDerefPtr<TTestStruct> ptr(&obj);
    UNIT_ASSERT_EQUAL(ptr->GetValue(), 42);
    ptr->SetValue(200);
    UNIT_ASSERT_EQUAL(ptr->GetValue(), 200);
    UNIT_ASSERT_EQUAL(obj.Value, 200);
}

Y_UNIT_TEST(ImplicitConversionToRawPointer) {
    TTestStruct obj;
    TCheckedDerefPtr<TTestStruct> ptr(&obj);
    TTestStruct* rawPtr = ptr;
    UNIT_ASSERT_EQUAL(rawPtr, &obj);
}

Y_UNIT_TEST(ImplicitConversionNullPointer) {
    TCheckedDerefPtr<TTestStruct> ptr;
    TTestStruct* rawPtr = ptr;
    UNIT_ASSERT_EQUAL(rawPtr, nullptr);
}

Y_UNIT_TEST(BoolConversion) {
    TTestStruct obj;
    TCheckedDerefPtr<TTestStruct> ptr1(&obj);
    TCheckedDerefPtr<TTestStruct> ptr2;

    UNIT_ASSERT(ptr1);
    UNIT_ASSERT(!ptr2);

    if (ptr1) {
        UNIT_ASSERT_EQUAL(ptr1->Value, 42);
    }
}

Y_UNIT_TEST(Reset) {
    TTestStruct obj;
    TCheckedDerefPtr<TTestStruct> ptr(&obj);
    UNIT_ASSERT(ptr);

    ptr.Reset();
    UNIT_ASSERT(!ptr);
    UNIT_ASSERT_EQUAL(ptr.Get(), nullptr);
}

Y_UNIT_TEST(ResetWithPointer) {
    TTestStruct obj1, obj2;
    TCheckedDerefPtr<TTestStruct> ptr(&obj1);
    UNIT_ASSERT_EQUAL(ptr.Get(), &obj1);

    ptr.Reset(&obj2);
    UNIT_ASSERT_EQUAL(ptr.Get(), &obj2);
}

Y_UNIT_TEST(Swap) {
    TTestStruct obj1, obj2;
    TCheckedDerefPtr<TTestStruct> ptr1(&obj1);
    TCheckedDerefPtr<TTestStruct> ptr2(&obj2);

    ptr1.Swap(ptr2);
    UNIT_ASSERT_EQUAL(ptr1.Get(), &obj2);
    UNIT_ASSERT_EQUAL(ptr2.Get(), &obj1);
}

Y_UNIT_TEST(StdSwap) {
    TTestStruct obj1, obj2;
    TCheckedDerefPtr<TTestStruct> ptr1(&obj1);
    TCheckedDerefPtr<TTestStruct> ptr2(&obj2);

    swap(ptr1, ptr2);
    UNIT_ASSERT_EQUAL(ptr1.Get(), &obj2);
    UNIT_ASSERT_EQUAL(ptr2.Get(), &obj1);
}

Y_UNIT_TEST(UseInFunction) {
    auto processPtr = [](TTestStruct* ptr) {
        if (ptr) {
            return ptr->Value;
        }
        return -1;
    };

    TTestStruct obj;
    obj.Value = 123;
    TCheckedDerefPtr<TTestStruct> safePtr(&obj);

    // Implicit conversion to raw pointer
    UNIT_ASSERT_EQUAL(processPtr(safePtr), 123);

    TCheckedDerefPtr<TTestStruct> nullPtr;
    UNIT_ASSERT_EQUAL(processPtr(nullPtr), -1);
}

Y_UNIT_TEST(ConstPointer) {
    const TTestStruct obj;
    TCheckedDerefPtr<const TTestStruct> ptr(&obj);
    UNIT_ASSERT_EQUAL(ptr->GetValue(), 42);
}

} // Y_UNIT_TEST_SUITE(TCheckedDerefPtrTests)
