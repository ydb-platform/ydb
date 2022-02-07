/*
 * Иногда требуется создать умные указатели (shared_ptr) на константные объекты.
 * Если при этом используется enable_shared_from_this, то первый shared_ptr не
 * инициализирует weak_ptr лежищий внутри enable_shared_from_this.
 * Это приводит к тому, что метод shared_from_this() кидает исключение.
 * Это происходит из-за того, что шаблонная функция ipcdetails::sp_enable_shared_from_this,
 * вызываемая в конструкторе shared_ptr не может сматчить входящие аргументы.
 * Данная ошибка исправляется путем довления константности к типу входящего артумента 'pe'.
 */

#include <library/cpp/testing/unittest/registar.h>
#include <boost/interprocess/smart_ptr/shared_ptr.hpp>
#include <boost/interprocess/smart_ptr/enable_shared_from_this.hpp>
#include <type_traits>
#include <stdlib.h>

using Allocator = std::allocator<void>;

template <bool Const>
struct TestTypes {
    class TestClass;

    class Deleter {
    public:
        using const_pointer = TestClass const*;
        using pointer = const_pointer;

    Deleter& operator ()(pointer p) {
        delete p;
        return *this;
    }
    };

    class TestClass:
        public boost::interprocess::enable_shared_from_this<typename std::conditional<Const, const TestClass, TestClass>::type, Allocator, Deleter>
    {
    };

    using shared_ptr = boost::interprocess::shared_ptr<typename std::conditional<Const, const TestClass, TestClass>::type, Allocator, Deleter>;
};

template <bool ConstPtr, bool ConstSharedPtr>
void test() {
    using T = typename TestTypes<ConstSharedPtr>::TestClass;
    using shared_ptr = typename TestTypes<ConstSharedPtr>::shared_ptr;
    T* p = new T;
    typename std::conditional<ConstPtr, T* const, T*>::type ptr = p;
    shared_ptr sptr1(ptr);
    UNIT_ASSERT_VALUES_EQUAL(sptr1.use_count(), 1);
        {
        shared_ptr sptr2 = p->shared_from_this();
        UNIT_ASSERT_VALUES_EQUAL(sptr2.use_count(), 2);
        }
    UNIT_ASSERT_VALUES_EQUAL(sptr1.use_count(), 1);
}

Y_UNIT_TEST_SUITE(TestSharedPtr) {
    Y_UNIT_TEST(NonConst_NonConst) {
        test<false, false>();
    }
    Y_UNIT_TEST(NonConst_Const) {
        test<false, true>();
    }
    Y_UNIT_TEST(Const_Const) {
        test<true, true>();
    }
};

