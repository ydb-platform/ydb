#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/public.h>

namespace NYT {
namespace {

using ::testing::Mock;
using ::testing::Return;
using ::testing::AllOf;
using ::testing::StrictMock;
using ::testing::TProbe;
using ::testing::TProbeState;
using ::testing::TCoercibleToProbe;
using ::testing::HasCopyMoveCounts;
using ::testing::NoCopies;
using ::testing::NoAssignments;

////////////////////////////////////////////////////////////////////////////////
// Auxiliary types and functions.

// An incomplete type (really).
class TIncompleteType;

// A simple mock.
class TObject
{
public:
    TObject() = default;

    MOCK_METHOD(void, VoidMethod0, (), ());
    MOCK_METHOD(void, VoidConstMethod0, (), (const));

    MOCK_METHOD(int, IntMethod0, (), ());
    MOCK_METHOD(int, IntConstMethod0, (), (const));

private:
    // Explicitly non-copyable and non-movable.
    // Particularly important in this test to ensure that no copies are made.
    TObject(const TObject&);
    TObject(TObject&&);
    TObject& operator=(const TObject&);
    TObject& operator=(TObject&&);
};

// A simple mock which also mocks Ref()/Unref() hence mocking reference counting
// behaviour.
class TObjectWithRC
    : public TObject
{
public:
    TObjectWithRC() = default;

    MOCK_METHOD(void, Ref, (), (const));
    MOCK_METHOD(void, Unref, (), (const));

private:
    // Explicitly non-copyable and non-movable.
    // Particularly important in this test to ensure that no copies are made.
    TObjectWithRC(const TObjectWithRC&);
    TObjectWithRC(TObjectWithRC&&);
    TObjectWithRC& operator=(const TObjectWithRC&);
    TObjectWithRC& operator=(TObjectWithRC&&);
};

void Ref(TObjectWithRC* obj)
{
    obj->Ref();
}

void Unref(TObjectWithRC* obj)
{
    obj->Unref();
}

using TObjectWithRCPtr = TIntrusivePtr<TObjectWithRC>;

// A simple mock object which mocks Ref()/Unref() and prohibits
// public destruction.
class TObjectWithRCAndPrivateDtor
    : public TObjectWithRC
{
private:
    ~TObjectWithRCAndPrivateDtor() = default;
};

// A simple mock object with weak ptr support.
class TObjectWithFullRC
    : public TObject
    , public TRefCounted
{ };

using TObjectWithFullRCPtr = TIntrusivePtr<TObjectWithFullRC>;
using TObjectWithFullRCConstPtr = TIntrusivePtr<const TObjectWithFullRC>;
using TObjectWithFullRCWkPtr = TWeakPtr<TObjectWithFullRC>;
using TObjectWithFullRCConstWkPtr = TWeakPtr<const TObjectWithFullRC>;

// Below there is a series of either reference-counted or not classes
// with simple inheritance and both virtual and non-virtual methods.

static const int SomeParentValue = 1;
static const int SomeChildValue = 2;

class TRefParent
{
public:
    // Stub methods for reference counting.
    void Ref()
    { }

    void Unref()
    { }

    virtual void VirtualSet()
    {
        Value = SomeParentValue;
    }

    void NonVirtualSet()
    {
        Value = SomeParentValue;
    }

    int Value;
};

class TRefChild
    : public TRefParent
{
public:
    void VirtualSet() override
    {
        Value = SomeChildValue;
    }

    void NonVirtualSet()
    {
        Value = SomeChildValue;
    }
};

class TNoRefParent
{
public:
    virtual void VirtualSet()
    {
        Value = SomeParentValue;
    }

    void NonVirtualSet()
    {
        Value = SomeParentValue;
    }

    int Value;
};

class NoRefChild
    : public TNoRefParent
{
    void VirtualSet() override
    {
        Value = SomeChildValue;
    }

    void NonVirtualSet()
    {
        Value = SomeChildValue;
    }
};

int UnwrapNoRefParent(TNoRefParent p)
{
    return p.Value;
}

int UnwrapNoRefParentPtr(TNoRefParent* p)
{
    return p->Value;
}

int UnwrapNoRefParentConstRef(const TNoRefParent& p)
{
    return p.Value;
}

// Various functions for testing purposes.

int IntegerIdentity(int n)
{
    return n;
}

const char* StringIdentity(const char* s)
{
    return s;
}

template <class T>
T PolymorphicIdentity(T t)
{
    return t; // Copy
}

template <class T>
T PolymorphicPassThrough(T&& t)
{
    return std::move(t); // Move
}

template <class T>
void VoidPolymorphic1(T t)
{
    Y_UNUSED(t);
}

int ArrayGet(const int array[], int n)
{
    return array[n];
}

int Sum(int a, int b, int c, int d, int e, int f)
{
    // Sum(1, 2, 3, 4, 5, 6) -> 123456.
    return f + 10 * (e + 10 * (d + 10 * (c + 10 * (b + 10 * a))));
}

void SetIntViaRef(int& n)
{
    n = 2012;
}

void SetIntViaPtr(int* n)
{
    *n = 2012;
}

template <class T>
int FunctionWithWeakParam(TWeakPtr<T> /*ptr*/, int n)
{
    return n;
}

void InvokeClosure(const TClosure& callback)
{
    callback();
}

void Touch(const TProbe& probe) {
    probe.Touch();
}

////////////////////////////////////////////////////////////////////////////////
// Test fixture.

class TBindTest
    : public ::testing::Test
{
public:
    TBindTest() = default;

    void SetUp() override
    {
        ConstObjectWithRCPtr = &ObjectWithRC;
        ConstObjectPtr = &Object;
        StaticObjectPtr = &StaticObject;
    }

    // Helper methods.
    static void StaticVoidFunc0()
    {
        StaticObjectPtr->VoidMethod0();
    }

    static int StaticIntFunc0()
    {
        return StaticObjectPtr->IntMethod0();
    }

protected:
    StrictMock<TObject> Object;
    StrictMock<TObjectWithRC> ObjectWithRC;

    const TObjectWithRC* ConstObjectWithRCPtr;
    const TObject* ConstObjectPtr;

    StrictMock<TObject> StaticObject;

    // Used by the static functions.
    static StrictMock<TObject>* StaticObjectPtr;

private:
    // Explicitly non-copyable and non-movable.
    // Thus we prevent BIND() from taking copy of the target (i. e. this fixture).
    TBindTest(const TBindTest&);
    TBindTest(TBindTest&&);
    TBindTest& operator=(const TBindTest&);
    TBindTest& operator=(TBindTest&&);
};

StrictMock<TObject>* TBindTest::StaticObjectPtr = 0;

////////////////////////////////////////////////////////////////////////////////
// Test definitions.

// Sanity check that we can instantiate a callback for each arity.
TEST_F(TBindTest, ArityTest)
{
    TCallback<int()> c0 = BIND(&Sum, 5, 4, 3, 2, 1, 0);
    EXPECT_EQ(543210, c0());

    TCallback<int(int)> c1 = BIND(&Sum, 5, 4, 3, 2, 1);
    EXPECT_EQ(543219, c1(9));

    TCallback<int(int, int)> c2 = BIND(&Sum, 5, 4, 3, 2);
    EXPECT_EQ(543298, c2(9, 8));

    TCallback<int(int, int, int)> c3 = BIND(&Sum, 5, 4, 3);
    EXPECT_EQ(543987, c3(9, 8, 7));

    TCallback<int(int, int, int, int)> c4 = BIND(&Sum, 5, 4);
    EXPECT_EQ(549876, c4(9, 8, 7, 6));

    TCallback<int(int, int, int, int, int)> c5 = BIND(&Sum, 5);
    EXPECT_EQ(598765, c5(9, 8, 7, 6, 5));

    TCallback<int(int, int, int, int, int, int)> c6 = BIND(&Sum);
    EXPECT_EQ(987654, c6(9, 8, 7, 6, 5, 4));
}

// Test the currying ability of the BIND().
TEST_F(TBindTest, CurryingTest)
{
    TCallback<int(int, int, int, int, int, int)> c6 = BIND(&Sum);
    EXPECT_EQ(987654, c6(9, 8, 7, 6, 5, 4));

    TCallback<int(int, int, int, int, int)> c5 = BIND(c6, 5);
    EXPECT_EQ(598765, c5(9, 8, 7, 6, 5));

    TCallback<int(int, int, int, int)> c4 = BIND(c5, 4);
    EXPECT_EQ(549876, c4(9, 8, 7, 6));

    TCallback<int(int, int, int)> c3 = BIND(c4, 3);
    EXPECT_EQ(543987, c3(9, 8, 7));

    TCallback<int(int, int)> c2 = BIND(c3, 2);
    EXPECT_EQ(543298, c2(9, 8));

    TCallback<int(int)> c1 = BIND(c2, 1);
    EXPECT_EQ(543219, c1(9));

    TCallback<int()> c0 = BIND(c1, 0);
    EXPECT_EQ(543210, c0());
}

// Test that currying the rvalue result of another BIND() works correctly.
//   - Rvalue should be usable as an argument to the BIND().
//   - Multiple runs of resulting TCallback remain valid.
TEST_F(TBindTest, CurryingRvalueResultOfBind)
{
    int x;
    TClosure cb = BIND(&InvokeClosure, BIND(&SetIntViaPtr, &x));

    // If we implement BIND() such that the return value has auto_ptr-like
    // semantics, the second call here will fail because ownership of
    // the internal BindState<> would have been transferred to a *temporary*
    // constructon of a TCallback object on the first call.
    x = 0;
    cb();
    EXPECT_EQ(2012, x);

    x = 0;
    cb();
    EXPECT_EQ(2012, x);
}

// Now we have to test that there are proper instantinations for various use cases.
// The following test cases try to cover most of the used cases.

// Function type support.
//   - Normal function.
//   - Normal function bound with a non-refcounted first argument.
//   - Method bound to an object via raw pointer.
//   - Method bound to an object via intrusive pointer.
//   - Const method bound to a non-const object.
//   - Const method bound to a const object.
//   - Derived classes can be used with pointers to non-virtual base functions.
//   - Derived classes can be used with pointers to virtual base functions
//     (preserves virtual dispatch).
TEST_F(TBindTest, FunctionTypeSupport)
{
    EXPECT_CALL(StaticObject, VoidMethod0());

    EXPECT_CALL(ObjectWithRC, Ref()).Times(1);
    EXPECT_CALL(ObjectWithRC, Unref()).Times(1);

    EXPECT_CALL(ObjectWithRC, VoidMethod0()).Times(2);
    EXPECT_CALL(ObjectWithRC, VoidConstMethod0()).Times(2);

    // Normal functions.
    TClosure normalFunc =
        BIND(&StaticVoidFunc0);
    TCallback<TObject*()> normalFuncNonRC =
        BIND(&PolymorphicIdentity<TObject*>, &Object);

    normalFunc();
    EXPECT_EQ(&Object, normalFuncNonRC());

    // Bound methods.
    TClosure boundMethodViaRawPtr =
        BIND(&TObjectWithRC::VoidMethod0, &ObjectWithRC); // (NoRef)
    TClosure boundMethodViaRefPtr =
        BIND(&TObjectWithRC::VoidMethod0, TObjectWithRCPtr(&ObjectWithRC)); // (Ref)

    boundMethodViaRawPtr();
    boundMethodViaRefPtr();

    // Const-methods.
    TClosure constMethodNonConstObject =
        BIND(&TObjectWithRC::VoidConstMethod0, &ObjectWithRC); // (NoRef)
    TClosure constMethodConstObject =
        BIND(&TObjectWithRC::VoidConstMethod0, ConstObjectWithRCPtr); // (NoRef)

    constMethodNonConstObject();
    constMethodConstObject();

    // Virtual calls.
    TRefChild child;

    child.Value = 0;
    TClosure virtualSet = BIND(&TRefParent::VirtualSet, &child);
    virtualSet();
    EXPECT_EQ(SomeChildValue, child.Value);

    child.Value = 0;
    TClosure nonVirtualSet = BIND(&TRefParent::NonVirtualSet, &child);
    nonVirtualSet();
    EXPECT_EQ(SomeParentValue, child.Value);
}

// Return value support.
//   - Function with a return value.
//   - Method with a return value.
//   - Const method with a return value.
TEST_F(TBindTest, ReturnValuesSupport)
{
    EXPECT_CALL(StaticObject, IntMethod0()).WillOnce(Return(13));

    EXPECT_CALL(ObjectWithRC, Ref()).Times(0);
    EXPECT_CALL(ObjectWithRC, Unref()).Times(0);

    EXPECT_CALL(ObjectWithRC, IntMethod0()).WillOnce(Return(17));
    EXPECT_CALL(ObjectWithRC, IntConstMethod0())
        .WillOnce(Return(19))
        .WillOnce(Return(23));

    TCallback<int()> normalFunc =
        BIND(&StaticIntFunc0);
    TCallback<int()> boundMethod =
        BIND(&TObjectWithRC::IntMethod0, &ObjectWithRC); // (NoRef)

    EXPECT_EQ(13, normalFunc());
    EXPECT_EQ(17, boundMethod());

    TCallback<int()> constMethodNonConstObject =
        BIND(&TObjectWithRC::IntConstMethod0, &ObjectWithRC); // (NoRef)
    TCallback<int()> constMethodConstObject =
        BIND(&TObjectWithRC::IntConstMethod0, ConstObjectWithRCPtr); // (NoRef)

    EXPECT_EQ(19, constMethodNonConstObject());
    EXPECT_EQ(23, constMethodConstObject());
}

// An ability to ignore returned value.
//   - Function with a return value.
//   - Method with a return value.
//   - Const Method with a return value.
//   - Method with a return value bound to a weak pointer.
//   - Const Method with a return value bound to a weak pointer.
TEST_F(TBindTest, IgnoreResultWrapper)
{
    EXPECT_CALL(StaticObject, IntMethod0()).WillOnce(Return(13));

    EXPECT_CALL(ObjectWithRC, Ref()).Times(0);
    EXPECT_CALL(ObjectWithRC, Unref()).Times(0);

    EXPECT_CALL(ObjectWithRC, IntMethod0()).WillOnce(Return(17));
    EXPECT_CALL(ObjectWithRC, IntConstMethod0()).WillOnce(Return(19));

    TClosure normalFunc =
        BIND(IgnoreResult(&StaticIntFunc0));
    normalFunc();

    TClosure boundMethod =
        BIND(IgnoreResult(&TObjectWithRC::IntMethod0), &ObjectWithRC); // (NoRef)
    boundMethod();

    TClosure constBoundMethod =
        BIND(IgnoreResult(&TObjectWithRC::IntConstMethod0), &ObjectWithRC); // (NoRef)
    constBoundMethod();
}

// Argument binding tests.
//   - Argument binding to a primitive.
//   - Argument binding to a primitive pointer.
//   - Argument binding to a literal integer.
//   - Argument binding to a literal string.
//   - Argument binding with template function.
//   - Argument binding to an object.
//   - Argument binding to a pointer to an incomplete type.
//   - Argument upcasts when required.
TEST_F(TBindTest, ArgumentBindingSupport)
{
    int n = 1;

    TCallback<int()> primitiveBind =
        BIND(&IntegerIdentity, n);
    EXPECT_EQ(n, primitiveBind());

    TCallback<int*()> primitivePointerBind =
        BIND(&PolymorphicIdentity<int*>, &n);
    EXPECT_EQ(&n, primitivePointerBind());

    TCallback<int()> literalIntegerBind
        = BIND(&IntegerIdentity, 2);
    EXPECT_EQ(2, literalIntegerBind());

    TCallback<const char*()> literalStringBind =
        BIND(&StringIdentity, "Dire Straits");
    EXPECT_STREQ("Dire Straits", literalStringBind());

    TCallback<int()> templateFunctionBind =
        BIND(&PolymorphicIdentity<int>, 3);
    EXPECT_EQ(3, templateFunctionBind());

    TNoRefParent p;
    p.Value = 4;

    TCallback<int()> objectBind = BIND(&UnwrapNoRefParent, p);
    EXPECT_EQ(4, objectBind());

    TIncompleteType* dummyPtr = reinterpret_cast<TIncompleteType*>(123);
    TCallback<TIncompleteType*()> incompleteTypeBind =
        BIND(&PolymorphicIdentity<TIncompleteType*>, dummyPtr);
    EXPECT_EQ(dummyPtr, incompleteTypeBind());

    NoRefChild c;

    c.Value = 5;
    TCallback<int()> upcastBind =
        BIND(&UnwrapNoRefParent, c);
    EXPECT_EQ(5, upcastBind());

    c.Value = 6;
    TCallback<int()> upcastPtrBind =
        BIND(&UnwrapNoRefParentPtr, &c);
    EXPECT_EQ(6, upcastPtrBind());

    c.Value = 7;
    TCallback<int()> upcastConstRefBind =
        BIND(&UnwrapNoRefParentConstRef, c);
    EXPECT_EQ(7, upcastConstRefBind());
}

// Unbound argument type support tests.
//   - Unbound value.
//   - Unbound pointer.
//   - Unbound reference.
//   - Unbound const reference.
//   - Unbound unsized array.
//   - Unbound sized array.
//   - Unbound array-of-arrays.
TEST_F(TBindTest, UnboundArgumentTypeSupport)
{
    // Check only for valid instatination.
    TCallback<void(int)> unboundValue =
        BIND(&VoidPolymorphic1<int>);
    TCallback<void(int*)> unboundPtr =
        BIND(&VoidPolymorphic1<int*>);
    TCallback<void(int&)> unboundRef =
        BIND(&VoidPolymorphic1<int&>);
    TCallback<void(const int&)> unboundConstRef =
        BIND(&VoidPolymorphic1<const int&>);
    TCallback<void(int[])> unboundUnsizedArray =
        BIND(&VoidPolymorphic1<int[]>);
    TCallback<void(int[3])> unboundSizedArray =
        BIND(&VoidPolymorphic1<int[3]>);
    TCallback<void(int[][3])> unboundArrayOfArrays =
        BIND(&VoidPolymorphic1<int[][3]>);

    SUCCEED();
}

// Function with unbound reference parameter.
//   - Original parameter is modified by the callback.
TEST_F(TBindTest, UnboundReference)
{
    int n = 0;
    TCallback<void(int&)> unboundRef = BIND(&SetIntViaRef);
    unboundRef(n);
    EXPECT_EQ(2012, n);
}

// Functions that take reference parameters.
//   - Forced reference parameter type still stores a copy.
//   - Forced const reference parameter type still stores a copy.
TEST_F(TBindTest, ReferenceArgumentBinding)
{
    int myInt = 1;
    int& myIntRef = myInt;
    const int& myIntConstRef = myInt;

    TCallback<int()> firstAction =
        BIND(&IntegerIdentity, myIntRef);
    EXPECT_EQ(1, firstAction());
    myInt++;
    EXPECT_EQ(1, firstAction());

    TCallback<int()> secondAction =
        BIND(&IntegerIdentity, myIntConstRef);
    EXPECT_EQ(2, secondAction());
    myInt++;
    EXPECT_EQ(2, secondAction());

    EXPECT_EQ(3, myInt);
}

// Check that we can pass in arrays and have them be stored as a pointer.
//   - Array of values stores a pointer.
//   - Array of const values stores a pointer.
TEST_F(TBindTest, ArrayArgumentBinding)
{
    int array[4] = { 1, 2, 3, 4 };
    const int (*constArrayPtr)[4] = &array;

    TCallback<int(int)> arrayPolyGet = BIND(&ArrayGet, array);
    EXPECT_EQ(1, arrayPolyGet(0));
    EXPECT_EQ(2, arrayPolyGet(1));
    EXPECT_EQ(3, arrayPolyGet(2));
    EXPECT_EQ(4, arrayPolyGet(3));

    TCallback<int()> arrayGet = BIND(&ArrayGet, array, 1);
    EXPECT_EQ(2, arrayGet());

    TCallback<int()> constArrayGet = BIND(&ArrayGet, *constArrayPtr, 1);
    EXPECT_EQ(2, constArrayGet());

    array[1] = 7;
    EXPECT_EQ(7, arrayGet());
    EXPECT_EQ(7, constArrayGet());
}

// Unretained() wrapper support.
//   - Method bound to Unretained() non-const object.
//   - Const method bound to Unretained() non-const object.
//   - Const method bound to Unretained() const object.
TEST_F(TBindTest, UnretainedWrapper)
{
    EXPECT_CALL(Object, VoidMethod0()).Times(1);
    EXPECT_CALL(Object, VoidConstMethod0()).Times(2);

    EXPECT_CALL(ObjectWithRC, Ref()).Times(0);
    EXPECT_CALL(ObjectWithRC, Unref()).Times(0);
    EXPECT_CALL(ObjectWithRC, VoidMethod0()).Times(1);
    EXPECT_CALL(ObjectWithRC, VoidConstMethod0()).Times(2);

    TCallback<void()> boundMethod =
        BIND(&TObject::VoidMethod0, Unretained(&Object));
    boundMethod();

    TCallback<void()> constMethodNonConstObject =
        BIND(&TObject::VoidConstMethod0, Unretained(&Object));
    constMethodNonConstObject();

    TCallback<void()> constMethodConstObject =
        BIND(&TObject::VoidConstMethod0, Unretained(ConstObjectPtr));
    constMethodConstObject();

    TCallback<void()> boundMethodWithoutRC =
        BIND(&TObjectWithRC::VoidMethod0, Unretained(&ObjectWithRC)); // (NoRef)
    boundMethodWithoutRC();

    TCallback<void()> constMethodNonConstObjectWithoutRC =
        BIND(&TObjectWithRC::VoidConstMethod0, Unretained(&ObjectWithRC)); // (NoRef)
    constMethodNonConstObjectWithoutRC();

    TCallback<void()> constMethodConstObjectWithoutRC =
        BIND(&TObjectWithRC::VoidConstMethod0, Unretained(ConstObjectWithRCPtr)); // (NoRef)
    constMethodConstObjectWithoutRC();
}

// Weak pointer support.
//   - Method bound to a weak pointer to a non-const object.
//   - Const method bound to a weak pointer to a non-const object.
//   - Const method bound to a weak pointer to a const object.
//   - Normal Function with WeakPtr<> as P1 can have return type and is
//     not canceled.
TEST_F(TBindTest, WeakPtr)
{
    TObjectWithFullRCPtr object = New<TObjectWithFullRC>();
    TObjectWithFullRCWkPtr objectWk(object);

    EXPECT_CALL(*object, VoidMethod0());
    EXPECT_CALL(*object, IntMethod0()).WillOnce(Return(42));
    EXPECT_CALL(*object, IntConstMethod0()).WillOnce(Return(13));
    EXPECT_CALL(*object, VoidConstMethod0()).Times(2);

    TClosure voidMethod =
        BIND(
            &TObjectWithFullRC::VoidMethod0,
            TObjectWithFullRCWkPtr(object));
    voidMethod();

    TCallback<int()> intMethod =
        BIND(
            ThrowOnDestroyed(&TObjectWithFullRC::IntMethod0),
            TObjectWithFullRCWkPtr(object));
    EXPECT_EQ(42, intMethod());

    TCallback<int()> constIntMethodNonConstObject =
        BIND(
            ThrowOnDestroyed(&TObjectWithFullRC::IntConstMethod0),
            TObjectWithFullRCWkPtr(object));
    EXPECT_EQ(13, constIntMethodNonConstObject());

    TClosure constVoidMethodNonConstObject =
        BIND(
            &TObject::VoidConstMethod0,
            TObjectWithFullRCWkPtr(object));
    constVoidMethodNonConstObject();

    TClosure constVoidMethodConstObject =
        BIND(
            &TObject::VoidConstMethod0,
            TObjectWithFullRCConstWkPtr(object));
    constVoidMethodConstObject();

    TCallback<int(int)> normalFunc =
        BIND(
            &FunctionWithWeakParam<TObjectWithFullRC>,
            TObjectWithFullRCWkPtr(object));

    EXPECT_EQ(1, normalFunc(1));

    object.Reset();
    ASSERT_TRUE(objectWk.IsExpired());

    voidMethod();
    constVoidMethodNonConstObject();
    constVoidMethodConstObject();

    EXPECT_THROW_WITH_ERROR_CODE(intMethod(), NYT::EErrorCode::Canceled);
    EXPECT_THROW_WITH_ERROR_CODE(constIntMethodNonConstObject(), NYT::EErrorCode::Canceled);

    EXPECT_EQ(2, normalFunc(2));
}

// ConstRef() wrapper support.
//   - Binding without ConstRef() takes a copy.
//   - Binding with a ConstRef() takes a reference.
//   - Binding ConstRef() to a function that accepts const reference does not copy on invoke.
TEST_F(TBindTest, ConstRefWrapper)
{
    int n = 1;

    TCallback<int()> withoutConstRef =
        BIND(&IntegerIdentity, n);
    TCallback<int()> withConstRef =
        BIND(&IntegerIdentity, ConstRef(n));

    EXPECT_EQ(1, withoutConstRef());
    EXPECT_EQ(1, withConstRef());
    n++;
    EXPECT_EQ(1, withoutConstRef());
    EXPECT_EQ(2, withConstRef());

    TProbeState state;
    TProbe probe(&state);

    TClosure everywhereConstRef =
        BIND(&Touch, ConstRef(probe));
    everywhereConstRef();

    EXPECT_THAT(probe, HasCopyMoveCounts(0, 0));
    EXPECT_THAT(probe, NoAssignments());
}

// Owned() wrapper support.
TEST_F(TBindTest, OwnedWrapper)
{
    TProbeState state;
    TProbe* probe;

    // If we don't capture, delete happens on TCallback destruction/reset.
    // return the same value.
    state.Reset();
    probe = new TProbe(&state);

    TCallback<TProbe*()> capturedArgument =
        BIND(&PolymorphicIdentity<TProbe*>, Owned(probe));

    ASSERT_EQ(probe, capturedArgument());
    ASSERT_EQ(probe, capturedArgument());
    EXPECT_EQ(0, state.Destructors);
    capturedArgument.Reset(); // This should trigger a delete.
    EXPECT_EQ(1, state.Destructors);

    state.Reset();
    probe = new TProbe(&state);
    TCallback<void()> capturedTarget =
        BIND(&TProbe::Touch, Owned(probe));

    capturedTarget();
    EXPECT_EQ(0, state.Destructors);
    capturedTarget.Reset();
    EXPECT_EQ(1, state.Destructors);
}

// Passed() wrapper support.
//   - Using Passed() gives TCallback ownership.
//   - Ownership is transferred from TCallback to callee on the first Run().
TEST_F(TBindTest, PassedWrapper1)
{
    TProbeState state;
    TProbe probe(&state);

    TCallback<TProbe()> cb =
        BIND(
            &PolymorphicPassThrough<TProbe>,
            Passed(std::move(probe)));

    // The argument has been passed.
    EXPECT_FALSE(probe.IsValid());
    EXPECT_EQ(0, state.Destructors);
    EXPECT_THAT(state, NoCopies());

    {
        // Check that ownership can be transferred back out.
        int n = state.MoveConstructors;
        TProbe result = cb();
        EXPECT_EQ(0, state.Destructors);
        EXPECT_LT(n, state.MoveConstructors);
        EXPECT_THAT(state, NoCopies());

        // Resetting does not delete since ownership was transferred.
        cb.Reset();
        EXPECT_EQ(0, state.Destructors);
        EXPECT_THAT(state, NoCopies());
    }

    // Ensure that we actually did get ownership (from the last scope).
    EXPECT_EQ(1, state.Destructors);
}

TEST_F(TBindTest, PassedWrapper2)
{
    TProbeState state;
    TProbe probe(&state);

    TCallback<TProbe()> cb =
        BIND(
            &PolymorphicIdentity<TProbe>,
            Passed(std::move(probe)));

    // The argument has been passed.
    EXPECT_FALSE(probe.IsValid());
    EXPECT_EQ(0, state.Destructors);
    EXPECT_THAT(state, NoCopies());

    {
        // Check that ownership can be transferred back out.
        int n = state.MoveConstructors;
        TProbe result = cb();
        EXPECT_EQ(0, state.Destructors);
        EXPECT_LT(n, state.MoveConstructors);
        EXPECT_THAT(state, NoCopies());

        // Resetting does not delete since ownership was transferred.
        cb.Reset();
        EXPECT_EQ(0, state.Destructors);
        EXPECT_THAT(state, NoCopies());
    }

    // Ensure that we actually did get ownership (from the last scope).
    EXPECT_EQ(1, state.Destructors);
}

TEST_F(TBindTest, PassedWrapper3)
{
    TProbeState state;
    TProbe sender(&state);
    TProbe receiver(TProbe::ExplicitlyCreateInvalidProbe());

    TCallback<TProbe(TProbe&&)> cb =
        BIND(&PolymorphicPassThrough<TProbe>);

    EXPECT_TRUE(sender.IsValid());
    EXPECT_FALSE(receiver.IsValid());

    EXPECT_EQ(0, state.Destructors);
    EXPECT_THAT(state, NoCopies());

    receiver = cb(std::move(sender));

    EXPECT_FALSE(sender.IsValid());
    EXPECT_TRUE(receiver.IsValid());

    EXPECT_EQ(0, state.Destructors);
    EXPECT_THAT(state, NoCopies());
}

TEST_F(TBindTest, PassedWrapper4)
{
    TProbeState state;
    TProbe sender(&state);
    TProbe receiver(TProbe::ExplicitlyCreateInvalidProbe());

    TCallback<TProbe(TProbe)> cb =
        BIND(&PolymorphicIdentity<TProbe>);

    EXPECT_TRUE(sender.IsValid());
    EXPECT_FALSE(receiver.IsValid());

    EXPECT_EQ(0, state.Destructors);
    EXPECT_THAT(state, NoCopies());

    receiver = cb(std::move(sender));

    EXPECT_FALSE(sender.IsValid());
    EXPECT_TRUE(receiver.IsValid());

    EXPECT_EQ(0, state.Destructors);
    EXPECT_THAT(state, NoCopies());
}


// Argument constructor usage for non-reference and const reference parameters.
TEST_F(TBindTest, ArgumentProbing)
{
    TProbeState state;
    TProbe probe(&state);

    TProbe& probeRef = probe;
    const TProbe& probeConstRef = probe;

    // {T, T&, const T&, T&&} -> T
    {
        // Bind T
        state.Reset();
        TClosure boundValue =
            BIND(&VoidPolymorphic1<TProbe>, probe);
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(1, 0), NoAssignments()));
        boundValue();
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(2, 0), NoAssignments()));

        // Bind T&
        state.Reset();
        TClosure boundRef =
            BIND(&VoidPolymorphic1<TProbe>, probeRef);
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(1, 0), NoAssignments()));
        boundRef();
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(2, 0), NoAssignments()));

        // Bind const T&
        state.Reset();
        TClosure boundConstRef =
            BIND(&VoidPolymorphic1<TProbe>, probeConstRef);
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(1, 0), NoAssignments()));
        boundConstRef();
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(2, 0), NoAssignments()));

        // Bind T&&
        state.Reset();
        TClosure boundRvRef =
            BIND(&VoidPolymorphic1<TProbe>, static_cast<TProbe&&>(TProbe(&state)));
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(0, 1), NoAssignments()));
        boundRvRef();
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(1, 1), NoAssignments()));

        // Pass all of above as a forwarded argument.
        // We expect almost perfect forwarding (copy + move)
        state.Reset();
        TCallback<void(TProbe)> forward = BIND(&VoidPolymorphic1<TProbe>);

        EXPECT_THAT(probe, HasCopyMoveCounts(0, 0));
        forward(probe);
        EXPECT_THAT(probe, HasCopyMoveCounts(1, 1));
        forward(probeRef);
        EXPECT_THAT(probe, HasCopyMoveCounts(2, 2));
        forward(probeConstRef);
        EXPECT_THAT(probe, HasCopyMoveCounts(3, 3));
        forward(TProbe(&state));
        EXPECT_THAT(probe, HasCopyMoveCounts(3, 4));

        EXPECT_THAT(probe, NoAssignments());
    }

    // {T, T&, const T&, T&&} -> const T&
    {
        // Bind T
        state.Reset();
        TClosure boundValue =
            BIND(&VoidPolymorphic1<const TProbe&>, probe);
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(1, 0), NoAssignments()));
        boundValue();
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(1, 0), NoAssignments()));

        // Bind T&
        state.Reset();
        TClosure boundRef =
            BIND(&VoidPolymorphic1<const TProbe&>, probeRef);
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(1, 0), NoAssignments()));
        boundRef();
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(1, 0), NoAssignments()));

        // Bind const T&
        state.Reset();
        TClosure boundConstRef =
            BIND(&VoidPolymorphic1<const TProbe&>, probeConstRef);
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(1, 0), NoAssignments()));
        boundConstRef();
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(1, 0), NoAssignments()));

        // Bind T&&
        state.Reset();
        TClosure boundRvRef =
            BIND(&VoidPolymorphic1<const TProbe&>, TProbe(&state));
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(0, 1), NoAssignments()));
        boundRvRef();
        EXPECT_THAT(probe, AllOf(HasCopyMoveCounts(0, 1), NoAssignments()));

        // Pass all of above as a forwarded argument.
        // We expect perfect forwarding.
        state.Reset();
        TCallback<void(const TProbe&)> forward = BIND(&VoidPolymorphic1<const TProbe&>);

        EXPECT_THAT(probe, HasCopyMoveCounts(0, 0));
        forward(probe);
        EXPECT_THAT(probe, HasCopyMoveCounts(0, 0));
        forward(probeRef);
        EXPECT_THAT(probe, HasCopyMoveCounts(0, 0));
        forward(probeConstRef);
        EXPECT_THAT(probe, HasCopyMoveCounts(0, 0));
        forward(static_cast<TProbe&&>(TProbe(&state)));
        EXPECT_THAT(probe, HasCopyMoveCounts(0, 0));

        EXPECT_THAT(probe, NoAssignments());
    }
}

// Argument constructor usage for non-reference and const reference parameters.
TEST_F(TBindTest, CoercibleArgumentProbing)
{
    TProbeState state;
    TCoercibleToProbe probe(&state);

    TCoercibleToProbe& probeRef = probe;
    const TCoercibleToProbe& probeConstRef = probe;

    // Pass {T, T&, const T&, T&&} as a forwarded argument.
    // We expect almost perfect forwarding (copy + move).
    state.Reset();
    TCallback<void(TProbe)> forward = BIND(&VoidPolymorphic1<TProbe>);

    EXPECT_THAT(state, HasCopyMoveCounts(0, 0));
    forward(probe);
    EXPECT_THAT(state, HasCopyMoveCounts(1, 1));
    forward(probeRef);
    EXPECT_THAT(state, HasCopyMoveCounts(2, 2));
    forward(probeConstRef);
    EXPECT_THAT(state, HasCopyMoveCounts(3, 3));
    forward(TProbe(&state));
    EXPECT_THAT(state, HasCopyMoveCounts(3, 4));

    EXPECT_THAT(state, NoAssignments());
}

// TCallback construction and assignment tests.
//   - Construction from an InvokerStorageHolder should not cause ref/deref.
//   - Assignment from other callback should only cause one ref
//
// TODO(ajwong): Is there actually a way to test this?

// Lambda support.
//   - Should be able to bind C++11 lambdas without any arguments.
//   - Should be able to bind C++11 lambdas with free arguments.
TEST_F(TBindTest, LambdaSupport)
{
    int n = 1;

    TClosure closure = BIND([&n] () { ++n; });
    EXPECT_EQ(1, n);
    closure();
    EXPECT_EQ(2, n);
    closure();
    EXPECT_EQ(3, n);

    TCallback<int()> cb1 = BIND([  ] () -> int { return 42;  });
    TCallback<int()> cb2 = BIND([&n] () -> int { return ++n; });

    EXPECT_EQ(42, cb1());
    EXPECT_EQ( 4, cb2());
    EXPECT_EQ( 4, n);
    EXPECT_EQ( 5, cb2());
    EXPECT_EQ( 5, n);

    TCallback<int(int, int)> plus  = BIND([] (int a, int b) -> int { return a + b; });
    TCallback<int(int)>      plus5 = BIND([] (int a, int b) -> int { return a + b; }, 5);

    EXPECT_EQ(3, plus. Run(1, 2));
    EXPECT_EQ(6, plus5(1));
}

TEST_F(TBindTest, MutableLambdaSupport)
{
    auto f = BIND([n = 1] () mutable { ++n; return n; });
    EXPECT_EQ(2, f());
    EXPECT_EQ(3, f());
    EXPECT_EQ(4, f());
}

TEST_F(TBindTest, ConstLambdaSupport)
{
    int n = 1;
    const auto l = [&n] () { ++n; return n; };
    const auto f = BIND(l);
    EXPECT_EQ(2, f());
    EXPECT_EQ(3, f());
    EXPECT_EQ(4, f());
}

TEST_F(TBindTest, MoveOnlyLambdaSupport)
{
    class TMoveOnly
    {
    public:
        TMoveOnly(size_t v)
            : Value_(v)
        { }

        TMoveOnly(const TMoveOnly&) = delete;

        TMoveOnly(TMoveOnly&& rhs)
            : Value_(rhs.Value_)
        {
            rhs.Value_ = 0;
        }

        size_t Value() const
        {
            return Value_;
        }

    private:
        size_t Value_ = 0;
    };

    TMoveOnly outer(5);
    EXPECT_EQ(5u, outer.Value());

    auto f = BIND([inner = std::move(outer)] () { return inner.Value(); });
    EXPECT_EQ(0u, outer.Value());

    // Call twice just in case
    EXPECT_EQ(5u, f());
    EXPECT_EQ(5u, f());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
