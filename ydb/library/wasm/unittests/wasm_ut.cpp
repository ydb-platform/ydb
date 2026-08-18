#include <ydb/library/wasm/api/allocation_registry.h>
#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/data_transfer.h>
#include <ydb/library/wasm/api/function.h>
#include <ydb/library/wasm/api/memory_pool.h>
#include <ydb/library/wasm/api/pointer.h>

#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/yt/error/error.h>

#include <util/generic/scope.h>

#include <cstring>

using NYT::Format;

namespace NYdb::NWasm {

bool EnableSystemLibraries();

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWebAssemblyTest
    : public ::testing::Test
{ };

TEST_F(TWebAssemblyTest, Create)
{
    std::vector<std::unique_ptr<IWebAssemblyCompartment>> compartments;
    for (int i = 0; i < 128; ++i) {
        compartments.push_back(CreateMinimalRuntimeImage());
    }
}

TEST_F(TWebAssemblyTest, AllocateAndFree)
{
    auto compartment = CreateMinimalRuntimeImage();
    std::vector<uintptr_t> offsets;
    for (int i = 0; i < 1024; ++i) {
        uintptr_t offset = compartment->AllocateBytes(1024);
        offsets.push_back(offset);
    }

    for (auto offset : offsets) {
        compartment->FreeBytes(offset);
    }
}

static const TStringBuf AddAndMul = R"(
    (module
        (type (;0;) (func (param i64 i64) (result i64)))

        (func $add (type 0) (param $first i64) (param $second i64) (result i64)
            (i64.add
                (local.get $first)
                (local.get $second)
            )
        )

        (func $mul (type 0) (param $0 i64) (param $1 i64) (result i64)
            (local.get $1)
            (local.get $0)
            (i64.mul)
        )

        (export "add" (func $add))
        (export "mul" (func $mul))
    ))";

TEST_F(TWebAssemblyTest, LinkAndStrip)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(AddAndMul);
    compartment->Strip();
}

TEST_F(TWebAssemblyTest, RunSimple)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(AddAndMul);

    auto add = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "add");
    for (int i = 0; i < 1024; ++i) {
        i64 a = std::rand();
        i64 b = std::rand();
        auto result = add(a, b);
        ASSERT_EQ(result, a + b);
    }

    auto mul = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "mul");
    for (int i = 0; i < 1024; ++i) {
        i64 a = std::rand();
        i64 b = std::rand();
        auto result = mul(a, b);
        ASSERT_EQ(result, a * b);
    }
}

static const TStringBuf Divide = R"(
    (module
        (type (;0;) (func (param i64 i64) (result i64)))

        (func $div (type 0) (param $dividend i64) (param $divisor i64) (result i64)
            (local.get $dividend)
            (local.get $divisor)
            (i64.div_s)
        )

        (export "div" (func $div))
    ))";

TEST_F(TWebAssemblyTest, BadDivision)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(Divide);
    auto div = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "div");

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    ASSERT_EQ(div(5, 2), 2);
    ASSERT_EQ(div(5, 3), 1);

    try {
        div(5, 0);
        GTEST_FAIL() << "Expected exception to be thrown";
    } catch (WAVM::Runtime::Exception* exception) {
        WAVM::Runtime::destroyException(exception);
    }
}

TEST_F(TWebAssemblyTest, Clone)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(AddAndMul);

    for (int i = 0; i < 1024; ++i) {
        compartment = compartment->Clone();
    }

    auto add = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "add");
    for (int i = 0; i < 1024; ++i) {
        i64 a = std::rand();
        i64 b = std::rand();
        auto result = add(a, b);
        ASSERT_EQ(result, a + b);
    }

    auto mul = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "mul");
    for (int i = 0; i < 1024; ++i) {
        i64 a = std::rand();
        i64 b = std::rand();
        auto result = mul(a, b);
        ASSERT_EQ(result, a * b);
    }
}

static const TStringBuf ArraySum = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))

        (type (;0;) (func (param i64 i64) (result i64)))

        (func $sum (type 0) (param $ptr i64) (param $length i64) (result i64)
            (local $result i64)
            (local $index i64)

            (local.set $result (i64.const 0))
            (local.set $index (i64.const 0))

            (block $IF
                (br_if $IF (i64.ge_s (local.get $index) (local.get $length)))

                (loop $LOOP
                    (local.set $result (i64.add (local.get $result) (i64.load (local.get $ptr))))

                    (local.set $ptr (i64.add (local.get $ptr) (i64.const 8)))

                    (local.set $index (i64.add (local.get $index) (i64.const 1)))

                    (br_if $LOOP (i64.ne (local.get $index) (local.get $length)))
                )
            )

            (local.get $result)
        )

        (export "sum" (func $sum))
    ))";

TEST_F(TWebAssemblyTest, SimpleArraySum)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(ArraySum);
    auto sum = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "sum");

    i64 maxLength = 42;

    for (int iteration = 0; iteration < 100; ++iteration) {
        i64 length = std::rand() % maxLength;
        auto byteLength = sizeof(i64) * length;
        uintptr_t offset = compartment->AllocateBytes(byteLength);
        void* hostPointer = compartment->GetHostPointer(offset, byteLength);
        i64* array = std::bit_cast<i64*>(hostPointer);

        i64 expected = 0;
        for (i64 i = 0; i < length; ++i) {
            array[i] = std::rand() % 10;
            expected += array[i];
        }

        auto actual = sum(offset, length);
        ASSERT_EQ(actual, expected);
    }
}

TEST_F(TWebAssemblyTest, DataTransfer)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(ArraySum);
    auto sum = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "sum");

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    constexpr i64 length = 42;

    auto first = std::vector<i64>(length);
    auto second = std::vector<i64>(length);

    i64 firstExpectedSum = 0;
    i64 secondExpectedSum = 0;
    for (i64 i = 0; i < length; ++i) {
        first[i] = std::rand() % 10;
        firstExpectedSum += first[i];

        second[i] = std::rand() % 10;
        secondExpectedSum += second[i];
    }

    auto firstCopied = CopyIntoCompartment<const std::vector<i64>&>(first, compartment.get());
    auto secondCopied = CopyIntoCompartment<const std::vector<i64>&>(second, compartment.get());

    auto firstActualSum = sum(firstCopied.GetCopiedOffset(), length);
    ASSERT_EQ(firstActualSum, firstExpectedSum);

    auto secondActualSum = sum(secondCopied.GetCopiedOffset(), length);
    ASSERT_EQ(secondActualSum, secondExpectedSum);
}

TEST_F(TWebAssemblyTest, GuestBufferZeroCopyWrite)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(ArraySum);
    auto sum = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "sum");

    constexpr i64 length = 32;
    const auto byteLength = sizeof(i64) * length;

    auto buffer = TGuestBuffer::Allocate(compartment.get(), byteLength);
    ASSERT_NE(buffer.Offset(), 0u);
    ASSERT_EQ(buffer.Size(), byteLength);
    ASSERT_NE(buffer.HostData(), nullptr);

    i64 expected = 0;
    auto* array = std::bit_cast<i64*>(buffer.HostData());
    for (i64 i = 0; i < length; ++i) {
        array[i] = i + 1;
        expected += array[i];
    }

    const auto offset = buffer.Release();
    ASSERT_EQ(sum(offset, length), expected);
    compartment->FreeBytes(offset);
}

TEST_F(TWebAssemblyTest, GuestBufferResidentOffset)
{
    // Allocate in WASM, write once, then resolve host pointer back to offset
    // (the PrepareArg skip-copy primitive).
    auto compartment = CreateMinimalRuntimeImage();

    const TString blob(64, 'z');
    auto buffer = TGuestBuffer::Allocate(compartment.get(), blob.size());
    std::memcpy(buffer.HostData(), blob.data(), blob.size());
    const uintptr_t offset = buffer.Offset();
    char* hostData = buffer.HostData();
    buffer.Release();

    ASSERT_EQ(compartment->GetCompartmentOffset(hostData), offset);
    ASSERT_EQ(
        TStringBuf(static_cast<char*>(compartment->GetHostPointer(offset, blob.size())), blob.size()),
        TStringBuf(blob));

    char hostBlob[64];
    std::memset(hostBlob, 'h', sizeof(hostBlob));
    ASSERT_THROW(
        compartment->GetCompartmentOffset(hostBlob),
        NYT::TErrorException);

    compartment->FreeBytes(offset);
}

TEST_F(TWebAssemblyTest, AllocationRegistryTryFree)
{
    auto compartment = CreateMinimalRuntimeImage();
    auto buffer = TGuestBuffer::Allocate(compartment.get(), 64);
    void* host = buffer.HostData();
    const uintptr_t offset = buffer.Offset();
    const size_t size = buffer.Size();
    buffer.Release();

    auto& registry = TWasmAllocationRegistry::Instance();
    registry.Register(host, compartment.get(), offset, size, /*generation*/ 1);
    ASSERT_TRUE(registry.TryFree(host));
    ASSERT_FALSE(registry.TryFree(host));
}

TEST_F(TWebAssemblyTest, AllocationRegistryOwnerOutlivesLiveAllocations)
{
    // A string materialized into linear memory keeps its refcount header there,
    // so even destroying the value reads guest memory. Releasing the owner while
    // an allocation is live must not unmap it.
    std::shared_ptr<IWebAssemblyCompartment> compartment = CreateMinimalRuntimeImage();
    auto buffer = TGuestBuffer::Allocate(compartment.get(), 64);
    void* host = buffer.HostData();
    const uintptr_t offset = buffer.Offset();
    const size_t size = buffer.Size();
    buffer.Release();

    constexpr ui64 generation = 99;
    auto& registry = TWasmAllocationRegistry::Instance();
    registry.RetainOwner(generation, compartment);
    registry.Register(host, compartment.get(), offset, size, generation);
    ASSERT_EQ(registry.CountGeneration(generation), 1u);

    std::weak_ptr<IWebAssemblyCompartment> weak = compartment;
    registry.ReleaseOwner(generation);
    compartment.reset();
    ASSERT_FALSE(weak.expired()) << "compartment died under a live allocation";

    ASSERT_TRUE(registry.TryFree(host));
    ASSERT_TRUE(weak.expired()) << "compartment leaked past the last allocation";
    ASSERT_FALSE(registry.TryFree(host));
}

TEST_F(TWebAssemblyTest, AllocationRegistryUnRefFreesBeforeOwnerRelease)
{
    // Models PreferWasm string lifetime: Make → drop UnboxedValue (UnRef) →
    // FreeBytes via UdfTryFreeExternalString while compartment is still alive.
    // Intermediate rows must not accumulate in the registry until teardown.
    auto compartment = CreateMinimalRuntimeImage();
    auto& registry = TWasmAllocationRegistry::Instance();
    constexpr ui64 generation = 77;

    const TString payload(64, 'x');
    for (int i = 0; i < 8; ++i) {
        {
            auto buffer = TGuestBuffer::Allocate(compartment.get(), 80);
            void* host = buffer.HostData();
            const uintptr_t offset = buffer.Offset();
            buffer.Release();
            registry.Register(host, compartment.get(), offset, 80, generation);
            ASSERT_EQ(registry.CountGeneration(generation), 1u);
            // Simulate TStringValue UnRef → UdfTryFreeExternalString.
            ASSERT_TRUE(registry.TryFree(host));
        }
        ASSERT_EQ(registry.CountGeneration(generation), 0u)
            << "row " << i << " leaked past UnRef";
    }

    registry.ReleaseOwner(generation);
    ASSERT_EQ(registry.CountGeneration(generation), 0u);
}

static const TStringBuf PointerDereference = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))

        (type (;0;) (func (param i64 i64) (result i64)))

        (func $arrayAt (type 0) (param $ptr i64) (param $index i64) (result i64)
            local.get $ptr
            local.get $index
            i64.const 3
            i64.shl
            i64.add
            i64.load)

        (export "arrayAt" (func $arrayAt))
    ))";

TEST_F(TWebAssemblyTest, BadPointerDereference)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(PointerDereference);
    auto arrayAt = TCompartmentFunction<i64(i64, i64)>(compartment.get(), "arrayAt");

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    i64 length = 5;
    auto byteLength = sizeof(i64) * length;
    uintptr_t offset = compartment->AllocateBytes(byteLength);
    auto* array = PtrFromVM(compartment.get(), std::bit_cast<i64*>(offset), length);

    for (int i = 0; i < length; ++i) {
        array[i] = i;
        ASSERT_EQ(arrayAt(offset, i), i);
    }

    try {
        arrayAt(offset, 0xeeeeeeee);
        GTEST_FAIL() << "Expected exception to be thrown";
    } catch (WAVM::Runtime::Exception* exception) {
        WAVM::Runtime::destroyException(exception);
    }

    try {
        arrayAt(0xeeeeeeee, 0);
        GTEST_FAIL() << "Expected exception to be thrown";
    } catch (WAVM::Runtime::Exception* exception) {
        WAVM::Runtime::destroyException(exception);
    }

    try {
        arrayAt(0xeeeeeeee, 0xeeeeeeee);
        GTEST_FAIL() << "Expected exception to be thrown";
    } catch (WAVM::Runtime::Exception* exception) {
        WAVM::Runtime::destroyException(exception);
    }
}

TEST_F(TWebAssemblyTest, MemoryPoolAlignedAlloc)
{
    auto compartment = CreateMinimalRuntimeImage();

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    for (int i = 0; i < 1000; ++i) {
        TWebAssemblyMemoryPool pool;

        pool.AllocateUnaligned(3);
        auto aligned = pool.AllocateAligned(16, 8);
        ASSERT_EQ(std::bit_cast<ui64>(aligned) % 8, 0ull);

        auto* atHost = PtrFromVM(compartment.get(), aligned);
        ASSERT_EQ(std::bit_cast<ui64>(atHost) % 8, 0ull);
    }
}

////////////////////////////////////////////////////////////////////////////////

static const TStringBuf MathematicalConstants = R"(
    (module
        (type $t1 (func (result i64)))

        (import "env" "memory" (memory $env.memory i64 1))
        (import "env" "__memory_base" (global $__memory_base i64))

        (func $Pi (type $t1) (result i64)
            global.get $__memory_base
            i64.const 0
            i64.add)

        (func $E (type $t1) (result i64)
            global.get $__memory_base
            i64.const 7
            i64.add)

        (export "Pi" (func $Pi))
        (export "E" (func $E))

        (data $.data (global.get $__memory_base) "3.1415\002.71828\00")
    ))";

static const TStringBuf Algorithms = R"(
    (module
        (type $t1 (func (result i64)))

        (import "env" "memory" (memory $env.memory i64 1))
        (import "env" "__memory_base" (global $__memory_base i64))

        (func $Gcd (type $t1) (result i64)
            global.get $__memory_base
            i64.const 0
            i64.add)

        (func $Fib (type $t1) (result i64)
            global.get $__memory_base
            i64.const 75
            i64.add)

        (export "Gcd" (func $Gcd))
        (export "Fib" (func $Fib))

        (data $.data (global.get $__memory_base) "int gcd(int a, int b) { if (b == 0) return a; else return gcd(b, a % b); }\00int fib (int n) { if (n <= 1) { return n; } return fib(n - 1) + fib(n - 2); }\00")
    ))";

TEST_F(TWebAssemblyTest, DynamicLinkingMemory)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(MathematicalConstants);

    {
        SetCurrentCompartment(compartment.get());
        Y_DEFER {
            SetCurrentCompartment(nullptr);
        };
        {
            auto pi = TCompartmentFunction<char*()>(compartment.get(), "Pi");
            char* piOffset = pi();
            char* piOnHost = PtrFromVM(compartment.get(), piOffset);
            ASSERT_EQ(TStringBuf(piOnHost), "3.1415");

            auto e = TCompartmentFunction<char*()>(compartment.get(), "E");
            char* eOffset = e();
            char* eOnHost = PtrFromVM(compartment.get(), eOffset);
            ASSERT_EQ(TStringBuf(eOnHost), "2.71828");
        }
    }

    compartment->AddModule(Algorithms);

    {
        SetCurrentCompartment(compartment.get());
        Y_DEFER {
            SetCurrentCompartment(nullptr);
        };
        {
            auto gcd = TCompartmentFunction<char*()>(compartment.get(), "Gcd");
            char* gcdOffset = gcd();
            char* gcdOnHost = PtrFromVM(compartment.get(), gcdOffset);
            ASSERT_EQ(TStringBuf(gcdOnHost), "int gcd(int a, int b) { if (b == 0) return a; else return gcd(b, a % b); }");

            auto Fib = TCompartmentFunction<char*()>(compartment.get(), "Fib");
            char* fibOffset = Fib();
            char* fibOnHost = PtrFromVM(compartment.get(), fibOffset);
            ASSERT_EQ(TStringBuf(fibOnHost), "int fib (int n) { if (n <= 1) { return n; } return fib(n - 1) + fib(n - 2); }");

            auto pi = TCompartmentFunction<char*()>(compartment.get(), "Pi");
            char* piOffset = pi();
            char* piOnHost = PtrFromVM(compartment.get(), piOffset);
            ASSERT_EQ(TStringBuf(piOnHost), "3.1415");

            auto e = TCompartmentFunction<char*()>(compartment.get(), "E");
            char* eOffset = e();
            char* eOnHost = PtrFromVM(compartment.get(), eOffset);
            ASSERT_EQ(TStringBuf(eOnHost), "2.71828");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

/** The module below corresponds to the following C++ code
 * struct __attribute__((visibility("default"))) TFoo
 * {
 *     int __attribute__((noinline)) A()
 *     {
 *         return 42;
 *     }
 * };
 *
 * extern "C" int return_42()
 * {
 *     auto foo = TFoo();
 *     return foo.A();
 * }
 */

static const TStringBuf ModuleWithWeakSymbols = R"(
    (module
        (type $t0 (func (result i32)))
        (type $t1 (func (param i64) (result i32)))

        (import "env" "memory" (memory $env.memory i64 1))
        (import "env" "__stack_pointer" (global $__stack_pointer (mut i64)))

        (import "env" "_ZN4TFoo1AEv" (func $TFoo::A__ (type $t1)))

        (func $return_42 (type $t0) (result i32)
            (local $l0 i64) (local $l1 i32)
            global.get $__stack_pointer
            i64.const 16
            i64.sub
            local.tee $l0
            global.set $__stack_pointer
            local.get $l0
            i32.const 0
            i32.store8 offset=15
            local.get $l0
            i64.const 15
            i64.add
            call $TFoo::A__
            local.set $l1
            local.get $l0
            i64.const 16
            i64.add
            global.set $__stack_pointer
            local.get $l1)

        (func $TFoo::A__.1 (type $t1) (param $p0 i64) (result i32)
            i32.const 42)

        (export "return_42" (func $return_42))
        (export "_ZN4TFoo1AEv" (func $TFoo::A__.1))
    ))";

TEST_F(TWebAssemblyTest, DynamicLinkingWeakSymbols)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(ModuleWithWeakSymbols);

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    auto function = TCompartmentFunction<int()>(compartment.get(), "return_42");
    ASSERT_EQ(function(), 42);
}

static const TStringBuf ModuleWithWeakSymbols2 = R"(
    (module
        (type $t0 (func (result i32)))
        (type $t1 (func (param i64) (result i32)))
        (type $t2 (func (result i32)))

        (import "env" "memory" (memory $env.memory i64 1))
        (import "env" "__stack_pointer" (global $__stack_pointer (mut i64)))

        (import "env" "_ZN4TFoo1AEv" (func $TFoo::A__ (type $t1)))
        (import "env" "_ZN4TBar1AEv" (func $TBar::A__ (type $t1)))
        (import "env" "_ZN4TBaz1AEv" (func $TBaz::A__ (type $t1)))
        (import "env" "_ZN4TQux1AEv" (func $TQux::A__ (type $t1)))

        (func $nothing0 (type $t1) (param $p0 i64) (result i32) i32.const 100)
        (func $nothing1 (type $t1) (param $p0 i64) (result i32) i32.const 101)
        (func $TQux::A__.1 (type $t1) (param $p0 i64) (result i32) i32.const 3)
        (func $nothing2 (type $t1) (param $p0 i64) (result i32) i32.const 102)
        (func $TFoo::A__.1 (type $t1) (param $p0 i64) (result i32) i32.const 0)
        (func $nothing3 (type $t1) (param $p0 i64) (result i32) i32.const 103)
        (func $TBaz::A__.1 (type $t1) (param $p0 i64) (result i32) i32.const 2)
        (func $nothing4 (type $t1) (param $p0 i64) (result i32) i32.const 104)
        (func $TBar::A__.1 (type $t1) (param $p0 i64) (result i32) i32.const 1)
        (func $nothing5 (type $t1) (param $p0 i64) (result i32) i32.const 105)

        (func $nothing6 (type $t1) (param $p0 i64) (result i32) i32.const 106)
        (func $invokeFoo0 (type $t2) (result i32) i64.const 0 call $TFoo::A__)
        (func $nothing7 (type $t1) (param $p0 i64) (result i32) i32.const 107)
        (func $invokeFoo1 (type $t2) (result i32) i64.const 0 call $TFoo::A__.1)
        (func $nothing8 (type $t1) (param $p0 i64) (result i32) i32.const 108)
        (func $invokeBar0 (type $t2) (result i32) i64.const 0 call $TBar::A__)
        (func $nothing9 (type $t1) (param $p0 i64) (result i32) i32.const 109)
        (func $invokeBar1 (type $t2) (result i32) i64.const 0 call $TBar::A__.1)
        (func $nothing10 (type $t1) (param $p0 i64) (result i32) i32.const 110)
        (func $invokeBaz0 (type $t2) (result i32) i64.const 0 call $TBaz::A__)
        (func $nothing11 (type $t1) (param $p0 i64) (result i32) i32.const 111)
        (func $invokeBaz1 (type $t2) (result i32) i64.const 0 call $TBaz::A__.1)
        (func $nothing12 (type $t1) (param $p0 i64) (result i32) i32.const 112)
        (func $invokeQux0 (type $t2) (result i32) i64.const 0 call $TQux::A__)
        (func $nothing13 (type $t1) (param $p0 i64) (result i32) i32.const 113)
        (func $invokeQux1 (type $t2) (result i32) i64.const 0 call $TQux::A__.1)
        (func $nothing14 (type $t1) (param $p0 i64) (result i32) i32.const 114)

        (export "_ZN4TBar1AEv" (func $TBar::A__.1))
        (export "_ZN4TBaz1AEv" (func $TBaz::A__.1))
        (export "nothing4" (func $nothing4))
        (export "_ZN4TQux1AEv" (func $TQux::A__.1))
        (export "_ZN4TFoo1AEv" (func $TFoo::A__.1))

        (export "invokeQux0" (func $invokeQux0))
        (export "invokeFoo0" (func $invokeFoo0))
        (export "invokeBaz1" (func $invokeBaz1))
        (export "invokeBar1" (func $invokeBar1))
        (export "invokeBaz0" (func $invokeBaz0))
        (export "invokeQux1" (func $invokeQux1))
        (export "invokeFoo1" (func $invokeFoo1))
        (export "invokeBar0" (func $invokeBar0))
    ))";

TEST_F(TWebAssemblyTest, DynamicLinkingWeakSymbols2)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(ModuleWithWeakSymbols2);

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    auto parameters = std::vector<std::pair<std::string, int>>{
        {"Foo", 0},
        {"Bar", 1},
        {"Baz", 2},
        {"Qux", 3},
    };

    for (auto& [name, value] : parameters) {
        auto function = TCompartmentFunction<int(i64)>(compartment.get(), Format("_ZN4T%v1AEv", name));
        ASSERT_EQ(function(0x0), value);
        auto invoke0 = TCompartmentFunction<int()>(compartment.get(), Format("invoke%v0", name));
        ASSERT_EQ(invoke0(), value);
        auto invoke1 = TCompartmentFunction<int()>(compartment.get(), Format("invoke%v1", name));
        ASSERT_EQ(invoke1(), value);
    }
}

////////////////////////////////////////////////////////////////////////////////

static const TStringBuf FunctionPointers1 = R"(
    (module
        (type $t0 (func (result i64)))
        (type $t1 (func (param i64) (result i64)))

        (import "env" "memory" (memory $env.memory i64 1))
        (import "env" "__indirect_function_table" (table $env.__indirect_function_table 2 funcref))
        (import "env" "__table_base" (global $__table_base i64))
        (import "env" "__memory_base" (global $__memory_base i64))
        (import "env" "__table_base32" (global $__table_base32 i32))

        (func $Foo__ (type $t0) (result i64)
            global.get $__memory_base
            i64.const 0
            i64.add)
        (func $Bar__ (type $t0) (result i64)
            global.get $__memory_base
            i64.const 4
            i64.add)

        (func $Select1 (type $t1) (param $p0 i64) (result i64)
            global.get $__table_base
            i64.const 0
            i64.const 1
            local.get $p0
            i64.eqz
            select
            i64.add
            i32.wrap_i64
            call_indirect $env.__indirect_function_table (type $t0))

        (export "_Z3Foov" (func $Foo__))
        (export "_Z3Barv" (func $Bar__))
        (export "Select1" (func $Select1))

        (elem $e0 (global.get $__table_base32) func $Foo__ $Bar__)

        (data $.data (global.get $__memory_base) "foo\00bar\00")
    ))";

TEST_F(TWebAssemblyTest, FunctionPointers1)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(FunctionPointers1);

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    auto foo = TCompartmentFunction<char*()>(compartment.get(), "_Z3Foov");
    ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), foo())), "foo");

    auto bar = TCompartmentFunction<char*()>(compartment.get(), "_Z3Barv");
    ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), bar())), "bar");

    auto select1 = TCompartmentFunction<char*(i64)>(compartment.get(), "Select1");
    ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select1(0))), "foo");
    ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select1(1))), "bar");
}

static const TStringBuf FunctionPointers2 = R"(
    (module
        (type $t0 (func (result i64)))
        (type $t1 (func (param i64) (result i64)))

        (import "env" "memory" (memory $env.memory i64 1))
        (import "env" "__indirect_function_table" (table $env.__indirect_function_table 2 funcref))
        (import "env" "__table_base" (global $__table_base i64))
        (import "env" "__memory_base" (global $__memory_base i64))
        (import "env" "__table_base32" (global $__table_base32 i32))

        (func $Baz__ (type $t0) (result i64)
            global.get $__memory_base
            i64.const 0
            i64.add)
        (func $Qux__ (type $t0) (result i64)
            global.get $__memory_base
            i64.const 4
            i64.add)

        (func $Select2 (type $t1) (param $p0 i64) (result i64)
            global.get $__table_base
            i64.const 0
            i64.const 1
            local.get $p0
            i64.eqz
            select
            i64.add
            i32.wrap_i64
            call_indirect $env.__indirect_function_table (type $t0))

        (export "_Z3Bazv" (func $Baz__))
        (export "_Z3Quxv" (func $Qux__))
        (export "Select2" (func $Select2))

        (elem $e0 (global.get $__table_base32) func $Baz__ $Qux__)

        (data $.data (global.get $__memory_base) "baz\00qux\00")
    ))";

TEST_F(TWebAssemblyTest, FunctionPointers2)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(FunctionPointers1);

    {
        SetCurrentCompartment(compartment.get());
        Y_DEFER {
            SetCurrentCompartment(nullptr);
        };

        auto foo = TCompartmentFunction<char*()>(compartment.get(), "_Z3Foov");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), foo())), "foo");

        auto bar = TCompartmentFunction<char*()>(compartment.get(), "_Z3Barv");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), bar())), "bar");

        auto select1 = TCompartmentFunction<char*(i64)>(compartment.get(), "Select1");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select1(0))), "foo");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select1(1))), "bar");
    }

    compartment->AddModule(FunctionPointers2);

    {
        SetCurrentCompartment(compartment.get());
        Y_DEFER {
            SetCurrentCompartment(nullptr);
        };

        auto foo = TCompartmentFunction<char*()>(compartment.get(), "_Z3Foov");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), foo())), "foo");

        auto bar = TCompartmentFunction<char*()>(compartment.get(), "_Z3Barv");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), bar())), "bar");

        auto select1 = TCompartmentFunction<char*(i64)>(compartment.get(), "Select1");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select1(0))), "foo");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select1(1))), "bar");

        auto baz = TCompartmentFunction<char*()>(compartment.get(), "_Z3Bazv");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), baz())), "baz");

        auto qux = TCompartmentFunction<char*()>(compartment.get(), "_Z3Quxv");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), qux())), "qux");

        auto select2 = TCompartmentFunction<char*(i64)>(compartment.get(), "Select2");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select2(0))), "baz");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select2(1))), "qux");
    }
}

/** The module below corresponds to the following C++ code
 * const char* Foo();
 * const char* Bar();
 * const char* Baz();
 * const char* Qux();
 *
 * extern "C" const char* Entry(long long index)
 * {
 *     const char*(*callback)(void);
 *
 *     switch (index) {
 *         case 0:
 *             callback = &Foo;
 *             break;
 *         case 1:
 *             callback = &Bar;
 *             break;
 *         case 2:
 *             callback = &Baz;
 *             break;
 *         case 3:
 *             callback = &Qux;
 *             break;
 *     }
 *
 *     return callback();
 * }
 */

static const TStringBuf FunctionPointers3 = R"(
    (module
        (type $t0 (func (result i64)))
        (type $t1 (func))
        (type $t2 (func (param i64) (result i64)))

        (import "env" "memory" (memory $env.memory i64 1))
        (import "env" "__indirect_function_table" (table $env.__indirect_function_table 0 funcref))
        (import "env" "__memory_base" (global $__memory_base i64))
        (import "env" "__table_base" (global $__table_base i64))
        (import "env" "__table_base32" (global $__table_base32 i32))

        (import "GOT.func" "_Z3Foov" (global $Foo__ (mut i64)))
        (import "GOT.func" "_Z3Barv" (global $Bar__ (mut i64)))
        (import "GOT.func" "_Z3Bazv" (global $Baz__ (mut i64)))
        (import "GOT.func" "_Z3Quxv" (global $Qux__ (mut i64)))

        (func $__wasm_call_ctors (type $t1))

        (func $__wasm_apply_data_relocs (type $t1)
            i64.const 944
            global.get $__memory_base
            i64.add
            global.get $Bar__
            i64.store align=4
            i64.const 952
            global.get $__memory_base
            i64.add
            global.get $Baz__
            i64.store align=4
            i64.const 960
            global.get $__memory_base
            i64.add
            global.get $Qux__
            i64.store align=4)

        (func $Select3 (type $t2) (param $p0 i64) (result i64)
            (local $l1 i64)
            global.get $Foo__
            local.set $l1
            block $B0
                local.get $p0
                i64.const -1
                i64.add
                local.tee $p0
                i64.const 2
                i64.gt_u
                br_if $B0
                global.get $__memory_base
                i64.const 944
                i64.add
                local.get $p0
                i64.const 3
                i64.shl
                i64.add
                i64.load
                local.set $l1
            end
            local.get $l1
            i32.wrap_i64
            call_indirect $env.__indirect_function_table (type $t0))

        (export "__wasm_apply_data_relocs" (func $__wasm_apply_data_relocs))
        (export "Select3" (func $Select3))
    ))";

TEST_F(TWebAssemblyTest, FunctionPointers3)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(FunctionPointers1);

    {
        SetCurrentCompartment(compartment.get());
        Y_DEFER {
            SetCurrentCompartment(nullptr);
        };

        auto foo = TCompartmentFunction<char*()>(compartment.get(), "_Z3Foov");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), foo())), "foo");

        auto bar = TCompartmentFunction<char*()>(compartment.get(), "_Z3Barv");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), bar())), "bar");

        auto select1 = TCompartmentFunction<char*(i64)>(compartment.get(), "Select1");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select1(0))), "foo");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select1(1))), "bar");
    }

    compartment->AddModule(FunctionPointers2);

    {
        SetCurrentCompartment(compartment.get());
        Y_DEFER {
            SetCurrentCompartment(nullptr);
        };

        auto foo = TCompartmentFunction<char*()>(compartment.get(), "_Z3Foov");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), foo())), "foo");

        auto bar = TCompartmentFunction<char*()>(compartment.get(), "_Z3Barv");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), bar())), "bar");

        auto select1 = TCompartmentFunction<char*(i64)>(compartment.get(), "Select1");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select1(0))), "foo");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select1(1))), "bar");

        auto baz = TCompartmentFunction<char*()>(compartment.get(), "_Z3Bazv");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), baz())), "baz");

        auto qux = TCompartmentFunction<char*()>(compartment.get(), "_Z3Quxv");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), qux())), "qux");

        auto select2 = TCompartmentFunction<char*(i64)>(compartment.get(), "Select2");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select2(0))), "baz");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select2(1))), "qux");
    }

    compartment->AddModule(FunctionPointers3);

    {
        SetCurrentCompartment(compartment.get());
        Y_DEFER {
            SetCurrentCompartment(nullptr);
        };

        auto foo = TCompartmentFunction<char*()>(compartment.get(), "_Z3Foov");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), foo())), "foo");

        auto bar = TCompartmentFunction<char*()>(compartment.get(), "_Z3Barv");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), bar())), "bar");

        auto select1 = TCompartmentFunction<char*(i64)>(compartment.get(), "Select1");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select1(0))), "foo");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select1(1))), "bar");

        auto baz = TCompartmentFunction<char*()>(compartment.get(), "_Z3Bazv");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), baz())), "baz");

        auto qux = TCompartmentFunction<char*()>(compartment.get(), "_Z3Quxv");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), qux())), "qux");

        auto select2 = TCompartmentFunction<char*(i64)>(compartment.get(), "Select2");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select2(0))), "baz");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select2(1))), "qux");

        auto select3 = TCompartmentFunction<char*(i64)>(compartment.get(), "Select3");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select3(0))), "foo");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select3(1))), "bar");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select3(2))), "baz");
        ASSERT_EQ(TStringBuf(PtrFromVM(compartment.get(), select3(3))), "qux");
    }
}

////////////////////////////////////////////////////////////////////////////////

/** The module below corresponds to the following C++ code
 * static bool InitCalled = false;
 *
 * long long __attribute__((noinline)) InitCounter()
 * {
 *     if (!InitCalled) {
 *         InitCalled = true;
 *         return 0;
 *     }
 *     __builtin_trap();
 * }
 *
 * extern "C" long long Entry()
 * {
 *     static long long Counter = InitCounter();
 *     return Counter++;
 * }
 */

static const TStringBuf ModuleWithStaticVariables = R"(
    (module
        (type $t0 (func (result i64)))

        (import "env" "memory" (memory $env.memory i64 1))
        (import "env" "__memory_base" (global $__memory_base i64))

        (func $InitCounter__ (type $t0) (result i64)
            block $B0
                global.get $__memory_base
                i64.const 944
                i64.add
                i32.load8_u
                br_if $B0
                global.get $__memory_base
                i64.const 944
                i64.add
                i32.const 1
                i32.store8
                i64.const 0
                return
            end
            unreachable
            unreachable)

        (func $Entry (type $t0) (result i64)
            (local $l0 i64)
            block $B0
                block $B1
                    global.get $__memory_base
                    i64.const 960
                    i64.add
                    i32.load8_u
                    i32.eqz
                    br_if $B1
                    global.get $__memory_base
                    i64.const 952
                    i64.add
                    i64.load
                    local.set $l0
                    br $B0
                end
                global.get $__memory_base
                local.set $l0
                call $InitCounter__
                drop
                local.get $l0
                i64.const 960
                i64.add
                i32.const 1
                i32.store8
                i64.const 0
                local.set $l0
            end
            global.get $__memory_base
            i64.const 952
            i64.add
            local.get $l0
            i64.const 1
            i64.add
            i64.store
            local.get $l0)

        (export "_Z11InitCounterv" (func $InitCounter__))
        (export "Entry" (func $Entry))
    ))";

TEST_F(TWebAssemblyTest, StaticVariables)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(ModuleWithStaticVariables);

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    auto function = TCompartmentFunction<i64()>(compartment.get(), "Entry");
    for (i64 i = 0; i < 1024; ++i) {
        ASSERT_EQ(function(), i);
    }

    auto errorOnSecondInitCall = TCompartmentFunction<i64()>(compartment.get(), "_Z11InitCounterv");
    try {
        errorOnSecondInitCall();
        GTEST_FAIL() << "Expected exception to be thrown";
    } catch (WAVM::Runtime::Exception* exception) {
        WAVM::Runtime::destroyException(exception);
    }
}

////////////////////////////////////////////////////////////////////////////////

/** The module below corresponds to the following C++ code
 * struct IInterface
 * {
 *     virtual ~IInterface() = default;
 *     virtual long long Call() = 0;
 * };
 *
 * struct TFoo
 *     : public IInterface
 * {
 *     long long Call() override
 *     {
 *         return 42;
 *     }
 * };
 *
 * struct TBar
 *     : public IInterface
 * {
 *     long long Call() override
 *     {
 *         return 271828;
 *     }
 * };
 *
 * long long __attribute__((noinline)) Invoke(IInterface* interface)
 * {
 *     return interface->Call();
 * };
 *
 * extern "C" long long Entry(long long index)
 * {
 *     switch (index) {
 *         case 0: {
 *             auto foo = TFoo();
 *             return Invoke(&foo);
 *         }
 *         case 1: {
 *             auto bar = TBar();
 *             return Invoke(&bar);
 *         }
 *     }
 *     __builtin_trap();
 * }
 */

static const TStringBuf ModuleWithVirtualFunctionCall = R"(
    (module
        (type $t0 (func (param i64) (result i64)))
        (type $t1 (func (param i64)))
        (type $t2 (func))
        (type $t3 (func (result i64)))
        (type $t4 (func (result i32)))
        (import "env" "memory" (memory $env.memory i64 1))
        (import "env" "__indirect_function_table" (table $env.__indirect_function_table 5 funcref))
        (import "env" "__stack_pointer" (global $__stack_pointer (mut i64)))
        (import "env" "__memory_base" (global $__memory_base i64))
        (import "env" "__table_base" (global $__table_base i64))
        (import "env" "__table_base32" (global $__table_base32 i32))
        (import "env" "_ZdlPv" (func $operator_delete_void*_ (type $t1)))
        (import "GOT.mem" "_ZTVN10__cxxabiv117__class_type_infoE" (global $vtable_for___cxxabiv1::__class_type_info (mut i64)))
        (import "GOT.mem" "_ZTVN10__cxxabiv120__si_class_type_infoE" (global $vtable_for___cxxabiv1::__si_class_type_info (mut i64)))
        (func $__wasm_call_ctors (type $t2))
        (func $__wasm_apply_data_relocs (type $t2)
            i64.const 960
            global.get $__memory_base
            i64.add
            global.get $__memory_base
            i64.const 1008
            i64.add
            i64.store align=4
            i64.const 968
            global.get $__memory_base
            i64.add
            global.get $__table_base
            i64.const 0
            i64.add
            i64.store align=4
            i64.const 976
            global.get $__memory_base
            i64.add
            global.get $__table_base
            i64.const 1
            i64.add
            i64.store align=4
            i64.const 984
            global.get $__memory_base
            i64.add
            global.get $__table_base
            i64.const 2
            i64.add
            i64.store align=4
            i64.const 992
            global.get $__memory_base
            i64.add
            global.get $vtable_for___cxxabiv1::__class_type_info
            i64.const 16
            i64.add
            i64.store align=4
            i64.const 1000
            global.get $__memory_base
            i64.add
            global.get $__memory_base
            i64.const 933
            i64.add
            i64.store align=4
            i64.const 1008
            global.get $__memory_base
            i64.add
            global.get $vtable_for___cxxabiv1::__si_class_type_info
            i64.const 16
            i64.add
            i64.store align=4
            i64.const 1016
            global.get $__memory_base
            i64.add
            global.get $__memory_base
            i64.const 927
            i64.add
            i64.store align=4
            i64.const 1024
            global.get $__memory_base
            i64.add
            global.get $__memory_base
            i64.const 992
            i64.add
            i64.store align=4
            i64.const 1040
            global.get $__memory_base
            i64.add
            global.get $__memory_base
            i64.const 1072
            i64.add
            i64.store align=4
            i64.const 1048
            global.get $__memory_base
            i64.add
            global.get $__table_base
            i64.const 0
            i64.add
            i64.store align=4
            i64.const 1056
            global.get $__memory_base
            i64.add
            global.get $__table_base
            i64.const 3
            i64.add
            i64.store align=4
            i64.const 1064
            global.get $__memory_base
            i64.add
            global.get $__table_base
            i64.const 4
            i64.add
            i64.store align=4
            i64.const 1072
            global.get $__memory_base
            i64.add
            global.get $vtable_for___cxxabiv1::__si_class_type_info
            i64.const 16
            i64.add
            i64.store align=4
            i64.const 1080
            global.get $__memory_base
            i64.add
            global.get $__memory_base
            i64.const 946
            i64.add
            i64.store align=4
            i64.const 1088
            global.get $__memory_base
            i64.add
            global.get $__memory_base
            i64.const 992
            i64.add
            i64.store align=4)
        (func $Invoke_IInterface*_ (type $t0) (param $p0 i64) (result i64)
            local.get $p0
            local.get $p0
            i64.load
            i64.load offset=16
            i32.wrap_i64
            call_indirect $env.__indirect_function_table (type $t0))
        (func $Entry (type $t0) (param $p0 i64) (result i64)
            (local $l1 i64)
            global.get $__stack_pointer
            i64.const 16
            i64.sub
            local.tee $l1
            global.set $__stack_pointer
            block $B0
                block $B1
                    block $B2
                        local.get $p0
                        i64.const 1
                        i64.gt_u
                        br_if $B2
                        block $B3
                            local.get $p0
                            i32.wrap_i64
                            br_table $B1 $B3 $B1
                        end
                        local.get $l1
                        global.get $__memory_base
                        i64.const 1032
                        i64.add
                        i64.const 16
                        i64.add
                        i64.store offset=8
                        local.get $l1
                        i64.const 8
                        i64.add
                        call $Invoke_IInterface*_
                        local.set $p0
                        br $B0
                    end
                    unreachable
                    unreachable
                end
                local.get $l1
                global.get $__memory_base
                i64.const 952
                i64.add
                i64.const 16
                i64.add
                i64.store offset=8
                local.get $l1
                i64.const 8
                i64.add
                call $Invoke_IInterface*_
                local.set $p0
            end
            local.get $l1
            i64.const 16
            i64.add
            global.set $__stack_pointer
            local.get $p0)
        (func $TFoo::~TFoo__ (type $t1) (param $p0 i64)
            local.get $p0
            call $operator_delete_void*_)
        (func $TFoo::Call__ (type $t0) (param $p0 i64) (result i64)
            i64.const 42)
        (func $IInterface::~IInterface__ (type $t0) (param $p0 i64) (result i64)
            local.get $p0)
        (func $TBar::~TBar__ (type $t1) (param $p0 i64)
            local.get $p0
            call $operator_delete_void*_)
        (func $TBar::Call__ (type $t0) (param $p0 i64) (result i64)
            i64.const 271828)
        (global $_ZTV4TBar i64 (i64.const 1032))
        (global $_ZTV4TFoo i64 (i64.const 952))
        (global $_ZTI4TFoo i64 (i64.const 1008))
        (global $_ZTS4TFoo i64 (i64.const 927))
        (global $_ZTS10IInterface i64 (i64.const 933))
        (global $_ZTI10IInterface i64 (i64.const 992))
        (global $_ZTI4TBar i64 (i64.const 1072))
        (global $_ZTS4TBar i64 (i64.const 946))
        (global $__dso_handle i64 (i64.const 0))
        (export "__wasm_call_ctors" (func $__wasm_call_ctors))
        (export "__wasm_apply_data_relocs" (func $__wasm_apply_data_relocs))
        (export "_Z6InvokeP10IInterface" (func $Invoke_IInterface*_))
        (export "Entry" (func $Entry))
        (export "_ZTV4TBar" (global $_ZTV4TBar))
        (export "_ZTV4TFoo" (global $_ZTV4TFoo))
        (export "_ZN4TFooD0Ev" (func $TFoo::~TFoo__))
        (export "_ZN4TFoo4CallEv" (func $TFoo::Call__))
        (export "_ZN10IInterfaceD2Ev" (func $IInterface::~IInterface__))
        (export "_ZN4TBarD0Ev" (func $TBar::~TBar__))
        (export "_ZN4TBar4CallEv" (func $TBar::Call__))
        (export "_ZTI4TFoo" (global $_ZTI4TFoo))
        (export "_ZTS4TFoo" (global $_ZTS4TFoo))
        (export "_ZTS10IInterface" (global $_ZTS10IInterface))
        (export "_ZTI10IInterface" (global $_ZTI10IInterface))
        (export "_ZTI4TBar" (global $_ZTI4TBar))
        (export "_ZTS4TBar" (global $_ZTS4TBar))
        (export "__dso_handle" (global $__dso_handle))
        (elem $e0 (global.get $__table_base32) func $IInterface::~IInterface__ $TFoo::~TFoo__ $TFoo::Call__ $TBar::~TBar__ $TBar::Call__)
    ))";

TEST_F(TWebAssemblyTest, VirtualFunctionCall)
{
    static const TStringBuf FakeLibcxx = R"(
        (module
            (type $t0 (func (param i64)))

            (import "env" "memory" (memory $env.memory i64 1))
            (import "env" "__memory_base" (global $__memory_base i64))
            (import "env" "__stack_pointer" (global $__stack_pointer (mut i64)))

            (import "env" "free" (func $free (type $t0)))

            (func $operator_delete_void*_ (type $t0) (param $p0 i64)
                global.get $__stack_pointer
                drop
                local.get $p0
                call $free)

            (global $_ZTVN10__cxxabiv117__class_type_infoE i64 (i64.const 0))
            (global $_ZTVN10__cxxabiv120__si_class_type_infoE i64 (i64.const 0))

            (export "_ZdlPv" (func $operator_delete_void*_))
            (export "_ZTVN10__cxxabiv117__class_type_infoE" (global $_ZTVN10__cxxabiv117__class_type_infoE))
            (export "_ZTVN10__cxxabiv120__si_class_type_infoE" (global $_ZTVN10__cxxabiv120__si_class_type_infoE))

            (data $.data (global.get $__memory_base) "foobar\00")
        ))";

    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(FakeLibcxx);
    compartment->AddModule(ModuleWithVirtualFunctionCall);

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    auto function = TCompartmentFunction<i64(i64)>(compartment.get(), "Entry");
    ASSERT_EQ(function(0), 42);
    ASSERT_EQ(function(1), 271828);
}

////////////////////////////////////////////////////////////////////////////////

/** The module below corresponds to the following C++ code (built with -Wno-call-to-pure-virtual-from-ctor-dtor)
 * struct TBase
 * {
 *     virtual ~TBase() = default;
 *
 *     virtual void Foo() = 0;
 *
 *     TBase()
 *     {
 *         Foo();
 *     }
 * };
 *
 * struct TDerived : public TBase
 * {
 *     void Foo() override
 *     { }
 * };
 *
 * int Work()
 * {
 *     TDerived d;
 *     return 0;
 * }
 */

static const TStringBuf ModuleWithPureVirtualFunctionCall = R"(
    (module
        (type $t0 (func))
        (type $t1 (func (result i64)))
        (type $t2 (func (result i32)))
        (type $t3 (func (param i64)))
        (type $t4 (func (param i64) (result i64)))
        (import "env" "memory" (memory $env.memory i64 1))
        (import "env" "__indirect_function_table" (table $env.__indirect_function_table 2 funcref))
        (import "env" "__stack_pointer" (global $__stack_pointer (mut i64)))
        (import "env" "__memory_base" (global $__memory_base i64))
        (import "env" "__table_base" (global $__table_base i64))
        (import "env" "__table_base32" (global $__table_base32 i32))
        (import "env" "__cxa_pure_virtual" (func $__cxa_pure_virtual (type $t0)))
        (import "GOT.mem" "_ZTVN10__cxxabiv117__class_type_infoE" (global $vtable_for___cxxabiv1::__class_type_info (mut i64)))
        (import "GOT.func" "__cxa_pure_virtual" (global $__cxa_pure_virtual (mut i64)))
        (func $__wasm_call_ctors (type $t0))
        (func $__wasm_apply_data_relocs (type $t0)
            i64.const 920
            global.get $__memory_base
            i64.add
            global.get $vtable_for___cxxabiv1::__class_type_info
            i64.const 16
            i64.add
            i64.store align=4
            i64.const 928
            global.get $__memory_base
            i64.add
            global.get $__memory_base
            i64.const 909
            i64.add
            i64.store align=4
            i64.const 944
            global.get $__memory_base
            i64.add
            global.get $__memory_base
            i64.const 920
            i64.add
            i64.store align=4
            i64.const 952
            global.get $__memory_base
            i64.add
            global.get $__table_base
            i64.const 0
            i64.add
            i64.store align=4
            i64.const 960
            global.get $__memory_base
            i64.add
            global.get $__table_base
            i64.const 1
            i64.add
            i64.store align=4
            i64.const 968
            global.get $__memory_base
            i64.add
            global.get $__cxa_pure_virtual
            i64.store align=4)
        (func $Work__ (type $t2) (result i32)
            (local $l0 i64)
            global.get $__stack_pointer
            i64.const 16
            i64.sub
            local.tee $l0
            global.set $__stack_pointer
            local.get $l0
            global.get $__memory_base
            i64.const 936
            i64.add
            i64.const 16
            i64.add
            i64.store offset=8
            local.get $l0
            i64.const 8
            i64.add
            call $.L__cxa_pure_virtual_bitcast
            local.get $l0
            i64.const 16
            i64.add
            global.set $__stack_pointer
            i32.const 0)
        (func $.L__cxa_pure_virtual_bitcast (type $t3) (param $p0 i64)
            call $__cxa_pure_virtual)
        (func $TBase::~TBase__ (type $t4) (param $p0 i64) (result i64)
            local.get $p0)
        (func $TBase::~TBase__.1 (type $t3) (param $p0 i64)
            unreachable
            unreachable)
        (global $_ZTV5TBase i64 (i64.const 936))
        (global $_ZTS5TBase i64 (i64.const 909))
        (global $_ZTI5TBase i64 (i64.const 920))
        (global $__dso_handle i64 (i64.const 0))
        (export "__wasm_call_ctors" (func $__wasm_call_ctors))
        (export "__wasm_apply_data_relocs" (func $__wasm_apply_data_relocs))
        (export "_Z4Workv" (func $Work__))
        (export "_ZTV5TBase" (global $_ZTV5TBase))
        (export "_ZN5TBaseD2Ev" (func $TBase::~TBase__))
        (export "_ZN5TBaseD0Ev" (func $TBase::~TBase__.1))
        (export "_ZTS5TBase" (global $_ZTS5TBase))
        (export "_ZTI5TBase" (global $_ZTI5TBase))
        (export "__dso_handle" (global $__dso_handle))
        (elem $e0 (global.get $__table_base32) func $TBase::~TBase__ $TBase::~TBase__.1)))";

TEST_F(TWebAssemblyTest, PureVirtualFunctionCall)
{
    static const TStringBuf FakeLibcxx = R"(
        (module
            (type $t0 (func (param i64)))
            (type $t3 (func))

            (import "env" "memory" (memory $env.memory i64 1))
            (import "env" "__memory_base" (global $__memory_base i64))
            (import "env" "__stack_pointer" (global $__stack_pointer (mut i64)))

            (import "env" "free" (func $free (type $t0)))

            (import "GOT.func" "__cxa_pure_virtual" (global $__cxa_pure_virtual (mut i64)))

            (func $operator_delete_void*_ (type $t0) (param $p0 i64)
                global.get $__stack_pointer
                drop
                local.get $p0
                call $free)

            (func $__cxa_pure_virtual (type $t3)
                unreachable)

            (global $_ZTVN10__cxxabiv117__class_type_infoE i64 (i64.const 0))
            (global $_ZTVN10__cxxabiv120__si_class_type_infoE i64 (i64.const 0))

            (export "_ZdlPv" (func $operator_delete_void*_))
            (export "_ZTVN10__cxxabiv117__class_type_infoE" (global $_ZTVN10__cxxabiv117__class_type_infoE))
            (export "_ZTVN10__cxxabiv120__si_class_type_infoE" (global $_ZTVN10__cxxabiv120__si_class_type_infoE))
            (export "__cxa_pure_virtual" (func $__cxa_pure_virtual))

            (data $.data (global.get $__memory_base) "foobar\00")
        ))";

    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(FakeLibcxx);
    compartment->AddModule(ModuleWithPureVirtualFunctionCall);

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    auto function = TCompartmentFunction<i32()>(compartment.get(), "_Z4Workv");
    try {
        function();
        GTEST_FAIL() << "Expected exception to be thrown";
    } catch (WAVM::Runtime::Exception* exception) {
        auto description = WAVM::Runtime::describeCallStack(exception->callStack);
        auto backtrace = std::string();
        int i = 0;
        for (auto& item : description) {
            backtrace += std::to_string(i++) + ". ";
            backtrace += item;
            backtrace += '\n';
        }
        ASSERT_NE(backtrace.find("wasm!!__cxa_pure_virtual"), std::string::npos);
        WAVM::Runtime::destroyException(exception);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TWebAssemblyTest, DLMalloc)
{
    if (!EnableSystemLibraries()) {
        GTEST_SKIP() << "System libraries are not enabled in this build";
    }

    auto compartment = CreateStandardRuntimeImage();

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    for (int i = 0; i < 1'000'000; ++i) {
        auto* offset = std::bit_cast<char*>(compartment->AllocateBytes(40));
        ::memset(PtrFromVM(compartment.get(), offset), 'a', 40);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TWebAssemblyTest, InfiniteRecursion)
{
    GTEST_SKIP() << "Requires YT fiber runtime; stack overflow is not catchable in a plain thread";
}

////////////////////////////////////////////////////////////////////////////////

static const TStringBuf InfiniteLoopModule = R"(
    (module
        (type (;0;) (func (result i64)))

        (func $infinite_loop (type 0) (result i64)
            (local $counter i64)
            (local.set $counter (i64.const 0))
            (loop $loop
                (local.set $counter (i64.add (local.get $counter) (i64.const 1)))
                (br $loop)
            )
            (local.get $counter)
        )

        (export "infinite_loop" (func $infinite_loop))
    ))";

TEST_F(TWebAssemblyTest, InfiniteLoop)
{
    Y_UNUSED(InfiniteLoopModule);

    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(InfiniteLoopModule);

    auto infiniteLoop = TCompartmentFunction<i64()>(compartment.get(), "infinite_loop");

    compartment->SetTimeout(TDuration::Seconds(1));
    compartment->StartDeadlineTimer();

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    try {
        infiniteLoop();
        GTEST_FAIL() << "Expected exception to be thrown";
    } catch (WAVM::Runtime::Exception* exception) {
        WAVM::Runtime::destroyException(exception);
    }
}

////////////////////////////////////////////////////////////////////////////////

static const TStringBuf SpinlockDeadlockModule = R"(
    (module
        (import "env" "memory" (memory i64 1))
        (import "env" "__memory_base" (global $__memory_base i64))

        (type $t0 (func (result i64)))
        (type $t1 (func (param i64)))

        (func $create_lock (type $t0) (result i64)
            (i64.store
                (global.get $__memory_base)
                (i64.const 0))
            (global.get $__memory_base))

        (func $lock (type $t1) (param $lock_addr i64)
            (block $acquired
                (loop $spin
                    (br_if $acquired
                        (i64.eqz (i64.load (local.get $lock_addr))))
                    (br $spin)))
            (i64.store
                (local.get $lock_addr)
                (i64.const 1)))

        (func $deadlock (type $t0) (result i64)
            (local $lock_addr i64)
            (local.set $lock_addr (call $create_lock))
            (call $lock (local.get $lock_addr))
            (call $lock (local.get $lock_addr))
            (i64.const 0))

        (export "create_lock" (func $create_lock))
        (export "lock" (func $lock))
        (export "deadlock" (func $deadlock))
    ))";

TEST_F(TWebAssemblyTest, SpinlockDeadlock)
{
    auto compartment = CreateMinimalRuntimeImage();
    compartment->AddModule(SpinlockDeadlockModule);

    auto deadlock = TCompartmentFunction<i64()>(compartment.get(), "deadlock");

    compartment->SetTimeout(TDuration::Seconds(1));
    compartment->StartDeadlineTimer();

    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(nullptr);
    };

    try {
        deadlock();
        GTEST_FAIL() << "Expected exception to be thrown";
    } catch (WAVM::Runtime::Exception* exception) {
        WAVM::Runtime::destroyException(exception);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYdb::NWasm
