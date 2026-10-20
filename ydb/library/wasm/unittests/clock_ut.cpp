#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/function.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <util/generic/scope.h>

#include <chrono>
#include <cstring>
#include <limits>

namespace NYdb::NWasm {
namespace {

ui64 WallNow()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

ui64 SteadyNow()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

const TString ClockModule = R"((module
    (import "wasi_snapshot_preview1" "clock_time_get" (func $clock (param i32 i64 i64) (result i32)))
    (import "env" "memory" (memory i64 1))
    (func (export "clock") (param i32 i64 i64) (result i32)
        (call $clock (local.get 0) (local.get 1) (local.get 2)))
    (func (export "read") (param i64) (result i64) (i64.load (local.get 0)))
))";

TEST(TWasmClock, RealtimeAndMonotonic)
{
    auto compartment = CreateEmptyImage();
    compartment->AddModule(ClockModule);
    auto* previous = GetCurrentCompartment();
    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(previous);
    };
    TCompartmentFunction<i32(i32, ui64, ui64)> clock(compartment.get(), "clock");
    TCompartmentFunction<ui64(ui64)> read(compartment.get(), "read");
    const auto wallBefore = WallNow();
    ASSERT_EQ(clock(0, 1, 1024), 0);
    const auto wall = read(1024);
    EXPECT_GE(wall, wallBefore);
    EXPECT_LE(wall, WallNow());
    EXPECT_GT(wall, std::numeric_limits<ui32>::max());
    const auto steadyBefore = SteadyNow();
    ASSERT_EQ(clock(1, 0, 1024), 0);
    const auto steady = read(1024);
    EXPECT_GE(steady, steadyBefore);
    EXPECT_LE(steady, SteadyNow());
    ASSERT_EQ(clock(1, std::numeric_limits<ui64>::max(), 1024), 0);
    EXPECT_GE(read(1024), steady);
}

TEST(TWasmClock, ErrorsDoNotWriteMemory)
{
    auto compartment = CreateEmptyImage();
    compartment->AddModule(ClockModule);
    auto* previous = GetCurrentCompartment();
    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(previous);
    };
    TCompartmentFunction<i32(i32, ui64, ui64)> clock(compartment.get(), "clock");
    auto* bytes = static_cast<unsigned char*>(compartment->GetHostPointer(1024, 8));
    std::memset(bytes, 0xa5, 8);
    EXPECT_EQ(clock(2, 1, 1024), 58);
    EXPECT_EQ(clock(3, 1, 1024), 58);
    EXPECT_EQ(clock(4, 1, 1024), 28);
    EXPECT_EQ(clock(-1, 1, 1024), 28);
    for (unsigned i = 0; i < 8; ++i) {
        EXPECT_EQ(bytes[i], 0xa5);
    }
    const auto size = compartment->GetLinearMemorySize();
    EXPECT_EQ(clock(0, 1, size - 7), 21);
    EXPECT_EQ(clock(0, 1, size), 21);
    EXPECT_EQ(clock(0, 1, std::numeric_limits<ui64>::max()), 21);
}

TEST(TWasmClock, UnalignedAndBoundaryPointers)
{
    auto compartment = CreateEmptyImage();
    compartment->AddModule(ClockModule);
    auto* previous = GetCurrentCompartment();
    SetCurrentCompartment(compartment.get());
    Y_DEFER {
        SetCurrentCompartment(previous);
    };
    TCompartmentFunction<i32(i32, ui64, ui64)> clock(compartment.get(), "clock");
    TCompartmentFunction<ui64(ui64)> read(compartment.get(), "read");
    auto* bytes = static_cast<unsigned char*>(compartment->GetHostPointer(1024, 10));
    std::memset(bytes, 0xa5, 10);
    const auto before = WallNow();
    ASSERT_EQ(clock(0, 1, 1025), 0);
    EXPECT_GE(read(1025), before);
    EXPECT_LE(read(1025), WallNow());
    EXPECT_EQ(bytes[0], 0xa5);
    EXPECT_EQ(bytes[9], 0xa5);
    EXPECT_EQ(clock(0, 1, compartment->GetLinearMemorySize() - 8), 0);
    EXPECT_EQ(clock(0, 1, 0), 0);
}

void CheckStandardLibrary(IWebAssemblyCompartment* compartment)
{
    const auto binary = NResource::Find("/clock.wasm");
    ASSERT_FALSE(binary.empty());
    const auto wallBefore = WallNow();
    const auto steadyBefore = SteadyNow();
    // Loading runs global C++ constructors before any async invocation exists.
    compartment->AddModule(NYT::TRef(binary.data(), binary.size()), "clock_fixture");
    const auto wallAfter = WallNow();
    const auto steadyAfter = SteadyNow();
    auto* previous = GetCurrentCompartment();
    SetCurrentCompartment(compartment);
    Y_DEFER {
        SetCurrentCompartment(previous);
    };
    const auto call = [compartment](const char* name) {
        return TCompartmentFunction<ui64()>(compartment, name)();
    };
    EXPECT_GE(call("ConstructorWall"), wallBefore);
    EXPECT_LE(call("ConstructorWall"), wallAfter);
    EXPECT_GE(call("ConstructorSteady"), steadyBefore);
    EXPECT_LE(call("ConstructorSteady"), steadyAfter);
    EXPECT_GE(call("ConstructorTime"), wallBefore / 1000000000);
    EXPECT_LE(call("ConstructorTime"), wallAfter / 1000000000);
    EXPECT_GE(call("WallNow"), wallAfter);
    EXPECT_GE(call("SteadyNow"), steadyAfter);
    EXPECT_GE(call("TimeNow"), wallAfter / 1000000000);
}

TEST(TWasmClock, StandardLibraryAndConstructors)
{
    auto compartment = CreateEmptyImage();
    CheckStandardLibrary(compartment.get());
}

TEST(TWasmClock, StandardLibraryInIsolatedAuthRealm)
{
    auto application = CreateEmptyImage();
    auto auth = application->CreateIsolatedRealm(64 * 1024 * 1024);
    CheckStandardLibrary(auth.get());
}

}   // namespace
}   // namespace NYdb::NWasm
