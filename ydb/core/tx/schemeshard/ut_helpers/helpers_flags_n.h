#pragma once

#include <string_view>

#include <util/system/compiler.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NSchemeShardUT_Private {

template <auto FuncPtr>
std::string_view GetFunctionName() {
    std::string_view fullSignature = Y_FUNC_SIGNATURE;

    // Find the pattern "FuncPtr = &" or "FuncPtr = "
    constexpr std::string_view pattern1 = "FuncPtr = &";
    constexpr std::string_view pattern2 = "FuncPtr = ";

    size_t start = fullSignature.find(pattern1);
    if (start != std::string_view::npos) {
        start += pattern1.size();
    } else {
        start = fullSignature.find(pattern2);
        if (start != std::string_view::npos) {
            start += pattern2.size();
        } else {
            return fullSignature;  // Fallback: return full signature
        }
    }

    // Find the end marker (closing bracket or semicolon)
    size_t end = fullSignature.find_first_of("])", start);
    if (end == std::string_view::npos) {
        end = fullSignature.size();
    }

    std::string_view funcName = fullSignature.substr(start, end - start);

    // Remove namespace prefix if present (everything before the last "::")
    size_t lastColon = funcName.rfind("::");
    if (lastColon != std::string_view::npos) {
        funcName = funcName.substr(lastColon + 2);
    }

    return funcName;
}

// Generalized version for arbitrary number of boolean flags
template <auto FuncPtr, size_t N, typename CurrentTest>
struct TTestRegistrationFlagsN {
    TTestRegistrationFlagsN() {
        // Test names must be in a static array and created at load time
        // because the unittest framework uses raw const char* pointers
        // and expects statically allocated strings.
        static std::vector<TString> TestNames;

        auto bit = [](size_t set, size_t n) {
            return (set & (1ULL << n)) != 0;
        };

        auto basename = GetFunctionName<FuncPtr>();

        const size_t totalCombinations = 1ULL << N;

        for (size_t i = 0; i < totalCombinations; ++i) {
            TStringBuilder nameBuilder;
            nameBuilder << basename;
            for (size_t j = 0; j < N; ++j) {
                nameBuilder << "-" << bit(i, j);
            }
            TestNames.emplace_back(nameBuilder);

            // Pre-compute boolean flags at test registration time
            auto testFunc = [i, bit]<size_t... Is>(std::index_sequence<Is...>) {
                // Capture pre-computed boolean values
                return [... flags = bit(i, Is)](NUnitTest::TTestContext& ctx) {
                    FuncPtr(ctx, flags...);
                };
            }(std::make_index_sequence<N>{});

            CurrentTest::AddTest(TestNames.back().c_str(),
                testFunc,
                /*forceFork*/ false
            );
        }
    }
};

}  // namespace NSchemeShardUT_Private


// Generalized macro for arbitrary number of boolean flags
// Usage example:
//   Y_UNIT_TEST_FLAGS_N(MyTest, bool flag1, bool flag2, bool flag3) {
//       // Test implementation using flag1, flag2, flag3
//   }
// This will generate 2^N test cases with all combinations of boolean flags.
// Test names will be: MyTest-0-0-0, MyTest-0-0-1, MyTest-0-1-0, etc.
#define Y_UNIT_TEST_FLAGS_N(N, ...)                                                                                    \
    void N(::NUnitTest::TTestContext&, __VA_ARGS__);                                                                   \
    static const ::NSchemeShardUT_Private::TTestRegistrationFlagsN<                                                    \
        &N,                                                                                                            \
        []<typename... Args>(void(*)(::NUnitTest::TTestContext&, Args...)) {                                           \
            return sizeof...(Args);                                                                                    \
        }(&N),                                                                                                         \
        TCurrentTest                                                                                                   \
    > testRegistration##N;                                                                                             \
    void N(::NUnitTest::TTestContext&, __VA_ARGS__)
