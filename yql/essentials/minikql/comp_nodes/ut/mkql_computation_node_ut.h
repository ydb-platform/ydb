#pragma once

#include <yql/essentials/minikql/comp_nodes/test_support/mkql_test_registrar.h>
#include <yql/essentials/minikql/comp_nodes/test_support/mkql_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/string.h>
#include <util/string/printf.h>

#define UNBOXED_VALUE_STR_EQUAL(unboxed, expected)                                              \
    do {                                                                                        \
        const auto& v = (unboxed);                                                              \
        if (!(v.AsStringRef() == (expected))) {                                                 \
            UNIT_FAIL_IMPL(                                                                     \
                "equal assertion failed",                                                       \
                Sprintf("%s %s == %s", #unboxed, TString(v.AsStringRef()).c_str(), #expected)); \
        }                                                                                       \
    } while (0)

namespace NKikimr::NMiniKQL {

extern const std::vector<std::pair<i8, double>> I8Samples;
extern const std::vector<std::pair<ui16, double>> Ui16Samples;
extern const std::vector<std::tuple<ui64, std::string, std::string, double, double, double, double>> TpchSamples;

} // namespace NKikimr::NMiniKQL
