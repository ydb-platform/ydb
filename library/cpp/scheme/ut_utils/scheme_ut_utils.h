#pragma once

#include <library/cpp/json/json_prettifier.h>
#include <library/cpp/scheme/scheme.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>

namespace NSc {
    namespace NUt {
        TValue AssertFromJson(TStringBuf json);

        inline TString NormalizeJson(const NSc::TValue& sc) {
            return sc.ToJson(true);
        }

        inline TString NormalizeJson(const NJson::TJsonValue& sc) {
            return NJson::WriteJson(sc, false, true, false);
        }

        template <class TStr>
        inline TString NormalizeJson(const TStr& val) {
            return AssertFromJson(val).ToJson(true);
        }

#define UNIT_ASSERT_JSON_EQ_JSON_C(A, B, c)                                                                                           \
    do {                                                                                                                              \
        const TString _a = NSc::NUt::NormalizeJson(A);                                                                                \
        const TString _b = NSc::NUt::NormalizeJson(B);                                                                                \
        if (_a != _b) {                                                                                                               \
            UNIT_FAIL_IMPL(                                                                                                           \
                "json values are different (" #A " != " #B ")",                                                                       \
                Sprintf("%s\n!=\n%s\n%s\n%s", _a.data(), _b.data(),                                                                               \
                        ::NUnitTest::ColoredDiff(NJson::PrettifyJson(_a), NJson::PrettifyJson(_b), " \t\n,:\"{}[]").data(), ToString(c).data())); \
        }                                                                                                                             \
    } while (false)

#define UNIT_ASSERT_JSON_EQ_JSON(A, B) UNIT_ASSERT_JSON_EQ_JSON_C(A, B, "")

        inline TString DumpJson(const TValue& json) {
            return NJson::CompactifyJson(json.ToJson(true), true, true);
        }

        // deprecated
        inline TString DumpJsonVS(const TValue& expected, const TValue& fact) {
            return DumpJson(expected) + "(expected) != (fact)" + DumpJson(fact);
        }

        void AssertScheme(const TValue& expected, const TValue& real);
        void AssertSchemeJson(TStringBuf expected, const TValue& real);
        void AssertJsonJson(TStringBuf expected, TStringBuf real);

    }
}
