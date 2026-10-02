#pragma once

#include "abstract.h"
#include "executor.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/conclusion/status.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp::Variator {

using Script = std::vector<TString>;
using Varinat = TString;

std::vector<std::tuple<Script, Varinat>> ScriptVariants(const TString& script);

Script SingleScript(const TString& script);

TScriptExecutor ToExecutor(const Script& script);

TString ToString(const Script& script);

#define Y_UNIT_TEST_STRING_VARIATOR(NAME, INPUT_STRING)                                    \
    struct TTestCase##NAME : public TCurrentTestCase {                                     \
        TString Script;                                                                    \
        TString Name;                                                                      \
                                                                                           \
        TTestCase##NAME(TString script, TString name)                                      \
            : Script(std::move(script))                                                    \
            , Name(TString(#NAME) + "[" + name + "]")                                      \
        {                                                                                  \
            Name_ = Name.c_str();                                                          \
            ForceFork_ = false;                                                             \
        }                                                                                  \
                                                                                           \
        static THolder<NUnitTest::TBaseTestCase> Create(TString script, TString name) {    \
            return ::MakeHolder<TTestCase##NAME>(std::move(script), std::move(name));      \
        }                                                                                  \
                                                                                           \
        void Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED) override {     \
            ExecuteImpl(Script);                                                           \
        }                                                                                  \
                                                                                           \
        void ExecuteImpl(const TString& script);                                           \
    };                                                                                     \
                                                                                           \
    struct TTestRegistration##NAME {                                                       \
        TTestRegistration##NAME() {                                                        \
            for (const auto& [script, variant] : Variator::ScriptVariants(INPUT_STRING)) { \
                TCurrentTest::AddTest([script, variant] {                                  \
                    return TTestCase##NAME::Create(Variator::ToString(script), variant);   \
                });                                                                        \
            }                                                                              \
        }                                                                                  \
    };                                                                                     \
                                                                                           \
    static const TTestRegistration##NAME testRegistration##NAME;                           \
                                                                                           \
    void TTestCase##NAME::ExecuteImpl(const TString& __SCRIPT_CONTENT)
} // namespace NKikimr::NKqp::Variator

