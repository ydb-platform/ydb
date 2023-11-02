#include <library/cpp/testing/gtest/gtest.h>
#include <util/system/compiler.h>
#include <util/system/backtrace.h>
#include <util/stream/str.h>

Y_FORCE_INLINE
TString InlinedFunction_866a4407b28483588033f95add111d() {
    TStringStream out;
    FormatBackTrace(&out);
    return out.Str();
}

Y_NO_INLINE
TString Function_3e15a2d04c8613ae64833c5407dd98() {
    return InlinedFunction_866a4407b28483588033f95add111d();
}

Y_NO_INLINE
TString NotInlinedFunction_3c7dbf1e3b2b71819241cb5ad2b142() {
    return Function_3e15a2d04c8613ae64833c5407dd98();
}

namespace NTestNamespace {
    Y_NO_INLINE
    TString NamespacedFunction() {
        return InlinedFunction_866a4407b28483588033f95add111d();
    }
}

using namespace ::testing;
TEST(dwarf_backtrace_should, handle_inlines) {
    const TString backtrace = NotInlinedFunction_3c7dbf1e3b2b71819241cb5ad2b142();

    EXPECT_THAT(
        backtrace,
        HasSubstr("InlinedFunction_866a4407b28483588033f95add111d")
    );

    EXPECT_THAT(
        backtrace,
        HasSubstr("backtrace_ut.cpp:9:0 in InlinedFunction")
    );

    EXPECT_THAT(
        backtrace,
        HasSubstr("Function_3e15a2d04c8613ae64833c5407dd98")
    );
}

TEST(dwarf_backtrace_should, handle_namespaces) {
    EXPECT_THAT(
        NTestNamespace::NamespacedFunction(),
        HasSubstr("NTestNamespace::NamespacedFunction")
    );
}
