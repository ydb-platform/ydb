#pragma once

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/system/compiler.h>

namespace NYT {

[[noreturn]] void ThrowUnimplementedClientMethodError(TStringBuf methodName, TStringBuf reason);

#define Y_COMMA() ,

#define Y_FUNCTION_ARG_UNUSED(x) x
#define Y_FUNCTION_ARG(i, x) x Y_CAT(a, i)
#define Y_PASS_ARG(i, x) Y_CAT(a, i)

#define Y_FUNCTION_ARG_UNUSED_COMMA(x) x Y_COMMA()
#define Y_FUNCTION_ARG_COMMA(i, x) x Y_CAT(a, i) Y_COMMA()
#define Y_PASS_ARG_COMMA(i, x) Y_CAT(a, i) Y_COMMA()

#define Y_VA_ARGS(...) __VA_ARGS__

// Map input arguments types.
// Example: Y_METHOD_UNUSED_ARGS_DECLARATION((int, TString)) ---> int, TString.
#define Y_METHOD_UNUSED_ARGS_DECLARATION(Args) Y_MAP_ARGS_WITH_LAST(Y_FUNCTION_ARG_UNUSED_COMMA, Y_FUNCTION_ARG_UNUSED, Y_PASS_VA_ARGS(Y_VA_ARGS Args))

// Pass capture list as is.
// Example: Y_PASS_CAPTURE_LIST((&a1, a2)) ---> &a1, a2.
#define Y_PASS_CAPTURE_LIST(Args) Y_METHOD_UNUSED_ARGS_DECLARATION(Args)

// Map input arguments types with parameter names introduction.
// Example: Y_METHOD_USED_ARGS_DECLARATION((int, TString)) ---> int a2, TString a1.
#define Y_METHOD_USED_ARGS_DECLARATION(Args) Y_MAP_ARGS_WITH_LAST_N(Y_FUNCTION_ARG_COMMA, Y_FUNCTION_ARG, Y_PASS_VA_ARGS(Y_VA_ARGS Args))

// Map input arguments types into corresponding parameter names.
// Example: Y_PASS_METHOD_USED_ARGS((int, TString)) ---> a2, a1.
#define Y_PASS_METHOD_USED_ARGS(Args) Y_MAP_ARGS_WITH_LAST_N(Y_PASS_ARG_COMMA, Y_PASS_ARG, Y_PASS_VA_ARGS(Y_VA_ARGS Args))

#define UNIMPLEMENTED_METHOD(ReturnType, MethodName, Args) NO_METHOD_IMPL(ReturnType, MethodName, "Not implemented", Args)
#define UNIMPLEMENTED_CONST_METHOD(ReturnType, MethodName, Args) NO_CONST_METHOD_IMPL(ReturnType, MethodName, "Not implemented", Args)

#define UNSUPPORTED_METHOD(ReturnType, MethodName, Args) NO_METHOD_IMPL(ReturnType, MethodName, "Not supported", Args)
#define UNSUPPORTED_CONST_METHOD(ReturnType, MethodName, Args) NO_CONST_METHOD_IMPL(ReturnType, MethodName, "Not supported", Args)

#define NO_METHOD_IMPL(ReturnType, MethodName, Reason, Args)        \
    ReturnType MethodName(Y_METHOD_UNUSED_ARGS_DECLARATION(Args)) override   \
    {                                                                        \
        ThrowUnimplementedClientMethodError(Y_STRINGIZE(MethodName), Y_STRINGIZE(Reason));            \
        Y_UNREACHABLE();                                                     \
    } Y_SEMICOLON_GUARD

#define NO_CONST_METHOD_IMPL(ReturnType, MethodName, Reason, Args)             \
    ReturnType MethodName(Y_METHOD_UNUSED_ARGS_DECLARATION(Args)) const override        \
    {                                                                                   \
        ThrowUnimplementedClientMethodError(Y_STRINGIZE(MethodName), Y_STRINGIZE(Reason));                       \
        Y_UNREACHABLE();                                                                \
    } Y_SEMICOLON_GUARD

} // namespace NYT
