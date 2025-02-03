#pragma once
#include <util/string/cast.h>

namespace NKikimr {
namespace NMiniKQL {

struct TValidateErrorPolicyNone {
};

struct TUdfValidateException: public yexception {
};

struct TValidateErrorPolicyThrow {
    static void Generate(const TString& message) {
        GenerateExc(TUdfValidateException() << message);
    }

    template<class TException>
    static void GenerateExc(const TException& exc) {
        static_assert(std::is_base_of<yexception, TException>::value, "Must be derived from yexception");
        ythrow TException() << exc.AsStrBuf();
    }
};

struct TValidateErrorPolicyFail {
    static void Generate(const TString& message) {
        Y_ABORT("value verify failed: %s", message.c_str());
    }

    template<class TException>
    static void GenerateExc(const TException& exc) {
        Generate(ToString(exc.AsStrBuf()));
    }
};

template<class TValidateMode>
struct TValidate<TValidateErrorPolicyNone, TValidateMode> {

static NUdf::TUnboxedValue Value(const NUdf::IValueBuilder* valueBuilder, const TType* type, NUdf::TUnboxedValue&& value, const TString& message, bool* wrapped) {
    Y_UNUSED(valueBuilder);
    Y_UNUSED(type);
    Y_UNUSED(message);
    Y_UNUSED(wrapped);
    return std::move(value);
}

static void WrapCallable(const TCallableType*, NUdf::TUnboxedValue&, const TString&) {}

};

} // namespace MiniKQL
} // namespace NKikimr
