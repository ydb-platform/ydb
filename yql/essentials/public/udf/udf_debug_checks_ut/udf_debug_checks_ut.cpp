#include <yql/essentials/public/udf/udf_value.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <cstdlib>

using namespace NYql::NUdf;

namespace {

// Boxed value that violates the invariant: returns non-Ok/false but writes
// a non-trivial (boxed) value to result/key/payload. Used to trigger
// Y_DEBUG_ABORT_UNLESS in TBoxedValueAccessor::Fetch, Next, NextPair.
class TBoxedValue: public TBoxedValueBase {
public:
    explicit TBoxedValue(TUnboxedValue result)
        : Result_(result)
    {
    }
    // Fetch: return Finish but assign non-trivial boxed value -> abort
    EFetchStatus Fetch(TUnboxedValue& result) override {
        result = Result_;
        return EFetchStatus::Finish;
    }

    // Next: return false but assign non-trivial boxed value -> abort
    bool Next(TUnboxedValue& value) override {
        value = Result_;
        return false;
    }

    // NextPair: return false but assign non-trivial boxed values -> abort
    bool NextPair(TUnboxedValue& key, TUnboxedValue& payload) override {
        key = Result_;
        payload = Result_;
        return false;
    }

    static IBoxedValuePtr CreateEvil() {
        IBoxedValuePtr inner(new TBoxedValue(TUnboxedValue()));
        TUnboxedValue boxedResult(TUnboxedValuePod(std::move(inner)));
        return new TBoxedValue(boxedResult);
    }

private:
    TUnboxedValue Result_;
};

TEST(TBoxedValueAccessorDebugChecks, FetchNonOkButReplacesResultWithNonTrivial_Aborts) {
    IBoxedValuePtr value(TBoxedValue::CreateEvil());
    EXPECT_DEBUG_DEATH(
        ({
            TUnboxedValue result;
            TBoxedValueAccessor::Fetch(*value, result);
        }),
        "");
}

TEST(TBoxedValueAccessorDebugChecks, NextReturnsFalseButReplacesResultWithNonTrivial_Aborts) {
    IBoxedValuePtr value(TBoxedValue::CreateEvil());
    EXPECT_DEBUG_DEATH(
        ({
            TUnboxedValue result;
            TBoxedValueAccessor::Next(*value, result);
        }),
        "");
}

TEST(TBoxedValueAccessorDebugChecks, NextPairReturnsFalseButReplacesKeyPayloadWithNonTrivial_Aborts) {
    IBoxedValuePtr value(TBoxedValue::CreateEvil());
    EXPECT_DEBUG_DEATH(
        ({
            TUnboxedValue key;
            TUnboxedValue payload;
            TBoxedValueAccessor::NextPair(*value, key, payload);
        }),
        "");
}

TEST(TBoxedValueAccessorDebugChecks, NextPairReturnsFalseButReplacesWithInvalid_Success) {
    IBoxedValuePtr value(new TBoxedValue(TUnboxedValue::Invalid()));
    TUnboxedValue key;
    TUnboxedValue payload;
    TBoxedValueAccessor::NextPair(*value, key, payload);
}

} // namespace
