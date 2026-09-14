#include <yql/essentials/public/udf/udf_helpers.h>

using namespace NYql::NUdf;

namespace {

bool IsLetter(unsigned char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}

bool IsDigit(unsigned char c) {
    return c >= '0' && c <= '9';
}

bool IsUpper(unsigned char c) {
    return c >= 'A' && c <= 'Z';
}

template <typename TPred>
i64 CountIf(TStringRef ref, TPred pred) {
    const char* data = ref.Data();
    const size_t size = ref.Size();
    i64 n = 0;
    for (size_t i = 0; i < size; ++i) {
        if (pred(static_cast<unsigned char>(data[i]))) {
            ++n;
        }
    }
    return n;
}

SIMPLE_STRICT_UDF(Tcount_letters, i64(TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(CountIf(args[0].AsStringRef(), IsLetter));
}

SIMPLE_STRICT_UDF(Tcount_digits, i64(TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(CountIf(args[0].AsStringRef(), IsDigit));
}

SIMPLE_STRICT_UDF(Tcount_upper, i64(TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(CountIf(args[0].AsStringRef(), IsUpper));
}

SIMPLE_STRICT_UDF(Ttext_length, i64(TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(static_cast<i64>(args[0].AsStringRef().Size()));
}

SIMPLE_STRICT_UDF(Tbyte_at, i64(TAutoMap<char*>, TAutoMap<i64>)) {
    Y_UNUSED(valueBuilder);
    const auto ref = args[0].AsStringRef();
    const i64 pos = args[1].Get<i64>();
    if (pos < 0 || static_cast<size_t>(pos) >= ref.Size()) {
        return TUnboxedValuePod(static_cast<i64>(0));
    }
    return TUnboxedValuePod(static_cast<i64>(static_cast<unsigned char>(ref.Data()[pos])));
}

SIMPLE_MODULE(TTextNativeModule,
              Tcount_letters,
              Tcount_digits,
              Tcount_upper,
              Ttext_length,
              Tbyte_at)

} // namespace

REGISTER_MODULES(TTextNativeModule)
