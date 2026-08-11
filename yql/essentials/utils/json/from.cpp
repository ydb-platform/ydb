#include "from.h"

namespace NYql::NJson {

TExpected<TJsonValue> TFromJson<TJsonValue>::operator()(TJsonValue json) const {
    return json;
}

TExpected<TString> TFromJson<TString>::operator()(TJsonValue json) const {
    if (!json.IsString()) {
        return Unexpected("must be a string");
    }

    return std::move(json.GetStringSafe());
}

TExpected<i64> TFromJson<i64>::operator()(TJsonValue json) const {
    if (!json.IsInteger()) {
        return Unexpected("must be an integer");
    }

    return static_cast<i64>(json.GetIntegerSafe());
}

TExpected<ui64> TFromJson<ui64>::operator()(TJsonValue json) const {
    if (!json.IsUInteger()) {
        return Unexpected("must be an unsigned integer");
    }

    return static_cast<ui64>(json.GetUIntegerSafe());
}

TExpected<bool> TFromJson<bool>::operator()(TJsonValue json) const {
    if (!json.IsBoolean()) {
        return Unexpected("must be a boolean");
    }

    return json.GetBooleanSafe();
}

} // namespace NYql::NJson
