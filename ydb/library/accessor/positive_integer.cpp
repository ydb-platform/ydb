#include "positive_integer.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr {

TPositiveControlInteger::TPositiveControlInteger(const i64 value)
	: Value(value) {
	AFL_VERIFY(0 <= value);
}

TPositiveControlInteger::TPositiveControlInteger(const ui64 value): Value(value) {
	AFL_VERIFY(Value.Val() >= 0)("value", Value.Val())("init", value);
}

ui64 TPositiveControlInteger::Add(const ui64 value) {
	const i64 result = Value.Add(value);
	AFL_VERIFY(result >= 0)("base", Value.Val())("delta", value)("result", result);
	return result;
}

ui64 TPositiveControlInteger::Sub(const ui64 value) {
	i64 valDelta = Value.Sub(value);
	AFL_VERIFY(valDelta >= 0)("base", Value.Val())("delta", value)("sub", valDelta);
	return valDelta;
}

ui64 TPositiveControlInteger::GetDec() const {
	const i64 result = Value.Val() - 1;
	AFL_VERIFY(result >= 0);
	return result;
}

ui64 TPositiveControlInteger::Val() const {
	return Value.Val();
}

TPositiveIncreasingControlInteger& TPositiveIncreasingControlInteger::operator=(const TPositiveIncreasingControlInteger& v) {
	if (&v != this) {
		AFL_VERIFY(Value < v.Value)("from", Value)("to", v.Value);
		Value = v.Value;
	}
	return *this;
}

} //namespace NKikimr

template<>
void Out<NKikimr::TPositiveControlInteger>(IOutputStream& o,
	typename TTypeTraits<NKikimr::TPositiveControlInteger>::TFuncParam x) {
	o << x.Val();
}


template<>
void Out<NKikimr::TPositiveIncreasingControlInteger>(IOutputStream& o,
	typename TTypeTraits<NKikimr::TPositiveIncreasingControlInteger>::TFuncParam x) {
	o << x.Val();
}
