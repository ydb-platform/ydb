#pragma once
#include <ydb/library/actors/core/log.h>
#include <util/system/types.h>
#include <util/stream/output.h>
#include <util/generic/typetraits.h>

namespace NKikimr {

class TPositiveControlInteger {
private:
	TAtomicCounter Value = 0;
public:
	TPositiveControlInteger() = default;
	TPositiveControlInteger(const ui64 value);
	TPositiveControlInteger(const ui32 value)
		: Value(value) {

	}
	TPositiveControlInteger(const i64 value);
	ui64 Add(const ui64 value);
	ui64 Sub(const ui64 value);
	ui64 Inc() {
		return Add(1);
	}
	ui64 Dec() {
		return Sub(1);
	}
	ui64 GetDec() const;
	ui64 Val() const;
	bool operator!() const {
		return !Value.Val();
	}
	operator ui64() const {
		return Value.Val();
	}
	ui64 operator++() {
		return Inc();
	}
	ui64 operator--() {
		return Dec();
	}
};

class TPositiveIncreasingControlInteger {
private:
	ui64 Value = 0;
public:
	constexpr TPositiveIncreasingControlInteger() = default;
	explicit constexpr TPositiveIncreasingControlInteger(const ui64 value)
		: Value(value) {
	}
	TPositiveIncreasingControlInteger(const TPositiveIncreasingControlInteger&) = default;
	TPositiveIncreasingControlInteger& operator=(const TPositiveIncreasingControlInteger& v);

	TPositiveIncreasingControlInteger operator+(ui64 v) const {
		return TPositiveIncreasingControlInteger(Value + v);
	}
	TPositiveIncreasingControlInteger operator-(const ui64 v) const {
		AFL_VERIFY(v <= Value);
		return TPositiveIncreasingControlInteger(Value - v);
	}
	ui64 Val() const {
		return Value;
	}
};
}
