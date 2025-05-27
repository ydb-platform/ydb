#pragma once

#include "yql_panic.h"

#include <util/generic/maybe.h>
#include <util/generic/variant.h>

namespace NYql {

template <typename S, typename R>
class TResetableSettingBase {
protected:
    using TSet = S;
    using TReset = R;

public:
    void Set(const TSet& value) {
        Value.ConstructInPlace(value);
    }

    void Reset(const TReset& value) {
        Value.ConstructInPlace(value);
    }

    bool Defined() const {
        return Value.Defined();
    }

    explicit operator bool() const {
        return Defined();
    }

    bool IsSet() const {
        YQL_ENSURE(Defined());
        return Value->index() == 0;
    }

    const TSet& GetValueSet() const {
        YQL_ENSURE(IsSet());
        return std::get<TSet>(*Value);
    }

    const TReset& GetValueReset() const {
        YQL_ENSURE(!IsSet());
        return std::get<TReset>(*Value);
    }

private:
    TMaybe<std::variant<TSet, TReset>> Value;
};

template <typename S, typename R>
class TResetableSetting: public TResetableSettingBase<S, R> {
};

template <typename S>
class TResetableSetting<S, void>: public TResetableSettingBase<S, TNothing> {
private:
    const TNothing& GetValueReset() const;

public:
    void Reset() {
        TResetableSettingBase<S, TNothing>::Reset(Nothing());
    }
};

}
