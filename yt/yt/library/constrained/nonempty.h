#pragma once

#include <library/cpp/yt/string/string_builder.h>

#include <util/system/compiler.h>

#include <initializer_list>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TContainer>
class TNonEmpty {
public:
    using TUnderlying = TContainer;

    explicit TNonEmpty(TContainer underlying);
    template <class TIterator>
    TNonEmpty(TIterator first, TIterator last);
    TNonEmpty(std::initializer_list<typename TContainer::value_type> values);

    typename TContainer::size_type size() const;

    typename TContainer::const_iterator begin() const;
    typename TContainer::const_iterator end() const;

    const TContainer& Get() const Y_LIFETIME_BOUND;
    operator const TContainer&() const Y_LIFETIME_BOUND;

private:
    TContainer Underlying_;
};

template <class TContainer>
void FormatValue(TStringBuilderBase* builder, const TNonEmpty<TContainer>& nonempty, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define NONEMPTY_INL_H_
#include "nonempty-inl.h"
#undef NONEMPTY_INL_H_
