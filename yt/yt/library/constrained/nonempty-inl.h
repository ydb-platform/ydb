#ifndef NONEMPTY_INL_H_
#error "Direct inclusion of this file is not allowed, include nonempty.h"
// For the sake of sane code completion.
#include "nonempty.h"
#endif

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TContainer>
TNonEmpty<TContainer>::TNonEmpty(TContainer underlying)
    : Underlying_(std::move(underlying))
{
    YT_VERIFY(!Underlying_.empty());
}

template <class TContainer>
template <class TIterator>
TNonEmpty<TContainer>::TNonEmpty(TIterator first, TIterator last)
    : TNonEmpty(TContainer(first, last))
{ }

template <class TContainer>
TNonEmpty<TContainer>::TNonEmpty(std::initializer_list<typename TContainer::value_type> values)
    : TNonEmpty(TContainer(values))
{ }

template <class TContainer>
typename TContainer::size_type TNonEmpty<TContainer>::size() const
{
    return Underlying_.size();
}

template <class TContainer>
typename TContainer::const_iterator TNonEmpty<TContainer>::begin() const
{
    return Underlying_.begin();
}

template <class TContainer>
typename TContainer::const_iterator TNonEmpty<TContainer>::end() const
{
    return Underlying_.end();
}

template <class TContainer>
const TContainer& TNonEmpty<TContainer>::Get() const
{
    return Underlying_;
}

template <class TContainer>
TNonEmpty<TContainer>::operator const TContainer&() const
{
    return Underlying_;
}

template <class TContainer>
void FormatValue(TStringBuilderBase* builder, const TNonEmpty<TContainer>& nonempty, TStringBuf spec)
{
    FormatValue(builder, nonempty.Get(), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
