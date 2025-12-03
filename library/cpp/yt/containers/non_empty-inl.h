#ifndef NON_EMPTY_INL_H_
#error "Direct inclusion of this file is not allowed, include non_empty.h"
// For the sake of sane code completion.
#include "non_empty.h"
#endif

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TContainer>
TNonEmpty<TContainer>::TNonEmpty(TContainer underlying)
    : Underlying_(std::move(underlying))
{
    YT_ASSERT(!Underlying_.empty());
}

template <class TContainer>
template <class TIterator>
TNonEmpty<TContainer>::TNonEmpty(TIterator first, TIterator last)
    : TNonEmpty(TContainer(first, last))
{ }

template <class TContainer>
TNonEmpty<TContainer>::TNonEmpty(std::initializer_list<value_type> values)
    : TNonEmpty(TContainer(values))
{ }

template <class TContainer>
typename TContainer::size_type TNonEmpty<TContainer>::size() const
{
    return Underlying_.size();
}

template <class TContainer>
void TNonEmpty<TContainer>::resize(size_type size)
{
    YT_ASSERT(size > 0);
    Underlying_.resize(size);
}

template <class TContainer>
void TNonEmpty<TContainer>::resize(size_type size, const value_type& value)
{
    YT_ASSERT(size > 0);
    Underlying_.resize(size, value);
}

template <class TContainer>
typename TContainer::iterator TNonEmpty<TContainer>::begin()
{
    return Underlying_.begin();
}

template <class TContainer>
typename TContainer::const_iterator TNonEmpty<TContainer>::begin() const
{
    return Underlying_.begin();
}

template <class TContainer>
typename TContainer::iterator TNonEmpty<TContainer>::end()
{
    return Underlying_.end();
}

template <class TContainer>
typename TContainer::const_iterator TNonEmpty<TContainer>::end() const
{
    return Underlying_.end();
}

template <class TContainer>
typename TContainer::reverse_iterator TNonEmpty<TContainer>::rbegin()
{
    return Underlying_.rbegin();
}

template <class TContainer>
typename TContainer::const_reverse_iterator TNonEmpty<TContainer>::rbegin() const
{
    return Underlying_.rbegin();
}

template <class TContainer>
typename TContainer::reverse_iterator TNonEmpty<TContainer>::rend()
{
    return Underlying_.rend();
}

template <class TContainer>
typename TContainer::const_reverse_iterator TNonEmpty<TContainer>::rend() const
{
    return Underlying_.rend();
}

template <class TContainer>
typename TContainer::pointer TNonEmpty<TContainer>::data()
{
    return Underlying_.data();
}

template <class TContainer>
typename TContainer::const_pointer TNonEmpty<TContainer>::data() const
{
    return Underlying_.data();
}

template <class TContainer>
typename TContainer::reference TNonEmpty<TContainer>::operator[](size_type index)
{
    return Underlying_[index];
}

template <class TContainer>
typename TContainer::const_reference TNonEmpty<TContainer>::operator[](size_type index) const
{
    return Underlying_[index];
}

template <class TContainer>
typename TContainer::reference TNonEmpty<TContainer>::front()
{
    return Underlying_.front();
}

template <class TContainer>
typename TContainer::const_reference TNonEmpty<TContainer>::front() const
{
    return Underlying_.front();
}

template <class TContainer>
typename TContainer::reference TNonEmpty<TContainer>::back()
{
    return Underlying_.back();
}

template <class TContainer>
typename TContainer::const_reference TNonEmpty<TContainer>::back() const
{
    return Underlying_.back();
}

template <class TContainer>
void TNonEmpty<TContainer>::push_back(const value_type& value)
{
    Underlying_.push_back(value);
}

template <class TContainer>
void TNonEmpty<TContainer>::push_back(value_type&& value)
{
    Underlying_.push_back(std::move(value));
}

template <class TContainer>
template <class... TArgs>
typename TContainer::reference TNonEmpty<TContainer>::emplace_back(TArgs&&... args)
{
    return Underlying_.emplace_back(std::forward<TArgs>(args)...);
}

template <class TContainer>
void TNonEmpty<TContainer>::pop_back()
{
    YT_ASSERT(Underlying_.size() > 1);
    Underlying_.pop_back();
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
