#include <jinja2cpp/generic_list.h>
#include <jinja2cpp/generic_list_iterator.h>

namespace jinja2 {

detail::GenericListIterator GenericList::begin() const
{
    return m_accessor && m_accessor() ? detail::GenericListIterator(m_accessor()->CreateEnumerator()) : detail::GenericListIterator();
}

detail::GenericListIterator GenericList::end() const
{
    return detail::GenericListIterator();
}

auto GenericList::cbegin() const {return begin();}
auto GenericList::cend() const {return end();}

bool GenericList::IsEqual(const GenericList& rhs) const
{
    if (IsValid() && rhs.IsValid() && !GetAccessor()->IsEqual(*rhs.GetAccessor()))
        return false;
    if ((IsValid() && !rhs.IsValid()) || (!IsValid() && rhs.IsValid()))
            return false;
    return true;
}

bool operator==(const GenericList& lhs, const GenericList& rhs)
{
    return lhs.IsEqual(rhs);
}

bool operator!=(const GenericList& lhs, const GenericList& rhs)
{
    return !(lhs == rhs);
}

} // namespace jinja2
