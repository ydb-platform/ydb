#ifndef JINJA2CPP_GENERIC_LIST_ITERATOR_H
#define JINJA2CPP_GENERIC_LIST_ITERATOR_H

#include "generic_list.h"
#include "value.h"
#include "value_ptr.h"

namespace jinja2
{
namespace detail
{
class JINJA2CPP_EXPORT GenericListIterator
{
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = const Value;
    using difference_type = std::ptrdiff_t;
    using reference = const Value&;
    using pointer = const Value*;
    using EnumeratorPtr = types::ValuePtr<IListEnumerator>;

    GenericListIterator() = default;

    GenericListIterator(ListEnumeratorPtr enumerator)
        : m_enumerator(types::ValuePtr<IListEnumerator>(enumerator))
    {
        if (m_enumerator)
            m_hasValue = m_enumerator->MoveNext();

        if (m_hasValue)
            m_current = m_enumerator->GetCurrent();
    }

    bool operator == (const GenericListIterator& other) const
    {
        if (m_hasValue != other.m_hasValue)
            return false;
        if (!m_enumerator && !other.m_enumerator)
            return true;
        if (this->m_enumerator && other.m_enumerator && !m_enumerator->IsEqual(*other.m_enumerator))
            return false;
        if ((m_enumerator && !other.m_enumerator) || (!m_enumerator && other.m_enumerator))
            return false;
        if (m_current != other.m_current)
            return false;
        return true;
    }

    bool operator != (const GenericListIterator& other) const
    {
        return !(*this == other);
    }

    reference operator *() const
    {
        return m_current;
    }

    GenericListIterator& operator ++()
    {
        if (!m_enumerator)
            return *this;
        m_hasValue = m_enumerator->MoveNext();
        if (m_hasValue)
        {
            m_current = m_enumerator->GetCurrent();
        }
        else
        {
            EnumeratorPtr temp;
            Value tempVal;
            using std::swap;
            swap(m_enumerator, temp);
            swap(m_current, tempVal);
        }

        return *this;
    }

    GenericListIterator operator++(int)
    {
        GenericListIterator result(std::move(m_current));

        this->operator++();
        return result;
    }
private:
    explicit GenericListIterator(Value&& val)
        : m_hasValue(true)
        , m_current(std::move(val))
    {

    }

private:
    EnumeratorPtr m_enumerator;
    bool m_hasValue = false;
    Value m_current;
};

} // namespace detail
} // namespace jinja2

#endif // JINJA2CPP_GENERIC_LIST_ITERATOR_H
