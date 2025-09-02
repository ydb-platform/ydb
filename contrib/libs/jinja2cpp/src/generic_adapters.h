#ifndef JINJA2CPP_SRC_GENERIC_ADAPTERS_H
#define JINJA2CPP_SRC_GENERIC_ADAPTERS_H

#include <jinja2cpp/value.h>
#include "internal_value.h"

namespace jinja2
{

template<typename ImplType, typename List, typename ValType, typename Base>
class IndexedEnumeratorImpl : public Base
{
public:
    using ValueType = ValType;
    using ThisType = IndexedEnumeratorImpl<ImplType, List, ValType, Base>;

    IndexedEnumeratorImpl(const List* list)
        : m_list(list)
        , m_maxItems(list->GetSize().value())
    { }

    void Reset() override
    {
        m_curItem = m_invalidIndex;
    }

    bool MoveNext() override
    {
        if (m_curItem == m_invalidIndex)
            m_curItem = 0;
        else
            ++ m_curItem;

        return m_curItem < m_maxItems;
    }

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const ThisType*>(&other);
        if (!val)
            return false;
        if (m_list && val->m_list && !m_list->IsEqual(*val->m_list))
            return false;
        if ((m_list && !val->m_list) || (!m_list && val->m_list))
            return false;
        if (m_curItem != val->m_curItem)
            return false;
        if (m_maxItems != val->m_maxItems)
            return false;
        return true;
    }

protected:
    constexpr static auto m_invalidIndex = std::numeric_limits<size_t>::max();
    const List* m_list{};
    size_t m_curItem = m_invalidIndex;
    size_t m_maxItems{};
};


template<typename T>
class IndexedListItemAccessorImpl : public IListItemAccessor, public IIndexBasedAccessor
{
public:
    using ThisType = IndexedListItemAccessorImpl<T>;
    class Enumerator : public IndexedEnumeratorImpl<Enumerator, ThisType, Value, IListEnumerator>
    {
    public:
        using BaseClass = IndexedEnumeratorImpl<Enumerator, ThisType, Value, IListEnumerator>;
#if defined(_MSC_VER)
        using IndexedEnumeratorImpl::IndexedEnumeratorImpl;
#else
        using BaseClass::BaseClass;
#endif

        typename BaseClass::ValueType GetCurrent() const override
        {
            auto indexer = this->m_list->GetIndexer();
            if (!indexer)
                return Value();

            return indexer->GetItemByIndex(this->m_curItem);
        }
        ListEnumeratorPtr Clone() const override
        {
            auto result = MakeEnumerator<Enumerator>(this->m_list);
            auto base = static_cast<Enumerator*>(&(*result));
            base->m_curItem = this->m_curItem;
            return result;
        }

        ListEnumeratorPtr Move() override
        {
            auto result = MakeEnumerator<Enumerator>(this->m_list);
            auto base = static_cast<Enumerator*>(&(*result));
            base->m_curItem = this->m_curItem;
            this->m_list = nullptr;
            this->m_curItem = this->m_invalidIndex;
            this->m_maxItems = 0;
            return result;
        }
    };

    Value GetItemByIndex(int64_t idx) const override
    {
        return IntValue2Value(std::move(static_cast<const T*>(this)->GetItem(idx).value()));
    }

    std::optional<size_t> GetSize() const override
    {
        return static_cast<const T*>(this)->GetItemsCountImpl();
    }

    const IIndexBasedAccessor* GetIndexer() const override
    {
        return this;
    }

    ListEnumeratorPtr CreateEnumerator() const override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const ThisType*>(&other);
        if (!val)
            return false;
        auto enumerator = CreateEnumerator();
        auto otherEnum = val->CreateEnumerator();
        if (enumerator && otherEnum && !enumerator->IsEqual(*otherEnum))
            return false;
        return true;
    }
};

template<typename T>
class IndexedListAccessorImpl : public IListAccessor, public IndexedListItemAccessorImpl<T>
{
public:
    using ThisType = IndexedListAccessorImpl<T>;
    class Enumerator : public IndexedEnumeratorImpl<Enumerator, ThisType, InternalValue, IListAccessorEnumerator>
    {
    public:
        using BaseClass = IndexedEnumeratorImpl<Enumerator, ThisType, InternalValue, IListAccessorEnumerator>;
#if defined(_MSC_VER)
        using IndexedEnumeratorImpl::IndexedEnumeratorImpl;
#else
        using BaseClass::BaseClass;
#endif

        typename BaseClass::ValueType GetCurrent() const override
        {
            const auto& result = this->m_list->GetItem(this->m_curItem);
            if (!result)
                return InternalValue();

            return result.value();
        }

        IListAccessorEnumerator* Clone() const override
        {
            auto result = new Enumerator(this->m_list);
            auto base = result;
            base->m_curItem = this->m_curItem;
            return result;
        }

        IListAccessorEnumerator* Transfer() override
        {
            auto result = new Enumerator(std::move(*this));
            auto base = result;
            base->m_curItem = this->m_curItem;
            this->m_list = nullptr;
            this->m_curItem = this->m_invalidIndex;
            this->m_maxItems = 0;
            return result;
        }
    };

    std::optional<size_t> GetSize() const override
    {
        return static_cast<const T*>(this)->GetItemsCountImpl();
    }
    ListAccessorEnumeratorPtr CreateListAccessorEnumerator() const override;
};

template<typename T>
class MapItemAccessorImpl : public IMapItemAccessor
{
public:
    Value GetValueByName(const std::string& name) const
    {
        return IntValue2Value(static_cast<const T*>(this)->GetItem(name));
    }
};

template<typename T>
class MapAccessorImpl : public IMapAccessor, public MapItemAccessorImpl<T>
{
public:
};

template<typename T>
inline ListAccessorEnumeratorPtr IndexedListAccessorImpl<T>::CreateListAccessorEnumerator() const
{
    return ListAccessorEnumeratorPtr(new Enumerator(this));
}

template<typename T>
inline ListEnumeratorPtr IndexedListItemAccessorImpl<T>::CreateEnumerator() const
{
    return MakeEnumerator<Enumerator>(this);
}

} // namespace jinja2

#endif // JINJA2CPP_SRC_GENERIC_ADAPTERS_H
