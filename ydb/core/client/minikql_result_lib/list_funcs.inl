#pragma once


template <typename T>
inline TListType::iterator<T>::iterator(google::protobuf::RepeatedPtrField<TProtoValue>::const_iterator item, const TProtoType& itemType)
    : Item(item)
    , ItemType(itemType)
{
}

template <typename T>
inline TListType::iterator<T>::iterator(const iterator& it)
    : Item(it.Item)
    , ItemType(it.ItemType)
{
}

template <typename T>
inline TListType::iterator<T>& TListType::iterator<T>::operator++() {
    ++Item;
    return *this;
}

template <typename T>
inline TListType::iterator<T> TListType::iterator<T>::operator++(int) {
    iterator it(*this);
    ++(*this);
    return it;
}

template <typename T>
inline bool TListType::iterator<T>::operator!=(const iterator& it) const {
    return Item != it.Item;
}

template <typename T>
inline bool TListType::iterator<T>::operator==(const iterator& it) const {
    return Item == it.Item;
}

template <typename T>
inline T TListType::iterator<T>::operator*() const {
    return Get();
}

template <typename T>
inline T TListType::iterator<T>::Get() const {
    ENSURE_KIND(ItemType, Data);
    auto schemeType = ItemType.GetData().GetScheme();
    return NPrivate::GetData<T>(*Item, schemeType);
}

template <>
inline void TListType::iterator<void>::Get() const {
    return;
}

template <>
inline TOptional TListType::iterator<TOptional>::Get() const {
    return TOptional(*Item, ItemType);
}

template <>
inline TListType TListType::iterator<TListType>::Get() const {
    return TListType(*Item, ItemType);
}

template <>
inline TTuple TListType::iterator<TTuple>::Get() const {
    return TTuple(*Item, ItemType);
}

template <>
inline TStruct TListType::iterator<TStruct>::Get() const {
    return TStruct(*Item, ItemType);
}

template <>
inline TDict TListType::iterator<TDict>::Get() const {
    return TDict(*Item, ItemType);
}
