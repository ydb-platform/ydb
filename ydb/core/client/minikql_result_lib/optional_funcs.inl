#pragma once


#define ENSURE_HAS_ITEM() \
    do { \
        Y_ENSURE(HasItem(), "Optional is empty!"); \
    } while (0);

template <>
inline void TOptional::GetItem<void>() const {
    return;
}

template <>
inline TOptional TOptional::GetItem<TOptional>() const {
    ENSURE_HAS_ITEM();
    const auto& itemType = RootType.GetOptional().GetItem();
    return TOptional(RootValue.GetOptional(), itemType);
}

template <>
inline TListType TOptional::GetItem<TListType>() const {
    ENSURE_HAS_ITEM();
    const auto& itemType = RootType.GetOptional().GetItem();
    return TListType(RootValue.GetOptional(), itemType);
}

template <>
inline TTuple TOptional::GetItem<TTuple>() const {
    ENSURE_HAS_ITEM();
    const auto& itemType = RootType.GetOptional().GetItem();
    return TTuple(RootValue.GetOptional(), itemType);
}

template <>
inline TStruct TOptional::GetItem<TStruct>() const {
    ENSURE_HAS_ITEM();
    const auto& itemType = RootType.GetOptional().GetItem();
    return TStruct(RootValue.GetOptional(), itemType);
}

template <>
inline TDict TOptional::GetItem<TDict>() const {
    ENSURE_HAS_ITEM();
    const auto& itemType = RootType.GetOptional().GetItem();
    return TDict(RootValue.GetOptional(), itemType);
}

#undef ENSURE_HAS_ITEM
