#pragma once


template <>
inline void TTuple::GetElement<void>(size_t index) const {
    Y_ENSURE(CheckIndex(index), "Tuple element index" << index << " is out of bounds.");
    return;
}

template <>
inline TOptional TTuple::GetElement<TOptional>(size_t index) const {
    Y_ENSURE(CheckIndex(index), "Tuple element index" << index << " is out of bounds.");
    const auto& elementType = RootType.GetTuple().GetElement(index);
    const auto& element = RootValue.GetTuple(index);
    return TOptional(element, elementType);
}

template <>
inline TListType TTuple::GetElement<TListType>(size_t index) const {
    Y_ENSURE(CheckIndex(index), "Tuple element index" << index << " is out of bounds.");
    const auto& elementType = RootType.GetTuple().GetElement(index);
    const auto& element = RootValue.GetTuple(index);
    return TListType(element, elementType);
}

template <>
inline TTuple TTuple::GetElement<TTuple>(size_t index) const {
    Y_ENSURE(CheckIndex(index), "Tuple element index" << index << " is out of bounds.");
    const auto& elementType = RootType.GetTuple().GetElement(index);
    const auto& element = RootValue.GetTuple(index);
    return TTuple(element, elementType);
}

template <>
inline TStruct TTuple::GetElement<TStruct>(size_t index) const {
    Y_ENSURE(CheckIndex(index), "Tuple element index" << index << " is out of bounds.");
    const auto& elementType = RootType.GetTuple().GetElement(index);
    const auto& element = RootValue.GetTuple(index);
    return TStruct(element, elementType);
}

template <>
inline TDict TTuple::GetElement<TDict>(size_t index) const {
    Y_ENSURE(CheckIndex(index), "Tuple element index" << index << " is out of bounds.");
    const auto& elementType = RootType.GetTuple().GetElement(index);
    const auto& element = RootValue.GetTuple(index);
    return TDict(element, elementType);
}
