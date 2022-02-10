#pragma once


template <>
inline void TStruct::GetMember<void>(size_t memberIndex) {
    Y_ENSURE(CheckIndex(memberIndex), "Struct member index" << memberIndex << " is out of bounds.");
    return;
}

template <>
inline TOptional TStruct::GetMember<TOptional>(size_t memberIndex) {
    Y_ENSURE(CheckIndex(memberIndex), "Struct member index" << memberIndex << " is out of bounds.");
    const auto& memberType = RootType.GetStruct().GetMember(memberIndex).GetType();
    const auto& member = RootValue.GetStruct(memberIndex);
    return TOptional(member, memberType);
}

template <>
inline TListType TStruct::GetMember<TListType>(size_t memberIndex) {
    Y_ENSURE(CheckIndex(memberIndex), "Struct member index" << memberIndex << " is out of bounds.");
    const auto& memberType = RootType.GetStruct().GetMember(memberIndex).GetType();
    const auto& member = RootValue.GetStruct(memberIndex);
    return TListType(member, memberType);
}

template <>
inline TTuple TStruct::GetMember<TTuple>(size_t memberIndex) {
    Y_ENSURE(CheckIndex(memberIndex), "Struct member index" << memberIndex << " is out of bounds.");
    const auto& memberType = RootType.GetStruct().GetMember(memberIndex).GetType();
    const auto& member = RootValue.GetStruct(memberIndex);
    return TTuple(member, memberType);
}

template <>
inline TStruct TStruct::GetMember<TStruct>(size_t memberIndex) {
    Y_ENSURE(CheckIndex(memberIndex), "Struct member index" << memberIndex << " is out of bounds.");
    const auto& memberType = RootType.GetStruct().GetMember(memberIndex).GetType();
    const auto& member = RootValue.GetStruct(memberIndex);
    return TStruct(member, memberType);
}

template <>
inline TDict TStruct::GetMember<TDict>(size_t memberIndex) {
    Y_ENSURE(CheckIndex(memberIndex), "Struct member index" << memberIndex << " is out of bounds.");
    const auto& memberType = RootType.GetStruct().GetMember(memberIndex).GetType();
    const auto& member = RootValue.GetStruct(memberIndex);
    return TDict(member, memberType);
}
