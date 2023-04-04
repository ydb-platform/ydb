#pragma once

#include <util/string/builder.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/singleton.h>
#include <util/generic/serialized_enum.h>

template <typename T>
class TLocalProcessKeyState {

template <typename U, const char* Name>
friend class TLocalProcessKey;
template <typename U, class TClass>
friend class TLocalProcessExtKey;
template <typename U, typename EnumT>
friend class TEnumProcessKey;

public:
    static TLocalProcessKeyState& GetInstance() {
        return *Singleton<TLocalProcessKeyState<T>>();
    }

    size_t GetCount() const {
        return StartIndex + Names.size();
    }

    TStringBuf GetNameByIndex(size_t index) const {
        if (index < StartIndex) {
            return StaticNames[index];
        } else {
            index -= StartIndex;
            Y_ENSURE(index < Names.size());
            return Names[index];
        }
    }

    size_t GetIndexByName(TStringBuf name) const {
        auto it = Map.find(name);
        Y_ENSURE(it != Map.end());
        return it->second;
    }

private:
    size_t Register(TStringBuf name) {
        auto x = Map.emplace(name, Names.size()+StartIndex);
        if (x.second) {
            Names.emplace_back(name);
        }

        return x.first->second;
    }

    size_t Register(TStringBuf name, ui32 index) {
        Y_VERIFY(index < StartIndex);
        auto x = Map.emplace(name, index);
        Y_VERIFY(x.second || x.first->second == index);
        StaticNames[index] = name;
        return x.first->second;
    }

private:
    static constexpr ui32 StartIndex = 2000;

    TVector<TString> FillStaticNames() {
        TVector<TString> staticNames;
        staticNames.reserve(StartIndex);
        for (ui32 i = 0; i < StartIndex; i++) {
            staticNames.push_back(TStringBuilder() << "Activity_" << i);
        }
        return staticNames;
    }

    TVector<TString> StaticNames = FillStaticNames();
    TVector<TString> Names;
    THashMap<TString, size_t> Map;
};

template <typename T, const char* Name>
class TLocalProcessKey {
public:
    static TStringBuf GetName() {
        return Name;
    }

    static size_t GetIndex() {
        return Index;
    }

private:
    inline static size_t Index = TLocalProcessKeyState<T>::GetInstance().Register(Name);
};

template <typename T, class TClass>
class TLocalProcessExtKey {
public:
    static TStringBuf GetName() {
        return Name;
    }

    static size_t GetIndex() {
        return Index;
    }

private:
    static const inline TString Name = TypeName<TClass>();
    inline static size_t Index = TLocalProcessKeyState<T>::GetInstance().Register(TypeName<TClass>());
};

template <typename T, typename EnumT>
class TEnumProcessKey {
public:
    static TStringBuf GetName(EnumT key) {
        return TLocalProcessKeyState<T>::GetInstance().GetNameByIndex(GetIndex(key));
    }

    static size_t GetIndex(EnumT key) {
        ui32 index = static_cast<ui32>(key);
        if (index < TLocalProcessKeyState<T>::StartIndex) {
            return index;
        }
        Y_VERIFY(index < Enum2Index.size());
        return Enum2Index[index];
    }

private:
    inline static TVector<size_t> RegisterAll() {
        static_assert(std::is_enum<EnumT>::value, "Enum is required");

        TVector<size_t> enum2Index;
        auto names = GetEnumNames<EnumT>();
        ui32 maxId = 0;
        for (const auto& [k, v] : names) {
            maxId = Max(maxId, static_cast<ui32>(k));
        }
        enum2Index.resize(maxId+1);
        for (ui32 i = 0; i <= maxId && i < TLocalProcessKeyState<T>::StartIndex; i++) {
            enum2Index[i] = i;
        }

        for (const auto& [k, v] : names) {
            ui32 enumId = static_cast<ui32>(k);
            enum2Index[enumId] = TLocalProcessKeyState<T>::GetInstance().Register(v, enumId);
        }
        return enum2Index;
    }

    inline static TVector<size_t> Enum2Index = RegisterAll();
};
