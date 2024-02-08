#pragma once

#include <util/string/builder.h>
#include <util/system/mutex.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/singleton.h>
#include <util/generic/serialized_enum.h>

class TLocalProcessKeyStateIndexLimiter {
public:
    static constexpr ui32 GetMaxKeysCount() {
        return 10000;
    }
};

template <class T>
class TLocalProcessKeyStateIndexConstructor {
public:
};

template <typename T>
class TLocalProcessKeyState {

template <typename U, const char* Name>
friend class TLocalProcessKey;
template <typename U, class TClass, ui32 KeyLengthLimit>
friend class TLocalProcessExtKey;
template <typename U, typename EnumT>
friend class TEnumProcessKey;

public:
    static TLocalProcessKeyState& GetInstance() {
        return *Singleton<TLocalProcessKeyState<T>>();
    }

    ui32 GetCount() const {
        return MaxKeysCount;
    }

    TStringBuf GetNameByIndex(size_t index) const {
        Y_ABORT_UNLESS(index < Names.size());
        return Names[index];
    }

    size_t GetIndexByName(TStringBuf name) const {
        TGuard<TMutex> g(Mutex);
        auto it = Map.find(name);
        Y_ENSURE(it != Map.end());
        return it->second;
    }

    TLocalProcessKeyState() {
        Names.resize(MaxKeysCount);
    }

    size_t Register(TStringBuf name) {
        TGuard<TMutex> g(Mutex);
        auto it = Map.find(name);
        if (it != Map.end()) {
            return it->second;
        }
        const ui32 index = TLocalProcessKeyStateIndexConstructor<T>::BuildCurrentIndex(name, Names.size());
        auto x = Map.emplace(name, index);
        if (x.second) {
            Y_ABORT_UNLESS(index < Names.size(), "a lot of actors or tags for memory monitoring");
            Names[index] = name;
        }

        return x.first->second;
    }

private:

    static constexpr ui32 MaxKeysCount = TLocalProcessKeyStateIndexLimiter::GetMaxKeysCount();

private:
    TVector<TString> Names;
    THashMap<TString, size_t> Map;
    TMutex Mutex;
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

template <typename T, class TClass, ui32 KeyLengthLimit = 0>
class TLocalProcessExtKey {
public:
    static TStringBuf GetName() {
        return Name;
    }

    static size_t GetIndex() {
        return Index;
    }

private:

    static TString TypeNameRobust() {
        const TString className = TypeName<TClass>();
        if (KeyLengthLimit && className.size() > KeyLengthLimit) {
            return className.substr(0, KeyLengthLimit - 3) + "...";
        } else {
            return className;
        }
    }

    static const inline TString Name = TypeName<TClass>();
    inline static size_t Index = TLocalProcessKeyState<T>::GetInstance().Register(TypeNameRobust());
};

template <typename T, typename EnumT>
class TEnumProcessKey {
public:
    static TStringBuf GetName(const EnumT key) {
        return TLocalProcessKeyState<T>::GetInstance().GetNameByIndex(GetIndex(key));
    }

    static size_t GetIndex(const EnumT key) {
        ui32 index = static_cast<ui32>(key);
        Y_ABORT_UNLESS(index < Enum2Index.size());
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
        enum2Index.resize(maxId + 1);
        for (const auto& [k, v] : names) {
            ui32 enumId = static_cast<ui32>(k);
            enum2Index[enumId] = TLocalProcessKeyState<T>::GetInstance().Register(v);
        }
        return enum2Index;
    }

    inline static TVector<size_t> Enum2Index = RegisterAll();
};
