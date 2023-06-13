#pragma once

#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>
#include <util/generic/yexception.h>

#include "scheme.h"

namespace NJsonConverters {
    class IJsonSerializable {
    public:
        virtual NSc::TValue ToTValue() const = 0;
        virtual void FromTValue(const NSc::TValue&, const bool validate) = 0;

        const TString ToJson(const bool sort = false) const {
            return ToTValue().ToJson(sort);
        }

        void FromJson(const TStringBuf& json, const bool validate = false) {
            NSc::TValue v = NSc::TValue::FromJson(json);
            FromTValue(v, validate);
        }

        virtual ~IJsonSerializable() = default;
    };
    //////////////////////////////////////////////////////////////////////
    // fwd declarations
    //////////////////////////////////////////////////////////////////////

    //TVector
    template <typename T, typename A>
    NSc::TValue ToTValue(const TVector<T, A>& x);
    template <typename T, typename A>
    void FromTValue(const NSc::TValue& x, TVector<T, A>& out, const bool validate);

    //THashMap
    template <class Key, class T, class HashFcn, class EqualKey, class Alloc>
    NSc::TValue ToTValue(const THashMap<Key, T, HashFcn, EqualKey, Alloc>& x);
    template <class Key, class T, class HashFcn, class EqualKey, class Alloc>
    void FromTValue(const NSc::TValue& x, THashMap<Key, T, HashFcn, EqualKey, Alloc>& out, const bool validate);

    //TMap
    template <class K, class V, class Less, class A>
    NSc::TValue ToTValue(const TMap<K, V, Less, A>& x);
    template <class K, class V, class Less, class A>
    void FromTValue(const NSc::TValue& x, TMap<K, V, Less, A>& out, const bool validate);

    //THashSet
    template <class V, class H, class E, class A>
    NSc::TValue ToTValue(const THashSet<V, H, E, A>& x);
    template <class V, class H, class E, class A>
    void FromTValue(const NSc::TValue& x, THashSet<V, H, E, A>& out, const bool validate);

    //TSet
    template <class K, class L, class A>
    NSc::TValue ToTValue(const TSet<K, L, A>& x);
    template <class K, class L, class A>
    void FromTValue(const NSc::TValue& x, TSet<K, L, A>& out, const bool validate);

    //std::pair
    template <class T1, class T2>
    NSc::TValue ToTValue(const std::pair<T1, T2>& x);
    template <class T1, class T2>
    void FromTValue(const NSc::TValue& x, std::pair<T1, T2>& out, const bool validate);

    //////////////////////////////////////////////////////////////////////
    // simple From, To helpers
    //////////////////////////////////////////////////////////////////////
    template <typename T, bool HasSerializer>
    struct TValueAndStrokaConv {};

    template <typename T>
    struct TValueAndStrokaConv<T, 0> {
        static NSc::TValue ToTValue(const T& x) {
            return NSc::TValue(x);
        }

        static void FromTValue(const NSc::TValue& x, T& out, const bool) {
            out = x;
        }

        static TString ToString(const T& x) {
            return ::ToString(x);
        }

        static void FromString(const TStringBuf& str, T& res, const bool) {
            res = ::FromString<T>(str);
        }
    };

    template <typename T>
    struct TValueAndStrokaConv<T, 1> {
        static NSc::TValue ToTValue(const T& x) {
            return x.ToTValue();
        }

        static void FromTValue(const NSc::TValue& x, T& out, const bool validate) {
            out.FromTValue(x, validate);
        }

        static TString ToString(const T& x) {
            return x.ToJson();
        }

        static void FromString(const TStringBuf& str, T& res, const bool validate) {
            res.FromJson(str, validate);
        }
    };

    template <typename T>
    NSc::TValue ToTValue(const T& x) {
        return TValueAndStrokaConv<T, std::is_base_of<IJsonSerializable, T>::value>::ToTValue(x);
    }

    template <typename T>
    void FromTValue(const NSc::TValue& x, T& out, const bool validate) {
        return TValueAndStrokaConv<T, std::is_base_of<IJsonSerializable, T>::value>::FromTValue(x, out, validate);
    }

    template <typename T>
    T FromTValue(const NSc::TValue& x, const bool validate) {
        T ret;
        FromTValue(x, ret, validate);
        return ret;
    }

    template <typename T>
    TString ToString(const T& x) {
        return TValueAndStrokaConv<T, std::is_base_of<IJsonSerializable, T>::value>::ToString(x);
    }

    template <typename T>
    void FromString(const TStringBuf& str, T& res, const bool validate) {
        return TValueAndStrokaConv<T, std::is_base_of<IJsonSerializable, T>::value>::FromString(str, res, validate);
    }

    template <typename T>
    T FromString(const TStringBuf& str, bool validate) {
        T ret;
        FromString(str, ret, validate);
        return ret;
    }

    namespace NPrivate {
        template <typename T>
        NSc::TValue ToTValueDict(const T& dict) {
            NSc::TValue out;
            out.SetDict();
            for (typename T::const_iterator it = dict.begin(); it != dict.end(); ++it) {
                out[ToString(it->first)] = NJsonConverters::ToTValue(it->second);
            }
            return out;
        }

        template <typename T>
        void FromTValueDict(const NSc::TValue& x, T& out, bool validate) {
            typedef typename T::key_type TKey;
            typedef typename T::mapped_type TMapped;
            if (validate)
                Y_ENSURE(x.IsDict() || x.IsNull(), "not valid input scheme");
            out.clear();
            if (x.IsDict()) {
                const NSc::TDict& dict = x.GetDict();
                for (const auto& it : dict) {
                    TKey key = NJsonConverters::FromString<TKey>(it.first, validate);
                    TMapped val = NJsonConverters::FromTValue<TMapped>(it.second, validate);
                    out.insert(std::pair<TKey, TMapped>(key, val));
                }
            }
        }

        template <typename T>
        NSc::TValue ToTValueSet(const T& set) {
            NSc::TValue out;
            out.SetDict();
            for (typename T::const_iterator it = set.begin(); it != set.end(); ++it) {
                out[ToString(*it)] = NSc::Null();
            }
            return out;
        }

        template <typename T>
        void FromTValueSet(const NSc::TValue& x, T& out, const bool validate) {
            typedef typename T::key_type TKey;
            if (validate)
                Y_ENSURE(x.IsDict() || x.IsNull(), "not valid input scheme");
            out.clear();
            if (x.IsDict()) {
                const NSc::TDict& dict = x.GetDict();
                for (const auto& it : dict) {
                    TKey key;
                    NJsonConverters::FromString<TKey>(it.first, key, validate);
                    out.insert(key);
                }
            }
        }
    }

    //////////////////////////////////////////////////////////////////////
    // TVector
    //////////////////////////////////////////////////////////////////////
    template <typename T, typename A>
    NSc::TValue ToTValue(const TVector<T, A>& x) {
        NSc::TValue out;
        out.SetArray();
        for (typename TVector<T, A>::const_iterator it = x.begin(); it != x.end(); ++it)
            out.Push(NJsonConverters::ToTValue(*it));
        return out;
    }

    template <typename T, typename A>
    void FromTValue(const NSc::TValue& x, TVector<T, A>& out, const bool validate) {
        if (validate)
            Y_ENSURE(x.IsArray() || x.IsNull(), "not valid input scheme");
        out.clear();
        if (x.IsArray()) {
            const NSc::TArray& arr = x.GetArray();
            out.reserve(arr.size());
            for (const auto& it : arr) {
                T val;
                NJsonConverters::FromTValue(it, val, validate);
                out.push_back(val);
            }
        }
    }

    //////////////////////////////////////////////////////////////////////
    // THashMap & TMap
    //////////////////////////////////////////////////////////////////////
    template <class Key, class T, class HashFcn, class EqualKey, class Alloc>
    NSc::TValue ToTValue(const THashMap<Key, T, HashFcn, EqualKey, Alloc>& x) {
        return NPrivate::ToTValueDict(x);
    }

    template <class Key, class T, class HashFcn, class EqualKey, class Alloc>
    void FromTValue(const NSc::TValue& x, THashMap<Key, T, HashFcn, EqualKey, Alloc>& out, const bool validate) {
        NPrivate::FromTValueDict(x, out, validate);
    }

    template <class K, class V, class Less, class A>
    NSc::TValue ToTValue(const TMap<K, V, Less, A>& x) {
        return NPrivate::ToTValueDict(x);
    }

    template <class K, class V, class Less, class A>
    void FromTValue(const NSc::TValue& x, TMap<K, V, Less, A>& out, const bool validate) {
        NPrivate::FromTValueDict(x, out, validate);
    }

    //////////////////////////////////////////////////////////////////////
    // THashSet & TSet
    //////////////////////////////////////////////////////////////////////
    template <class V, class H, class E, class A>
    NSc::TValue ToTValue(const THashSet<V, H, E, A>& x) {
        return NPrivate::ToTValueSet(x);
    }

    template <class V, class H, class E, class A>
    void FromTValue(const NSc::TValue& x, THashSet<V, H, E, A>& out, const bool validate) {
        NPrivate::FromTValueSet(x, out, validate);
    }

    template <class K, class L, class A>
    NSc::TValue ToTValue(const TSet<K, L, A>& x) {
        return NPrivate::ToTValueSet(x);
    }

    template <class K, class L, class A>
    void FromTValue(const NSc::TValue& x, TSet<K, L, A>& out, const bool validate) {
        NPrivate::FromTValueSet(x, out, validate);
    }

    //////////////////////////////////////////////////////////////////////
    // std::pair
    //////////////////////////////////////////////////////////////////////
    template <class T1, class T2>
    NSc::TValue ToTValue(const std::pair<T1, T2>& x) {
        NSc::TValue out;
        out.SetArray();
        out.Push(NJsonConverters::ToTValue(x.first));
        out.Push(NJsonConverters::ToTValue(x.second));
        return out;
    }

    template <class T1, class T2>
    void FromTValue(const NSc::TValue& x, std::pair<T1, T2>& out, const bool validate) {
        if (validate)
            Y_ENSURE(x.IsArray() || x.IsNull(), "not valid input scheme");
        if (x.IsArray()) {
            const NSc::TArray& arr = x.GetArray();
            if (arr.size() == 2) {
                T1 val0;
                T2 val1;
                NJsonConverters::FromTValue(arr[0], val0, validate);
                NJsonConverters::FromTValue(arr[1], val1, validate);
                out.first = val0;
                out.second = val1;
            }
        }
    }

    //////////////////////////////////////////////////////////////////////
    // global user functions
    //////////////////////////////////////////////////////////////////////
    template <typename T>
    TString ToJson(const T& val, const bool sort = false) {
        return NJsonConverters::ToTValue(val).ToJson(sort);
    }

    template <typename T>
    T FromJson(const TStringBuf& json, bool validate = false) {
        NSc::TValue v = NSc::TValue::FromJson(json);
        T ret;
        NJsonConverters::FromTValue(v, ret, validate);
        return ret;
    }
}
