#pragma once

#include <library/cpp/containers/sorted_vector/sorted_vector.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>
#include <util/generic/yexception.h>

namespace NYql {
namespace NCommon {

const TString ALL_CLUSTERS = "$all";

enum class EConfSettingType {
    Static,
    StaticPerCluster,
    Dynamic,
};

#ifdef YQL_BETTER_CONF_SETTING_API
template <typename TType, EConfSettingType SettingType = EConfSettingType::Dynamic>
#else
template <typename TType, bool RUNTIME = true, bool PERCLUSTER = false>
#endif
class TConfSetting {
public:
    TConfSetting() = default;
    TConfSetting(const TType& value) {
        PerClusterValue[ALL_CLUSTERS] = value;
    }
    TConfSetting(const TConfSetting&) = default;
    TConfSetting(TConfSetting&&) = default;
    ~TConfSetting() = default;

    bool IsRuntime() const {
#ifdef YQL_BETTER_CONF_SETTING_API
        return SettingType == EConfSettingType::Dynamic;
#else
        return RUNTIME;
#endif
    }

    TType& operator[](const TString& cluster) {
        if (ALL_CLUSTERS == cluster) {
            PerClusterValue.clear();
        }
        return PerClusterValue[cluster];
    }
    TConfSetting& operator =(const TType& value) {
        PerClusterValue.clear();
        PerClusterValue[ALL_CLUSTERS] = value;
        return *this;
    }
    TConfSetting& operator =(const TConfSetting&) = default;
    TConfSetting& operator =(TConfSetting&&) = default;

    template <typename TFunc>
    void UpdateAll(TFunc func) {
        PerClusterValue[ALL_CLUSTERS]; // insert record for all clusters if it is not present
        for (auto& it: PerClusterValue) {
            func(it.first, it.second);
        }
    }

    TMaybe<TType> Get(const TString& cluster) const {
        if (!PerClusterValue.empty()) {
            auto it = PerClusterValue.find(cluster);
            if (it != PerClusterValue.end()) {
                return MakeMaybe(it->second);
            }
            it = PerClusterValue.find(ALL_CLUSTERS);
            if (it != PerClusterValue.end()) {
                return MakeMaybe(it->second);
            }
        }
        return Nothing();
    }

    void Clear() {
        PerClusterValue.clear();
    }

    void Clear(const TString& cluster) {
        if (ALL_CLUSTERS == cluster) {
            PerClusterValue.clear();
        } else {
            PerClusterValue.erase(cluster);
        }
    }

private:
    NSorted::TSimpleMap<TString, TType> PerClusterValue; // Uses special '$all' key for all clusters
};

template <typename TType>
#ifdef YQL_BETTER_CONF_SETTING_API
class TConfSetting<TType, EConfSettingType::Static> {
#else
class TConfSetting<TType, false, false> {
#endif
public:
    TConfSetting() = default;
    TConfSetting(const TType& value)
        : Value(value)
    {
    }
    TConfSetting(const TConfSetting&) = default;
    TConfSetting(TConfSetting&&) = default;
    ~TConfSetting() = default;

    bool IsRuntime() const {
        return false;
    }

    TType& operator[](const TString& cluster) {
        if (cluster != ALL_CLUSTERS) {
            ythrow yexception() << "Global static setting cannot be set for specific cluster";
        }
        Value.ConstructInPlace();
        return Value.GetRef();
    }
    TConfSetting& operator =(const TType& value) {
        Value = value;
        return *this;
    }
    TConfSetting& operator =(const TConfSetting&) = default;
    TConfSetting& operator =(TConfSetting&&) = default;

    TMaybe<TType> Get() const {
        return Value;
    }

    void Clear() {
        Value.Clear();
    }

    void Clear(const TString&) {
        Value.Clear();
    }

private:
    TMaybe<TType> Value;
};

} // namespace NCommon
} // namespace NYql
