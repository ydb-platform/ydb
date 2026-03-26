#pragma once

#include <library/cpp/containers/sorted_vector/sorted_vector.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>
#include <util/generic/yexception.h>

namespace NYql::NCommon {

const TString ALL_CLUSTERS = "$all";

enum class EConfSettingType {
    Static,
    StaticPerCluster,
    Dynamic,
};

template <typename TType, EConfSettingType SettingType = EConfSettingType::Dynamic>
class TConfSetting {
public:
    TConfSetting() = default;
    explicit TConfSetting(const TType& value) {
        PerClusterValue_[ALL_CLUSTERS] = value;
    }
    TConfSetting(const TConfSetting&) = default;
    TConfSetting(TConfSetting&&) = default;
    ~TConfSetting() = default;

    bool IsRuntime() const {
        return SettingType == EConfSettingType::Dynamic;
    }

    bool IsPerCluster() const {
        return SettingType == EConfSettingType::Dynamic || SettingType == EConfSettingType::StaticPerCluster;
    }

    TType& operator[](const TString& cluster) {
        if (ALL_CLUSTERS == cluster) {
            PerClusterValue_.clear();
        }
        return PerClusterValue_[cluster];
    }
    TConfSetting& operator=(const TType& value) {
        PerClusterValue_.clear();
        PerClusterValue_[ALL_CLUSTERS] = value;
        return *this;
    }
    TConfSetting& operator=(const TConfSetting&) = default;
    TConfSetting& operator=(TConfSetting&&) = default;

    template <typename TFunc>
    void UpdateAll(TFunc func) {
        PerClusterValue_[ALL_CLUSTERS]; // insert record for all clusters if it is not present
        for (auto& it : PerClusterValue_) {
            func(it.first, it.second);
        }
    }

    TMaybe<TType> Get(const TString& cluster) const {
        if (!PerClusterValue_.empty()) {
            auto it = PerClusterValue_.find(cluster);
            if (it != PerClusterValue_.end()) {
                return MakeMaybe(it->second);
            }
            it = PerClusterValue_.find(ALL_CLUSTERS);
            if (it != PerClusterValue_.end()) {
                return MakeMaybe(it->second);
            }
        }
        return Nothing();
    }

    void Clear() {
        PerClusterValue_.clear();
    }

    void Clear(const TString& cluster) {
        if (ALL_CLUSTERS == cluster) {
            PerClusterValue_.clear();
        } else {
            PerClusterValue_.erase(cluster);
        }
    }

private:
    NSorted::TSimpleMap<TString, TType> PerClusterValue_; // Uses special '$all' key for all clusters
};

template <typename TType>
class TConfSetting<TType, EConfSettingType::Static> {
public:
    TConfSetting() = default;
    explicit TConfSetting(const TType& value)
        : Value_(value)
    {
    }
    TConfSetting(const TConfSetting&) = default;
    TConfSetting(TConfSetting&&) = default;
    ~TConfSetting() = default;

    bool IsRuntime() const {
        return false;
    }

    bool IsPerCluster() const {
        return false;
    }

    TType& operator[](const TString& cluster) {
        if (cluster != ALL_CLUSTERS) {
            ythrow yexception() << "Global static setting cannot be set for specific cluster";
        }
        Value_.ConstructInPlace();
        return Value_.GetRef();
    }
    TConfSetting& operator=(const TType& value) {
        Value_ = value;
        return *this;
    }
    TConfSetting& operator=(const TConfSetting&) = default;
    TConfSetting& operator=(TConfSetting&&) = default;

    TMaybe<TType> Get() const {
        return Value_;
    }

    void Clear() {
        Value_.Clear();
    }

    void Clear(const TString&) {
        Value_.Clear();
    }

private:
    TMaybe<TType> Value_;
};

} // namespace NYql::NCommon
