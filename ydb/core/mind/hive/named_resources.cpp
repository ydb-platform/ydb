#include "named_resources.h"

#include <ydb/core/tablet/resource_broker.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr {
namespace NHive {

TNamedResourceValues::TStorage::iterator TNamedResourceValues::LowerBound(TStringBuf name) {
    return std::lower_bound(Values.begin(), Values.end(), name, [](const TNamedResourceValue& v, TStringBuf n) {
        return TStringBuf(v.Name) < n;
    });
}

TNamedResourceValues::TStorage::const_iterator TNamedResourceValues::LowerBound(TStringBuf name) const {
    return std::lower_bound(Values.begin(), Values.end(), name, [](const TNamedResourceValue& v, TStringBuf n) {
        return TStringBuf(v.Name) < n;
    });
}

const TNamedResourceValue* TNamedResourceValues::Find(TStringBuf name) const {
    auto it = LowerBound(name);
    if (it != Values.end() && it->Name == name) {
        return &*it;
    }
    return nullptr;
}

ui64 TNamedResourceValues::GetValue(TStringBuf name) const {
    const auto* value = Find(name);
    return value ? value->Value : 0;
}

ui64 TNamedResourceValues::GetTransferable(TStringBuf name) const {
    const auto* value = Find(name);
    return value ? value->Transferable : 0;
}

void TNamedResourceValues::Set(const TString& name, ui64 value, ui64 transferable, TInstant estimateDeadline) {
    auto it = LowerBound(name);
    bool found = it != Values.end() && it->Name == name;
    if (value == 0) {
        if (found) {
            Values.erase(it);
        }
        return;
    }
    transferable = std::min(transferable, value);
    if (found) {
        it->Value = value;
        it->Transferable = transferable;
        it->EstimateDeadline = estimateDeadline;
    } else {
        Values.insert(it, TNamedResourceValue{.Name = name, .Value = value, .Transferable = transferable, .EstimateDeadline = estimateDeadline});
    }
}

bool TNamedResourceValues::CarryOverTransferable(TInstant estimateDeadline) {
    bool changed = false;
    for (auto it = Values.begin(); it != Values.end();) {
        if (it->Transferable == 0) {
            it = Values.erase(it);
            changed = true;
            continue;
        }
        changed |= it->Value != it->Transferable || it->EstimateDeadline != estimateDeadline;
        it->Value = it->Transferable;
        it->EstimateDeadline = estimateDeadline;
        ++it;
    }
    return changed;
}

bool TNamedResourceValues::DropExpiredEstimates(TInstant now) {
    auto size = Values.size();
    EraseIf(Values, [now](const TNamedResourceValue& v) {
        return v.EstimateDeadline && v.EstimateDeadline <= now;
    });
    return Values.size() != size;
}

TInstant TNamedResourceValues::GetEarliestEstimateDeadline() const {
    TInstant result;
    for (const auto& v : Values) {
        if (v.EstimateDeadline && (!result || v.EstimateDeadline < result)) {
            result = v.EstimateDeadline;
        }
    }
    return result;
}

void TNamedResourceValues::Apply(const TNamedResourceValues& other, bool subtract) {
    for (const auto& v : other.Values) {
        auto it = LowerBound(v.Name);
        bool found = it != Values.end() && it->Name == v.Name;
        if (subtract) {
            if (!found) {
                continue;
            }
            it->Value -= std::min(it->Value, v.Value);
            it->Transferable -= std::min(it->Transferable, v.Transferable);
            if (it->Value == 0) {
                Values.erase(it);
            } else {
                it->Transferable = std::min(it->Transferable, it->Value);
            }
        } else if (found) {
            it->Value += v.Value;
            it->Transferable += v.Transferable;
        } else {
            // sums are not estimates by themselves
            Values.insert(it, TNamedResourceValue{.Name = v.Name, .Value = v.Value, .Transferable = v.Transferable});
        }
    }
}

void TNamedResourceValues::Add(const TNamedResourceValues& other) {
    Apply(other, false);
}

void TNamedResourceValues::Subtract(const TNamedResourceValues& other) {
    Apply(other, true);
}

void TNamedResourceValues::ToProto(::google::protobuf::RepeatedPtrField<NKikimrTabletBase::TNamedResource>* proto) const {
    proto->Clear();
    for (const auto& v : Values) {
        auto* resource = proto->Add();
        resource->SetName(v.Name);
        resource->SetValue(v.Value);
        if (v.Transferable != v.Value) {
            resource->SetTransferable(v.Transferable);
        }
        if (v.EstimateDeadline) {
            resource->SetEstimateDeadline(v.EstimateDeadline.MilliSeconds());
        }
    }
}

void TNamedResourceValues::FromProto(const ::google::protobuf::RepeatedPtrField<NKikimrTabletBase::TNamedResource>& proto) {
    Values.clear();
    for (const auto& resource : proto) {
        Set(resource.GetName(), resource.GetValue(), resource.HasTransferable() ? resource.GetTransferable() : resource.GetValue(),
            TInstant::MilliSeconds(resource.GetEstimateDeadline()));
    }
}

TString TNamedResourceValues::ToString() const {
    TStringBuilder result;
    for (const auto& v : Values) {
        if (!result.empty()) {
            result << ", ";
        }
        result << v.Name << "=" << v.Value;
        if (v.Transferable != v.Value) {
            result << "(" << v.Transferable << " transferable)";
        }
        if (v.EstimateDeadline) {
            result << "(estimate)";
        }
    }
    return result;
}

TNamedResourceRegistry::TNamedResourceRegistry()
    : BuiltInDefaults(GetBuiltInDefaults())
{}

TNamedResourceRegistry::TConfig TNamedResourceRegistry::GetBuiltInDefaults() {
    TConfig defaults;
    {
        // initial scans of changefeeds (e.g. async replication) wait in this queue on the source datashards
        auto* settings = defaults.Add();
        settings->SetName(NResourceBroker::MakeQueueResourceName("queue_cdc_initial_scan"));
        settings->SetBalanceMode(NKikimrConfig::THiveConfig::TNamedResourceSettings::SATURATION);
    }
    return defaults;
}

void TNamedResourceRegistry::Configure(const TConfig& config) {
    Config = config;
    Cache.clear();
}

const NKikimrConfig::THiveConfig::TNamedResourceSettings* TNamedResourceRegistry::FindBestMatch(const TConfig& config, const TString& name) {
    const NKikimrConfig::THiveConfig::TNamedResourceSettings* best = nullptr;
    size_t bestPrefixLength = 0;
    for (const auto& settings : config) {
        const TString& pattern = settings.GetName();
        if (pattern == name) {
            return &settings; // exact match always wins
        }
        if (!pattern.empty() && pattern.back() == '*') {
            TStringBuf prefix(pattern.data(), pattern.size() - 1);
            if (TStringBuf(name).StartsWith(prefix) && (best == nullptr || prefix.size() > bestPrefixLength)) {
                best = &settings;
                bestPrefixLength = prefix.size();
            }
        }
    }
    return best;
}

TNamedResourceSettings TNamedResourceRegistry::MakeSettings(const NKikimrConfig::THiveConfig::TNamedResourceSettings* config) {
    TNamedResourceSettings settings;
    if (config) {
        settings.BalanceMode = config->GetBalanceMode();
        settings.MinScatterToBalance = config->GetMinScatterToBalance();
        if (config->HasDefaultMaximum()) {
            settings.DefaultMaximum = config->GetDefaultMaximum();
        }
    }
    return settings;
}

const TNamedResourceSettings& TNamedResourceRegistry::GetSettings(const TString& name) const {
    auto it = Cache.find(name);
    if (it == Cache.end()) {
        const auto* config = FindBestMatch(Config, name);
        if (config == nullptr) {
            config = FindBestMatch(BuiltInDefaults, name);
        }
        it = Cache.emplace(name, MakeSettings(config)).first;
    }
    return it->second;
}

void TNamedResourceRegistry::Register(const TString& name) {
    if (KnownNamesSet.insert(name).second) {
        KnownNames.insert(std::lower_bound(KnownNames.begin(), KnownNames.end(), name), name);
    }
}

} // NHive
} // NKikimr
