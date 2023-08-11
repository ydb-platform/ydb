#include "hive_impl.h"

#include <library/cpp/json/json_value.h>

namespace NKikimr {
namespace NHive {

TResourceRawValues ResourceRawValuesFromMetrics(const NKikimrTabletBase::TMetrics& metrics) {
    TResourceRawValues values = {};
    if (metrics.HasCounter()) {
        std::get<NMetrics::EResource::Counter>(values) = metrics.GetCounter();
    }
    if (metrics.HasCPU()) {
        std::get<NMetrics::EResource::CPU>(values) = metrics.GetCPU();
    }
    if (metrics.HasMemory()) {
        std::get<NMetrics::EResource::Memory>(values) = metrics.GetMemory();
    }
    if (metrics.HasNetwork()) {
        std::get<NMetrics::EResource::Network>(values) = metrics.GetNetwork();
    }
    return values;
}

NKikimrTabletBase::TMetrics MetricsFromResourceRawValues(const TResourceRawValues& values) {
    NKikimrTabletBase::TMetrics metrics;
    if (std::get<NMetrics::EResource::Counter>(values) != 0) {
        metrics.SetCounter(std::get<NMetrics::EResource::Counter>(values));
    }
    if (std::get<NMetrics::EResource::CPU>(values) != 0) {
        metrics.SetCPU(std::get<NMetrics::EResource::CPU>(values));
    }
    if (std::get<NMetrics::EResource::Memory>(values) != 0) {
        metrics.SetMemory(std::get<NMetrics::EResource::Memory>(values));
    }
    if (std::get<NMetrics::EResource::Network>(values) != 0) {
        metrics.SetNetwork(std::get<NMetrics::EResource::Network>(values));
    }
    return metrics;
}

TResourceRawValues ResourceRawValuesFromMetrics(const NKikimrHive::TTabletMetrics& tabletMetrics) {
    TResourceRawValues values = {};
    if (tabletMetrics.HasResourceUsage()) {
        const NKikimrTabletBase::TMetrics& metrics(tabletMetrics.GetResourceUsage());
        values = ResourceRawValuesFromMetrics(metrics);
    }
    return values;
}

TString GetResourceValuesText(const NKikimrTabletBase::TMetrics& values) {
    TStringStream str;
    str << '(';
    str << GetCounter(values.GetCounter());
    str << ",&nbsp;";
    str << GetTimes(values.GetCPU());
    str << ",&nbsp;";
    str << GetBytes(values.GetMemory());
    str << ",&nbsp;";
    str << GetBytesPerSecond(values.GetNetwork());
    str << ')';
    return str.Str();
}

TString GetResourceValuesText(const TResourceRawValues& values) {
    TStringStream str;
    str << '(';
    str << GetCounter(std::get<NMetrics::EResource::Counter>(values));
    str << ',';
    str << GetTimes(std::get<NMetrics::EResource::CPU>(values));
    str << ',';
    str << GetBytes(std::get<NMetrics::EResource::Memory>(values));
    str << ',';
    str << GetBytesPerSecond(std::get<NMetrics::EResource::Network>(values));
    str << ')';
    return str.Str();
}

TString GetResourceValuesText(const TResourceNormalizedValues& values) {
    TStringStream str;
    str << '(';
    str << Sprintf("%.9f", std::get<NMetrics::EResource::Counter>(values));
    str << ',';
    str << Sprintf("%.9f", std::get<NMetrics::EResource::CPU>(values));
    str << ',';
    str << Sprintf("%.9f", std::get<NMetrics::EResource::Memory>(values));
    str << ',';
    str << Sprintf("%.9f", std::get<NMetrics::EResource::Network>(values));
    str << ')';
    return str.Str();
}

TString GetResourceValuesText(const TTabletInfo& tablet) {
    TStringStream str;
    const auto& values(tablet.GetResourceValues());
    auto current(tablet.GetResourceCurrentValues());
    auto maximum(tablet.GetResourceMaximumValues());
    tablet.FilterRawValues(current);
    tablet.FilterRawValues(maximum);
    NMetrics::EResource resource = GetDominantResourceType(current, maximum);
    TVector<i64> allowedMetricIds = tablet.GetTabletAllowedMetricIds();
    str << '(';
    str << GetConditionalBoldString(
               GetConditionalGreyString(
                   GetCounter(values.GetCounter()),
                   Find(allowedMetricIds, NKikimrTabletBase::TMetrics::kCounterFieldNumber) == allowedMetricIds.end()),
               resource == NMetrics::EResource::Counter);
    str << ",&nbsp;";
    str << GetConditionalBoldString(
               GetConditionalGreyString(
                   GetTimes(values.GetCPU()),
                   Find(allowedMetricIds, NKikimrTabletBase::TMetrics::kCPUFieldNumber) == allowedMetricIds.end()),
               resource == NMetrics::EResource::CPU);
    str << ",&nbsp;";
    str << GetConditionalBoldString(
               GetConditionalGreyString(
                   GetBytes(values.GetMemory()),
                   Find(allowedMetricIds, NKikimrTabletBase::TMetrics::kMemoryFieldNumber) == allowedMetricIds.end()),
               resource == NMetrics::EResource::Memory);
    str << ",&nbsp;";
    str << GetConditionalBoldString(
               GetConditionalGreyString(
                   GetBytesPerSecond(values.GetNetwork()),
                   Find(allowedMetricIds, NKikimrTabletBase::TMetrics::kNetworkFieldNumber) == allowedMetricIds.end()),
               resource == NMetrics::EResource::Network);
    str << ')';
    return str.Str();
}

TString GetResourceValuesHtml(const TResourceRawValues& values) {
    TStringStream str;
    str << "<td>";
    str << GetCounter(std::get<NMetrics::EResource::Counter>(values));
    str << "</td><td>";
    str << GetTimes(std::get<NMetrics::EResource::CPU>(values));
    str << "</td><td>";
    str << GetBytes(std::get<NMetrics::EResource::Memory>(values));
    str << "</td><td>";
    str << GetBytesPerSecond(std::get<NMetrics::EResource::Network>(values));
    str << "</td>";
    return str.Str();
}

NJson::TJsonValue GetResourceValuesJson(const TResourceRawValues& values) {
    NJson::TJsonValue value;
    value.AppendValue(GetCounter(std::get<NMetrics::EResource::Counter>(values)));
    value.AppendValue(GetTimes(std::get<NMetrics::EResource::CPU>(values)));
    value.AppendValue(GetBytes(std::get<NMetrics::EResource::Memory>(values)));
    value.AppendValue(GetBytesPerSecond(std::get<NMetrics::EResource::Network>(values)));
    return value;
}

NJson::TJsonValue GetResourceValuesJson(const TResourceRawValues& values, const TResourceRawValues& maximum) {
    NMetrics::EResource resource = GetDominantResourceType(values, maximum);
    NJson::TJsonValue value;
    value.AppendValue(GetConditionalRedString(
                          GetConditionalBoldString(
                              GetCounter(std::get<NMetrics::EResource::Counter>(values)), resource == NMetrics::EResource::Counter),
                          std::get<NMetrics::EResource::Counter>(values) >= std::get<NMetrics::EResource::Counter>(maximum)));
    value.AppendValue(GetConditionalRedString(
                          GetConditionalBoldString(
                              GetTimes(std::get<NMetrics::EResource::CPU>(values)), resource == NMetrics::EResource::CPU),
                          std::get<NMetrics::EResource::CPU>(values) >= std::get<NMetrics::EResource::CPU>(maximum)));
    value.AppendValue(GetConditionalRedString(
                          GetConditionalBoldString(
                              GetBytes(std::get<NMetrics::EResource::Memory>(values)), resource == NMetrics::EResource::Memory),
                          std::get<NMetrics::EResource::Memory>(values) >= std::get<NMetrics::EResource::Memory>(maximum)));
    value.AppendValue(GetConditionalRedString(
                          GetConditionalBoldString(
                              GetBytesPerSecond(std::get<NMetrics::EResource::Network>(values)), resource == NMetrics::EResource::Network),
                          std::get<NMetrics::EResource::Network>(values) >= std::get<NMetrics::EResource::Network>(maximum)));
    return value;
}

TString GetConditionalGreyString(const TString& str, bool condition) {
    if (condition) {
        return "<span style='color:lightgrey'>" + str + "</span>";
    } else {
        return str;
    }
}

TString GetConditionalBoldString(const TString& str, bool condition) {
    if (condition) {
        return "<b>" + str + "</b>";
    } else {
        return str;
    }
}

TString GetConditionalRedString(const TString& str, bool condition) {
    if (condition) {
        return "<span style='color:red'>" + str + "</span>";
    } else {
        return str;
    }
}

TString GetColoredValue(double val, double maxVal) {
    double ratio = val / maxVal;
    TString color;
    if (ratio < 0.9) {
        color = "green";
    } else if (ratio < 1.0) {
        color = "yellow";
    } else {
        color = "red";
    }

    return Sprintf("<span style='color:%s'>%.2f</span>", color.c_str(), val);
}

ui64 GetReadThroughput(const NKikimrTabletBase::TMetrics& values) {
    ui64 acc = 0;
    for (const auto& throughput : values.GetGroupReadThroughput()) {
        acc += throughput.GetThroughput();
    }
    return acc;
}

ui64 GetWriteThroughput(const NKikimrTabletBase::TMetrics& values) {
    ui64 acc = 0;
    for (const auto& throughput : values.GetGroupWriteThroughput()) {
        acc += throughput.GetThroughput();
    }
    return acc;
}

TString GetCounter(ui64 counter, const TString& zero) {
    if (counter == 0) {
        return zero;
    }
    return Sprintf("%lu", counter);
}

TString GetBytes(ui64 bytes, const TString& zero) {
    if (bytes == 0) {
        return zero;
    }
    double value = bytes;
    const char* format = "%.0fB";
    if (value > 1024) {
        value /= 1024;
        format = "%.2fKB";
    }
    if (value > 1024) {
        value /= 1024;
        format = "%.1fMB";
    }
    if (value > 1024) {
        value /= 1024;
        format = "%.0fGB";
    }
    return Sprintf(format, value);
}

TString GetBytesPerSecond(ui64 bytes, const TString& zero) {
    if (bytes == 0) {
        return zero;
    }
    return GetBytes(bytes) + "/s";
}

TString GetTimes(ui64 times, const TString& zero) {
    if (times == 0) {
        return zero;
    }
    return Sprintf("%.2f%%", (double)times * 100 / 1000000);
}

TString GetResourceValuesHtml(const NKikimrTabletBase::TMetrics& values) {
    TStringStream str;
    str << "<td>";
    str << GetCounter(values.GetCounter(), TString());
    str << "</td><td data-text='" << values.GetCPU() << "'>";
    str << GetTimes(values.GetCPU(), TString());
    str << "</td><td data-text='" << values.GetMemory() << "'>";
    str << GetBytes(values.GetMemory(), TString());
    str << "</td><td data-text='" << values.GetNetwork() << "'>";
    str << GetBytesPerSecond(values.GetNetwork(), TString());
    str << "</td><td data-text='" << values.GetStorage() << "'>";
    str << GetBytes(values.GetStorage(), TString());
    ui64 bytes = GetReadThroughput(values);
    str << "</td><td data-text='" << bytes << "'>";
    str << GetBytesPerSecond(bytes, TString());
    bytes = GetWriteThroughput(values);
    str << "</td><td data-text='" << bytes << "'>";
    str << GetBytesPerSecond(bytes, TString());
    str << "</td>";
    return str.Str();
}

NJson::TJsonValue GetResourceValuesJson(const NKikimrTabletBase::TMetrics& values) {
    NJson::TJsonValue value;
    value.AppendValue(GetCounter(values.GetCounter()));
    value.AppendValue(GetTimes(values.GetCPU()));
    value.AppendValue(GetBytes(values.GetMemory()));
    value.AppendValue(GetBytesPerSecond(values.GetNetwork()));
    value.AppendValue(GetBytes(values.GetStorage()));
    value.AppendValue(GetBytesPerSecond(GetReadThroughput(values)));
    value.AppendValue(GetBytesPerSecond(GetWriteThroughput(values)));
    return value;
}

TString GetDataCenterName(ui64 dataCenterId) {
    switch (dataCenterId) {
    case '\0sas':
    case '\0SAS':
        return "sas";
    case '\0man':
    case '\0MAN':
    case '\0nam':
    case '\0NAM':
        return "man";
    case '\0myt':
    case '\0MYT':
    case '\0tym':
    case '\0TYM':
        return "myt";
    case '\0vla':
    case '\0VLA':
    case '\0alv':
    case '\0ALV':
        return "vla";
    case '\0iva':
    case '\0IVA':
    case '\0avi':
    case '\0AVI':
        return "iva";
    case 0:
        return "?";
    default:
        return ToString(dataCenterId);
    }
}

TString LongToShortTabletName(const TString& longTabletName) {
    TString shortName;

    for (char c : longTabletName) {
        if (c >= 'A' && c <= 'Z') {
            shortName += c;
        }
    }
    if (shortName.empty()) {
        shortName = longTabletName;
    }
    return shortName;
}

TString GetLocationString(const NActors::TNodeLocation& location) {
    NActorsInterconnect::TNodeLocation proto;
    location.Serialize(&proto, false);
    return proto.ShortDebugString();
}

void MakeTabletTypeSet(std::vector<TTabletTypes::EType>& list) {
    std::sort(list.begin(), list.end());
    list.erase(std::unique(list.begin(), list.end()), list.end());
}

bool IsValidTabletType(TTabletTypes::EType type) {
    return (type > TTabletTypes::Unknown
            && type < TTabletTypes::Reserved40
            );
}

TString GetBalancerProgressText(i32 balancerProgress, EBalancerType balancerType) {
    TStringBuilder str;
    if (balancerProgress >= 0) {
        str << balancerProgress << "% (" << EBalancerTypeName(balancerType) << ")";
    }
    return str;
}

TString GetRunningTabletsText(ui64 runningTablets, ui64 totalTablets, bool warmUp) {
    TStringBuilder str;
    str << (totalTablets == 0 ? 0 : runningTablets * 100 / totalTablets) << "% "<< runningTablets << "/" << totalTablets;
    if (warmUp) {
        str << " (Warming up...)";
    }
    return str;
}

} // NHive
} // NKikimr
