#include "util.h"

#include <yql/essentials/utils/yql_panic.h>

#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/strip.h>

#include <contrib/libs/re2/re2/re2.h>

namespace {

THolder<re2::RE2> CompileRE2WithCheck(const std::string& pattern) {
    THolder<re2::RE2> re(new re2::RE2(pattern));
    YQL_ENSURE(re->ok(), "Unable to compile regex " << pattern << ": " << re->error());
    return re;
}

const TString LABEL_NAME_PATTERN        = R"( *[a-zA-Z0-9-._/]{1,50} *)";
const TString LABEL_VALUE_PATTERN       = R"( *["'][ -!#-&(-)+->@-_a-{}-~*|-]{1,200}["'] *)";

const TString SENSOR_NAME_PATTERN       = "(" + LABEL_VALUE_PATTERN + ")?({.*})";
THolder<re2::RE2> SENSOR_NAME_RE        = CompileRE2WithCheck(SENSOR_NAME_PATTERN);

const TString SELECTOR_PATTERN          = "(" + LABEL_NAME_PATTERN + "=" + LABEL_VALUE_PATTERN + ")";
THolder<re2::RE2> SELECTOR_RE           = CompileRE2WithCheck(SELECTOR_PATTERN);

const TString SELECTORS_FULL_PATTERN    = "{((" + SELECTOR_PATTERN + ",)*" + SELECTOR_PATTERN + ")?}";
THolder<re2::RE2> SELECTORS_FULL_RE     = CompileRE2WithCheck(SELECTORS_FULL_PATTERN);

const TString USER_LABELS_PATTERN       = "(" + LABEL_NAME_PATTERN + ")(?: (?i:as) (" + LABEL_NAME_PATTERN + "))?";
THolder<re2::RE2> USER_LABELS_RE        = CompileRE2WithCheck(USER_LABELS_PATTERN);

TMaybe<TString> InsertOrCheck(std::map<TString, TString>& selectors, const TString& name, const TString& value) {
    auto [it, inserted] = selectors.emplace(name, value);
    if (!inserted && it->second != value) {
        return TStringBuilder() << "You shouldn't specify \"" << name << "\" label in selectors";
    }
    return {};
}

} // namespace

namespace NYql::NSo {

TMaybe<TString> ParseSelectorValues(const TString& selectors, std::map<TString, TString>& result) {
    std::optional<TString> sensorName;
    TString fullBrackets;
    if (!RE2::FullMatch(selectors, *SENSOR_NAME_RE, &sensorName, &fullBrackets)) {
        return "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format";
    };

    if (sensorName) {
        TString name = StripString(*sensorName);
        result["name"] = name.substr(1, name.size() - 2);
    }

    if (!RE2::FullMatch(fullBrackets, *SELECTORS_FULL_RE)) {
        return "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format";
    }

    std::string_view fullBracketsView = fullBrackets;
    TString selectorValue;
    while (RE2::FindAndConsume(&fullBracketsView, *SELECTOR_RE, &selectorValue)) {
        size_t eqPos = selectorValue.find("=");
    
        TString key = StripString(selectorValue.substr(0, eqPos));
        TString value = StripString(selectorValue.substr(eqPos + 1, selectorValue.size() - eqPos - 1));
    
        result[key] = value.substr(1, value.size() - 2);
    }

    return {};
}

TMaybe<TString> BuildSelectorValues(const NSo::NProto::TDqSolomonSource& source, const TString& selectors, std::map<TString, TString>& result) {
    #define RET_ON_ERROR(expr)      \
        if (auto error = expr) {    \
            return error;           \
        }

    RET_ON_ERROR(ParseSelectorValues(selectors, result));

    if (source.GetClusterType() == NSo::NProto::CT_MONITORING) {
        RET_ON_ERROR(InsertOrCheck(result, "cloudId", source.GetProject()));
        RET_ON_ERROR(InsertOrCheck(result, "folderId", source.GetCluster()));
        RET_ON_ERROR(InsertOrCheck(result, "service", source.GetService()));
    } else {
        RET_ON_ERROR(InsertOrCheck(result, "project", source.GetProject()));
    }

    if (source.GetClusterType() == NSo::NProto::CT_MONITORING) {
        if (auto it = result.find("cloudId"); it != result.end()) {
            result["project"] = it->second;
            result.erase(it);
        }
        if (auto it = result.find("folderId"); it != result.end()) {
            result["cluster"] = it->second;
            result.erase(it);
        }
    }

    #undef RET_ON_ERROR
    return {};
}

TMaybe<TString> ParseLabelNames(const TString& labelNames, TVector<TString>& names, TVector<TString>& aliases) {
    auto labels = StringSplitter(labelNames).Split(',').SkipEmpty().ToList<TString>();
    names.reserve(labels.size());
    aliases.reserve(labels.size());
    
    for (TString& label : labels) {
        TString name;
        std::optional<TString> alias;

        if (!RE2::FullMatch(label, *USER_LABELS_RE, &name, &alias)) {
            return "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format";
        }

        names.push_back(StripString(name));
        aliases.push_back(StripString(alias ? *alias : name));
    }

    return {};
}

NSo::NProto::ESolomonClusterType MapClusterType(TSolomonClusterConfig::ESolomonClusterType clusterType) {
    switch (clusterType) {
        case TSolomonClusterConfig::SCT_SOLOMON:
            return NSo::NProto::ESolomonClusterType::CT_SOLOMON;
        case TSolomonClusterConfig::SCT_MONITORING:
            return NSo::NProto::ESolomonClusterType::CT_MONITORING;
        default:
            YQL_ENSURE(false, "Invalid cluster type " << ToString<ui32>(clusterType));
    }
}

NProto::TDqSolomonSource FillSolomonSource(const TSolomonClusterConfig* config, const TString& project) {
    NSo::NProto::TDqSolomonSource source;

    source.SetClusterType(NSo::MapClusterType(config->GetClusterType()));
    source.SetUseSsl(config->GetUseSsl());
    
    if (source.GetClusterType() == NSo::NProto::CT_MONITORING) {
        source.SetProject(config->GetPath().GetProject());
        source.SetCluster(config->GetPath().GetCluster());
        source.SetService(project);
    } else {
        source.SetProject(project);
    }
    
    source.SetEndpoint(config->GetCluster()); // Backward compatibility
    source.SetHttpEndpoint(config->GetCluster());
    source.SetGrpcEndpoint(config->GetCluster());
    for (const auto& attr : config->settings()) {
        if (attr.name() == "grpc_location"sv) {
            source.SetGrpcEndpoint(attr.value());
        }
    }

    return source;
}

} // namespace NYql::NSo
