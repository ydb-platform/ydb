#include <ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/uri/uri.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace NOperationId {

using namespace NUri;

static const std::string QueryIdPrefix = "ydb://preparedqueryid/4?id=";

std::string FormatPreparedQueryIdCompat(const std::string& in) {
    return QueryIdPrefix + in;
}

bool DecodePreparedQueryIdCompat(const std::string& in, std::string& out) {
    if (in.size() <= QueryIdPrefix.size()) {
        ythrow yexception() << "Unable to parse input string";
    }
    if (in.compare(0, QueryIdPrefix.size(), QueryIdPrefix) == 0) {
        out = in.substr(QueryIdPrefix.size());
        return true;
    }
    return false;
}

std::string TOperationId::ToString() const {
    TStringStream res;
    switch (Kind) {
        case TOperationId::OPERATION_DDL:
        case TOperationId::OPERATION_DML:
            res << "ydb://operation";
            break;
        case TOperationId::SESSION_YQL:
            res << "ydb://session";
            break;
        case TOperationId::PREPARED_QUERY_ID:
            res << "ydb://preparedqueryid";
            break;
        case TOperationId::CMS_REQUEST:
            res << "ydb://cmsrequest";
            break;
        case TOperationId::EXPORT:
            res << "ydb://export";
            break;
        case TOperationId::IMPORT:
            res << "ydb://import";
            break;
        case TOperationId::BUILD_INDEX:
            res << "ydb://buildindex";
            break;
        case TOperationId::SCRIPT_EXECUTION:
            res << "ydb://scriptexec";
            break;
        default:
            Y_ABORT_UNLESS(false, "unexpected kind");
    }

    res << "/" << static_cast<int>(Kind);
    if (!Data.empty()) {
        res << "?";
    }

    for (size_t i = 0; i < Data.size(); ++i) {
        TUri::ReEncode(res, Data[i]->Key);
        res << "=";
        TUri::ReEncode(res, Data[i]->Value);
        if (i < Data.size() - 1) {
            res << "&";
        }
    }

    return res.Str();
}

TOperationId::TOperationId() {
    Kind = TOperationId::UNUSED;
}

TOperationId::TOperationId(const std::string &string, bool allowEmpty) {
    if (allowEmpty && string.empty()) {
        Kind = TOperationId::UNUSED;
        return;
    }

    TUri uri;
    TState::EParsed er = uri.Parse(string, TFeature::FeaturesDefault | TFeature::FeatureSchemeFlexible);
    if (er != TState::ParsedOK) {
        ythrow yexception() << "Unable to parse input string";
    }

    std::string path = uri.PrintS(TField::FlagPath).substr(1); // start from 1 to remove first '/'
    if (path.length() < 1) {
        ythrow yexception() << "Invalid path length";
    }

    int kind;
    if (!TryFromString(path, kind)) {
        ythrow yexception() << "Unable to cast \"kind\" field: " << path;
    }

    if (!IsValidKind(kind)) {
        ythrow yexception() << "Invalid operation kind: " << kind;
    }

    Kind = static_cast<TOperationId::EKind>(kind);

    std::string query = uri.PrintS(TField::FlagQuery);

    if (!query.empty()) {
        TCgiParameters params(query.substr(1)); // start from 1 to remove first '?'
        for (auto it : params) {
            Data.push_back(std::make_unique<TData>(it.first, it.second));
            Index[it.first].push_back(&Data.back()->Value);
        }
    }
}

TOperationId::EKind TOperationId::GetKind() const {
    return Kind;
}

TOperationId::EKind& TOperationId::GetMutableKind() {
    return Kind;
}

const TOperationId::TDataList& TOperationId::GetData() const {
    return Data;
}

TOperationId::TDataList& TOperationId::GetMutableData() {
    return Data;
}

const std::vector<const std::string*>& TOperationId::GetValue(const std::string &key) const {
    auto it = Index.find(key);
    if (it != Index.end()) {
        return it->second;
    }
    ythrow yexception() << "Unable to find key: " << key;
}

std::string TOperationId::GetSubKind() const {
    auto it = Index.find("kind");
    if (it == Index.end()) {
        return std::string();
    }

    if (it->second.size() != 1) {
        ythrow yexception() << "Unable to retreive sub-kind";
    }

    return *it->second.at(0);
}

void AddOptionalValue(TOperationId& operationId, const std::string& key, const std::string& value) {
    operationId.GetMutableData().push_back(std::make_unique<TOperationId::TData>(key, value));
}

TOperationId::EKind ParseKind(const std::string_view value) {
    if (value.starts_with("ss/backgrounds")) {
        return TOperationId::SS_BG_TASKS;
    }

    if (value.starts_with("export")) {
        return TOperationId::EXPORT;
    }

    if (value.starts_with("import")) {
        return TOperationId::IMPORT;
    }

    if (value.starts_with("buildindex")) {
        return TOperationId::BUILD_INDEX;
    }

    if (value.starts_with("scriptexec")) {
        return TOperationId::SCRIPT_EXECUTION;
    }

    return TOperationId::UNUSED;
}

bool TOperationId::IsValidKind(int kind) {
    if (kEKindMinValue <= kind && kind <= kEKindMaxValue) {
        return true;
    }
    return false;
}

} // namespace NOperationId
} // namespace NKikimr
