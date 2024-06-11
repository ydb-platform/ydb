#include "operation_id.h"

#include <google/protobuf/message.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/uri/uri.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace NOperationId {

using namespace NUri;

static const TString QueryIdPrefix = "ydb://preparedqueryid/4?id=";

TString FormatPreparedQueryIdCompat(const TString& in) {
    return QueryIdPrefix + in;
}

bool DecodePreparedQueryIdCompat(const TString& in, TString& out) {
    if (in.size() <= QueryIdPrefix.size()) {
        ythrow yexception() << "Unable to parse input string";
    }
    if (in.compare(0, QueryIdPrefix.size(), QueryIdPrefix) == 0) {
        out = in.substr(QueryIdPrefix.size());
        return true;
    }
    return false;
}

TString ProtoToString(const Ydb::TOperationId& proto) {
    using namespace ::google::protobuf;
    const Reflection& reflection = *proto.GetReflection();
    std::vector<const FieldDescriptor*> fields;
    reflection.ListFields(proto, &fields);
    TStringStream res;
    switch (proto.GetKind()) {
        case Ydb::TOperationId::OPERATION_DDL:
        case Ydb::TOperationId::OPERATION_DML:
            res << "ydb://operation";
            break;
        case Ydb::TOperationId::SESSION_YQL:
            res << "ydb://session";
            break;
        case Ydb::TOperationId::PREPARED_QUERY_ID:
            res << "ydb://preparedqueryid";
            break;
        case Ydb::TOperationId::CMS_REQUEST:
            res << "ydb://cmsrequest";
            break;
        case Ydb::TOperationId::EXPORT:
            res << "ydb://export";
            break;
        case Ydb::TOperationId::IMPORT:
            res << "ydb://import";
            break;
        case Ydb::TOperationId::BUILD_INDEX:
            res << "ydb://buildindex";
            break;
        case Ydb::TOperationId::SCRIPT_EXECUTION:
            res << "ydb://scriptexec";
            break;
        default:
            Y_ABORT_UNLESS(false, "unexpected kind");
    }
    // According to protobuf documentation:
    // Fields (both normal fields and extension fields) will be listed ordered by field number,
    // so we can rely on it to build url string
    for (const FieldDescriptor* field : fields) {
        Y_ASSERT(field != nullptr);
        if (field) {
            if (field->is_repeated()) {
                int size = reflection.FieldSize(proto, field);
                if (size) {
                    res << "?";
                }
                for (int i = 0; i < size; i++) {
                    const auto& message = reflection.GetRepeatedMessage(proto, field, i);
                    const auto& data = dynamic_cast<const Ydb::TOperationId::TData&>(message);
                    TUri::ReEncode(res, data.GetKey());
                    res << "=";
                    TUri::ReEncode(res, data.GetValue());
                    if (i < size - 1) {
                        res << "&";
                    }
                }
            } else {
                res << "/";
                const FieldDescriptor::CppType type = field->cpp_type();
                switch (type) {
                    case FieldDescriptor::CPPTYPE_ENUM:
                        res << reflection.GetEnumValue(proto, field);
                    break;
                    default:
                        Y_ABORT_UNLESS(false, "unexpected protobuf field type");
                    break;
                }
            }
        }
    }
    return res.Str();
}

TOperationId::TOperationId() {
    SetKind(Ydb::TOperationId::UNUSED);
}

TOperationId::TOperationId(const TString &string, bool allowEmpty) {
    if (allowEmpty && string.empty()) {
        SetKind(Ydb::TOperationId::UNUSED);
        return;
    }

    TUri uri;
    TState::EParsed er = uri.Parse(string, TFeature::FeaturesDefault | TFeature::FeatureSchemeFlexible);
    if (er != TState::ParsedOK) {
        ythrow yexception() << "Unable to parse input string";
    }

    const TString& path = uri.PrintS(TField::FlagPath).substr(1); // start from 1 to remove first '/'
    if (path.length() < 1) {
        ythrow yexception() << "Invalid path length";
    }

    int kind;
    if (!TryFromString(path, kind)) {
        ythrow yexception() << "Unable to cast \"kind\" field: " << path;
    }

    if (!EKind_IsValid(kind)) {
        ythrow yexception() << "Invalid operation kind: " << kind;
    }

    SetKind(static_cast<Ydb::TOperationId::EKind>(kind));

    const TString& query = uri.PrintS(TField::FlagQuery);

    if (query) {
        TCgiParameters params(query.substr(1)); // start from 1 to remove first '?'
        for (auto it : params) {
            auto data = AddData();
            data->SetKey(it.first);
            data->SetValue(it.second);
            Index_[it.first].push_back(&data->GetValue());
        }
    }
}

const TVector<const TString*>& TOperationId::GetValue(const TString &key) const {
    auto it = Index_.find(key);
    if (it != Index_.end()) {
        return it->second;
    }
    ythrow yexception() << "Unable to find key: " << key;
}

TString TOperationId::GetSubKind() const {
    auto it = Index_.find("kind");
    if (it == Index_.end()) {
        return TString();
    }

    if (it->second.size() != 1) {
        ythrow yexception() << "Unable to retreive sub-kind";
    }

    return *it->second.at(0);
}

void AddOptionalValue(Ydb::TOperationId& proto, const TString& key, const TString& value) {
    auto data = proto.AddData();
    data->SetKey(key);
    data->SetValue(value);
}

Ydb::TOperationId::EKind ParseKind(const TStringBuf value) {
    if (value.StartsWith("ss/backgrounds")) {
        return Ydb::TOperationId::SS_BG_TASKS;
    }

    if (value.StartsWith("export")) {
        return Ydb::TOperationId::EXPORT;
    }

    if (value.StartsWith("import")) {
        return Ydb::TOperationId::IMPORT;
    }

    if (value.StartsWith("buildindex")) {
        return Ydb::TOperationId::BUILD_INDEX;
    }

    if (value.StartsWith("scriptexec")) {
        return Ydb::TOperationId::SCRIPT_EXECUTION;
    }

    return Ydb::TOperationId::UNUSED;
}

} // namespace NOperationId
} // namespace NKikimr
