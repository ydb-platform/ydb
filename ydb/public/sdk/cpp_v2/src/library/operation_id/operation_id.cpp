#include <ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <google/protobuf/message.h>

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

std::string ProtoToString(const Ydb::TOperationId& proto) {
    using namespace ::google::protobuf;
    const Reflection& reflection = *proto.GetReflection();
    std::vector<const FieldDescriptor*> fields;
    reflection.ListFields(proto, &fields);
    TStringStream res;
    switch (proto.kind()) {
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
                    TUri::ReEncode(res, data.key());
                    res << "=";
                    TUri::ReEncode(res, data.value());
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
    Proto_.set_kind(Ydb::TOperationId::UNUSED);
}

TOperationId::TOperationId(const std::string &string, bool allowEmpty) {
    if (allowEmpty && string.empty()) {
        Proto_.set_kind(Ydb::TOperationId::UNUSED);
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

    if (!Proto_.EKind_IsValid(kind)) {
        ythrow yexception() << "Invalid operation kind: " << kind;
    }

    Proto_.set_kind(static_cast<Ydb::TOperationId::EKind>(kind));

    std::string query = uri.PrintS(TField::FlagQuery);

    if (!query.empty()) {
        TCgiParameters params(query.substr(1)); // start from 1 to remove first '?'
        for (auto it : params) {
            auto data = Proto_.add_data();
            data->set_key(it.first);
            data->set_value(it.second);
            Index_[it.first].push_back(&data->value());
        }
    }
}

const Ydb::TOperationId& TOperationId::GetProto() const {
    return Proto_;
}

const std::vector<const TStringType*>& TOperationId::GetValue(const std::string &key) const {
    auto it = Index_.find(key);
    if (it != Index_.end()) {
        return it->second;
    }
    ythrow yexception() << "Unable to find key: " << key;
}

std::string TOperationId::GetSubKind() const {
    auto it = Index_.find("kind");
    if (it == Index_.end()) {
        return std::string();
    }

    if (it->second.size() != 1) {
        ythrow yexception() << "Unable to retreive sub-kind";
    }

    return *it->second.at(0);
}

void AddOptionalValue(Ydb::TOperationId& proto, const std::string& key, const std::string& value) {
    auto data = proto.add_data();
    data->set_key(key);
    data->set_value(value);
}

Ydb::TOperationId::EKind ParseKind(const std::string_view value) {
    if (value.starts_with("ss/backgrounds")) {
        return Ydb::TOperationId::SS_BG_TASKS;
    }

    if (value.starts_with("export")) {
        return Ydb::TOperationId::EXPORT;
    }

    if (value.starts_with("import")) {
        return Ydb::TOperationId::IMPORT;
    }

    if (value.starts_with("buildindex")) {
        return Ydb::TOperationId::BUILD_INDEX;
    }

    if (value.starts_with("scriptexec")) {
        return Ydb::TOperationId::SCRIPT_EXECUTION;
    }

    return Ydb::TOperationId::UNUSED;
}

} // namespace NOperationId
} // namespace NKikimr
