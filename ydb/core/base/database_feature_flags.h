#pragma once

#include <ydb/core/protos/feature_flags.pb.h>

#include <library/cpp/protobuf/json/util.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr {

inline TString GetDatabaseSqlFeatureFlagName(const google::protobuf::FieldDescriptor& field) {
    TString name = field.name();
    NProtobufJson::ToSnakeCaseDense(&name);
    name.to_upper();
    return name;
}

// Keep SQL validation and the config writer on the same explicit allowlist.
inline const google::protobuf::FieldDescriptor* FindDatabaseSqlFeatureFlag(TStringBuf name) {
    const auto normalizedName = to_upper(TString(name));
    const auto* descriptor = NKikimrConfig::TFeatureFlags::descriptor();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* field = descriptor->field(i);
        if (field->type() == google::protobuf::FieldDescriptor::TYPE_BOOL
            && field->options().GetExtension(NKikimrConfig::AllowInDatabaseSql)
            && !field->options().GetExtension(NKikimrConfig::RequireRestart)
            && GetDatabaseSqlFeatureFlagName(*field) == normalizedName)
        {
            return field;
        }
    }
    return nullptr;
}

} // namespace NKikimr
