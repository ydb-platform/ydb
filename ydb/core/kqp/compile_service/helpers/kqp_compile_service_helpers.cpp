#include "kqp_compile_service_helpers.h"
#include "google/protobuf/descriptor.pb.h"
#include "util/generic/yexception.h"
#include "util/string/builder.h"

#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/core/protos/kqp_compile_settings.pb.h>

#include <google/protobuf/util/message_differencer.h>
#include <google/protobuf/descriptor.h>

namespace NKikimr::NKqp {

std::vector<const google::protobuf::FieldDescriptor*> BuildFieldsToInvalidateCacheOnDiff(const google::protobuf::Descriptor* d) {
    std::vector<const google::protobuf::FieldDescriptor*> invalidateCacheOn;

    for (int fieldIndex = 0; fieldIndex < d->field_count(); ++fieldIndex) {
        const auto* field = d->field(fieldIndex);
        if (field->options().GetExtension(NKikimrConfig::InvalidateCompileCache)) {
            invalidateCacheOn.push_back(field);
        }
    }

    return invalidateCacheOn;
}


std::optional<TString> ShouldInvalidateCompileCache(const NKikimrConfig::TTableServiceConfig& prev, const NKikimrConfig::TTableServiceConfig& next)
{
    TStringBuilder finalMessage;

    bool ok = true;
    static const auto TopLevelOptions = BuildFieldsToInvalidateCacheOnDiff(NKikimrConfig::TTableServiceConfig::descriptor());
    static const auto RMOptions = BuildFieldsToInvalidateCacheOnDiff(NKikimrConfig::TTableServiceConfig::TResourceManager::descriptor());

    {
        TString logMessage;
        ::google::protobuf::util::MessageDifferencer differ;
        differ.ReportDifferencesToString(&logMessage);
        if (!differ.CompareWithFields(prev, next, TopLevelOptions, TopLevelOptions)) {
            ok = false;
            finalMessage << logMessage;
        }
    }

    {
        TString logMessage;
        ::google::protobuf::util::MessageDifferencer differ;
        differ.ReportDifferencesToString(&logMessage);
        if (!differ.CompareWithFields(prev.GetResourceManager(), next.GetResourceManager(), RMOptions, RMOptions)) {
            ok = false;
            finalMessage << logMessage;
        }
    }

    if (ok) {
        return std::nullopt;
    }

    return TString(finalMessage);
}

}