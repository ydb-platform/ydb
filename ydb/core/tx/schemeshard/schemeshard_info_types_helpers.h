#pragma once

#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <variant>

namespace NKikimr::NSchemeShard {

template <typename TItem>
TString GetItemSource(const TItem& item);

template <>
inline TString GetItemSource(const Ydb::Import::ImportFromS3Settings::Item& item) {
    return item.source_prefix();
}

template <>
inline TString GetItemSource(const Ydb::Import::ImportFromFsSettings::Item& item) {
    return item.source_path();
}

template <typename TSettings>
inline TString GetItemSource(const TSettings& settings, size_t i) {
    if (i < ui32(settings.items_size())) {
        return GetItemSource(settings.items(i));
    }
    return {};
}

template <typename TVariant, typename TFunc>
auto VisitSettings(const TVariant& settings, TFunc&& func) {
    return std::visit(std::forward<TFunc>(func), settings);
}

template <typename TSettings>
TSettings ParseSettings(const TString& serializedSettings) {
    TSettings tmpSettings;
    Y_ABORT_UNLESS(tmpSettings.ParseFromString(serializedSettings));
    return tmpSettings;
}

} // namespace NKikimr::NSchemeShard

