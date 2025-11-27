#pragma once

#include <ydb/public/api/protos/ydb_import.pb.h>
#include <variant>

namespace NKikimr::NSchemeShard {

template <typename TSettings>
struct TItemSourcePathGetter;

template <>
struct TItemSourcePathGetter<Ydb::Import::ImportFromS3Settings> {
    static TString Get(const Ydb::Import::ImportFromS3Settings& settings, size_t i) {
        if (i < ui32(settings.items_size())) {
            return settings.items(i).source_prefix();
        }
        return {};
    }
};

template <>
struct TItemSourcePathGetter<Ydb::Import::ImportFromFsSettings> {
    static TString Get(const Ydb::Import::ImportFromFsSettings& settings, size_t i) {
        if (i < ui32(settings.items_size())) {
            return settings.items(i).source_path();
        }
        return {};
    }
};


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

