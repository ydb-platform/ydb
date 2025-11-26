#pragma once

#include <ydb/public/api/protos/ydb_import.pb.h>
#include <variant>

template <typename TSettings>
struct TItemSourcePathGetter;

#define DEFINE_ITEM_SOURCE_PATH_GETTER(SettingsType, Method) \
    template <> \
    struct TItemSourcePathGetter<SettingsType> { \
        static TString Get(const SettingsType& settings, size_t i) { \
            if (i < ui32(settings.items_size())) { \
                return settings.items(i).Method(); \
            } \
            return {}; \
        } \
    };

DEFINE_ITEM_SOURCE_PATH_GETTER(Ydb::Import::ImportFromS3Settings, source_prefix)
DEFINE_ITEM_SOURCE_PATH_GETTER(Ydb::Import::ImportFromFsSettings, source_path)

#undef DEFINE_ITEM_SOURCE_PATH_GETTER

// Macro to generate a getter method that retrieves a field from settings.
#define IMPORT_SETTINGS_GETTER(MethodName, FieldAccessor) \
    bool MethodName() const { \
        return std::visit([](const auto& settings) { \
            return settings.FieldAccessor(); \
        }, Settings); \
    }

// Macro to generate a case statement for parsing settings from a serialized string.
#define PARSE_SETTINGS_CASE(KindValue, SettingsType) \
    case EKind::KindValue: { \
        SettingsType tmpSettings; \
        Y_ABORT_UNLESS(tmpSettings.ParseFromString(serializedSettings)); \
        Settings = std::move(tmpSettings); \
        break; \
    }

