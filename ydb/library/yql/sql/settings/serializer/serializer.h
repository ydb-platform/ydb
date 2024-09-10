#pragma once

#include <ydb/library/yql/sql/settings/serializer/proto/translation_settings.pb.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>

namespace NSQLTranslation {

struct TTranslationSettingsSerializer {
    void Serialize(const TTranslationSettings& settings, TString& serializedSettings) const;
    void Serialize(const TTranslationSettings& settings, NYql::NProto::TTranslationSettings& serializedSettings) const;

    void Deserialize(const TString& serializedSettings, TTranslationSettings& settings) const;
    void Deserialize(const NYql::NProto::TTranslationSettings& serializedSettings, TTranslationSettings& settings) const;

    std::function<void(TString&)> PathPrefixSetter;
};

}
