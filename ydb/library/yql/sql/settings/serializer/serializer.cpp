#include "serializer.h"

namespace NSQLTranslation {

void TTranslationSettingsSerializer::Serialize(
    const TTranslationSettings& settings, NYql::NProto::TTranslationSettings& serializedSettings
) const {
    serializedSettings.SetPathPrefix(settings.PathPrefix);
    serializedSettings.SetSyntaxVersion(settings.SyntaxVersion);
    serializedSettings.SetAnsiLexer(settings.AnsiLexer);
    serializedSettings.SetAntlr4Parser(settings.Antlr4Parser);
    serializedSettings.SetPgParser(settings.PgParser);

    auto* pragmas = serializedSettings.MutablePragmas();
    pragmas->Add(settings.Flags.begin(), settings.Flags.end());
}

void TTranslationSettingsSerializer::Deserialize(
    const NYql::NProto::TTranslationSettings& serializedSettings, TTranslationSettings& settings
) const {
    #define DeserializeSetting(settingName) \
        if (serializedSettings.Has##settingName()) { \
            settings.settingName = serializedSettings.Get##settingName(); \
        }

        DeserializeSetting(PathPrefix);
        DeserializeSetting(SyntaxVersion);
        DeserializeSetting(AnsiLexer);
        DeserializeSetting(Antlr4Parser);
        DeserializeSetting(PgParser);

    #undef DeserializeSetting

    settings.Flags.insert(serializedSettings.GetPragmas().begin(), serializedSettings.GetPragmas().end());
}

}
