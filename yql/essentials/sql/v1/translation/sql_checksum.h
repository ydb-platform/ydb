#pragma once

#include "checksum.h"
#include "sql_translation.h"

namespace NSQLTranslationV1 {

class TChecksumTranslation final: public TSqlTranslation {
public:
    TChecksumTranslation(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_checksum_table_stmt& rule);

private:
    bool ParseSetting(const TRule_table_settings_entry& entry, TMaybe<TString>& algorithm);
    bool ParseSettings(const TRule_with_table_settings& settings, TMaybe<TString>& algorithm);
};

} // namespace NSQLTranslationV1
