#pragma once

#include "ddl_session.h"
#include "sql_translation.h"

namespace NSQLTranslationV1 {

class TSessionTranslation final: public TSqlTranslation {
public:
    TSessionTranslation(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_kill_session_stmt& rule);

private:
    TNodePtr ParseSessionId(const TRule_kill_session_stmt::TBlock3& target);
};

} // namespace NSQLTranslationV1
