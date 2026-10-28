#include "sql_ddl_session.h"

#include <utility>

namespace NSQLTranslationV1 {

TNodePtr TSessionTranslation::ParseSessionId(const TRule_kill_session_stmt::TBlock3& target) {
    const auto pos = Ctx_.Pos();
    TNodePtr sessionId;
    switch (target.Alt_case()) {
        case TRule_kill_session_stmt::TBlock3::kAlt1:
            sessionId = BuildLiteralRawString(pos, Id(target.GetAlt1().GetRule_id1(), *this), /*isUtf8=*/true);
            break;
        case TRule_kill_session_stmt::TBlock3::kAlt2: {
            TString name;
            if (!NamedNodeImpl(target.GetAlt2().GetRule_bind_parameter1(), name, *this)) {
                return nullptr;
            }
            // Keep the parameter for execution: EvaluateAtom would evaluate it during compilation.
            sessionId = GetNamedNode(name);
            break;
        }
        case TRule_kill_session_stmt::TBlock3::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
    return sessionId;
}

TNodePtr TSessionTranslation::Build(const TRule_kill_session_stmt& rule) {
    Ctx_.BodyPart();
    Ctx_.Token(rule.GetToken1());
    const auto pos = Ctx_.Pos();
    auto sessionId = ParseSessionId(rule.GetBlock3());
    if (!sessionId) {
        return nullptr;
    }
    return BuildKillSession(pos, std::move(sessionId), Ctx_.Scoped);
}

} // namespace NSQLTranslationV1
