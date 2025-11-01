#include "error_listener.h"

#include <yql/essentials/core/issue/yql_issue.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/join.h>

namespace antlr4 {

TVector<size_t> ToVector(const antlrcpp::BitSet& ambigAlts) {
    TVector<size_t> result;
    for (size_t i = 0; i < ambigAlts.size(); ++i) {
        if (ambigAlts.test(i)) {
            result.push_back(i);
        }
    }
    return result;
}

YqlErrorListener::YqlErrorListener(NAST::IErrorCollector* errors, bool* error, bool isAmbiguityError)
    : Errors_(errors)
    , Error_(error)
    , IsAmbiguityError_(isAmbiguityError)
{
}

void YqlErrorListener::syntaxError(
    Recognizer* /*recognizer*/, Token* /*offendingSymbol*/,
    size_t line, size_t charPositionInLine,
    const std::string& msg, std::exception_ptr /*e*/) {
    *Error_ = true;
    Errors_->Error(line, charPositionInLine, msg.c_str());
}

void YqlErrorListener::reportAmbiguity(
    Parser* recognizer,
    const dfa::DFA& dfa,
    size_t startIndex,
    size_t stopIndex,
    bool exact,
    const antlrcpp::BitSet& ambigAlts,
    atn::ATNConfigSet* configs)
{
    Y_UNUSED(configs);

    size_t ruleIndex = dfa.atnStartState->ruleIndex;
    std::string_view ruleName = recognizer->getRuleNames()[ruleIndex];

    TokenStream* tokens = recognizer->getTokenStream();
    Token* start = tokens->get(startIndex);
    Token* stop = tokens->get(stopIndex);

    TString alternatives = JoinSeq(", ", ToVector(ambigAlts));

    NYql::TPosition startPos(start->getCharPositionInLine(), start->getLine(), "unknown");
    NYql::TPosition stopPos(stop->getCharPositionInLine(), stop->getLine(), "unknown");

    TString message = TStringBuilder()
                      << "An" << (exact ? " exactly " : " ")
                      << "ambiguous decision " << dfa.decision
                      << " at rule '" << ruleName << "'"
                      << " with conflicted alternatives {" << alternatives << "}";

    NYql::TIssue issue(std::move(startPos), std::move(stopPos), std::move(message));

    if (IsAmbiguityError_) {
        *Error_ = true;
        issue.SetCode(NYql::UNEXPECTED_ERROR, NYql::TSeverityIds::S_FATAL);
    } else {
        issue.SetCode(NYql::TIssuesIds::YQL_SYNTAX_AMBIGUITY, NYql::TSeverityIds::S_WARNING);
    }

    Errors_->Report(std::move(issue));
}

} // namespace antlr4
