#pragma once

#include <yql/essentials/parser/common/error.h>

#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>

namespace antlr4 {

class ANTLR4CPP_PUBLIC YqlErrorListener: public BaseErrorListener {
    NAST::IErrorCollector* Errors_;
    bool* Error_;
    const bool IsAmbiguityError_;

public:
    YqlErrorListener(NAST::IErrorCollector* errors, bool* error, bool isAmbiguityError = false);

    virtual void syntaxError(
        Recognizer* recognizer, Token* offendingSymbol,
        size_t line, size_t charPositionInLine,
        const std::string& msg, std::exception_ptr e) override;

    void reportAmbiguity(
        Parser* recognizer,
        const dfa::DFA& dfa,
        size_t startIndex,
        size_t stopIndex,
        bool exact,
        const antlrcpp::BitSet& ambigAlts,
        atn::ATNConfigSet* configs) override;
};

} // namespace antlr4
