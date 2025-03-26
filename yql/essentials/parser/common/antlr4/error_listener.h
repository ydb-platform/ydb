#pragma once

#include <yql/essentials/parser/common/error.h>

#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>

namespace antlr4 {

    class ANTLR4CPP_PUBLIC YqlErrorListener: public BaseErrorListener {
        NAST::IErrorCollector* errors;
        bool* error;

    public:
        YqlErrorListener(NAST::IErrorCollector* errors, bool* error);

        virtual void syntaxError(
            Recognizer* recognizer, Token* offendingSymbol,
            size_t line, size_t charPositionInLine,
            const std::string& msg, std::exception_ptr e) override;
    };

} // namespace antlr4
