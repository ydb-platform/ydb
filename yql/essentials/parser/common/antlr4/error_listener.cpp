#include "error_listener.h"

namespace antlr4 {

    YqlErrorListener::YqlErrorListener(NAST::IErrorCollector* errors, bool* error)
        : errors(errors)
        , error(error)
    {
    }

    void YqlErrorListener::syntaxError(
        Recognizer* /*recognizer*/, Token* /*offendingSymbol*/,
        size_t line, size_t charPositionInLine,
        const std::string& msg, std::exception_ptr /*e*/) {
        *error = true;
        errors->Error(line, charPositionInLine, msg.c_str());
    }

} // namespace antlr4
