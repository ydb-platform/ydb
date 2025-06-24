#include "proto_ast_antlr4.h"

antlr4::YqlErrorListener::YqlErrorListener(NProtoAST::IErrorCollector* errors, bool* error)
    : Errors_(errors), Error_(error)
{
}

void antlr4::YqlErrorListener::syntaxError(Recognizer * /*recognizer*/, Token * /*offendingSymbol*/,
  size_t line, size_t charPositionInLine, const std::string &msg, std::exception_ptr /*e*/)  {
    *Error_ = true;
    Errors_->Error(line, charPositionInLine, msg.c_str());
}
