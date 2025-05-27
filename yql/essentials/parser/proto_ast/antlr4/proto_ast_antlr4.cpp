#include "proto_ast_antlr4.h"

antlr4::YqlErrorListener::YqlErrorListener(NProtoAST::IErrorCollector* errors, bool* error)
    : errors(errors), error(error)
{
}

void antlr4::YqlErrorListener::syntaxError(Recognizer * /*recognizer*/, Token * /*offendingSymbol*/,
  size_t line, size_t charPositionInLine, const std::string &msg, std::exception_ptr /*e*/)  {
    *error = true;
    errors->Error(line, charPositionInLine, msg.c_str());
}
