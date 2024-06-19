#include "proto_ast.h"

using namespace NProtoAST;

IErrorCollector::IErrorCollector(size_t maxErrors)
    : MaxErrors(maxErrors)
    , NumErrors(0)
{
}

IErrorCollector::~IErrorCollector()
{
}

void IErrorCollector::Error(ui32 line, ui32 col, const TString& message) {
    if (NumErrors + 1 == MaxErrors) {
        AddError(0, 0, "Too many errors");
        ++NumErrors;
    }

    if (NumErrors >= MaxErrors) {
        ythrow TTooManyErrors() << "Too many errors";
    }

    AddError(line, col, message);
    ++NumErrors;
}

TErrorOutput::TErrorOutput(IOutputStream& err, const TString& name, size_t maxErrors)
    : IErrorCollector(maxErrors)
    , Err(err)
    , Name(name)
{
}

TErrorOutput::~TErrorOutput()
{
}

void TErrorOutput::AddError(ui32 line, ui32 col, const TString& message) {
    if (!Name.empty()) {
        Err << "Query " << Name << ": ";
    }
    Err << "Line " << line << " column " << col << " error: " << message;
}

antlr4::YqlErrorListener::YqlErrorListener(NProtoAST::IErrorCollector* errors, bool* error)
    : errors(errors), error(error)
{
}

void antlr4::YqlErrorListener::syntaxError(Recognizer * /*recognizer*/, Token * /*offendingSymbol*/,
  size_t line, size_t charPositionInLine, const std::string &msg, std::exception_ptr /*e*/)  {
    *error = true;
    errors->Error(line, charPositionInLine, msg.c_str());
}