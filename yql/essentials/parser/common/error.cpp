#include "error.h"

namespace NAST {

    IErrorCollector::IErrorCollector(size_t maxErrors)
        : MaxErrors_(maxErrors)
        , NumErrors_(0)
    {
        Y_ENSURE(0 < MaxErrors_);
    }

    IErrorCollector::~IErrorCollector()
    {
    }

    void IErrorCollector::Error(ui32 line, ui32 col, const TString& message) {
        if (NumErrors_ + 1 == MaxErrors_) {
            AddError(0, 0, "Too many errors");
            ++NumErrors_;
        }

        if (NumErrors_ >= MaxErrors_) {
            ythrow TTooManyErrors() << "Too many errors";
        }

        AddError(line, col, message);
        ++NumErrors_;
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

} // namespace NAST
