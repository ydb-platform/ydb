#pragma once

#include <util/generic/yexception.h>
#include <util/generic/fwd.h>

namespace NAST {
    static const char* INVALID_TOKEN_NAME = "nothing";
    static const char* ABSENCE = " absence";

    class TTooManyErrors: public yexception {
    };

    class IErrorCollector {
    public:
        explicit IErrorCollector(size_t maxErrors);
        virtual ~IErrorCollector();

        // throws TTooManyErrors
        void Error(ui32 line, ui32 col, const TString& message);

    private:
        virtual void AddError(ui32 line, ui32 col, const TString& message) = 0;

    protected:
        const size_t MaxErrors;
        size_t NumErrors;
    };

    class TErrorOutput: public IErrorCollector {
    public:
        TErrorOutput(IOutputStream& err, const TString& name, size_t maxErrors);
        virtual ~TErrorOutput();

    private:
        void AddError(ui32 line, ui32 col, const TString& message) override;

    public:
        IOutputStream& Err;
        TString Name;
    };

} // namespace NAST
