#pragma once

#include <util/stream/output.h>
#include <util/generic/flags.h>

enum class EFieldOutputFlag {
    EscapeTab = 0x1,      // escape \t in field value
    EscapeNewLine = 0x2,  // escape \n in field value
    EscapeBackSlash = 0x4 // escape \ in field value
};

Y_DECLARE_FLAGS(EFieldOutputFlags, EFieldOutputFlag);
Y_DECLARE_OPERATORS_FOR_FLAGS(EFieldOutputFlags);

class TEventFieldOutput: public IOutputStream {
public:
    TEventFieldOutput(IOutputStream& output, EFieldOutputFlags flags);

    IOutputStream& GetOutputStream();
    EFieldOutputFlags GetFlags() const;

protected:
    void DoWrite(const void* buf, size_t len) override;

private:
    IOutputStream& Output;
    EFieldOutputFlags Flags;
    TString Separators;
};
