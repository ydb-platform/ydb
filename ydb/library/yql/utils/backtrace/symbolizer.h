#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

struct TDllInfo {
    TString Path;
    ui64 BaseAddress;
};

class IBacktraceSymbolizer {
public:
    virtual TString SymbolizeFrame(void* ptr);
    virtual ~IBacktraceSymbolizer();
};

class TBacktraceSymbolizer;

std::unique_ptr<IBacktraceSymbolizer> BuildSymbolizer(bool KikimrFormat = true);
