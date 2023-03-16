#include "path.h"

#include <util/generic/array_size.h>
#include <util/string/builder.h>

namespace NFq {

namespace {

bool ValidResourcePathComponentSymbols[256] = {};

bool MakeValidResourcePathComponentSymbols() {
    char symbols[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-:#";
    for (size_t i = 0; i < Y_ARRAY_SIZE(symbols) - 1; ++i) {
        ValidResourcePathComponentSymbols[static_cast<unsigned char>(symbols[i])] = true;
    }
    return true;
}

const bool ValidResourcePathComponentSymbolsAreInitialized = MakeValidResourcePathComponentSymbols();

bool AreAllValid(TStringBuf component) {
    for (char i : component) {
        if (!ValidResourcePathComponentSymbols[static_cast<unsigned char>(i)]) {
            return false;
        }
    }
    return true;
}

void AppendPath(TStringBuilder& result, TStringBuf component) {
    if (!result.empty()) {
        result << '/';
    }
    if (AreAllValid(component)) {
        result << component;
    } else {
        for (char i : component) {
            if (ValidResourcePathComponentSymbols[static_cast<unsigned char>(i)]) {
                result << i;
            } else {
                result << '_';
            }
        }
    }
}

} // namespace

TString GetRateLimiterResourcePath(TStringBuf cloud, TStringBuf folder, TStringBuf query) {
    TStringBuilder result;
    AppendPath(result, cloud);
    AppendPath(result, folder);
    AppendPath(result, query);
    return result;
}

TString GetRateLimiterResourcePath(TStringBuf cloud) {
    TStringBuilder result;
    AppendPath(result, cloud);
    return result;
}

} // namespace NFq
