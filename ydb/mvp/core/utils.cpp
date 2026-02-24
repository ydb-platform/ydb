#include "utils.h"

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

namespace NMVP {

const TStringBuf CONFIG_ERROR_PREFIX = "Configuration error: ";

bool TryLoadTokenFromFile(const TString& tokenPath, TString& token, TString& error, const TString& tokenName) {
    const TString tokenSuffix = tokenName.empty() ? TString() : TStringBuilder() << " for token " << tokenName;
    try {
        token = Strip(TUnbufferedFileInput(tokenPath).ReadAll());
    } catch (const yexception& ex) {
        error = TStringBuilder() << "Failed to read token from '" << tokenPath << "'" << tokenSuffix << ": " << ex.what();
        return false;
    }
    if (token.empty()) {
        error = TStringBuilder() << "Token read from '" << tokenPath << "'" << tokenSuffix << " is empty";
        return false;
    }
    return true;
}

} // namespace NMVP
