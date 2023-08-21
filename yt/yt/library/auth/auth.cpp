#include "auth.h"

#include <yt/yt/core/misc/error.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>

#include <util/stream/file.h>

#include <util/string/strip.h>

#include <util/system/env.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

void ValidateToken(TStringBuf token)
{
    for (size_t i = 0; i < token.size(); ++i) {
        auto ch = static_cast<ui8>(token[i]);
        if (ch < 0x21 || ch > 0x7e) {
            THROW_ERROR_EXCEPTION("Incorrect token character %qv at position %v", ch, i);
        }
    }
}

std::optional<TString> LoadTokenFromFile(TStringBuf tokenPath)
{
    TFsPath path(tokenPath);
    if (path.IsFile()) {
        auto token = Strip(TIFStream(path).ReadAll());
        ValidateToken(token);
        return token;
    } else {
        return std::nullopt;
    }
}

std::optional<TString> LoadToken()
{
    std::optional<TString> token;
    if (auto envToken = GetEnv("YT_TOKEN")) {
        token = envToken;
    } else if (auto envToken = GetEnv("YT_SECURE_VAULT_YT_TOKEN")) {
        // If this code runs inside a vanilla operation in YT
        // it should not use regular environment variable `YT_TOKEN`
        // because it would be visible in UI.
        // Token should be passed via `secure_vault` parameter in operation spec.
        token = envToken;
    } else if (auto tokenPath = GetEnv("YT_TOKEN_PATH")) {
        token = LoadTokenFromFile(tokenPath);
    } else {
        token = LoadTokenFromFile(JoinFsPaths(GetHomeDir(), ".yt/token"));
    }
    if (token) {
        ValidateToken(*token);
    }
    return token;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
