#pragma once

#include <util/generic/string.h>

#include <optional>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

// Checks that |token| contains only allowed symbols, throws exception otherwise.
void ValidateToken(TStringBuf token);

// Tries to load the token from the specified file.
// Returns |std::nullopt| if |tokenPath| is not a valid file name.
// Else validates the token and returns it.
std::optional<TString> LoadTokenFromFile(TStringBuf tokenPath);

// Performs standard sequence of attempts to find the token:
// $YT_TOKEN, $YT_SECURE_VAULT_YT_TOKEN, $YT_TOKEN_PATH, "$HOME/.yt/token".
std::optional<TString> LoadToken();

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
