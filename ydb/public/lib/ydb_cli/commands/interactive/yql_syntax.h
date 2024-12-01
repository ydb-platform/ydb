#pragma once

#include <util/generic/fwd.h>

namespace NYdb {
    namespace NConsoleClient {

        // Permits invalid special comments
        bool IsAnsiQuery(const TString& queryUtf8);

        THashSet<TString> GetAllTokens();

        bool IsOperationTokenName(TStringBuf tokenName);
        bool IsPlainIdentifierTokenName(TStringBuf tokenName);
        bool IsQuotedIdentifierTokenName(TStringBuf tokenName);
        bool IsNamespaceTokenName(TStringBuf tokenName);
        bool IsStringTokenName(TStringBuf tokenName);
        bool IsNumberTokenName(TStringBuf tokenName);
        bool IsCommentTokenName(TStringBuf tokenName);

    } // namespace NConsoleClient
} // namespace NYdb
