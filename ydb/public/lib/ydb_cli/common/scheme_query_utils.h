#pragma once

#include <util/generic/strbuf.h>

namespace NYdb::NConsoleClient {

// True if the line starts with a YDB scheme (DDL) statement.
//
// Recognizes the leading keyword after optional EXPLAIN [QUERY PLAN].
// Used to block DDL inside interactive transactions before sending to the server.
bool LooksLikeSchemeQuery(TStringBuf line);

// Text of the current statement (segment after the last ';' before the cursor).
TStringBuf GetCurrentStatementPrefix(TStringBuf textBeforeCursor);

// True when TAB-completion is inside a scheme (DDL) statement.
bool IsSchemeQueryCompletionContext(TStringBuf textBeforeCursor);

// True if a keyword completion candidate must be hidden in interactive tx mode.
bool IsExcludedSchemeQueryCompletionKeyword(TStringBuf keywordContent, TStringBuf textBeforeCursor);

} // namespace NYdb::NConsoleClient
