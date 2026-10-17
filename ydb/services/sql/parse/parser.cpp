#include "parser.h"

#include "lexer.h"

namespace NKikimr::NSql {

namespace {

TParseResult Unsupported(TStringBuf node) {
    TParseResult result;
    result.SqlState = "0A000";
    result.Message = TString(node.empty() ? TStringBuf("end of statement") : node);
    return result;
}

bool IsKeyword(const TToken& token, TStringBuf keyword) {
    return token.Kind == ETokenKind::Identifier && !token.Quoted && token.Text == keyword;
}

} // namespace

TParseResult Parse(TStringBuf sql) {
    TLexer lexer(sql);
    const TToken first = lexer.Next();
    if (first.Kind == ETokenKind::Error) {
        return Unsupported(first.Text);
    }
    if (!IsKeyword(first, "select")) {
        return Unsupported(first.Text);
    }

    TSelectStmt select;
    const TToken item = lexer.Next();
    if (item.Kind == ETokenKind::Star) {
        select.Star = true;
    } else if (item.Kind == ETokenKind::Identifier) {
        select.Columns.push_back({item.Text, item.Quoted});
        for (;;) {
            const TToken separator = lexer.Next();
            if (separator.Kind != ETokenKind::Comma) {
                if (!IsKeyword(separator, "from")) {
                    return Unsupported(separator.Text);
                }
                break;
            }
            const TToken column = lexer.Next();
            if (column.Kind != ETokenKind::Identifier) {
                return Unsupported(column.Text);
            }
            select.Columns.push_back({column.Text, column.Quoted});
        }
    } else {
        return Unsupported(item.Text);
    }

    if (select.Star) {
        const TToken from = lexer.Next();
        if (!IsKeyword(from, "from")) {
            return Unsupported(from.Text);
        }
    }

    const TToken table = lexer.Next();
    if (table.Kind != ETokenKind::Identifier) {
        return Unsupported(table.Text);
    }
    select.From = {table.Text, table.Quoted};

    const TToken tail = lexer.Next();
    if (tail.Kind == ETokenKind::End) {
        TParseResult result;
        result.Select = std::move(select);
        return result;
    }
    if (tail.Kind == ETokenKind::Semicolon) {
        const TToken after = lexer.Next();
        if (after.Kind == ETokenKind::End) {
            TParseResult result;
            result.Select = std::move(select);
            return result;
        }
        return Unsupported(after.Text);
    }
    return Unsupported(tail.Text);
}

} // namespace NKikimr::NSql
