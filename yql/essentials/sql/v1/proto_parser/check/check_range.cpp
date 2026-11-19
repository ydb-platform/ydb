#include "check_range.h"

#include <yql/essentials/sql/v1/proto_parser/range.h>
#include <yql/essentials/utils/strong_alias.h>

#include <google/protobuf/descriptor.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/string/builder.h>

namespace NSQLTranslationV1 {
namespace {

using google::protobuf::FieldDescriptor;
using google::protobuf::Message;

using TPositionKey = NYql::TStrongAlias<class TPositionKeyTag, ui64>;

TPositionKey PositionKey(ui32 line, ui32 column) {
    return TPositionKey((static_cast<ui64>(line) << 32) | column);
}

struct TDerivedRange {
    bool HasToken = false;
    ui32 Start = 0;
    ui32 End = 0;

    void Include(const TDerivedRange& child) {
        if (!child.HasToken) {
            return;
        }
        if (!HasToken) {
            *this = child;
        } else {
            Start = Min(Start, child.Start);
            End = Max(End, child.End);
        }
    }
};

struct TVisitResult {
    TDerivedRange Range;
    ui32 NextTokenIndex;
};

class TRangeChecker final {
public:
    TRangeChecker(const NSQLTranslation::TParsedTokenList& tokens, NYql::TIssues& issues)
        : Tokens_(tokens)
        , Issues_(issues)
    {
        // The full lexer stream retains hidden tokens, so its indices match the parser's.
        for (ui32 index = 0; index < tokens.size(); ++index) {
            const auto& token = tokens[index];
            TokenIndices_.emplace(PositionKey(token.Line, token.LinePos), index);
        }
    }

    bool Check(const Message& tree) {
        if (Tokens_.empty()) {
            Issues_.AddIssue("expected EOF as a last token, but got an empty token stream");
            return false;
        }
        if (Tokens_.back().Name != "EOF") {
            Issues_.AddIssue(TStringBuilder() << "expected EOF as a last token, but got " << Tokens_.back().Name);
            return false;
        }
        if (TokenIndices_.size() != Tokens_.size()) {
            Issues_.AddIssue("lexer tokens have duplicate line and column positions");
            return false;
        }
        return Visit(tree, tree.GetDescriptor()->name(), Tokens_.size() - 1).Defined();
    }

private:
    TMaybe<ui32> TokenIndex(const Message& token, const TString& path) {
        const auto* descriptor = token.GetDescriptor();
        const auto* reflection = token.GetReflection();
        const ui32 line = reflection->GetUInt32(token, descriptor->FindFieldByName("Line"));
        const ui32 column = reflection->GetUInt32(token, descriptor->FindFieldByName("Column"));
        const auto& value = reflection->GetString(token, descriptor->FindFieldByName("Value"));

        const auto* index = TokenIndices_.FindPtr(PositionKey(line, column));
        if (index && Tokens_[*index].Content == value) {
            return *index;
        }
        Issues_.AddIssue(TStringBuilder() << path << ": no lexer token for parser token at " << line << ':' << column << " <" << value << '>');
        return Nothing();
    }

    TMaybe<TVisitResult> Visit(const Message& node, const TString& path, ui32 nextTokenIndex) {
        const auto* descriptor = node.GetDescriptor();
        if (descriptor->name() == "TToken") {
            const auto index = TokenIndex(node, path);
            if (!index) {
                return Nothing();
            }
            return TVisitResult{.Range = {.HasToken = true, .Start = *index, .End = *index}, .NextTokenIndex = *index};
        }

        const auto* reflection = node.GetReflection();
        TDerivedRange derived;
        // Reverse AST child order carries the next token's index to an empty rule.
        for (int i = descriptor->field_count() - 1; i >= 0; --i) {
            const auto* field = descriptor->field(i);
            if (field->cpp_type() != FieldDescriptor::CPPTYPE_MESSAGE) {
                continue;
            }
            if (field->is_repeated()) {
                for (int j = reflection->FieldSize(node, field) - 1; j >= 0; --j) {
                    const auto child = Visit(reflection->GetRepeatedMessage(node, field, j),
                                             TStringBuilder() << path << '.' << field->name() << '[' << j << ']', nextTokenIndex);
                    if (!child) {
                        return Nothing();
                    }
                    derived.Include(child->Range);
                    nextTokenIndex = child->NextTokenIndex;
                }
            } else if (reflection->HasField(node, field)) {
                const auto child = Visit(reflection->GetMessage(node, field),
                                         TStringBuilder() << path << '.' << field->name(), nextTokenIndex);
                if (!child) {
                    return Nothing();
                }
                derived.Include(child->Range);
                nextTokenIndex = child->NextTokenIndex;
            }
        }

        // The EOF terminal is consumed but intentionally has no TToken message.
        if (descriptor->name() == "TRule_sql_stmt_list" || descriptor->name() == "TRule_ansi_sql_stmt_list") {
            derived.Include({.HasToken = true, .Start = static_cast<ui32>(Tokens_.size() - 1), .End = static_cast<ui32>(Tokens_.size() - 1)});
        }

        TTokenIndexRange expected{.Start = nextTokenIndex, .End = static_cast<ui32>(nextTokenIndex - 1)};
        if (derived.HasToken) {
            expected = {.Start = derived.Start, .End = derived.End};
        }
        if (!CheckStoredRange(node, path, expected)) {
            return Nothing();
        }
        return TVisitResult{.Range = derived, .NextTokenIndex = nextTokenIndex};
    }

    bool CheckStoredRange(const Message& node, const TString& path, TTokenIndexRange expected) {
        const auto* descriptor = node.GetDescriptor();
        const auto* start = descriptor->FindFieldByName("StartTokenIndex");
        const auto* end = descriptor->FindFieldByName("EndTokenIndex");
        if (!start || !end || start->cpp_type() != FieldDescriptor::CPPTYPE_UINT32 || end->cpp_type() != FieldDescriptor::CPPTYPE_UINT32) {
            Issues_.AddIssue(TStringBuilder() << path << ": missing token index range on " << descriptor->full_name());
            return false;
        }
        const auto* reflection = node.GetReflection();
        const TTokenIndexRange actual{.Start = reflection->GetUInt32(node, start), .End = reflection->GetUInt32(node, end)};
        if (actual.Start != expected.Start || actual.End != expected.End) {
            Issues_.AddIssue(TStringBuilder() << path << " (" << descriptor->full_name() << ") has token range [" << actual.Start << ", " << actual.End
                                              << "], expected [" << expected.Start << ", " << expected.End << ']');
            return false;
        }
        return true;
    }

    const NSQLTranslation::TParsedTokenList& Tokens_;
    NYql::TIssues& Issues_;
    THashMap<TPositionKey, ui32> TokenIndices_;
};

} // namespace

bool CheckParserRanges(const Message& tree, const NSQLTranslation::TParsedTokenList& tokens, NYql::TIssues& issues) {
    return TRangeChecker(tokens, issues).Check(tree);
}

} // namespace NSQLTranslationV1
