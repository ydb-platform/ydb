#include "sql_format.h"

#include <yql/essentials/sql/v1/proto_parser/parse_tree.h>

#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/core/sql_types/simple_types.h>

#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>

#include <library/cpp/protobuf/util/simple_reflection.h>
#include <library/cpp/resource/resource.h>

#include <util/string/builder.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/string/subst.h>
#include <util/generic/hash_set.h>

namespace NSQLFormat {

namespace {

using namespace NSQLv1Generated;
using namespace NSQLTranslationV1;

using NSQLTranslation::TParsedToken;
using NSQLTranslation::TParsedTokenList;
using TTokenIterator = TParsedTokenList::const_iterator;

TTokenIterator SkipWS(TTokenIterator curr, TTokenIterator end) {
    while (curr != end && curr->Name == "WS") {
        ++curr;
    }
    return curr;
}

TTokenIterator SkipWSOrComment(TTokenIterator curr, TTokenIterator end) {
    while (curr != end && (curr->Name == "WS" || curr->Name == "COMMENT")) {
        ++curr;
    }
    return curr;
}

TTokenIterator SkipWSOrCommentBackward(TTokenIterator curr, TTokenIterator begin) {
    while (curr != begin && (curr->Name == "WS" || curr->Name == "COMMENT")) {
        --curr;
    }
    return curr;
}

ui32 LeadingNLsCount(TStringBuf str) {
    ui32 count = 0;
    for (ui32 i = 0; i < str.size(); ++i) {
        char c = str[i];

        if (c == '\n') {
            count++;
        } else if (c == '\r') {
            count++;
            if (i + 1 < str.size() && str[i + 1] == '\n') {
                i++;
            }
        } else if (!IsAsciiSpace(c)) {
            break;
        }
    }
    return count;
}

void SkipForValidate(
    TTokenIterator& in,
    TTokenIterator& out,
    const TParsedTokenList& query,
    const TParsedTokenList& formattedQuery,
    i32& parenthesesBalance) {
    in = SkipWS(in, query.end());
    out = SkipWS(out, formattedQuery.end());

    auto inSkippedComments = SkipWSOrComment(in, query.end());

    auto skipAddedToken = [&](const TString& addedToken, const TVector<TString>& beforeTokens, const TVector<TString>& afterTokens) {
        if (
            out != formattedQuery.end() && out->Name == addedToken &&
            (in == query.end() || in->Name != addedToken) &&
            (beforeTokens.empty() || (inSkippedComments != query.end() && IsIn(beforeTokens, inSkippedComments->Name))) &&
            (afterTokens.empty() || (in != query.begin() && IsIn(afterTokens, SkipWSOrCommentBackward(in - 1, query.begin())->Name)))) {
            out = SkipWS(++out, formattedQuery.end());
            if (addedToken == "LPAREN") {
                ++parenthesesBalance;
            } else if (addedToken == "RPAREN") {
                --parenthesesBalance;
            }
        }
    };

    skipAddedToken("LPAREN", {}, {"EQUALS"});
    skipAddedToken("RPAREN", {"END", "EOF", "SEMICOLON"}, {});
    skipAddedToken("SEMICOLON", {"END", "RBRACE_CURLY"}, {});

    auto skipDeletedToken = [&](const TString& deletedToken, const TVector<TString>& afterTokens) {
        if (
            in != query.end() && in->Name == deletedToken &&
            (out == formattedQuery.end() || out->Name != deletedToken) &&
            in != query.begin() && IsIn(afterTokens, SkipWSOrCommentBackward(in - 1, query.begin())->Name)) {
            in = SkipWS(++in, query.end());
            return true;
        }
        return false;
    };

    while (skipDeletedToken("SEMICOLON", {"AS", "BEGIN", "LBRACE_CURLY", "SEMICOLON"})) {
    }
}

TParsedToken TransformTokenForValidate(TParsedToken token) {
    if (token.Name == "EQUALS2") {
        token.Name = "EQUALS";
        token.Content = "=";
    } else if (token.Name == "NOT_EQUALS2") {
        token.Name = "NOT_EQUALS";
        token.Content = "!=";
    }
    return token;
}

TStringBuf SkipQuotes(const TString& content) {
    TStringBuf str = content;
    str.SkipPrefix("\"");
    str.ChopSuffix("\"");
    str.SkipPrefix("'");
    str.ChopSuffix("'");
    return str;
}

TStringBuf SkipNewline(const TString& content) {
    TStringBuf str = content;
    str.ChopSuffix("\n");
    return str;
}

bool Validate(const TParsedTokenList& query, const TParsedTokenList& formattedQuery) {
    auto in = query.begin();
    auto out = formattedQuery.begin();
    auto inEnd = query.end();
    auto outEnd = formattedQuery.end();

    i32 parenthesesBalance = 0;

    while (in != inEnd && out != outEnd) {
        SkipForValidate(in, out, query, formattedQuery, parenthesesBalance);
        if (in != inEnd && out != outEnd) {
            auto inToken = TransformTokenForValidate(*in);
            auto outToken = TransformTokenForValidate(*out);
            if (inToken.Name != outToken.Name) {
                return false;
            }
            if (IsProbablyKeyword(inToken)) {
                if (!AsciiEqualsIgnoreCase(inToken.Content, outToken.Content)) {
                    return false;
                }
            } else if (inToken.Name == "STRING_VALUE") {
                if (SkipQuotes(inToken.Content) != SkipQuotes(outToken.Content)) {
                    return false;
                }
            } else if (inToken.Name == "COMMENT") {
                if (SkipNewline(inToken.Content) != SkipNewline(outToken.Content)) {
                    return false;
                }
            } else {
                if (inToken.Content != outToken.Content) {
                    return false;
                }
            }
            ++in;
            ++out;
        }
    }
    SkipForValidate(in, out, query, formattedQuery, parenthesesBalance);

    return in == inEnd && out == outEnd && parenthesesBalance == 0;
}

enum class EScope {
    Default,
    TypeName,
    Identifier,
    DoubleQuestion
};

class TPrettyVisitor;
using TPrettyFunctor = std::function<void(TPrettyVisitor&, const NProtoBuf::Message& msg)>;
class TObfuscatingVisitor;
using TObfuscatingFunctor = std::function<void(TObfuscatingVisitor&, const NProtoBuf::Message& msg)>;

struct TStaticData {
    TStaticData();
    static const TStaticData& GetInstance() {
        return *Singleton<TStaticData>();
    }

    THashSet<TString> Keywords;
    THashMap<const NProtoBuf::Descriptor*, EScope> ScopeDispatch;
    THashMap<const NProtoBuf::Descriptor*, TPrettyFunctor> PrettyVisitDispatch;
    THashMap<const NProtoBuf::Descriptor*, TObfuscatingFunctor> ObfuscatingVisitDispatch;
};

template <typename T, void (T::*Func)(const NProtoBuf::Message&)>
void VisitAllFieldsImpl(T* obj, const NProtoBuf::Descriptor* descr, const NProtoBuf::Message& msg) {
    for (int i = 0; i < descr->field_count(); ++i) {
        const NProtoBuf::FieldDescriptor* fd = descr->field(i);
        NProtoBuf::TConstField field(msg, fd);
        if (field.IsMessage()) {
            for (size_t j = 0; j < field.Size(); ++j) {
                (obj->*Func)(*field.template Get<NProtoBuf::Message>(j));
            }
        }
    }
}

class TObfuscatingVisitor {
    friend struct TStaticData;

public:
    TObfuscatingVisitor()
        : StaticData_(TStaticData::GetInstance())
    {
    }

    TString Process(const NProtoBuf::Message& msg) {
        Scopes_.push_back(EScope::Default);
        Visit(msg);
        return Sb_;
    }

private:
    void VisitToken(const TToken& token) {
        auto str = token.GetValue();
        if (str == "<EOF>") {
            return;
        }

        if (!First_) {
            Sb_ << ' ';
        } else {
            First_ = false;
        }

        if (str == "$" && FuncCall_) {
            FuncCall_ = false;
        }

        if (Scopes_.back() == EScope::Identifier && !FuncCall_) {
            if (str != "$" && !NYql::LookupSimpleTypeBySqlAlias(str, true)) {
                Sb_ << "id";
            } else {
                Sb_ << str;
            }
        } else if (NextToken_) {
            Sb_ << *NextToken_;
            NextToken_ = Nothing();
        } else {
            Sb_ << str;
        }
    }

    void VisitPragmaValue(const TRule_pragma_value& msg) {
        switch (msg.Alt_case()) {
            case TRule_pragma_value::kAltPragmaValue1: {
                NextToken_ = "0";
                break;
            }
            case TRule_pragma_value::kAltPragmaValue3: {
                NextToken_ = "'str'";
                break;
            }
            case TRule_pragma_value::kAltPragmaValue4: {
                NextToken_ = "false";
                break;
            }
            default:;
        }
        VisitAllFields(TRule_pragma_value::GetDescriptor(), msg);
    }

    void VisitLiteralValue(const TRule_literal_value& msg) {
        switch (msg.Alt_case()) {
            case TRule_literal_value::kAltLiteralValue1: {
                NextToken_ = "0";
                break;
            }
            case TRule_literal_value::kAltLiteralValue2: {
                NextToken_ = "0.0";
                break;
            }
            case TRule_literal_value::kAltLiteralValue3: {
                NextToken_ = "'str'";
                break;
            }
            case TRule_literal_value::kAltLiteralValue9: {
                NextToken_ = "false";
                break;
            }
            default:;
        }

        VisitAllFields(TRule_literal_value::GetDescriptor(), msg);
    }

    void VisitAtomExpr(const TRule_atom_expr& msg) {
        switch (msg.Alt_case()) {
            case TRule_atom_expr::kAltAtomExpr7: {
                FuncCall_ = true;
                break;
            }
            default:;
        }

        VisitAllFields(TRule_atom_expr::GetDescriptor(), msg);
        FuncCall_ = false;
    }

    void VisitInAtomExpr(const TRule_in_atom_expr& msg) {
        switch (msg.Alt_case()) {
            case TRule_in_atom_expr::kAltInAtomExpr6: {
                FuncCall_ = true;
                break;
            }
            default:;
        }

        VisitAllFields(TRule_in_atom_expr::GetDescriptor(), msg);
        FuncCall_ = false;
    }

    void VisitUnaryCasualSubexpr(const TRule_unary_casual_subexpr& msg) {
        bool invoke = false;
        for (auto& b : msg.GetRule_unary_subexpr_suffix2().GetBlock1()) {
            switch (b.GetBlock1().Alt_case()) {
                case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt2: {
                    invoke = true;
                    break;
                }
                default:;
            }

            break;
        }

        if (invoke) {
            FuncCall_ = true;
        }

        Visit(msg.GetBlock1());
        if (invoke) {
            FuncCall_ = false;
        }

        Visit(msg.GetRule_unary_subexpr_suffix2());
    }

    void VisitInUnaryCasualSubexpr(const TRule_in_unary_casual_subexpr& msg) {
        bool invoke = false;
        for (auto& b : msg.GetRule_unary_subexpr_suffix2().GetBlock1()) {
            switch (b.GetBlock1().Alt_case()) {
                case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt2: {
                    invoke = true;
                    break;
                }
                default:;
            }

            break;
        }

        if (invoke) {
            FuncCall_ = true;
        }

        Visit(msg.GetBlock1());
        if (invoke) {
            FuncCall_ = false;
        }

        Visit(msg.GetRule_unary_subexpr_suffix2());
    }

    void Visit(const NProtoBuf::Message& msg) {
        const NProtoBuf::Descriptor* descr = msg.GetDescriptor();
        auto scopePtr = StaticData_.ScopeDispatch.FindPtr(descr);
        if (scopePtr) {
            Scopes_.push_back(*scopePtr);
        }

        auto funcPtr = StaticData_.ObfuscatingVisitDispatch.FindPtr(descr);
        if (funcPtr) {
            (*funcPtr)(*this, msg);
        } else {
            VisitAllFields(descr, msg);
        }

        if (scopePtr) {
            Scopes_.pop_back();
        }
    }

    void VisitAllFields(const NProtoBuf::Descriptor* descr, const NProtoBuf::Message& msg) {
        VisitAllFieldsImpl<TObfuscatingVisitor, &TObfuscatingVisitor::Visit>(this, descr, msg);
    }

    const TStaticData& StaticData_;
    TStringBuilder Sb_;
    bool First_ = true;
    TMaybe<TString> NextToken_;
    TVector<EScope> Scopes_;
    bool FuncCall_ = false;
};

class TPrettyVisitor {
    friend struct TStaticData;

public:
    TPrettyVisitor(const TParsedTokenList& parsedTokens, const TParsedTokenList& comments, bool ansiLexer)
        : StaticData_(TStaticData::GetInstance())
        , ParsedTokens_(parsedTokens)
        , Comments_(comments)
        , AnsiLexer_(ansiLexer)
    {
    }

    TString Process(const NProtoBuf::Message& msg, bool& addLineBefore, bool& addLineAfter, TMaybe<ui32>& stmtCoreAltCase) {
        Scopes_.push_back(EScope::Default);
        MarkedTokens_.reserve(ParsedTokens_.size());
        MarkTokens(msg);
        Y_ENSURE(MarkTokenStack_.empty());
        Y_ENSURE(TokenIndex_ == ParsedTokens_.size());

        TokenIndex_ = 0;
        Visit(msg);
        Y_ENSURE(TokenIndex_ == ParsedTokens_.size());
        Y_ENSURE(MarkTokenStack_.empty());

        for (; LastComment_ < Comments_.size(); ++LastComment_) {
            const auto text = Comments_[LastComment_].Content;
            AddComment(text);
        }

        ui32 lines = OutLine_ - (OutColumn_ == 0 ? 1 : 0);
        addLineBefore = AddLine_.GetOrElse(true) || lines > 1;
        addLineAfter = AddLine_.GetOrElse(true) || lines - CommentLines_ > 1;
        stmtCoreAltCase = StmtCoreAltCase_;

        return Sb_;
    }

private:
    struct TTokenInfo {
        bool OpeningBracket = false;
        bool ClosingBracket = false;
        bool BracketForcedExpansion = false;
        ui32 ClosingBracketIndex = 0;
    };

    using TMarkTokenStack = TVector<ui32>;

    void Out(TStringBuf s) {
        for (ui32 i = 0; i < s.size(); ++i) {
            Out(s[i], i == 0);
        }
    }

    void Out(char c, bool useIndent = true) {
        if (c == '\n' || c == '\r') {
            Sb_ << c;
            if (!(c == '\n' && !Sb_.empty() && Sb_.back() == '\r')) {
                // do not increase OutLine if \n is preceded by \r
                // this way we handle \r, \n, or \r\n as single new line
                ++OutLine_;
            }
            OutColumn_ = 0;
        } else {
            if (!OutColumn_ && useIndent) {
                ui32 indent = (CurrentIndent_ >= 0) ? CurrentIndent_ : 0;
                for (ui32 i = 0; i < indent; ++i) {
                    Sb_ << ' ';
                }
            }

            Sb_ << c;
            ++OutColumn_;
        }
    }

    void NewLine() {
        if (TokenIndex_ >= ParsedTokens_.size() || ParsedTokens_[TokenIndex_].Line > LastLine_) {
            WriteComments(true);
        }

        if (OutColumn_) {
            Out('\n');
        }
    }

    void AddComment(TStringBuf text) {
        if (!AfterComment_ && OutLine_ > BlockFirstLine_ && OutColumn_ == 0) {
            Out('\n');
        }
        AfterComment_ = true;

        if (!Sb_.empty() && Sb_.back() != ' ' && Sb_.back() != '\n') {
            Out(' ');
        }

        if (OutColumn_ == 0) {
            ++CommentLines_;
        }

        if (!text.StartsWith("--")) {
            CommentLines_ += CountIf(text, [](auto c) { return c == '\n'; });
        }

        Out(text);

        if (text.StartsWith("--") && !text.EndsWith("\n")) {
            Out('\n');
        }

        if (!text.StartsWith("--") &&
            TokenIndex_ < ParsedTokens_.size() &&
            Comments_[LastComment_].Line < ParsedTokens_[TokenIndex_].Line &&
            (LastComment_ + 1 >= Comments_.size() || Comments_[LastComment_].Line < Comments_[LastComment_ + 1].Line)) {
            Out('\n');
        }
    }

    bool HasCommentBetweenTokens(ui32 prevTokenIndex, ui32 curTokenIndex) const {
        return curTokenIndex < ParsedTokens_.size() &&
               prevTokenIndex < ParsedTokens_.size() &&
               LastComment_ < Comments_.size() &&
               Comments_[LastComment_].Line > ParsedTokens_[prevTokenIndex].Line &&
               Comments_[LastComment_].Line < ParsedTokens_[curTokenIndex].Line;
    }

    void AddNewlineBetweenStatementsIfNeeded(bool curIsSimpleStatement, bool prevIsSimpleStatement) {
        bool hasCommentAfterPrevStatement = TokenIndex_ > 0 && HasCommentBetweenTokens(TokenIndex_ - 1, TokenIndex_);
        bool hasNewlinesAfterPrevStatement = TokenIndex_ > 0 && TokenIndex_ < ParsedTokens_.size() &&
                                             ParsedTokens_[TokenIndex_].Line - ParsedTokens_[TokenIndex_ - 1].Line > 1;
        if (!curIsSimpleStatement || hasNewlinesAfterPrevStatement &&
                                         !hasCommentAfterPrevStatement && prevIsSimpleStatement) {
            Out('\n');
        }
    }

    void MarkTokens(const NProtoBuf::Message& msg) {
        const NProtoBuf::Descriptor* descr = msg.GetDescriptor();
        auto scopePtr = StaticData_.ScopeDispatch.FindPtr(descr);
        if (scopePtr) {
            if (*scopePtr == EScope::TypeName) {
                ++InsideType_;
            }

            Scopes_.push_back(*scopePtr);
        }

        bool suppressExpr = false;
        if (descr == TToken::GetDescriptor()) {
            const auto& token = dynamic_cast<const TToken&>(msg);
            MarkToken(token);
        } else if (descr == TRule_sql_stmt_core::GetDescriptor()) {
            if (AddLine_.Empty()) {
                const auto& rule = dynamic_cast<const TRule_sql_stmt_core&>(msg);
                AddLine_ = !IsSimpleStatement(rule).GetOrElse(false);
                StmtCoreAltCase_ = rule.Alt_case();
            }
        } else if (descr == TRule_lambda_body::GetDescriptor()) {
            Y_ENSURE(TokenIndex_ >= 1);
            auto prevIndex = TokenIndex_ - 1;
            Y_ENSURE(prevIndex < ParsedTokens_.size());
            Y_ENSURE(ParsedTokens_[prevIndex].Content == "{");
            MarkedTokens_[prevIndex].OpeningBracket = false;
            ForceExpandedColumn_ = ParsedTokens_[prevIndex].LinePos;
            ForceExpandedLine_ = ParsedTokens_[prevIndex].Line;
        } else if (descr == TRule_in_atom_expr::GetDescriptor()) {
            const auto& value = dynamic_cast<const TRule_in_atom_expr&>(msg);
            if (value.Alt_case() == TRule_in_atom_expr::kAltInAtomExpr7) {
                suppressExpr = true;
            }
        } else if (descr == TRule_select_kind_parenthesis::GetDescriptor()) {
            const auto& value = dynamic_cast<const TRule_select_kind_parenthesis&>(msg);
            if (value.Alt_case() == TRule_select_kind_parenthesis::kAltSelectKindParenthesis2) {
                suppressExpr = true;
            }
        } else if (descr == TRule_window_specification::GetDescriptor()) {
            const auto& value = dynamic_cast<const TRule_window_specification&>(msg);
            const auto& details = value.GetRule_window_specification_details2();
            const bool needsNewline = details.HasBlock1() || details.HasBlock2() ||
                                      details.HasBlock3() || details.HasBlock4();
            if (needsNewline) {
                auto& paren = value.GetToken1();
                ForceExpandedColumn_ = paren.GetColumn();
                ForceExpandedLine_ = paren.GetLine();
            }
            suppressExpr = true;
        } else if (descr == TRule_exists_expr::GetDescriptor()) {
            const auto& value = dynamic_cast<const TRule_exists_expr&>(msg);
            auto& paren = value.GetToken2();
            ForceExpandedColumn_ = paren.GetColumn();
            ForceExpandedLine_ = paren.GetLine();
            suppressExpr = true;
        } else if (descr == TRule_smart_parenthesis::GetDescriptor()) {
            const auto& value = dynamic_cast<const TRule_smart_parenthesis&>(msg);
            if (IsSelect(value)) {
                auto& paren = value.GetToken1();
                ForceExpandedColumn_ = paren.GetColumn();
                ForceExpandedLine_ = paren.GetLine();
                suppressExpr = true;
            }
        } else if (descr == TRule_case_expr::GetDescriptor()) {
            const auto& value = dynamic_cast<const TRule_case_expr&>(msg);
            auto& token = value.GetToken1();
            ForceExpandedColumn_ = token.GetColumn();
            ForceExpandedLine_ = token.GetLine();
        }

        const bool expr = (descr == TRule_expr::GetDescriptor() || descr == TRule_in_expr::GetDescriptor() || descr == TRule_type_name_composite::GetDescriptor());
        if (expr) {
            ++InsideExpr_;
        }

        ui64 prevInsideExpr = InsideExpr_;
        if (suppressExpr) {
            InsideExpr_ = 0;
        }

        VisitAllFieldsImpl<TPrettyVisitor, &TPrettyVisitor::MarkTokens>(this, descr, msg);
        if (suppressExpr) {
            InsideExpr_ = prevInsideExpr;
        }

        if (scopePtr) {
            if (*scopePtr == EScope::TypeName) {
                --InsideType_;
            }

            Scopes_.pop_back();
        }

        if (expr) {
            --InsideExpr_;
        }
    }

    void MarkToken(const TToken& token) {
        auto str = token.GetValue();
        if (str == "<EOF>") {
            return;
        }

        MarkedTokens_.emplace_back();
        if (str == "(" || str == "[" || str == "{" || str == "<|" || (InsideType_ && str == "<")) {
            MarkTokenStack_.push_back(TokenIndex_);
            auto& info = MarkedTokens_[TokenIndex_];
            info.OpeningBracket = (InsideExpr_ > 0);
        } else if (str == ")") {
            PopBracket("(");
        } else if (str == "]") {
            PopBracket("[");
        } else if (str == "}") {
            PopBracket("{");
        } else if (str == "|>") {
            PopBracket("<|");
        } else if (InsideType_ && str == ">") {
            PopBracket("<");
        }

        TokenIndex_++;
    }

    void PopBracket(const TString& expected) {
        Y_ENSURE(!MarkTokenStack_.empty());
        Y_ENSURE(MarkTokenStack_.back() < ParsedTokens_.size());
        auto& openToken = ParsedTokens_[MarkTokenStack_.back()];
        Y_ENSURE(openToken.Content == expected);
        auto& openInfo = MarkedTokens_[MarkTokenStack_.back()];
        auto& closeInfo = MarkedTokens_[TokenIndex_];
        const bool forcedExpansion = openToken.Line == ForceExpandedLine_ && openToken.LinePos <= ForceExpandedColumn_;

        if (openInfo.OpeningBracket) {
            openInfo.ClosingBracketIndex = TokenIndex_;
            openInfo.BracketForcedExpansion = forcedExpansion;
            closeInfo.BracketForcedExpansion = forcedExpansion;
            closeInfo.ClosingBracket = true;
        }

        MarkTokenStack_.pop_back();
    }

    void Visit(const NProtoBuf::Message& msg) {
        const NProtoBuf::Descriptor* descr = msg.GetDescriptor();
        // Cerr << descr->name() << "\n";
        auto scopePtr = StaticData_.ScopeDispatch.FindPtr(descr);
        if (descr == TRule_invoke_expr::GetDescriptor()) {
            AfterInvokeExpr_ = true;
        }

        if (descr == TRule_unary_op::GetDescriptor()) {
            AfterUnaryOp_ = true;
        }

        if (scopePtr) {
            if (*scopePtr == EScope::TypeName) {
                ++InsideType_;
            }

            Scopes_.push_back(*scopePtr);
        }

        auto funcPtr = StaticData_.PrettyVisitDispatch.FindPtr(descr);
        if (funcPtr) {
            (*funcPtr)(*this, msg);
        } else {
            VisitAllFields(descr, msg);
        }

        if (scopePtr) {
            if (*scopePtr == EScope::TypeName) {
                --InsideType_;
            }

            Scopes_.pop_back();
        }
    }

    bool HasSelectInRHS(const TRule_named_nodes_stmt& stmt) {
        return ((stmt.GetBlock3().HasAlt1() &&
                 IsSelect(stmt.GetBlock3().GetAlt1().GetRule_expr1())) ||
                (stmt.GetBlock3().HasAlt2()));
    }

    TMaybe<bool> IsSimpleStatement(const TRule_sql_stmt_core& msg) {
        switch (msg.Alt_case()) {
            case TRule_sql_stmt_core::kAltSqlStmtCore1:  // pragma
            case TRule_sql_stmt_core::kAltSqlStmtCore5:  // drop table
            case TRule_sql_stmt_core::kAltSqlStmtCore6:  // use
            case TRule_sql_stmt_core::kAltSqlStmtCore8:  // commit
            case TRule_sql_stmt_core::kAltSqlStmtCore11: // rollback
            case TRule_sql_stmt_core::kAltSqlStmtCore12: // declare
            case TRule_sql_stmt_core::kAltSqlStmtCore13: // import
            case TRule_sql_stmt_core::kAltSqlStmtCore14: // export
            case TRule_sql_stmt_core::kAltSqlStmtCore32: // drop external data source
            case TRule_sql_stmt_core::kAltSqlStmtCore34: // drop replication
            case TRule_sql_stmt_core::kAltSqlStmtCore47: // drop resource pool
            case TRule_sql_stmt_core::kAltSqlStmtCore54: // drop resource pool classifier
            case TRule_sql_stmt_core::kAltSqlStmtCore60: // drop transfer
            case TRule_sql_stmt_core::kAltSqlStmtCore65: // drop streaming query
                return true;
            case TRule_sql_stmt_core::kAltSqlStmtCore3: { // named nodes
                const auto& stmt = msg.GetAlt_sql_stmt_core3().GetRule_named_nodes_stmt1();

                if (!HasSelectInRHS(stmt)) {
                    return true;
                }
                break;
            }
            case TRule_sql_stmt_core::kAltSqlStmtCore17: { // do
                const auto& stmt = msg.GetAlt_sql_stmt_core17().GetRule_do_stmt1();
                if (stmt.GetBlock2().HasAlt1()) {
                    return true;
                }
                break;
            }
            case TRule_sql_stmt_core::kAltSqlStmtCore19: // if
            case TRule_sql_stmt_core::kAltSqlStmtCore20: // for
                return false;
            default:
                break;
        }

        return {};
    }

    bool IsSimpleLambdaStatement(const TRule_lambda_stmt& msg) {
        if (msg.HasAlt_lambda_stmt1()) {
            const auto& stmt = msg.GetAlt_lambda_stmt1().GetRule_named_nodes_stmt1();
            if (HasSelectInRHS(stmt)) {
                return false;
            }
        }
        return true;
    }

    template <typename T>
    void VisitRepeated(const ::google::protobuf::RepeatedPtrField<T>& field) {
        for (const auto& m : field) {
            Visit(m);
        }
    }

    template <typename T>
    void SkipSemicolons(const ::google::protobuf::RepeatedPtrField<T>& field, bool printOne = false) {
        for (const auto& m : field) {
            if (printOne) {
                Visit(m);
                printOne = false;
            } else {
                ++TokenIndex_;
            }
        }
        if (printOne) {
            Out(';');
        }
    }

    void VisitValueConstructor(const TRule_value_constructor& msg) {
        switch (msg.Alt_case()) {
            case TRule_value_constructor::kAltValueConstructor1: {
                auto& ctor = msg.GetAlt_value_constructor1();
                Scopes_.push_back(EScope::TypeName);
                Visit(ctor.GetToken1());
                Scopes_.pop_back();
                AfterInvokeExpr_ = true;
                Visit(ctor.GetToken2());
                Visit(ctor.GetRule_expr3());
                Visit(ctor.GetToken4());
                Visit(ctor.GetRule_expr5());
                Visit(ctor.GetToken6());
                Visit(ctor.GetRule_expr7());
                Visit(ctor.GetToken8());
                break;
            }
            case TRule_value_constructor::kAltValueConstructor2: {
                auto& ctor = msg.GetAlt_value_constructor2();
                Scopes_.push_back(EScope::TypeName);
                Visit(ctor.GetToken1());
                Scopes_.pop_back();
                AfterInvokeExpr_ = true;
                Visit(ctor.GetToken2());
                Visit(ctor.GetRule_expr3());
                Visit(ctor.GetToken4());
                Visit(ctor.GetRule_expr5());
                Visit(ctor.GetToken6());
                break;
            }
            case TRule_value_constructor::kAltValueConstructor3: {
                auto& ctor = msg.GetAlt_value_constructor3();
                Scopes_.push_back(EScope::TypeName);
                Visit(ctor.GetToken1());
                Scopes_.pop_back();
                AfterInvokeExpr_ = true;
                Visit(ctor.GetToken2());
                Visit(ctor.GetRule_expr3());
                Visit(ctor.GetToken4());
                Visit(ctor.GetRule_expr5());
                Visit(ctor.GetToken6());
                break;
            }
            case TRule_value_constructor::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    void VisitDefineActionOrSubqueryBody(const TRule_define_action_or_subquery_body& msg) {
        SkipSemicolons(msg.GetBlock1());
        if (msg.HasBlock2()) {
            const auto& b = msg.GetBlock2();
            bool prevIsSimpleStatement = IsSimpleStatement(b.GetRule_sql_stmt_core1()).GetOrElse(false);
            Visit(b.GetRule_sql_stmt_core1());
            for (auto block : b.GetBlock2()) {
                SkipSemicolons(block.GetBlock1(), /* printOne = */ true);
                bool curIsSimpleStatement = IsSimpleStatement(block.GetRule_sql_stmt_core2()).GetOrElse(false);
                AddNewlineBetweenStatementsIfNeeded(curIsSimpleStatement, prevIsSimpleStatement);
                Visit(block.GetRule_sql_stmt_core2());
                prevIsSimpleStatement = curIsSimpleStatement;
            }
            SkipSemicolons(b.GetBlock3(), /* printOne = */ true);
        }
    }

    void VisitPragma(const TRule_pragma_stmt& msg) {
        NewLine();
        VisitKeyword(msg.GetToken1());
        auto prefix = msg.GetRule_opt_id_prefix_or_type2();
        if (prefix.HasBlock1()) {
            Visit(prefix.GetBlock1().GetRule_an_id_or_type1());
            VisitKeyword(prefix.GetBlock1().GetToken2());
            AfterDot_ = true;
        }

        Visit(msg.GetRule_an_id3());
        if (msg.GetBlock4().HasAlt2()) {
            AfterInvokeExpr_ = true;
            const auto& alt2 = msg.GetBlock4().GetAlt2();
            VisitKeyword(alt2.GetToken1());
            Visit(alt2.GetRule_pragma_value2());
            VisitRepeated(alt2.GetBlock3());
            VisitKeyword(alt2.GetToken4());
        } else {
            Visit(msg.GetBlock4());
        }
    }

    void VisitSelect(const TRule_select_stmt& msg) {
        NewLine();
        Visit(msg.GetRule_select_stmt_intersect1());
        for (const auto& block : msg.GetBlock2()) {
            NewLine();
            Visit(block.GetRule_union_op1());
            NewLine();
            Visit(block.GetRule_select_stmt_intersect2());
        }
    }

    void VisitSelectIntersect(const TRule_select_stmt_intersect& msg) {
        NewLine();
        Visit(msg.GetRule_select_kind_parenthesis1());
        for (const auto& block : msg.GetBlock2()) {
            NewLine();
            Visit(block.GetRule_intersect_op1());
            NewLine();
            Visit(block.GetRule_select_kind_parenthesis2());
        }
    }

    void VisitSmartParenthesis(const TRule_smart_parenthesis& msg) {
        if (!IsSelect(msg)) {
            return VisitAllFields(msg.GetDescriptor(), msg);
        }

        Y_ENSURE(msg.GetBlock2().HasAlt1());

        Visit(msg.GetToken1());
        PushCurrentIndent();
        NewLine();
        Visit(msg.GetBlock2().GetAlt1().GetRule_select_subexpr1());
        NewLine();
        PopCurrentIndent();
        Visit(msg.GetToken3());
    }

    void VisitSelectSubExpr(const TRule_select_subexpr& msg) {
        Visit(msg.GetRule_select_subexpr_intersect1());
        for (const auto& block : msg.GetBlock2()) {
            NewLine();
            Visit(block.GetRule_union_op1());
            NewLine();
            Visit(block.GetRule_select_subexpr_intersect2());
        }
    }

    void VisitSelectSubExprIntersect(const TRule_select_subexpr_intersect& msg) {
        Visit(msg.GetRule_select_or_expr1());
        for (const auto& block : msg.GetBlock2()) {
            NewLine();
            Visit(block.GetRule_intersect_op1());
            NewLine();
            Visit(block.GetRule_select_or_expr2());
        }
    }

    void VisitSelectUnparenthesized(const TRule_select_unparenthesized_stmt& msg) {
        NewLine();
        Visit(msg.GetRule_select_unparenthesized_stmt_intersect1());
        for (const auto& block : msg.GetBlock2()) {
            NewLine();
            Visit(block.GetRule_union_op1());
            NewLine();
            Visit(block.GetRule_select_stmt_intersect2());
        }
    }

    void VisitSelectUnparenthesizedIntersect(const TRule_select_unparenthesized_stmt_intersect& msg) {
        NewLine();
        Visit(msg.GetRule_select_kind_partial1());
        for (const auto& block : msg.GetBlock2()) {
            NewLine();
            Visit(block.GetRule_intersect_op1());
            NewLine();
            Visit(block.GetRule_select_kind_parenthesis2());
        }
    }

    void VisitNamedNodes(const TRule_named_nodes_stmt& msg) {
        NewLine();
        Visit(msg.GetRule_bind_parameter_list1());
        Visit(msg.GetToken2());
        ExprLineIndent_ = CurrentIndent_;

        switch (msg.GetBlock3().Alt_case()) {
            case TRule_named_nodes_stmt::TBlock3::kAlt1: {
                const auto& alt = msg.GetBlock3().GetAlt1();
                Visit(alt);
                break;
            }

            case TRule_named_nodes_stmt::TBlock3::kAlt2: {
                const auto& alt = msg.GetBlock3().GetAlt2();
                Out(" (");
                NewLine();
                PushCurrentIndent();
                Visit(alt);
                PopCurrentIndent();
                NewLine();
                Out(')');
                break;
            }

            default:
                ythrow yexception() << "Alt is not supported";
        }

        ExprLineIndent_ = 0;
    }

    void VisitAlterDatabase(const TRule_alter_database_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_alter_database_stmt::GetDescriptor(), msg);
    }

    void VisitTruncateTable(const TRule_truncate_table_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_truncate_table_stmt::GetDescriptor(), msg);
    }

    void VisitCreateTable(const TRule_create_table_stmt& msg) {
        NewLine();
        Visit(msg.GetToken1());
        Visit(msg.GetBlock2());
        Visit(msg.GetBlock3());
        Visit(msg.GetBlock4());
        Visit(msg.GetRule_simple_table_ref5());
        Visit(msg.GetToken6());
        PushCurrentIndent();
        NewLine();
        Visit(msg.GetRule_create_table_entry7());
        for (const auto& b : msg.GetBlock8()) {
            Visit(b.GetToken1());
            NewLine();
            Visit(b.GetRule_create_table_entry2());
        }
        if (msg.HasBlock9()) {
            Visit(msg.GetBlock9());
        }

        PopCurrentIndent();
        NewLine();
        Visit(msg.GetToken10());
        if (msg.HasBlock11()) {
            NewLine();
            Visit(msg.GetBlock11());
        }
        if (msg.HasBlock12()) {
            NewLine();
            Visit(msg.GetBlock12());
        }
        if (msg.HasBlock13()) {
            NewLine();
            Visit(msg.GetBlock13());
        }
        if (msg.HasBlock14()) {
            NewLine();
            Visit(msg.GetBlock14());
        }
        if (msg.HasBlock15()) {
            NewLine();
            Visit(msg.GetBlock15());
        }
    }

    void VisitDropTable(const TRule_drop_table_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_drop_table_stmt::GetDescriptor(), msg);
    }

    void VisitAnalyze(const TRule_analyze_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_analyze_stmt::GetDescriptor(), msg);
    }

    void VisitBackup(const TRule_backup_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_backup_stmt::GetDescriptor(), msg);
    }

    void VisitRestore(const TRule_restore_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_restore_stmt::GetDescriptor(), msg);
    }

    void VisitUse(const TRule_use_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_use_stmt::GetDescriptor(), msg);
    }

    void VisitAlterSequence(const TRule_alter_sequence_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_alter_sequence_stmt::GetDescriptor(), msg);
    }

    void VisitShowCreateTable(const TRule_show_create_table_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_show_create_table_stmt::GetDescriptor(), msg);
    }

    void VisitCreateSecret(const TRule_create_secret_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_create_secret_stmt::GetDescriptor(), msg);
    }

    void VisitAlterSecret(const TRule_alter_secret_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_alter_secret_stmt::GetDescriptor(), msg);
    }

    void VisitDropSecret(const TRule_drop_secret_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_drop_secret_stmt::GetDescriptor(), msg);
    }

    void VisitIntoTable(const TRule_into_table_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_into_table_stmt::GetDescriptor(), msg);
    }

    void VisitCommit(const TRule_commit_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_commit_stmt::GetDescriptor(), msg);
    }

    void VisitUpdate(const TRule_update_stmt& msg) {
        NewLine();
        Visit(msg.GetToken2());
        Visit(msg.GetRule_simple_table_ref3());
        switch (msg.GetBlock4().Alt_case()) {
            case TRule_update_stmt_TBlock4::kAlt1: {
                const auto& alt = msg.GetBlock4().GetAlt1();
                NewLine();
                Visit(alt.GetToken1());
                const auto& choice = alt.GetRule_set_clause_choice2();
                NewLine();

                switch (choice.Alt_case()) {
                    case TRule_set_clause_choice::kAltSetClauseChoice1: {
                        const auto& clauses = choice.GetAlt_set_clause_choice1().GetRule_set_clause_list1();
                        NewLine();
                        PushCurrentIndent();
                        Visit(clauses.GetRule_set_clause1());
                        for (auto& block : clauses.GetBlock2()) {
                            Visit(block.GetToken1());
                            NewLine();
                            Visit(block.GetRule_set_clause2());
                        }

                        PopCurrentIndent();
                        NewLine();
                        break;
                    }
                    case TRule_set_clause_choice::kAltSetClauseChoice2: {
                        const auto& multiColumn = choice.GetAlt_set_clause_choice2().GetRule_multiple_column_assignment1();
                        const auto& targets = multiColumn.GetRule_set_target_list1();
                        Visit(targets.GetToken1());
                        NewLine();
                        PushCurrentIndent();
                        Visit(targets.GetRule_set_target2());
                        for (auto& block : targets.GetBlock3()) {
                            Visit(block.GetToken1());
                            NewLine();
                            Visit(block.GetRule_set_target2());
                        }

                        NewLine();
                        PopCurrentIndent();
                        Visit(targets.GetToken4());
                        Visit(multiColumn.GetToken2());

                        const auto& parenthesis = multiColumn.GetRule_smart_parenthesis3();

                        const auto* tuple_or_expr = GetTupleOrExpr(parenthesis);
                        if (!tuple_or_expr) {
                            Visit(parenthesis);
                            break;
                        }

                        const bool isHeadNamed = tuple_or_expr->HasBlock2();
                        const bool isTailNamed = AnyOf(tuple_or_expr->GetBlock3(), [](const auto& block) {
                            return block.GetRule_named_expr2().HasBlock2();
                        });
                        if (isHeadNamed || isTailNamed) {
                            Visit(parenthesis);
                            break;
                        }

                        Visit(parenthesis.GetToken1());
                        PushCurrentIndent();
                        NewLine();

                        Visit(tuple_or_expr->GetRule_expr1());
                        for (auto& block : tuple_or_expr->GetBlock3()) {
                            Visit(block.GetToken1());
                            NewLine();
                            Visit(block.GetRule_named_expr2().GetRule_expr1());
                        }
                        if (tuple_or_expr->HasBlock4()) {
                            Visit(tuple_or_expr->GetBlock4().GetToken1());
                        }

                        NewLine();
                        PopCurrentIndent();
                        Visit(parenthesis.GetToken3());

                        break;
                    }
                    default:
                        ythrow yexception() << "Alt is not supported";
                }

                PopCurrentIndent();
                if (alt.HasBlock3()) {
                    NewLine();
                    Visit(alt.GetBlock3());
                }

                PopCurrentIndent();
                break;
            }
            case TRule_update_stmt_TBlock4::kAlt2: {
                const auto& alt = msg.GetBlock4().GetAlt2();
                NewLine();
                Visit(alt.GetToken1());
                Visit(alt.GetRule_into_values_source2());
                break;
            }
            default:
                ythrow yexception() << "Alt is not supported";
        }
    }

    void VisitDelete(const TRule_delete_stmt& msg) {
        NewLine();
        Visit(msg.GetToken2());
        Visit(msg.GetToken3());
        Visit(msg.GetRule_simple_table_ref4());
        if (msg.HasBlock5()) {
            switch (msg.GetBlock5().Alt_case()) {
                case TRule_delete_stmt_TBlock5::kAlt1: {
                    const auto& alt = msg.GetBlock5().GetAlt1();
                    NewLine();
                    Visit(alt);
                    break;
                }
                case TRule_delete_stmt_TBlock5::kAlt2: {
                    const auto& alt = msg.GetBlock5().GetAlt2();
                    NewLine();
                    Visit(alt);
                    break;
                }
                default:
                    ythrow yexception() << "Alt is not supported";
            }
        }
    }

    void VisitRollback(const TRule_rollback_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_rollback_stmt::GetDescriptor(), msg);
    }

    void VisitDeclare(const TRule_declare_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_declare_stmt::GetDescriptor(), msg);
    }

    void VisitImport(const TRule_import_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_import_stmt::GetDescriptor(), msg);
    }

    void VisitExport(const TRule_export_stmt& msg) {
        NewLine();
        VisitKeyword(msg.GetToken1());

        NewLine();
        PushCurrentIndent();

        const auto& list = msg.GetRule_bind_parameter_list2();
        Visit(list.GetRule_bind_parameter1());
        for (auto& b : list.GetBlock2()) {
            Visit(b.GetToken1());
            NewLine();
            Visit(b.GetRule_bind_parameter2());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitAlterTable(const TRule_alter_table_stmt& msg) {
        NewLine();
        VisitKeyword(msg.GetToken1());
        VisitKeyword(msg.GetToken2());
        Visit(msg.GetRule_simple_table_ref3());
        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_alter_table_action4());
        for (auto& b : msg.GetBlock5()) {
            Visit(b.GetToken1());
            NewLine();
            Visit(b.GetRule_alter_table_action2());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitAlterTableStore(const TRule_alter_table_store_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_alter_table_store_stmt::GetDescriptor(), msg);
    }

    void VisitAlterExternalTable(const TRule_alter_external_table_stmt& msg) {
        NewLine();
        VisitKeyword(msg.GetToken1());
        VisitKeyword(msg.GetToken2());
        VisitKeyword(msg.GetToken3());
        Visit(msg.GetRule_simple_table_ref4());
        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_alter_external_table_action5());
        for (auto& b : msg.GetBlock6()) {
            Visit(b.GetToken1());
            NewLine();
            Visit(b.GetRule_alter_external_table_action2());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitDo(const TRule_do_stmt& msg) {
        VisitKeyword(msg.GetToken1());
        switch (msg.GetBlock2().Alt_case()) {
            case TRule_do_stmt_TBlock2::kAlt1: { // CALL
                PushCurrentIndent();
                NewLine();
                const auto& alt = msg.GetBlock2().GetAlt1().GetRule_call_action1();
                Visit(alt.GetBlock1());
                AfterInvokeExpr_ = true;
                Visit(alt.GetToken2());
                if (alt.HasBlock3()) {
                    Visit(alt.GetBlock3());
                }
                Visit(alt.GetToken4());
                PopCurrentIndent();
                NewLine();
                break;
            }
            case TRule_do_stmt_TBlock2::kAlt2: { // INLINE
                const auto& alt = msg.GetBlock2().GetAlt2().GetRule_inline_action1();
                VisitKeyword(alt.GetToken1());
                PushCurrentIndent();
                NewLine();
                Visit(alt.GetRule_define_action_or_subquery_body2());
                PopCurrentIndent();
                NewLine();
                VisitKeyword(alt.GetToken3());
                VisitKeyword(alt.GetToken4());
                break;
            }
            default:
                ythrow yexception() << "Alt is not supported";
        }
    }

    void VisitAction(const TRule_define_action_or_subquery_stmt& msg) {
        NewLine();
        VisitKeyword(msg.GetToken1());
        VisitKeyword(msg.GetToken2());
        Visit(msg.GetRule_bind_parameter3());
        AfterInvokeExpr_ = true;
        Visit(msg.GetToken4());
        if (msg.HasBlock5()) {
            Visit(msg.GetBlock5());
        }

        Visit(msg.GetToken6());
        VisitKeyword(msg.GetToken7()); // AS
        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_define_action_or_subquery_body8());
        PopCurrentIndent();
        NewLine();
        VisitKeyword(msg.GetToken9());
        VisitKeyword(msg.GetToken10());
    }

    void VisitIf(const TRule_if_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_if_stmt::GetDescriptor(), msg);
    }

    void VisitFor(const TRule_for_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_for_stmt::GetDescriptor(), msg);
    }

    void VisitValues(const TRule_values_stmt& msg) {
        NewLine();
        VisitKeyword(msg.GetToken1());
        const auto& rowList = msg.GetRule_values_source_row_list2();
        PushCurrentIndent();
        NewLine();
        Visit(rowList.GetRule_values_source_row1());
        for (const auto& b : rowList.GetBlock2()) {
            Visit(b.GetToken1());
            NewLine();
            Visit(b.GetRule_values_source_row2());
        }

        if (rowList.HasBlock3()) {
            Visit(rowList.GetBlock3().GetToken1());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitGrantPermissions(const TRule_grant_permissions_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_grant_permissions_stmt::GetDescriptor(), msg);
    }

    void VisitRevokePermissions(const TRule_revoke_permissions_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_revoke_permissions_stmt::GetDescriptor(), msg);
    }

    void VisitCreateUser(const TRule_create_user_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_create_user_stmt::GetDescriptor(), msg);
    }

    void VisitAlterUser(const TRule_alter_user_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_alter_user_stmt::GetDescriptor(), msg);
    }

    void VisitCreateGroup(const TRule_create_group_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_create_group_stmt::GetDescriptor(), msg);
    }

    void VisitAlterGroup(const TRule_alter_group_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_alter_group_stmt::GetDescriptor(), msg);
    }

    void VisitDropRole(const TRule_drop_role_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_drop_role_stmt::GetDescriptor(), msg);
    }

    void VisitUpsertObject(const TRule_upsert_object_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_upsert_object_stmt::GetDescriptor(), msg);
    }

    void VisitCreateObject(const TRule_create_object_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_create_object_stmt::GetDescriptor(), msg);
    }

    void VisitAlterObject(const TRule_alter_object_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_alter_object_stmt::GetDescriptor(), msg);
    }

    void VisitDropObject(const TRule_drop_object_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_drop_object_stmt::GetDescriptor(), msg);
    }

    void VisitCreateTopic(const TRule_create_topic_stmt& msg) {
        NewLine();
        VisitKeyword(msg.GetToken1());
        VisitKeyword(msg.GetToken2());
        Visit(msg.GetBlock3());
        Visit(msg.GetRule_topic_ref4());
        if (msg.HasBlock5()) {
            auto& b = msg.GetBlock5().GetRule_create_topic_entries1();
            Visit(b.GetToken1());
            NewLine();
            PushCurrentIndent();
            Visit(b.GetRule_create_topic_entry2());
            for (auto& subEntry : b.GetBlock3()) {
                Visit(subEntry.GetToken1());
                NewLine();
                Visit(subEntry.GetRule_create_topic_entry2());
            }
            NewLine();
            PopCurrentIndent();
            Visit(b.GetToken4());
        }
        if (msg.HasBlock6()) {
            auto& b = msg.GetBlock6().GetRule_with_topic_settings1();
            VisitKeyword(b.GetToken1());
            VisitKeyword(b.GetToken2());
            PushCurrentIndent();
            NewLine();
            Visit(b.GetRule_topic_settings3());
            PopCurrentIndent();
            NewLine();
            VisitKeyword(b.GetToken4());
        }
    }

    void VisitAlterTopic(const TRule_alter_topic_stmt& msg) {
        NewLine();
        VisitKeyword(msg.GetToken1());
        VisitKeyword(msg.GetToken2());
        Visit(msg.GetBlock3());
        Visit(msg.GetRule_topic_ref4());
        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_alter_topic_action5());
        for (auto& b : msg.GetBlock6()) {
            Visit(b.GetToken1());
            NewLine();
            Visit(b.GetRule_alter_topic_action2());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitDropTopic(const TRule_drop_topic_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_drop_topic_stmt::GetDescriptor(), msg);
    }

    void VisitCreateExternalDataSource(const TRule_create_external_data_source_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_create_external_data_source_stmt::GetDescriptor(), msg);
    }

    void VisitAlterExternalDataSource(const TRule_alter_external_data_source_stmt& msg) {
        NewLine();
        VisitToken(msg.GetToken1());
        VisitToken(msg.GetToken2());
        VisitToken(msg.GetToken3());
        VisitToken(msg.GetToken4());
        Visit(msg.GetRule_object_ref5());

        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_alter_external_data_source_action6());
        for (const auto& action : msg.GetBlock7()) {
            Visit(action.GetToken1()); // comma
            NewLine();
            Visit(action.GetRule_alter_external_data_source_action2());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitDropExternalDataSource(const TRule_drop_external_data_source_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_drop_external_data_source_stmt::GetDescriptor(), msg);
    }

    void VisitCreateView(const TRule_create_view_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_create_view_stmt::GetDescriptor(), msg);
    }

    void VisitDropView(const TRule_drop_view_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_drop_view_stmt::GetDescriptor(), msg);
    }

    void VisitCreateAsyncReplication(const TRule_create_replication_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_create_replication_stmt::GetDescriptor(), msg);
    }

    void VisitAlterAsyncReplication(const TRule_alter_replication_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_alter_replication_stmt::GetDescriptor(), msg);
    }

    void VisitDropAsyncReplication(const TRule_drop_replication_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_drop_replication_stmt::GetDescriptor(), msg);
    }

    void VisitCreateTransfer(const TRule_create_transfer_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_create_transfer_stmt::GetDescriptor(), msg);
    }

    void VisitAlterTransfer(const TRule_alter_transfer_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_alter_transfer_stmt::GetDescriptor(), msg);
    }

    void VisitDropTransfer(const TRule_drop_transfer_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_drop_transfer_stmt::GetDescriptor(), msg);
    }

    void VisitCreateResourcePool(const TRule_create_resource_pool_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_create_resource_pool_stmt::GetDescriptor(), msg);
    }

    void VisitAlterResourcePool(const TRule_alter_resource_pool_stmt& msg) {
        NewLine();
        VisitToken(msg.GetToken1());
        VisitToken(msg.GetToken2());
        VisitToken(msg.GetToken3());
        Visit(msg.GetRule_object_ref4());

        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_alter_resource_pool_action5());
        for (const auto& action : msg.GetBlock6()) {
            Visit(action.GetToken1()); // comma
            NewLine();
            Visit(action.GetRule_alter_resource_pool_action2());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitDropResourcePool(const TRule_drop_resource_pool_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_drop_resource_pool_stmt::GetDescriptor(), msg);
    }

    void VisitCreateBackupCollection(const TRule_create_backup_collection_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_create_backup_collection_stmt::GetDescriptor(), msg);
    }

    void VisitAlterBackupCollection(const TRule_alter_backup_collection_stmt& msg) {
        NewLine();
        VisitToken(msg.GetToken1());
        Visit(msg.GetRule_backup_collection2());

        NewLine();
        PushCurrentIndent();
        switch (msg.GetBlock3().Alt_case()) {
            case TRule_alter_backup_collection_stmt_TBlock3::kAlt1: {
                Visit(msg.GetBlock3().GetAlt1().GetRule_alter_backup_collection_actions1().GetRule_alter_backup_collection_action1());
                for (const auto& action : msg.GetBlock3().GetAlt1().GetRule_alter_backup_collection_actions1().GetBlock2()) {
                    Visit(action.GetToken1()); // comma
                    NewLine();
                    Visit(action.GetRule_alter_backup_collection_action2());
                }
                break;
            }
            case TRule_alter_backup_collection_stmt_TBlock3::kAlt2: {
                Visit(msg.GetBlock3().GetAlt2().GetRule_alter_backup_collection_entries1().GetRule_alter_backup_collection_entry1());
                for (const auto& entry : msg.GetBlock3().GetAlt2().GetRule_alter_backup_collection_entries1().GetBlock2()) {
                    Visit(entry.GetToken1()); // comma
                    NewLine();
                    Visit(entry.GetRule_alter_backup_collection_entry2());
                }
                break;
            }
            default:
                ythrow yexception() << "Alt is not supported";
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitDropBackupCollection(const TRule_drop_backup_collection_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_drop_backup_collection_stmt::GetDescriptor(), msg);
    }

    void VisitCreateResourcePoolClassifier(const TRule_create_resource_pool_classifier_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_create_resource_pool_classifier_stmt::GetDescriptor(), msg);
    }

    void VisitAlterResourcePoolClassifier(const TRule_alter_resource_pool_classifier_stmt& msg) {
        NewLine();
        VisitToken(msg.GetToken1());
        VisitToken(msg.GetToken2());
        VisitToken(msg.GetToken3());
        VisitToken(msg.GetToken4());
        Visit(msg.GetRule_object_ref5());

        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_alter_resource_pool_classifier_action6());
        for (const auto& action : msg.GetBlock7()) {
            Visit(action.GetToken1()); // comma
            NewLine();
            Visit(action.GetRule_alter_resource_pool_classifier_action2());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitDropResourcePoolClassifier(const TRule_drop_resource_pool_classifier_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_drop_resource_pool_classifier_stmt::GetDescriptor(), msg);
    }

    void VisitStreamingQuerySettings(const TRule_streaming_query_settings& msg) {
        VisitKeyword(msg.GetToken1());
        NewLine();
        PushCurrentIndent();

        Visit(msg.GetRule_streaming_query_setting2());
        for (const auto& setting : msg.GetBlock3()) {
            Visit(setting.GetToken1());
            NewLine();
            Visit(setting.GetRule_streaming_query_setting2());
        }

        if (msg.HasBlock4()) {
            TokenIndex_++;
        }

        PopCurrentIndent();
        NewLine();
        VisitKeyword(msg.GetToken5());
    }

    void VisitCreateStreamingQuery(const TRule_create_streaming_query_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_create_streaming_query_stmt::GetDescriptor(), msg);
    }

    void VisitAlterStreamingQuery(const TRule_alter_streaming_query_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_alter_streaming_query_stmt::GetDescriptor(), msg);
    }

    void VisitDropStreamingQuery(const TRule_drop_streaming_query_stmt& msg) {
        NewLine();
        VisitAllFields(TRule_drop_streaming_query_stmt::GetDescriptor(), msg);
    }

    void VisitAllFields(const NProtoBuf::Descriptor* descr, const NProtoBuf::Message& msg) {
        VisitAllFieldsImpl<TPrettyVisitor, &TPrettyVisitor::Visit>(this, descr, msg);
    }

    void WriteComments(bool completeLine) {
        while (LastComment_ < Comments_.size()) {
            const auto& c = Comments_[LastComment_];
            if (c.Line > LastLine_ || !completeLine && c.Line == LastLine_ && c.LinePos > LastColumn_) {
                break;
            }

            AddComment(c.Content);
            ++LastComment_;
        }
    }

    void PosFromToken(const TToken& token) {
        LastLine_ = token.GetLine();
        LastColumn_ = token.GetColumn();
        WriteComments(false);
    }

    void VisitToken(const TToken& token) {
        VisitTokenImpl(token, false);
    }

    void VisitKeyword(const TToken& token) {
        VisitTokenImpl(token, true);
    }

    void VisitTokenImpl(const TToken& token, bool forceKeyword) {
        PosFromToken(token);
        auto str = token.GetValue();

        if (str == "<EOF>") {
            return;
        }

        // Cerr << str << "\n";
        auto currentScope = Scopes_.back();
        if (!SkipSpaceAfterUnaryOp_ && !InMultiTokenOp_) {
            if (AfterLess_ && str == ">") {
                Out(' ');
            } else if (AfterDigits_ && str == ".") {
                Out(' ');
            } else if (OutColumn_ &&
                       (currentScope == EScope::DoubleQuestion || str != "?") &&
                       str != ":" && str != "." && str != "," && str != ";" && str != ")" &&
                       str != "]" && str != "}" && str != "|>" && str != "::" &&
                       !AfterNamespace_ && !AfterBracket_ && !AfterInvokeExpr_ &&
                       !AfterDollarOrAt_ && !AfterDot_ && (!AfterQuestion_ || str != "?") &&
                       (!InsideType_ || (str != "<" && str != ">" && str != "<>")) &&
                       (!InsideType_ || !AfterLess_) && (!AfterKeyExpr_ || str != "["))
            {
                Out(' ');
            }
        }

        SkipSpaceAfterUnaryOp_ = false;
        if (AfterUnaryOp_) {
            if (str == "+" || str == "-" || str == "~") {
                SkipSpaceAfterUnaryOp_ = true;
            }

            AfterUnaryOp_ = false;
        }

        AfterInvokeExpr_ = false;
        AfterNamespace_ = (str == "::");
        AfterBracket_ = (str == "(" || str == "[" || str == "{" || str == "<|");
        AfterDot_ = (str == ".");
        AfterDigits_ = !str.empty() && AllOf(str, [](char c) { return c >= '0' && c <= '9'; });
        AfterQuestion_ = (str == "?");
        AfterLess_ = (str == "<");
        AfterKeyExpr_ = false;
        AfterComment_ = false;

        if (forceKeyword) {
            str = to_upper(str);
        } else if (currentScope == EScope::Default) {
            if (auto p = StaticData_.Keywords.find(to_upper(str)); p != StaticData_.Keywords.end()) {
                str = *p;
            }
        }

        AfterDollarOrAt_ = (str == "$" || str == "@");

        const auto& markedInfo = MarkedTokens_[TokenIndex_];
        if (markedInfo.ClosingBracket) {
            Y_ENSURE(!MarkTokenStack_.empty());
            auto beginTokenIndex = MarkTokenStack_.back();
            if (markedInfo.BracketForcedExpansion || ParsedTokens_[beginTokenIndex].Line != ParsedTokens_[TokenIndex_].Line) {
                // multiline
                PopCurrentIndent();
                NewLine();
            }

            MarkTokenStack_.pop_back();
        }

        if (InCondExpr_) {
            if (str == "=") {
                str = "==";
            } else if (str == "<>") {
                str = "!=";
            }
        }

        if (!AnsiLexer_ && ParsedTokens_[TokenIndex_].Name == "STRING_VALUE") {
            TStringBuf checkStr = str;
            if (checkStr.SkipPrefix("\"") && checkStr.ChopSuffix("\"") && !checkStr.Contains("'")) {
                str = TStringBuilder() << '\'' << checkStr << '\'';
            }
        }

        Out(str);

        if (TokenIndex_ + 1 >= ParsedTokens_.size() || ParsedTokens_[TokenIndex_ + 1].Line > LastLine_) {
            WriteComments(true);
        }

        if (str == ";") {
            NewLine();
        }

        if (markedInfo.OpeningBracket) {
            MarkTokenStack_.push_back(TokenIndex_);
            if (markedInfo.BracketForcedExpansion || ParsedTokens_[TokenIndex_].Line != ParsedTokens_[markedInfo.ClosingBracketIndex].Line) {
                // multiline
                PushCurrentIndent();
                NewLine();
            }
        }

        if (str == "," && !MarkTokenStack_.empty()) {
            const bool addNewline =
                (TokenIndex_ + 1 < ParsedTokens_.size() && ParsedTokens_[TokenIndex_].Line != ParsedTokens_[TokenIndex_ + 1].Line) || (TokenIndex_ > 0 && ParsedTokens_[TokenIndex_ - 1].Line != ParsedTokens_[TokenIndex_].Line);
            // add line for trailing comma
            if (addNewline) {
                NewLine();
            }
        }

        TokenIndex_++;
    }

    void VisitIntoValuesSource(const TRule_into_values_source& msg) {
        switch (msg.Alt_case()) {
            case TRule_into_values_source::kAltIntoValuesSource1: {
                const auto& alt = msg.GetAlt_into_values_source1();
                if (alt.HasBlock1()) {
                    const auto& columns = alt.GetBlock1().GetRule_pure_column_list1();
                    Visit(columns.GetToken1());
                    NewLine();
                    PushCurrentIndent();
                    Visit(columns.GetRule_an_id2());
                    for (const auto& block : columns.GetBlock3()) {
                        Visit(block.GetToken1());
                        NewLine();
                        Visit(block.GetRule_an_id2());
                    }

                    if (columns.HasBlock4()) {
                        Visit(columns.GetBlock4().GetToken1());
                    }

                    PopCurrentIndent();
                    NewLine();
                    Visit(columns.GetToken5());
                    NewLine();
                }

                Visit(alt.GetRule_values_source2());
                break;
            }
            case TRule_into_values_source::kAltIntoValuesSource2: {
                VisitAllFields(TRule_into_values_source::GetDescriptor(), msg);
                break;
            }
            default:
                ythrow yexception() << "Alt is not supported";
        }
    }

    void VisitSelectKind(const TRule_select_kind& msg) {
        if (msg.HasBlock1()) {
            Visit(msg.GetBlock1());
        }

        Visit(msg.GetBlock2());
        if (msg.HasBlock3()) {
            NewLine();
            Visit(msg.GetBlock3());
        }
    }

    void VisitProcessCore(const TRule_process_core& msg) {
        Visit(msg.GetToken1());
        if (msg.HasBlock2()) {
            Visit(msg.GetBlock2());
        }

        Visit(msg.GetRule_named_single_source3());
        VisitRepeated(msg.GetBlock4());
        if (msg.HasBlock5()) {
            NewLine();
            const auto& block5 = msg.GetBlock5();
            Visit(block5.GetToken1());
            Visit(block5.GetRule_using_call_expr2());
            if (block5.HasBlock3()) {
                Visit(block5.GetBlock3());
            }

            if (block5.HasBlock4()) {
                NewLine();
                Visit(block5.GetBlock4());
            }

            if (block5.HasBlock5()) {
                NewLine();
                const auto& whereBlock = block5.GetBlock5();
                Visit(whereBlock.GetToken1());
                NewLine();
                PushCurrentIndent();
                Visit(whereBlock.GetRule_expr2());
                PopCurrentIndent();
                NewLine();
            }

            if (block5.HasBlock6()) {
                NewLine();
                const auto& havingBlock = block5.GetBlock6();
                Visit(havingBlock.GetToken1());
                NewLine();
                PushCurrentIndent();
                Visit(havingBlock.GetRule_expr2());
                PopCurrentIndent();
                NewLine();
            }

            if (block5.HasBlock7()) {
                NewLine();
                Visit(block5.GetBlock7());
            }
        }
    }

    void VisitReduceCore(const TRule_reduce_core& msg) {
        Visit(msg.GetToken1());
        Visit(msg.GetRule_named_single_source2());
        VisitRepeated(msg.GetBlock3());

        if (msg.HasBlock4()) {
            NewLine();
            Visit(msg.GetBlock4());
        }

        NewLine();
        Visit(msg.GetToken5());
        const auto& columns = msg.GetRule_column_list6();
        NewLine();
        PushCurrentIndent();
        Visit(columns.GetRule_column_name1());
        for (const auto& block : columns.GetBlock2()) {
            Visit(block.GetToken1());
            NewLine();
            Visit(block.GetRule_column_name2());
        }

        if (columns.HasBlock3()) {
            Visit(columns.GetBlock3());
        }

        PopCurrentIndent();
        NewLine();
        Visit(msg.GetToken7());
        if (msg.HasBlock8()) {
            Visit(msg.GetBlock8());
        }

        Visit(msg.GetRule_using_call_expr9());
        if (msg.HasBlock10()) {
            Visit(msg.GetBlock10());
        }

        if (msg.HasBlock11()) {
            NewLine();
            const auto& whereBlock = msg.GetBlock11();
            Visit(whereBlock.GetToken1());
            NewLine();
            PushCurrentIndent();
            Visit(whereBlock.GetRule_expr2());
            PopCurrentIndent();
            NewLine();
        }

        if (msg.HasBlock12()) {
            NewLine();
            const auto& havingBlock = msg.GetBlock12();
            Visit(havingBlock.GetToken1());
            NewLine();
            PushCurrentIndent();
            Visit(havingBlock.GetRule_expr2());
            PopCurrentIndent();
            NewLine();
        }

        if (msg.HasBlock13()) {
            NewLine();
            Visit(msg.GetBlock13());
        }
    }

    void VisitSortSpecificationList(const TRule_sort_specification_list& msg) {
        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_sort_specification1());
        for (const auto& block : msg.GetBlock2()) {
            Visit(block.GetToken1());
            NewLine();
            Visit(block.GetRule_sort_specification2());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitSelectCore(const TRule_select_core& msg) {
        if (msg.HasBlock1()) {
            Visit(msg.GetBlock1());
            NewLine();
        }

        Visit(msg.GetToken2());
        if (msg.HasBlock3()) {
            Visit(msg.GetBlock3());
        }

        Visit(msg.GetRule_opt_set_quantifier4());
        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_result_column5());
        for (const auto& block : msg.GetBlock6()) {
            Visit(block.GetToken1());
            NewLine();
            Visit(block.GetRule_result_column2());
        }

        if (msg.HasBlock7()) {
            Visit(msg.GetBlock7());
        }

        PopCurrentIndent();
        NewLine();

        if (msg.HasBlock8()) {
            NewLine();
            Visit(msg.GetBlock8());
        }

        if (msg.HasBlock9()) {
            NewLine();
            Visit(msg.GetBlock9());
        }

        if (msg.HasBlock10()) {
            NewLine();
            const auto& whereBlock = msg.GetBlock10();
            Visit(whereBlock.GetToken1());
            NewLine();
            PushCurrentIndent();
            Visit(whereBlock.GetRule_expr2());
            PopCurrentIndent();
            NewLine();
        }

        if (msg.HasBlock11()) {
            NewLine();
            Visit(msg.GetBlock11());
        }

        if (msg.HasBlock12()) {
            NewLine();
            const auto& havingBlock = msg.GetBlock12();
            Visit(havingBlock.GetToken1());
            NewLine();
            PushCurrentIndent();
            Visit(havingBlock.GetRule_expr2());
            PopCurrentIndent();
            NewLine();
        }

        if (msg.HasBlock13()) {
            NewLine();
            Visit(msg.GetBlock13());
        }

        if (msg.HasBlock14()) {
            NewLine();
            Visit(msg.GetBlock14());
        }
    }

    void VisitRowPatternRecognitionClause(const TRule_row_pattern_recognition_clause& msg) {
        VisitToken(msg.GetToken1());
        VisitToken(msg.GetToken2());

        NewLine();
        PushCurrentIndent();

        if (msg.HasBlock3()) {
            Visit(msg.GetBlock3());
            NewLine();
        }

        if (msg.HasBlock4()) {
            Visit(msg.GetBlock4());
            NewLine();
        }

        if (msg.HasBlock5()) {
            const auto& block = msg.GetBlock5().GetRule_row_pattern_measures1();
            VisitToken(block.GetToken1());
            NewLine();
            PushCurrentIndent();
            const auto& measureList = block.GetRule_row_pattern_measure_list2();
            Visit(measureList.GetRule_row_pattern_measure_definition1());
            for (const auto& measureDefinitionBlock : measureList.GetBlock2()) {
                VisitToken(measureDefinitionBlock.GetToken1());
                NewLine();
                Visit(measureDefinitionBlock.GetRule_row_pattern_measure_definition2());
            }
            PopCurrentIndent();
            NewLine();
        }

        if (msg.HasBlock6()) {
            Visit(msg.GetBlock6());
            NewLine();
        }

        const auto& common = msg.GetRule_row_pattern_common_syntax7();
        if (common.HasBlock1()) {
            Visit(common.GetBlock1());
            NewLine();
        }

        if (common.HasBlock2()) {
            Visit(common.GetBlock2());
        }

        VisitToken(common.GetToken3());
        VisitToken(common.GetToken4());
        Visit(common.GetRule_row_pattern5());
        VisitToken(common.GetToken6());
        NewLine();

        if (common.HasBlock7()) {
            const auto& block = common.GetBlock7().GetRule_row_pattern_subset_clause1();
            VisitToken(block.GetToken1());
            NewLine();
            PushCurrentIndent();
            const auto& subsetList = block.GetRule_row_pattern_subset_list2();
            Visit(subsetList.GetRule_row_pattern_subset_item1());
            for (const auto& subsetItemBlock : subsetList.GetBlock2()) {
                VisitToken(subsetItemBlock.GetToken1());
                NewLine();
                Visit(subsetItemBlock.GetRule_row_pattern_subset_item2());
            }
            PopCurrentIndent();
            NewLine();
        }

        VisitToken(common.GetToken8());
        NewLine();
        PushCurrentIndent();
        const auto& definitionList = common.GetRule_row_pattern_definition_list9();
        Visit(definitionList.GetRule_row_pattern_definition1());
        for (const auto& definitionBlock : definitionList.GetBlock2()) {
            VisitToken(definitionBlock.GetToken1());
            NewLine();
            Visit(definitionBlock.GetRule_row_pattern_definition2());
        }
        PopCurrentIndent();
        NewLine();

        PopCurrentIndent();
        NewLine();

        VisitToken(msg.GetToken8());
    }

    void VisitJoinSource(const TRule_join_source& msg) {
        if (msg.HasBlock1()) {
            Visit(msg.GetBlock1());
        }

        Visit(msg.GetRule_flatten_source2());
        for (const auto& block : msg.GetBlock3()) {
            NewLine();
            Visit(block.GetRule_join_op1());
            if (block.HasBlock2()) {
                Visit(block.GetBlock2());
            }

            Visit(block.GetRule_flatten_source3());
            if (block.HasBlock4()) {
                NewLine();
                Visit(block.GetBlock4());
            }
        }
    }

    void VisitJoinConstraint(const TRule_join_constraint& msg) {
        if (msg.Alt_case() == TRule_join_constraint::kAltJoinConstraint1) {
            const auto& alt = msg.GetAlt_join_constraint1();
            Visit(alt.GetToken1());
            NewLine();
            PushCurrentIndent();
            Visit(alt.GetRule_expr2());
            PopCurrentIndent();
            NewLine();
        } else {
            VisitAllFields(TRule_join_constraint::GetDescriptor(), msg);
        }
    }

    void VisitSingleSource(const TRule_single_source& msg) {
        switch (msg.Alt_case()) {
            case TRule_single_source::kAltSingleSource1: {
                const auto& alt = msg.GetAlt_single_source1();
                Visit(alt);
                break;
            }
            case TRule_single_source::kAltSingleSource2: {
                const auto& alt = msg.GetAlt_single_source2();
                Visit(alt.GetToken1());
                NewLine();
                PushCurrentIndent();
                Visit(alt.GetRule_select_stmt2());
                PopCurrentIndent();
                NewLine();
                Visit(alt.GetToken3());
                break;
            }
            case TRule_single_source::kAltSingleSource3: {
                const auto& alt = msg.GetAlt_single_source3();
                Visit(alt.GetToken1());
                NewLine();
                PushCurrentIndent();
                Visit(alt.GetRule_values_stmt2());
                PopCurrentIndent();
                NewLine();
                Visit(alt.GetToken3());
                break;
            }
            default:
                ythrow yexception() << "Alt is not supported";
        }
    }

    void VisitFlattenSource(const TRule_flatten_source& msg) {
        const auto& namedSingleSource = msg.GetRule_named_single_source1();
        bool indentBeforeSource = namedSingleSource.GetRule_single_source1().Alt_case() == TRule_single_source::kAltSingleSource1;

        if (indentBeforeSource) {
            NewLine();
            PushCurrentIndent();
        }
        Visit(namedSingleSource);
        if (indentBeforeSource) {
            PopCurrentIndent();
            NewLine();
        }

        if (msg.HasBlock2()) {
            NewLine();
            PushCurrentIndent();
            Visit(msg.GetBlock2());
            PopCurrentIndent();
            NewLine();
        }
    }

    void VisitNamedSingleSource(const TRule_named_single_source& msg) {
        Visit(msg.GetRule_single_source1());
        if (msg.HasBlock2()) {
            Visit(msg.GetBlock2());
        }
        if (msg.HasBlock3()) {
            const auto& block3 = msg.GetBlock3();
            Visit(block3.GetBlock1());
            if (block3.HasBlock2()) {
                const auto& columns = block3.GetBlock2().GetRule_pure_column_list1();
                Visit(columns.GetToken1());
                NewLine();
                PushCurrentIndent();
                Visit(columns.GetRule_an_id2());
                for (const auto& block : columns.GetBlock3()) {
                    Visit(block.GetToken1());
                    NewLine();
                    Visit(block.GetRule_an_id2());
                }

                if (columns.HasBlock4()) {
                    Visit(columns.GetBlock4().GetToken1());
                }

                NewLine();
                PopCurrentIndent();
                Visit(columns.GetToken5());
            }
        }

        if (msg.HasBlock4()) {
            NewLine();
            Visit(msg.GetBlock4());
        }
    }

    void VisitTableHints(const TRule_table_hints& msg) {
        Visit(msg.GetToken1());
        const auto& block2 = msg.GetBlock2();
        if (block2.Alt_case() == TRule_table_hints::TBlock2::kAlt2) {
            const auto& alt = block2.GetAlt2();

            Visit(alt.GetToken1());

            NewLine();
            PushCurrentIndent();

            Visit(alt.GetRule_table_hint2());
            for (const auto& block : alt.GetBlock3()) {
                Visit(block.GetToken1());
                NewLine();
                Visit(block.GetRule_table_hint2());
            }

            NewLine();
            PopCurrentIndent();

            if (alt.HasBlock4()) {
                Visit(alt.GetBlock4().GetToken1());
                NewLine();
            }

            Visit(alt.GetToken5());
        } else {
            Visit(block2);
        }
    }

    void VisitSimpleTableRef(const TRule_simple_table_ref& msg) {
        Visit(msg.GetRule_simple_table_ref_core1());
        if (msg.HasBlock2()) {
            Visit(msg.GetBlock2());
        }
    }

    void VisitIntoSimpleTableRef(const TRule_into_simple_table_ref& msg) {
        Visit(msg.GetRule_simple_table_ref1());
        if (msg.HasBlock2()) {
            const auto& block2 = msg.GetBlock2();
            NewLine();
            PushCurrentIndent();
            Visit(block2.GetToken1());
            Visit(block2.GetToken2());
            const auto& columns = block2.GetRule_pure_column_list3();
            Visit(columns.GetToken1());
            NewLine();
            PushCurrentIndent();
            Visit(columns.GetRule_an_id2());
            for (const auto& block : columns.GetBlock3()) {
                Visit(block.GetToken1());
                NewLine();
                Visit(block.GetRule_an_id2());
            }

            if (columns.HasBlock4()) {
                Visit(columns.GetBlock4().GetToken1());
            }

            PopCurrentIndent();
            NewLine();
            Visit(columns.GetToken5());
            PopCurrentIndent();
        }
    }

    void VisitSelectKindPartial(const TRule_select_kind_partial& msg) {
        Visit(msg.GetRule_select_kind1());
        if (msg.HasBlock2()) {
            NewLine();
            Visit(msg.GetBlock2());
        }
    }

    void VisitFlattenByArg(const TRule_flatten_by_arg& msg) {
        switch (msg.Alt_case()) {
            case TRule_flatten_by_arg::kAltFlattenByArg1: {
                const auto& alt = msg.GetAlt_flatten_by_arg1();
                Visit(alt);
                break;
            }
            case TRule_flatten_by_arg::kAltFlattenByArg2: {
                const auto& alt = msg.GetAlt_flatten_by_arg2();
                Visit(alt.GetToken1());
                NewLine();
                PushCurrentIndent();
                const auto& exprs = alt.GetRule_named_expr_list2();
                Visit(exprs.GetRule_named_expr1());
                for (const auto& block : exprs.GetBlock2()) {
                    Visit(block.GetToken1());
                    NewLine();
                    Visit(block.GetRule_named_expr2());
                }

                if (alt.HasBlock3()) {
                    Visit(alt.GetBlock3());
                }

                NewLine();
                PopCurrentIndent();
                Visit(alt.GetToken4());
                break;
            }
            default:
                ythrow yexception() << "Alt is not supported";
        }
    }

    void VisitWithoutColumnList(const TRule_without_column_list& msg) {
        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_without_column_name1());
        for (const auto& block : msg.GetBlock2()) {
            Visit(block.GetToken1());
            NewLine();
            Visit(block.GetRule_without_column_name2());
        }

        if (msg.HasBlock3()) {
            Visit(msg.GetBlock3());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitTableRef(const TRule_table_ref& msg) {
        if (msg.HasBlock1()) {
            Visit(msg.GetBlock1());
        }

        if (msg.HasBlock2()) {
            Visit(msg.GetBlock2());
        }

        const auto& block3 = msg.GetBlock3();
        switch (block3.Alt_case()) {
            case TRule_table_ref::TBlock3::kAlt1: {
                const auto& alt = block3.GetAlt1();
                const auto& key = alt.GetRule_table_key1();
                Visit(key.GetRule_id_table_or_type1());
                Visit(key.GetBlock2());

                break;
            }
            case TRule_table_ref::TBlock3::kAlt2: {
                const auto& alt = block3.GetAlt2();
                Visit(alt.GetRule_an_id_expr1());
                AfterInvokeExpr_ = true;
                Visit(alt.GetToken2());
                if (alt.HasBlock3()) {
                    Visit(alt.GetBlock3());
                }

                Visit(alt.GetToken4());
                break;
            }
            case TRule_table_ref::TBlock3::kAlt3: {
                const auto& alt = block3.GetAlt3();
                Visit(alt.GetRule_bind_parameter1());
                if (alt.HasBlock2()) {
                    AfterInvokeExpr_ = true;
                    Visit(alt.GetBlock2());
                }

                if (alt.HasBlock3()) {
                    Visit(alt.GetBlock3());
                }

                break;
            }
            default:
                ythrow yexception() << "Alt is not supported";
        }

        if (msg.HasBlock4()) {
            Visit(msg.GetBlock4());
        }
    }

    void VisitGroupingElementList(const TRule_grouping_element_list& msg) {
        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_grouping_element1());
        for (const auto& block : msg.GetBlock2()) {
            Visit(block.GetToken1());
            NewLine();
            Visit(block.GetRule_grouping_element2());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitGroupByClause(const TRule_group_by_clause& msg) {
        Visit(msg.GetToken1());
        if (msg.HasBlock2()) {
            Visit(msg.GetBlock2());
        }

        Visit(msg.GetToken3());
        Visit(msg.GetRule_opt_set_quantifier4());
        Visit(msg.GetRule_grouping_element_list5());
        if (msg.HasBlock6()) {
            NewLine();
            PushCurrentIndent();
            Visit(msg.GetBlock6());
            PopCurrentIndent();
            NewLine();
        }
    }

    void VisitWindowDefinitionList(const TRule_window_definition_list& msg) {
        NewLine();
        PushCurrentIndent();

        Visit(msg.GetRule_window_definition1());
        for (const auto& block : msg.GetBlock2()) {
            Visit(block.GetToken1());
            NewLine();
            Visit(block.GetRule_window_definition2());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitWindowSpecification(const TRule_window_specification& msg) {
        Visit(msg.GetToken1());
        const auto& details = msg.GetRule_window_specification_details2();
        const bool needsNewline = details.HasBlock1() || details.HasBlock2() ||
                                  details.HasBlock3() || details.HasBlock4();
        if (needsNewline) {
            NewLine();
            PushCurrentIndent();
        }

        if (details.HasBlock1()) {
            NewLine();
            Visit(details.GetBlock1());
        }

        if (details.HasBlock2()) {
            NewLine();
            Visit(details.GetBlock2());
        }

        if (details.HasBlock3()) {
            NewLine();
            Visit(details.GetBlock3());
        }

        if (details.HasBlock4()) {
            NewLine();
            Visit(details.GetBlock4());
        }

        if (needsNewline) {
            NewLine();
            PopCurrentIndent();
        }

        Visit(msg.GetToken3());
    }

    void VisitWindowParitionClause(const TRule_window_partition_clause& msg) {
        Visit(msg.GetToken1());
        if (msg.HasBlock2()) {
            Visit(msg.GetBlock2());
        }

        Visit(msg.GetToken3());
        const auto& exprs = msg.GetRule_named_expr_list4();
        PushCurrentIndent();
        NewLine();
        Visit(exprs.GetRule_named_expr1());
        for (const auto& block : exprs.GetBlock2()) {
            Visit(block.GetToken1());
            NewLine();
            Visit(block.GetRule_named_expr2());
        }

        PopCurrentIndent();
        NewLine();
    }

    void VisitLambdaBody(const TRule_lambda_body& msg) {
        PushCurrentIndent();
        NewLine();
        SkipSemicolons(msg.GetBlock1());

        if (msg.Block2Size() != 0) {
            const auto& block = msg.GetBlock2(0);
            bool prevIsSimpleStatement = IsSimpleLambdaStatement(block.GetRule_lambda_stmt1());
            Visit(block.GetRule_lambda_stmt1());
            SkipSemicolons(block.GetBlock2(), /* printOne = */ true);
            NewLine();
            for (ui32 i = 1; i < msg.Block2Size(); ++i) {
                const auto& block = msg.GetBlock2(i);
                bool curIsSimpleStatement = IsSimpleLambdaStatement(block.GetRule_lambda_stmt1());
                AddNewlineBetweenStatementsIfNeeded(curIsSimpleStatement, prevIsSimpleStatement);
                Visit(block.GetRule_lambda_stmt1());
                SkipSemicolons(block.GetBlock2(), /* printOne = */ true);
                NewLine();
                prevIsSimpleStatement = curIsSimpleStatement;
            }
        }

        Visit(msg.GetToken3());
        ExprLineIndent_ = CurrentIndent_;

        Visit(msg.GetRule_expr4());

        SkipSemicolons(msg.GetBlock5(), /* printOne = */ true);
        ExprLineIndent_ = 0;

        PopCurrentIndent();
        NewLine();
    }

    void VisitSelectKindParenthesis(const TRule_select_kind_parenthesis& msg) {
        if (msg.Alt_case() == TRule_select_kind_parenthesis::kAltSelectKindParenthesis2) {
            const auto& alt = msg.GetAlt_select_kind_parenthesis2();
            Visit(alt.GetToken1());
            NewLine();
            PushCurrentIndent();
            Visit(alt.GetRule_select_kind_partial2());
            PopCurrentIndent();
            NewLine();
            Visit(alt.GetToken3());
        } else {
            VisitAllFields(TRule_select_kind_parenthesis::GetDescriptor(), msg);
        }
    }

    void VisitCastExpr(const TRule_cast_expr& msg) {
        Visit(msg.GetToken1());
        AfterInvokeExpr_ = true;
        Visit(msg.GetToken2());
        Visit(msg.GetRule_expr3());
        Visit(msg.GetToken4());
        Visit(msg.GetRule_type_name_or_bind5());
        Visit(msg.GetToken6());
    }

    void VisitBitCastExpr(const TRule_bitcast_expr& msg) {
        Visit(msg.GetToken1());
        AfterInvokeExpr_ = true;
        Visit(msg.GetToken2());
        Visit(msg.GetRule_expr3());
        Visit(msg.GetToken4());
        Visit(msg.GetRule_type_name_simple5());
        Visit(msg.GetToken6());
    }

    void VisitExtOrderByClause(const TRule_ext_order_by_clause& msg) {
        if (msg.HasBlock1()) {
            Visit(msg.GetBlock1());
        }

        Visit(msg.GetRule_order_by_clause2());
    }

    void VisitKeyExpr(const TRule_key_expr& msg) {
        AfterKeyExpr_ = true;
        VisitAllFields(TRule_key_expr::GetDescriptor(), msg);
    }

    void VisitExistsExpr(const TRule_exists_expr& msg) {
        VisitKeyword(msg.GetToken1());
        VisitToken(msg.GetToken2());

        NewLine();
        PushCurrentIndent();

        Visit(msg.GetBlock3());

        PopCurrentIndent();
        NewLine();

        VisitToken(msg.GetToken4());
    }

    void VisitCaseExpr(const TRule_case_expr& msg) {
        VisitKeyword(msg.GetToken1());
        if (msg.HasBlock2()) {
            Visit(msg.GetBlock2());
        }
        NewLine();
        PushCurrentIndent();

        for (const auto& block : msg.GetBlock3()) {
            Visit(block);
            NewLine();
        }

        if (msg.HasBlock4()) {
            const auto& block = msg.GetBlock4();
            VisitKeyword(block.GetToken1());
            Visit(block.GetRule_expr2());
        }

        PopCurrentIndent();
        NewLine();
        Visit(msg.GetToken5());
    }

    void VisitWithTableSettingsExpr(const TRule_with_table_settings& msg) {
        VisitKeyword(msg.GetToken1());
        Visit(msg.GetToken2());

        const bool needIndent = msg.Block4Size() > 0; // more then one setting
        if (needIndent) {
            NewLine();
            PushCurrentIndent();
            Visit(msg.GetRule_table_settings_entry3()); // first setting

            for (const auto& entry : msg.GetBlock4()) {
                Visit(entry.GetToken1()); // comma
                NewLine();
                Visit(entry.GetRule_table_settings_entry2()); // other settings
            }
            PopCurrentIndent();
            NewLine();
        } else {
            Visit(msg.GetRule_table_settings_entry3());
        }

        if (msg.HasBlock5()) {
            Visit(msg.GetBlock5().GetToken1());
            NewLine();
        }

        Visit(msg.GetToken6());
    }

    void VisitTableSettingValue(const TRule_table_setting_value& msg) {
        switch (msg.GetAltCase()) {
            case TRule_table_setting_value::kAltTableSettingValue5: {
                // | ttl_tier_list ON an_id (AS (SECONDS | MILLISECONDS | MICROSECONDS | NANOSECONDS))?
                const auto& ttlSettings = msg.GetAlt_table_setting_value5();
                const auto& tierList = ttlSettings.GetRule_ttl_tier_list1();
                const bool needIndent = tierList.HasBlock2() && tierList.GetBlock2().Block2Size() > 0; // more then one tier
                if (needIndent) {
                    NewLine();
                    PushCurrentIndent();
                    Visit(tierList.GetRule_expr1());
                    VisitTtlTierAction(tierList.GetBlock2().GetRule_ttl_tier_action1());

                    for (const auto& tierEntry : tierList.GetBlock2().GetBlock2()) {
                        Visit(tierEntry.GetToken1()); // comma
                        NewLine();
                        Visit(tierEntry.GetRule_expr2());
                        VisitTtlTierAction(tierEntry.GetRule_ttl_tier_action3());
                    }

                    PopCurrentIndent();
                    NewLine();
                } else {
                    Visit(tierList.GetRule_expr1());
                    if (tierList.HasBlock2()) {
                        VisitTtlTierAction(tierList.GetBlock2().GetRule_ttl_tier_action1());
                    }
                }

                VisitKeyword(ttlSettings.GetToken2());
                Visit(ttlSettings.GetRule_an_id3());
                if (ttlSettings.HasBlock4()) {
                    VisitKeyword(ttlSettings.GetBlock4().GetToken1());
                    VisitKeyword(ttlSettings.GetBlock4().GetToken2());
                }
            } break;
            default:
                VisitAllFields(TRule_table_setting_value::GetDescriptor(), msg);
        }
    }

    void VisitTtlTierAction(const TRule_ttl_tier_action& msg) {
        switch (msg.GetAltCase()) {
            case TRule_ttl_tier_action::kAltTtlTierAction1:
                // | TO EXTERNAL DATA SOURCE an_id
                VisitKeyword(msg.GetAlt_ttl_tier_action1().GetToken1());
                VisitKeyword(msg.GetAlt_ttl_tier_action1().GetToken2());
                VisitKeyword(msg.GetAlt_ttl_tier_action1().GetToken3());
                VisitKeyword(msg.GetAlt_ttl_tier_action1().GetToken4());
                Visit(msg.GetAlt_ttl_tier_action1().GetRule_an_id5());
                break;
            case TRule_ttl_tier_action::kAltTtlTierAction2:
                // | DELETE
                VisitKeyword(msg.GetAlt_ttl_tier_action2().GetToken1());
                break;
            case TRule_ttl_tier_action::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    void VisitExpr(const TRule_expr& msg) {
        if (msg.HasAlt_expr2()) {
            Visit(msg.GetAlt_expr2());
            return;
        }
        const auto& orExpr = msg.GetAlt_expr1();
        auto getExpr = [](const TRule_expr::TAlt1::TBlock2& b) -> const TRule_or_subexpr& { return b.GetRule_or_subexpr2(); };
        auto getOp = [](const TRule_expr::TAlt1::TBlock2& b) -> const TToken& { return b.GetToken1(); };
        VisitBinaryOp(orExpr.GetRule_or_subexpr1(), getOp, getExpr, orExpr.GetBlock2().begin(), orExpr.GetBlock2().end());
    }

    void VisitCondExpr(const TRule_cond_expr& msg) {
        if (msg.Alt_case() == TRule_cond_expr::kAltCondExpr5) {
            for (const auto& block : msg.GetAlt_cond_expr5().GetBlock1()) {
                InCondExpr_ = true;
                Visit(block.GetBlock1());
                InCondExpr_ = false;
                Visit(block.GetRule_eq_subexpr2());
            }
        } else {
            VisitAllFields(TRule_cond_expr::GetDescriptor(), msg);
        }
    }

    void VisitOrSubexpr(const TRule_or_subexpr& msg) {
        auto getExpr = [](const TRule_or_subexpr::TBlock2& b) -> const TRule_and_subexpr& { return b.GetRule_and_subexpr2(); };
        auto getOp = [](const TRule_or_subexpr::TBlock2& b) -> const TToken& { return b.GetToken1(); };
        VisitBinaryOp(msg.GetRule_and_subexpr1(), getOp, getExpr, msg.GetBlock2().begin(), msg.GetBlock2().end());
    }

    void VisitAndSubexpr(const TRule_and_subexpr& msg) {
        auto getExpr = [](const TRule_and_subexpr::TBlock2& b) -> const TRule_xor_subexpr& { return b.GetRule_xor_subexpr2(); };
        auto getOp = [](const TRule_and_subexpr::TBlock2& b) -> const TToken& { return b.GetToken1(); };
        VisitBinaryOp(msg.GetRule_xor_subexpr1(), getOp, getExpr, msg.GetBlock2().begin(), msg.GetBlock2().end());
    }

    void VisitEqSubexpr(const TRule_eq_subexpr& msg) {
        auto getExpr = [](const TRule_eq_subexpr::TBlock2& b) -> const TRule_neq_subexpr& { return b.GetRule_neq_subexpr2(); };
        auto getOp = [](const TRule_eq_subexpr::TBlock2& b) -> const TToken& { return b.GetToken1(); };
        VisitBinaryOp(msg.GetRule_neq_subexpr1(), getOp, getExpr, msg.GetBlock2().begin(), msg.GetBlock2().end());
    }

    void VisitNeqSubexpr(const TRule_neq_subexpr& msg) {
        bool pushedIndent = false;
        VisitNeqSubexprImpl(msg, pushedIndent, true);
    }

    void VisitNeqSubexprImpl(const TRule_neq_subexpr& msg, bool& pushedIndent, bool top) {
        auto getExpr = [](const TRule_neq_subexpr::TBlock2& b) -> const TRule_bit_subexpr& { return b.GetRule_bit_subexpr2(); };
        auto getOp = [](const TRule_neq_subexpr::TBlock2& b) -> const TRule_neq_subexpr::TBlock2::TBlock1& { return b.GetBlock1(); };
        VisitBinaryOp(msg.GetRule_bit_subexpr1(), getOp, getExpr, msg.GetBlock2().begin(), msg.GetBlock2().end());

        if (msg.HasBlock3()) {
            const auto& b = msg.GetBlock3();
            switch (b.Alt_case()) {
                case TRule_neq_subexpr_TBlock3::kAlt1: {
                    const auto& alt = b.GetAlt1();
                    const bool hasFirstNewline = LastLine_ != ParsedTokens_[TokenIndex_].Line;
                    // 2 is `??` size in tokens
                    const bool hasSecondNewline = ParsedTokens_[TokenIndex_].Line != ParsedTokens_[TokenIndex_ + 2].Line;
                    const ui32 currentOutLine = OutLine_;

                    if (currentOutLine != OutLine_ || (hasFirstNewline && hasSecondNewline)) {
                        NewLine();
                        if (!pushedIndent) {
                            PushCurrentIndent();
                            pushedIndent = true;
                        }
                    }

                    Visit(alt.GetRule_double_question1());
                    if (hasFirstNewline || hasSecondNewline) {
                        NewLine();
                        if (!pushedIndent) {
                            PushCurrentIndent();
                            pushedIndent = true;
                        }
                    }

                    VisitNeqSubexprImpl(alt.GetRule_neq_subexpr2(), pushedIndent, false);
                    if (pushedIndent && top) {
                        PopCurrentIndent();
                        pushedIndent = false;
                    }

                    break;
                }
                case TRule_neq_subexpr_TBlock3::kAlt2:
                    Visit(b.GetAlt2());
                    break;
                default:
                    ythrow yexception() << "Alt is not supported";
            }
        }
    }

    void VisitBitSubexpr(const TRule_bit_subexpr& msg) {
        auto getExpr = [](const TRule_bit_subexpr::TBlock2& b) -> const TRule_add_subexpr& { return b.GetRule_add_subexpr2(); };
        auto getOp = [](const TRule_bit_subexpr::TBlock2& b) -> const TToken& { return b.GetToken1(); };
        VisitBinaryOp(msg.GetRule_add_subexpr1(), getOp, getExpr, msg.GetBlock2().begin(), msg.GetBlock2().end());
    }

    void VisitAddSubexpr(const TRule_add_subexpr& msg) {
        auto getExpr = [](const TRule_add_subexpr::TBlock2& b) -> const TRule_mul_subexpr& { return b.GetRule_mul_subexpr2(); };
        auto getOp = [](const TRule_add_subexpr::TBlock2& b) -> const TToken& { return b.GetToken1(); };
        VisitBinaryOp(msg.GetRule_mul_subexpr1(), getOp, getExpr, msg.GetBlock2().begin(), msg.GetBlock2().end());
    }

    void VisitMulSubexpr(const TRule_mul_subexpr& msg) {
        auto getExpr = [](const TRule_mul_subexpr::TBlock2& b) -> const TRule_con_subexpr& { return b.GetRule_con_subexpr2(); };
        auto getOp = [](const TRule_mul_subexpr::TBlock2& b) -> const TToken& { return b.GetToken1(); };
        VisitBinaryOp(msg.GetRule_con_subexpr1(), getOp, getExpr, msg.GetBlock2().begin(), msg.GetBlock2().end());
    }

    ui32 BinaryOpTokenSize(const TToken&) {
        return 1;
    }

    ui32 BinaryOpTokenSize(const TRule_neq_subexpr::TBlock2::TBlock1& block) {
        switch (block.Alt_case()) {
            case TRule_neq_subexpr::TBlock2::TBlock1::kAlt1:
            case TRule_neq_subexpr::TBlock2::TBlock1::kAlt3:
            case TRule_neq_subexpr::TBlock2::TBlock1::kAlt5:
            case TRule_neq_subexpr::TBlock2::TBlock1::kAlt6:
            case TRule_neq_subexpr::TBlock2::TBlock1::kAlt7:
                return 1;
            case TRule_neq_subexpr::TBlock2::TBlock1::kAlt2:
                return 2;
            case TRule_neq_subexpr::TBlock2::TBlock1::kAlt4:
                return 3;
            default:
                ythrow yexception() << "Alt is not supported";
        }
    }

    void VisitShiftRight(const TRule_shift_right& msg) {
        VisitToken(msg.GetToken1());
        InMultiTokenOp_ = true;
        VisitToken(msg.GetToken2());
        InMultiTokenOp_ = false;
    }

    void VisitRotRight(const TRule_rot_right& msg) {
        VisitToken(msg.GetToken1());
        InMultiTokenOp_ = true;
        VisitToken(msg.GetToken2());
        VisitToken(msg.GetToken3());
        InMultiTokenOp_ = false;
    }

    template <typename TExpr, typename TGetOp, typename TGetExpr, typename TIter>
    void VisitBinaryOp(const TExpr& expr, TGetOp getOp, TGetExpr getExpr, TIter begin, TIter end) {
        Visit(expr);

        bool pushedIndent = false;
        for (; begin != end; ++begin) {
            const auto op = getOp(*begin);
            const auto opSize = BinaryOpTokenSize(op);
            const bool hasFirstNewline = LastLine_ != ParsedTokens_[TokenIndex_].Line;
            const bool hasSecondNewline = ParsedTokens_[TokenIndex_].Line != ParsedTokens_[TokenIndex_ + opSize].Line;
            const ui32 currentOutLine = OutLine_;

            if (currentOutLine != OutLine_ || hasFirstNewline || hasSecondNewline) {
                NewLine();
                if (!pushedIndent && CurrentIndent_ == ExprLineIndent_) {
                    PushCurrentIndent();
                    pushedIndent = true;
                }
            }

            Visit(op);

            if (hasFirstNewline && hasSecondNewline) {
                NewLine();
            }

            Visit(getExpr(*begin));
        }

        if (pushedIndent) {
            PopCurrentIndent();
        }
    }

    void PushCurrentIndent() {
        CurrentIndent_ += OneIndent;

        BlockFirstLine_ = OutLine_;
        if (OutColumn_ > 0) {
            ++BlockFirstLine_;
        }
    }

    void PopCurrentIndent() {
        CurrentIndent_ -= OneIndent;
    }

private:
    const TStaticData& StaticData_;
    const TParsedTokenList& ParsedTokens_;
    const TParsedTokenList& Comments_;
    const bool AnsiLexer_;
    TStringBuilder Sb_;
    ui32 OutColumn_ = 0;
    ui32 OutLine_ = 1;
    ui32 LastLine_ = 0;
    ui32 LastColumn_ = 0;
    ui32 LastComment_ = 0;
    ui32 CommentLines_ = 0;
    i32 CurrentIndent_ = 0;
    TVector<EScope> Scopes_;
    TMaybe<bool> AddLine_;
    TMaybe<ui32> StmtCoreAltCase_;
    ui64 InsideType_ = 0;
    bool AfterNamespace_ = false;
    bool AfterBracket_ = false;
    bool AfterInvokeExpr_ = false;
    bool AfterUnaryOp_ = false;
    bool SkipSpaceAfterUnaryOp_ = false;
    bool AfterDollarOrAt_ = false;
    bool AfterDot_ = false;
    bool AfterDigits_ = false;
    bool AfterQuestion_ = false;
    bool AfterLess_ = false;
    bool AfterKeyExpr_ = false;
    bool AfterComment_ = false;
    bool InMultiTokenOp_ = false;
    bool InCondExpr_ = false;
    ui32 ForceExpandedLine_ = 0;
    ui32 ForceExpandedColumn_ = 0;
    ui32 BlockFirstLine_ = 1;
    i32 ExprLineIndent_ = 0;

    ui32 TokenIndex_ = 0;
    TMarkTokenStack MarkTokenStack_;
    TVector<TTokenInfo> MarkedTokens_;
    ui64 InsideExpr_ = 0;
};

template <typename T>
TPrettyFunctor MakePrettyFunctor(void (TPrettyVisitor::*memberPtr)(const T& msg)) {
    return [memberPtr](TPrettyVisitor& visitor, const NProtoBuf::Message& rawMsg) {
        (visitor.*memberPtr)(dynamic_cast<const T&>(rawMsg));
    };
}

template <typename T>
TObfuscatingFunctor MakeObfuscatingFunctor(void (TObfuscatingVisitor::*memberPtr)(const T& msg)) {
    return [memberPtr](TObfuscatingVisitor& visitor, const NProtoBuf::Message& rawMsg) {
        (visitor.*memberPtr)(dynamic_cast<const T&>(rawMsg));
    };
}

TStaticData::TStaticData()
    : Keywords(GetKeywords())
    , ScopeDispatch({
          {TRule_type_name::GetDescriptor(), EScope::TypeName},
          {TRule_type_name_composite::GetDescriptor(), EScope::TypeName},
          {TRule_double_question::GetDescriptor(), EScope::DoubleQuestion},
          {TRule_id::GetDescriptor(), EScope::Identifier},
          {TRule_id_or_type::GetDescriptor(), EScope::Identifier},
          {TRule_id_schema::GetDescriptor(), EScope::Identifier},
          {TRule_id_expr::GetDescriptor(), EScope::Identifier},
          {TRule_id_expr_in::GetDescriptor(), EScope::Identifier},
          {TRule_id_window::GetDescriptor(), EScope::Identifier},
          {TRule_id_table::GetDescriptor(), EScope::Identifier},
          {TRule_id_without::GetDescriptor(), EScope::Identifier},
          {TRule_id_hint::GetDescriptor(), EScope::Identifier},
          {TRule_identifier::GetDescriptor(), EScope::Identifier},
          {TRule_id_table_or_type::GetDescriptor(), EScope::Identifier},
          {TRule_bind_parameter::GetDescriptor(), EScope::Identifier},
          {TRule_an_id_as_compat::GetDescriptor(), EScope::Identifier},
      })
    , PrettyVisitDispatch({
          {TToken::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitToken)},
          {TRule_value_constructor::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitValueConstructor)},
          {TRule_into_values_source::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitIntoValuesSource)},
          {TRule_select_kind::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectKind)},
          {TRule_process_core::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitProcessCore)},
          {TRule_reduce_core::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitReduceCore)},
          {TRule_sort_specification_list::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSortSpecificationList)},
          {TRule_select_core::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectCore)},
          {TRule_row_pattern_recognition_clause::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitRowPatternRecognitionClause)},
          {TRule_join_source::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitJoinSource)},
          {TRule_join_constraint::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitJoinConstraint)},
          {TRule_single_source::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSingleSource)},
          {TRule_flatten_source::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitFlattenSource)},
          {TRule_named_single_source::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitNamedSingleSource)},
          {TRule_table_hints::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitTableHints)},
          {TRule_simple_table_ref::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSimpleTableRef)},
          {TRule_into_simple_table_ref::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitIntoSimpleTableRef)},
          {TRule_select_kind_partial::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectKindPartial)},
          {TRule_flatten_by_arg::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitFlattenByArg)},
          {TRule_without_column_list::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitWithoutColumnList)},
          {TRule_table_ref::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitTableRef)},
          {TRule_grouping_element_list::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitGroupingElementList)},
          {TRule_group_by_clause::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitGroupByClause)},
          {TRule_window_definition_list::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitWindowDefinitionList)},
          {TRule_window_specification::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitWindowSpecification)},
          {TRule_window_partition_clause::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitWindowParitionClause)},
          {TRule_lambda_body::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitLambdaBody)},
          {TRule_select_kind_parenthesis::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectKindParenthesis)},
          {TRule_cast_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCastExpr)},
          {TRule_bitcast_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitBitCastExpr)},
          {TRule_ext_order_by_clause::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitExtOrderByClause)},
          {TRule_key_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitKeyExpr)},
          {TRule_define_action_or_subquery_body::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDefineActionOrSubqueryBody)},
          {TRule_exists_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitExistsExpr)},
          {TRule_case_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCaseExpr)},
          {TRule_with_table_settings::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitWithTableSettingsExpr)},
          {TRule_table_setting_value::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitTableSettingValue)},
          {TRule_ttl_tier_action::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitTtlTierAction)},

          {TRule_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitExpr)},
          {TRule_cond_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCondExpr)},
          {TRule_or_subexpr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitOrSubexpr)},
          {TRule_and_subexpr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAndSubexpr)},
          {TRule_eq_subexpr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitEqSubexpr)},
          {TRule_neq_subexpr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitNeqSubexpr)},
          {TRule_bit_subexpr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitBitSubexpr)},
          {TRule_add_subexpr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAddSubexpr)},
          {TRule_mul_subexpr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitMulSubexpr)},

          {TRule_rot_right::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitRotRight)},
          {TRule_shift_right::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitShiftRight)},

          {TRule_pragma_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitPragma)},
          {TRule_select_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelect)},
          {TRule_select_stmt_intersect::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectIntersect)},
          {TRule_smart_parenthesis::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSmartParenthesis)},
          {TRule_select_subexpr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectSubExpr)},
          {TRule_select_subexpr_intersect::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectSubExprIntersect)},
          {TRule_select_unparenthesized_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectUnparenthesized)},
          {TRule_select_unparenthesized_stmt_intersect::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectUnparenthesizedIntersect)},
          {TRule_named_nodes_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitNamedNodes)},
          {TRule_create_table_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateTable)},
          {TRule_drop_table_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropTable)},
          {TRule_use_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitUse)},
          {TRule_into_table_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitIntoTable)},
          {TRule_commit_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCommit)},
          {TRule_update_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitUpdate)},
          {TRule_delete_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDelete)},
          {TRule_rollback_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitRollback)},
          {TRule_declare_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDeclare)},
          {TRule_import_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitImport)},
          {TRule_export_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitExport)},
          {TRule_alter_table_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterTable)},
          {TRule_alter_external_table_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterExternalTable)},
          {TRule_do_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDo)},
          {TRule_define_action_or_subquery_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAction)},
          {TRule_if_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitIf)},
          {TRule_for_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitFor)},
          {TRule_values_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitValues)},
          {TRule_create_user_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateUser)},
          {TRule_alter_user_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterUser)},
          {TRule_create_group_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateGroup)},
          {TRule_alter_group_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterGroup)},
          {TRule_drop_role_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropRole)},
          {TRule_upsert_object_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitUpsertObject)},
          {TRule_create_object_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateObject)},
          {TRule_alter_object_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterObject)},
          {TRule_drop_object_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropObject)},
          {TRule_create_external_data_source_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateExternalDataSource)},
          {TRule_alter_external_data_source_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterExternalDataSource)},
          {TRule_drop_external_data_source_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropExternalDataSource)},
          {TRule_create_replication_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateAsyncReplication)},
          {TRule_alter_replication_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterAsyncReplication)},
          {TRule_drop_replication_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropAsyncReplication)},
          {TRule_create_transfer_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateTransfer)},
          {TRule_alter_transfer_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterTransfer)},
          {TRule_drop_transfer_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropTransfer)},
          {TRule_create_topic_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateTopic)},
          {TRule_alter_topic_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterTopic)},
          {TRule_drop_topic_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropTopic)},
          {TRule_grant_permissions_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitGrantPermissions)},
          {TRule_revoke_permissions_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitRevokePermissions)},
          {TRule_alter_table_store_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterTableStore)},
          {TRule_create_view_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateView)},
          {TRule_drop_view_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropView)},
          {TRule_create_resource_pool_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateResourcePool)},
          {TRule_alter_resource_pool_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterResourcePool)},
          {TRule_drop_resource_pool_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropResourcePool)},
          {TRule_create_backup_collection_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateBackupCollection)},
          {TRule_alter_backup_collection_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterBackupCollection)},
          {TRule_drop_backup_collection_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropBackupCollection)},
          {TRule_analyze_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAnalyze)},
          {TRule_create_resource_pool_classifier_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateResourcePoolClassifier)},
          {TRule_alter_resource_pool_classifier_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterResourcePoolClassifier)},
          {TRule_drop_resource_pool_classifier_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropResourcePoolClassifier)},
          {TRule_backup_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitBackup)},
          {TRule_restore_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitRestore)},
          {TRule_alter_sequence_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterSequence)},
          {TRule_alter_database_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterDatabase)},
          {TRule_truncate_table_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitTruncateTable)},
          {TRule_show_create_table_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitShowCreateTable)},
          {TRule_streaming_query_settings::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitStreamingQuerySettings)},
          {TRule_create_streaming_query_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateStreamingQuery)},
          {TRule_alter_streaming_query_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterStreamingQuery)},
          {TRule_drop_streaming_query_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropStreamingQuery)},
          {TRule_create_secret_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCreateSecret)},
          {TRule_alter_secret_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitAlterSecret)},
          {TRule_drop_secret_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDropSecret)},
      })
    , ObfuscatingVisitDispatch({
          {TToken::GetDescriptor(), MakeObfuscatingFunctor(&TObfuscatingVisitor::VisitToken)},
          {TRule_literal_value::GetDescriptor(), MakeObfuscatingFunctor(&TObfuscatingVisitor::VisitLiteralValue)},
          {TRule_pragma_value::GetDescriptor(), MakeObfuscatingFunctor(&TObfuscatingVisitor::VisitPragmaValue)},
          {TRule_atom_expr::GetDescriptor(), MakeObfuscatingFunctor(&TObfuscatingVisitor::VisitAtomExpr)},
          {TRule_in_atom_expr::GetDescriptor(), MakeObfuscatingFunctor(&TObfuscatingVisitor::VisitInAtomExpr)},
          {TRule_unary_casual_subexpr::GetDescriptor(), MakeObfuscatingFunctor(&TObfuscatingVisitor::VisitUnaryCasualSubexpr)},
          {TRule_in_unary_casual_subexpr::GetDescriptor(), MakeObfuscatingFunctor(&TObfuscatingVisitor::VisitInUnaryCasualSubexpr)},
      })
{
    // ensure that all statements have a visitor
    auto coreDescr = TRule_sql_stmt_core::GetDescriptor();
    for (int i = 0; i < coreDescr->field_count(); ++i) {
        const NProtoBuf::FieldDescriptor* fd = coreDescr->field(i);
        if (fd->cpp_type() != NProtoBuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            continue;
        }

        auto altDescr = fd->message_type();
        for (int j = 0; j < altDescr->field_count(); ++j) {
            auto fd2 = altDescr->field(j);
            if (fd2->cpp_type() != NProtoBuf::FieldDescriptor::CPPTYPE_MESSAGE) {
                continue;
            }

            auto stmtMessage = fd2->message_type();
            Y_ENSURE(PrettyVisitDispatch.contains(stmtMessage), TStringBuilder() << "Missing visitor for " << stmtMessage->name());
        }
    }
}

class TSqlFormatter: public NSQLFormat::ISqlFormatter {
public:
    TSqlFormatter(const NSQLTranslationV1::TLexers& lexers,
                  const NSQLTranslationV1::TParsers& parsers,
                  const NSQLTranslation::TTranslationSettings& settings)
        : Lexers_(lexers)
        , Parsers_(parsers)
        , Settings_(settings)
    {
    }

    bool Format(const TString& query, TString& formattedQuery, NYql::TIssues& issues, EFormatMode mode) override {
        formattedQuery = (mode == EFormatMode::Obfuscate) ? "" : query;
        auto parsedSettings = Settings_;
        if (!NSQLTranslation::ParseTranslationSettings(query, parsedSettings, issues)) {
            return false;
        }

        if (parsedSettings.PgParser) {
            return mode != EFormatMode::Obfuscate;
        }

        if (mode == EFormatMode::Obfuscate) {
            auto message = NSQLTranslationV1::SqlAST(Parsers_, query, parsedSettings.File, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS, parsedSettings.AnsiLexer, parsedSettings.Arena);
            if (!message) {
                return false;
            }

            TObfuscatingVisitor visitor;
            return Format(visitor.Process(*message), formattedQuery, issues, EFormatMode::Pretty);
        }

        auto lexer = NSQLTranslationV1::MakeLexer(Lexers_, parsedSettings.AnsiLexer);
        TVector<TString> statements;
        if (!NSQLTranslationV1::SplitQueryToStatements(query, lexer, statements, issues, parsedSettings.File, false)) {
            return false;
        }

        TStringBuilder finalFormattedQuery;
        bool prevAddLine = false;
        TMaybe<ui32> prevStmtCoreAltCase;
        for (const TString& stmt : statements) {
            bool hasNewlinesBefore = LeadingNLsCount(stmt) > 1;
            TString currentQuery = StripStringLeft(stmt);
            if (AllOf(currentQuery, [](char x) { return x == ';'; })) {
                continue;
            }

            TVector<NSQLTranslation::TParsedToken> comments;
            TParsedTokenList parsedTokens, stmtTokens;
            auto onNextRawToken = [&](NSQLTranslation::TParsedToken&& token) {
                stmtTokens.push_back(token);
                if (token.Name == "COMMENT") {
                    comments.emplace_back(std::move(token));
                } else if (token.Name != "WS" && token.Name != "EOF") {
                    parsedTokens.emplace_back(std::move(token));
                }
            };

            if (!lexer->Tokenize(currentQuery, parsedSettings.File, onNextRawToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
                return false;
            }

            NYql::TIssues parserIssues;
            auto message = NSQLTranslationV1::SqlAST(Parsers_, currentQuery, parsedSettings.File, parserIssues, NSQLTranslation::SQL_MAX_PARSER_ERRORS, parsedSettings.AnsiLexer, parsedSettings.Arena);
            if (!message) {
                finalFormattedQuery << currentQuery;
                if (!currentQuery.EndsWith("\n")) {
                    finalFormattedQuery << "\n";
                }

                continue;
            }

            TPrettyVisitor visitor(parsedTokens, comments, parsedSettings.AnsiLexer);
            bool addLineBefore = false;
            bool addLineAfter = false;
            TMaybe<ui32> stmtCoreAltCase;
            bool hasCommentBefore = !comments.empty() && !parsedTokens.empty() && comments.front().Line < parsedTokens.front().Line;
            auto currentFormattedQuery = visitor.Process(*message, addLineBefore, addLineAfter, stmtCoreAltCase);

            TParsedTokenList stmtFormattedTokens;
            auto onNextFormattedToken = [&](NSQLTranslation::TParsedToken&& token) {
                stmtFormattedTokens.push_back(token);
            };

            if (!lexer->Tokenize(currentFormattedQuery, parsedSettings.File, onNextFormattedToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
                return false;
            }

            if (!Validate(stmtTokens, stmtFormattedTokens)) {
                issues.AddIssue(NYql::TIssue({}, TStringBuilder() << "Validation failed: " << currentQuery.Quote() << " != " << currentFormattedQuery.Quote()));
                return false;
            }

            const bool differentStmtAltCase = prevStmtCoreAltCase.Defined() && stmtCoreAltCase != prevStmtCoreAltCase;
            if ((addLineBefore || prevAddLine || differentStmtAltCase || (hasNewlinesBefore && !hasCommentBefore)) && !finalFormattedQuery.empty()) {
                finalFormattedQuery << "\n";
            }
            prevAddLine = addLineAfter;
            prevStmtCoreAltCase = stmtCoreAltCase;

            finalFormattedQuery << currentFormattedQuery;
            if (parsedTokens.back().Name != "SEMICOLON") {
                finalFormattedQuery << ";\n";
            }
        }

        formattedQuery = finalFormattedQuery;
        return true;
    }

private:
    const NSQLTranslationV1::TLexers Lexers_;
    const NSQLTranslationV1::TParsers Parsers_;
    const NSQLTranslation::TTranslationSettings Settings_;
};

} // namespace

ISqlFormatter::TPtr MakeSqlFormatter(const NSQLTranslationV1::TLexers& lexers,
                                     const NSQLTranslationV1::TParsers& parsers,
                                     const NSQLTranslation::TTranslationSettings& settings) {
    return ISqlFormatter::TPtr(new TSqlFormatter(lexers, parsers, settings));
}

TString MutateQuery(const NSQLTranslationV1::TLexers& lexers,
                    const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    auto parsedSettings = settings;
    NYql::TIssues issues;
    if (!NSQLTranslation::ParseTranslationSettings(query, parsedSettings, issues)) {
        throw yexception() << issues.ToString();
    }

    auto lexer = NSQLTranslationV1::MakeLexer(lexers, parsedSettings.AnsiLexer);
    TVector<NSQLTranslation::TParsedToken> allTokens;
    auto onNextToken = [&](NSQLTranslation::TParsedToken&& token) {
        if (token.Name != "EOF") {
            allTokens.push_back(token);
        }
    };

    if (!lexer->Tokenize(query, parsedSettings.File, onNextToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
        throw yexception() << issues.ToString();
    }

    TStringBuilder newQueryBuilder;
    ui32 index = 0;
    for (const auto& x : allTokens) {
        newQueryBuilder << " /*" << index++ << "*/ ";
        newQueryBuilder << x.Content;
    }

    newQueryBuilder << " /*" << index++ << "*/ ";
    return newQueryBuilder;
}

bool SqlFormatSimple(const NSQLTranslationV1::TLexers& lexers,
                     const NSQLTranslationV1::TParsers& parsers,
                     const TString& query, TString& formattedQuery, TString& error) {
    try {
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;

        auto formatter = MakeSqlFormatter(lexers, parsers, settings);
        NYql::TIssues issues;
        const bool result = formatter->Format(query, formattedQuery, issues);
        if (!result) {
            error = issues.ToString();
        }
        return result;
    } catch (const std::exception& e) {
        error = e.what();
        return false;
    }
}

THashSet<TString> GetKeywords() {
    TString grammar;
    Y_ENSURE(NResource::FindExact("SQLv1Antlr4.g.in", &grammar));
    THashSet<TString> res;
    TVector<TString> lines;
    Split(grammar, "\n", lines);
    for (auto s : lines) {
        s = StripString(s);
        if (s.StartsWith("//")) {
            continue;
        }

        auto pos1 = s.find(':');
        auto pos2 = s.find(';');
        if (pos1 == TString::npos || pos2 == TString::npos || pos2 < pos1 + 2) {
            continue;
        }

        auto before = s.substr(0, pos1);
        auto after = s.substr(pos1 + 1, pos2 - pos1 - 1);
        SubstGlobal(after, " ", "");
        SubstGlobal(after, "'", "");
        if (after == before) {
            // Cerr << before << "\n";
            res.insert(before);
        }
    }

    return res;
}

} // namespace NSQLFormat
