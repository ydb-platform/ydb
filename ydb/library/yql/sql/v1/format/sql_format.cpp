#include "sql_format.h"

#include <ydb/library/yql/parser/lexer_common/lexer.h>
#include <ydb/library/yql/core/sql_types/simple_types.h>

#include <ydb/library/yql/sql/v1/lexer/lexer.h>
#include <ydb/library/yql/sql/v1/proto_parser/proto_parser.h>

#include <ydb/library/yql/parser/proto_ast/gen/v1_proto_split/SQLv1Parser.pb.main.h>

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

bool Validate(const TParsedTokenList& query, const TParsedTokenList& formattedQuery) {
    auto in = query.begin();
    auto out = formattedQuery.begin();
    auto inEnd = query.end();
    auto outEnd = formattedQuery.end();

    while (in != inEnd && out != outEnd) {
        in = SkipWS(in, inEnd);
        out = SkipWS(out, outEnd);
        if (in != inEnd && out != outEnd) {
            if (in->Name != out->Name) {
                return false;
            }
            if (AsciiEqualsIgnoreCase(in->Name, in->Content)) {
                if (!AsciiEqualsIgnoreCase(in->Content, out->Content)) {
                    return false;
                }
            } else {
                if (in->Content != out->Content) {
                    return false;
                }
            }
            ++in;
            ++out;
        }
    }
    in = SkipWS(in, inEnd);
    out = SkipWS(out, outEnd);
    return in == inEnd && out == outEnd;
}

enum EParenType {
    Open,
    Close,
    None
};

using TAdvanceCallback = std::function<EParenType(TTokenIterator& curr, TTokenIterator end)>;

TTokenIterator SkipToNextBalanced(TTokenIterator begin, TTokenIterator end, const TAdvanceCallback& advance) {
    i64 level = 0;
    TTokenIterator curr = begin;
    while (curr != end) {
        switch (advance(curr, end)) {
            case EParenType::Open: {
                ++level;
                break;
            }
            case EParenType::Close: {
                --level;
                if (level < 0) {
                    return end;
                } else if (level == 0) {
                    return curr;
                }
                break;
            }
            case EParenType::None:
                break;
        }
    }
    return curr;
}

TTokenIterator GetNextStatementBegin(TTokenIterator begin, TTokenIterator end) {
    TAdvanceCallback advanceLambdaBody = [](TTokenIterator& curr, TTokenIterator end) -> EParenType {
        Y_UNUSED(end);
        if (curr->Name == "LBRACE_CURLY") {
            ++curr;
            return EParenType::Open;
        } else if (curr->Name == "RBRACE_CURLY") {
            ++curr;
            return EParenType::Close;
        } else {
            ++curr;
            return EParenType::None;
        }
    };

    TAdvanceCallback advanceAction = [](TTokenIterator& curr, TTokenIterator end) -> EParenType {
        auto tmp = curr;
        if (curr->Name == "DEFINE") {
            ++curr;
            curr = SkipWSOrComment(curr, end);
            if (curr != end && (curr->Name == "ACTION" || curr->Name == "SUBQUERY")) {
                ++curr;
                return EParenType::Open;
            }
        } else if (curr->Name == "END") {
            ++curr;
            curr = SkipWSOrComment(curr, end);
            if (curr != end && curr->Name == "DEFINE") {
                ++curr;
                return EParenType::Close;
            }
        }

        curr = tmp;
        ++curr;
        return EParenType::None;
    };

    TAdvanceCallback advanceInlineAction = [](TTokenIterator& curr, TTokenIterator end) -> EParenType {
        auto tmp = curr;
        if (curr->Name == "DO") {
            ++curr;
            curr = SkipWSOrComment(curr, end);
            if (curr != end && curr->Name == "BEGIN") {
                ++curr;
                return EParenType::Open;
            }
        } else if (curr->Name == "END") {
            ++curr;
            curr = SkipWSOrComment(curr, end);
            if (curr != end && curr->Name == "DO") {
                ++curr;
                return EParenType::Close;
            }
        }

        curr = tmp;
        ++curr;
        return EParenType::None;
    };

    TTokenIterator curr = begin;
    while (curr != end) {
        bool matched = false;
        for (auto cb : {advanceLambdaBody, advanceAction, advanceInlineAction}) {
            TTokenIterator tmp = curr;
            if (cb(tmp, end) == EParenType::Open) {
                curr = SkipToNextBalanced(curr, end, cb);
                matched = true;
                if (curr == end) {
                    return curr;
                }
            }
        }
        if (matched) {
            continue;
        }
        if (curr->Name == "SEMICOLON") {
            ++curr;
            break;
        }
        ++curr;
    }

    return curr;
}

void SplitByStatements(TTokenIterator begin, TTokenIterator end, TVector<TTokenIterator>& output) {
    output.clear();
    if (begin == end) {
        return;
    }
    output.push_back(begin);
    auto curr = begin;
    while (curr != end) {
        curr = GetNextStatementBegin(curr, end);
        output.push_back(curr);
    }
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
        : StaticData(TStaticData::GetInstance())
    {}

    TString Process(const NProtoBuf::Message& msg) {
        Scopes.push_back(EScope::Default);
        Visit(msg);
        return SB;
    }

private:
    void VisitToken(const TToken& token) {
        auto str = token.GetValue();
        if (str == "<EOF>") {
            return;
        }

        if (!First) {
            SB << ' ';
        } else {
            First = false;
        }

        if (str == "$" && FuncCall) {
            FuncCall = false;
        }

        if (Scopes.back() == EScope::Identifier && !FuncCall) {
            if (str != "$" && !NYql::LookupSimpleTypeBySqlAlias(str, true)) {
                SB << "id";
            } else {
                SB << str;
            }
        } else if (NextToken) {
            SB << *NextToken;
            NextToken = Nothing();
        } else {
            SB << str;
        }
    }

    void VisitPragmaValue(const TRule_pragma_value& msg) {
        switch (msg.Alt_case()) {
        case TRule_pragma_value::kAltPragmaValue1: {
            NextToken = "0";
            break;
        }
        case TRule_pragma_value::kAltPragmaValue3: {
            NextToken = "'str'";
            break;
        }
        case TRule_pragma_value::kAltPragmaValue4: {
            NextToken = "false";
            break;
        }
        default:;
        }
        VisitAllFields(TRule_pragma_value::GetDescriptor(), msg);
    }

    void VisitLiteralValue(const TRule_literal_value& msg) {
        switch (msg.Alt_case()) {
        case TRule_literal_value::kAltLiteralValue1: {
            NextToken = "0";
            break;
        }
        case TRule_literal_value::kAltLiteralValue2: {
            NextToken = "0.0";
            break;
        }
        case TRule_literal_value::kAltLiteralValue3: {
            NextToken = "'str'";
            break;
        }
        case TRule_literal_value::kAltLiteralValue9: {
            NextToken = "false";
            break;
        }
        default:;
        }

        VisitAllFields(TRule_literal_value::GetDescriptor(), msg);
    }

    void VisitAtomExpr(const TRule_atom_expr& msg) {
        switch (msg.Alt_case()) {
        case TRule_atom_expr::kAltAtomExpr7: {
            FuncCall = true;
            break;
        }
        default:;
        }

        VisitAllFields(TRule_atom_expr::GetDescriptor(), msg);
        FuncCall = false;
    }

    void VisitInAtomExpr(const TRule_in_atom_expr& msg) {
        switch (msg.Alt_case()) {
        case TRule_in_atom_expr::kAltInAtomExpr6: {
            FuncCall = true;
            break;
        }
        default:;
        }

        VisitAllFields(TRule_in_atom_expr::GetDescriptor(), msg);
        FuncCall = false;
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
            FuncCall = true;
        }

        Visit(msg.GetBlock1());
        if (invoke) {
            FuncCall = false;
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
            FuncCall = true;
        }

        Visit(msg.GetBlock1());
        if (invoke) {
            FuncCall = false;
        }

        Visit(msg.GetRule_unary_subexpr_suffix2());
    }

    void Visit(const NProtoBuf::Message& msg) {
        const NProtoBuf::Descriptor* descr = msg.GetDescriptor();
        auto scopePtr = StaticData.ScopeDispatch.FindPtr(descr);
        if (scopePtr) {
            Scopes.push_back(*scopePtr);
        }

        auto funcPtr = StaticData.ObfuscatingVisitDispatch.FindPtr(descr);
        if (funcPtr) {
            (*funcPtr)(*this, msg);
        } else {
            VisitAllFields(descr, msg);
        }

        if (scopePtr) {
            Scopes.pop_back();
        }
    }

    void VisitAllFields(const NProtoBuf::Descriptor* descr, const NProtoBuf::Message& msg) {
        VisitAllFieldsImpl<TObfuscatingVisitor, &TObfuscatingVisitor::Visit>(this, descr, msg);
    }

    const TStaticData& StaticData;
    TStringBuilder SB;
    bool First = true;
    TMaybe<TString> NextToken;
    TVector<EScope> Scopes;
    bool FuncCall = false;
};

class TPrettyVisitor {
friend struct TStaticData;
public:
    TPrettyVisitor(const TParsedTokenList& parsedTokens, const TParsedTokenList& comments)
        : StaticData(TStaticData::GetInstance())
        , ParsedTokens(parsedTokens)
        , Comments(comments)
    {
    }

    TString Process(const NProtoBuf::Message& msg, bool& addLine) {
        Scopes.push_back(EScope::Default);
        MarkedTokens.reserve(ParsedTokens.size());
        MarkTokens(msg);
        Y_ENSURE(MarkTokenStack.empty());
        Y_ENSURE(TokenIndex == ParsedTokens.size());
        TokenIndex = 0;
        Visit(msg);
        Y_ENSURE(TokenIndex == ParsedTokens.size());
        Y_ENSURE(MarkTokenStack.empty());
        for (; LastComment < Comments.size(); ++LastComment) {
            const auto text = Comments[LastComment].Content;
            AddComment(text);
        }
        addLine = AddLine.GetOrElse(true);

        return SB;
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
        for (ui32 i = 0; i < s.Size(); ++i) {
            Out(s[i], i == 0);
        }
    }

    void Out(char c, bool useIndent = true) {
        if (c == '\n' || c == '\r') {
            SB << c;
            if (!(c == '\n' && !SB.empty() && SB.back() == '\r')) {
                // do not increase OutLine if \n is preceded by \r
                // this way we handle \r, \n, or \r\n as single new line
                ++OutLine;
            }
            OutColumn = 0;
        } else {
            if (!OutColumn && useIndent) {
                ui32 indent = (CurrentIndent >= 0) ? CurrentIndent : 0;
                for (ui32 i = 0; i < indent; ++i) {
                    SB << ' ';
                }
            }

            SB << c;
            ++OutColumn;
        }
    }

    void NewLine() {
        if (OutColumn) {
            Out('\n');
        }
    }

    void AddComment(TStringBuf text) {
        if (text.StartsWith("--") && !SB.Empty() && SB.back() == '-') {
            Out(' ');
        }

        Out(text);
    }

    void MarkTokens(const NProtoBuf::Message& msg) {
        const NProtoBuf::Descriptor* descr = msg.GetDescriptor();
        auto scopePtr = StaticData.ScopeDispatch.FindPtr(descr);
        if (scopePtr) {
            if (*scopePtr == EScope::TypeName) {
                ++InsideType;
            }

            Scopes.push_back(*scopePtr);
        }

        bool suppressExpr = false;
        if (descr == TToken::GetDescriptor()) {
            const auto& token = dynamic_cast<const TToken&>(msg);
            MarkToken(token);
        } else if (descr == TRule_sql_stmt_core::GetDescriptor()) {
            if (AddLine.Empty()) {
                AddLine = !IsSimpleStatement(dynamic_cast<const TRule_sql_stmt_core&>(msg)).GetOrElse(false);
            }
        } else if (descr == TRule_lambda_body::GetDescriptor()) {
            Y_ENSURE(TokenIndex >= 1);
            auto prevIndex = TokenIndex - 1;
            Y_ENSURE(prevIndex < ParsedTokens.size());
            Y_ENSURE(ParsedTokens[prevIndex].Content == "{");
            MarkedTokens[prevIndex].OpeningBracket = false;
            ForceExpandedColumn = ParsedTokens[prevIndex].LinePos;
            ForceExpandedLine = ParsedTokens[prevIndex].Line;
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
                ForceExpandedColumn = paren.GetColumn();
                ForceExpandedLine = paren.GetLine();
            }
            suppressExpr = true;
        } else if (descr == TRule_exists_expr::GetDescriptor()) {
            const auto& value = dynamic_cast<const TRule_exists_expr&>(msg);
            auto& paren = value.GetToken2();
            ForceExpandedColumn = paren.GetColumn();
            ForceExpandedLine = paren.GetLine();
            suppressExpr = true;
        } else if (descr == TRule_case_expr::GetDescriptor()) {
            const auto& value = dynamic_cast<const TRule_case_expr&>(msg);
            auto& token = value.GetToken1();
            ForceExpandedColumn = token.GetColumn();
            ForceExpandedLine = token.GetLine();
        }

        const bool expr = (descr == TRule_expr::GetDescriptor() || descr == TRule_in_expr::GetDescriptor());
        if (expr) {
            ++InsideExpr;
        }

        ui64 prevInsideExpr = InsideExpr;
        if (suppressExpr) {
            InsideExpr = 0;
        }

        VisitAllFieldsImpl<TPrettyVisitor, &TPrettyVisitor::MarkTokens>(this, descr, msg);
        if (suppressExpr) {
            InsideExpr = prevInsideExpr;
        }

        if (scopePtr) {
            if (*scopePtr == EScope::TypeName) {
                --InsideType;
            }

            Scopes.pop_back();
        }

        if (expr) {
            --InsideExpr;
        }
    }

    void MarkToken(const TToken& token) {
        auto str = token.GetValue();
        if (str == "<EOF>") {
            return;
        }

        MarkedTokens.emplace_back();
        if (str == "(" || str == "[" || str == "{" || str == "<|" || (InsideType && str == "<")) {
            MarkTokenStack.push_back(TokenIndex);
            auto& info = MarkedTokens[TokenIndex];
            info.OpeningBracket = (InsideExpr > 0);
        } else if (str == ")") {
            PopBracket("(");
        } else if (str == "]") {
            PopBracket("[");
        } else if (str == "}") {
            PopBracket("{");
        } else if (str == "|>") {
            PopBracket("<|");
        } else if (InsideType && str == ">") {
            PopBracket("<");
        }

        TokenIndex++;
    }

    void PopBracket(const TString& expected) {
        Y_ENSURE(!MarkTokenStack.empty());
        Y_ENSURE(MarkTokenStack.back() < ParsedTokens.size());
        auto& openToken = ParsedTokens[MarkTokenStack.back()];
        Y_ENSURE(openToken.Content == expected);
        auto& openInfo = MarkedTokens[MarkTokenStack.back()];
        auto& closeInfo = MarkedTokens[TokenIndex];
        const bool forcedExpansion = openToken.Line == ForceExpandedLine && openToken.LinePos <= ForceExpandedColumn;

        if (openInfo.OpeningBracket) {
            openInfo.ClosingBracketIndex = TokenIndex;
            openInfo.BracketForcedExpansion = forcedExpansion;
            closeInfo.BracketForcedExpansion = forcedExpansion;
            closeInfo.ClosingBracket = true;
        }

        MarkTokenStack.pop_back();
    }

    void Visit(const NProtoBuf::Message& msg) {
        const NProtoBuf::Descriptor* descr = msg.GetDescriptor();
        //Cerr << descr->name() << "\n";
        auto scopePtr = StaticData.ScopeDispatch.FindPtr(descr);
        if (descr == TRule_invoke_expr::GetDescriptor()) {
            AfterInvokeExpr = true;
        }

        if (descr == TRule_unary_op::GetDescriptor()) {
            AfterUnaryOp = true;
        }

        if (scopePtr) {
            if (*scopePtr == EScope::TypeName) {
                ++InsideType;
            }

            Scopes.push_back(*scopePtr);
        }

        auto funcPtr = StaticData.PrettyVisitDispatch.FindPtr(descr);
        if (funcPtr) {
            (*funcPtr)(*this, msg);
        } else {
            VisitAllFields(descr, msg);
        }

        if (scopePtr) {
            if (*scopePtr == EScope::TypeName) {
                --InsideType;
            }

            Scopes.pop_back();
        }
    }

    TMaybe<bool> IsSimpleStatement(const TRule_sql_stmt_core& msg) {
        switch (msg.Alt_case()) {
            case TRule_sql_stmt_core::kAltSqlStmtCore1: // pragma
            case TRule_sql_stmt_core::kAltSqlStmtCore5: // drop table
            case TRule_sql_stmt_core::kAltSqlStmtCore6: // use
            case TRule_sql_stmt_core::kAltSqlStmtCore8: // commit
            case TRule_sql_stmt_core::kAltSqlStmtCore11: // rollback
            case TRule_sql_stmt_core::kAltSqlStmtCore12: // declare
            case TRule_sql_stmt_core::kAltSqlStmtCore13: // import
            case TRule_sql_stmt_core::kAltSqlStmtCore14: // export
            case TRule_sql_stmt_core::kAltSqlStmtCore32: // drop external data source
            case TRule_sql_stmt_core::kAltSqlStmtCore34: // drop replication
                return true;
            case TRule_sql_stmt_core::kAltSqlStmtCore3: { // named nodes
                const auto& stmt = msg.GetAlt_sql_stmt_core3().GetRule_named_nodes_stmt1();
                if (stmt.GetBlock3().HasAlt1()) {
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

    template <typename T>
    void VisitRepeated(const ::google::protobuf::RepeatedPtrField<T>& field) {
        for (const auto& m : field) {
            Visit(m);
        }
    }

    void VisitDefineActionOrSubqueryBody(const TRule_define_action_or_subquery_body& msg) {
        VisitRepeated(msg.GetBlock1());
        if (msg.HasBlock2()) {
            const auto& b = msg.GetBlock2();
            Visit(b.GetRule_sql_stmt_core1());
            for (auto block : b.GetBlock2()) {
                VisitRepeated(block.GetBlock1());
                if (!IsSimpleStatement(block.GetRule_sql_stmt_core2()).GetOrElse(false)) {
                    Out('\n');
                }
                Visit(block.GetRule_sql_stmt_core2());
            }

            VisitRepeated(b.GetBlock3());
        }
    }

    void VisitPragma(const TRule_pragma_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitKeyword(msg.GetToken1());
        auto prefix = msg.GetRule_opt_id_prefix_or_type2();
        if (prefix.HasBlock1()) {
            Visit(prefix.GetBlock1().GetRule_an_id_or_type1());
            VisitKeyword(prefix.GetBlock1().GetToken2());
            AfterDot = true;
        }

        Visit(msg.GetRule_an_id3());
        if (msg.GetBlock4().HasAlt2()) {
            AfterInvokeExpr = true;
            const auto& alt2 = msg.GetBlock4().GetAlt2();
            VisitKeyword(alt2.GetToken1());
            Visit(alt2.GetRule_pragma_value2());
            VisitRepeated(alt2.GetBlock3());
            VisitKeyword(alt2.GetToken4());
        } else {
            Visit(msg.GetBlock4());
        }
    }

    void PosFromPartial(const TRule_select_kind_partial& partial) {
        const auto& kind = partial.GetRule_select_kind1();
        if (kind.HasBlock1()) { // DISCARD
            PosFromToken(kind.GetBlock1().GetToken1());
        } else {
            switch (kind.GetBlock2().Alt_case()) {
            case TRule_select_kind_TBlock2::kAlt1:
                PosFromToken(kind.GetBlock2().GetAlt1().GetRule_process_core1().GetToken1());
                break;
            case TRule_select_kind_TBlock2::kAlt2:
                PosFromToken(kind.GetBlock2().GetAlt2().GetRule_reduce_core1().GetToken1());
                break;
            case TRule_select_kind_TBlock2::kAlt3: {
                const auto& selCore = kind.GetBlock2().GetAlt3().GetRule_select_core1();
                if (selCore.HasBlock1()) {
                    PosFromToken(selCore.GetBlock1().GetToken1());
                } else {
                    PosFromToken(selCore.GetToken2());
                }

                break;
            }

            default:
                ythrow yexception() << "Alt is not supported";
            }
        }
    }

    void VisitSelect(const TRule_select_stmt& msg) {
        const auto& paren = msg.GetRule_select_kind_parenthesis1();
        if (paren.Alt_case() == TRule_select_kind_parenthesis::kAltSelectKindParenthesis1) {
            const auto& partial = paren.GetAlt_select_kind_parenthesis1().GetRule_select_kind_partial1();
            PosFromPartial(partial);
        } else {
            PosFromToken(paren.GetAlt_select_kind_parenthesis2().GetToken1());
        }

        NewLine();
        Visit(msg.GetRule_select_kind_parenthesis1());
        for (const auto& block : msg.GetBlock2()) {
            NewLine();
            Visit(block.GetRule_select_op1());
            NewLine();
            Visit(block.GetRule_select_kind_parenthesis2());
        }
    }

    void VisitSelectUnparenthesized(const TRule_select_unparenthesized_stmt& msg) {
        const auto& partial = msg.GetRule_select_kind_partial1();
        PosFromPartial(partial);
        NewLine();
        Visit(msg.GetRule_select_kind_partial1());
        for (const auto& block : msg.GetBlock2()) {
            NewLine();
            Visit(block.GetRule_select_op1());
            NewLine();
            Visit(block.GetRule_select_kind_parenthesis2());
        }
    }

    void VisitNamedNodes(const TRule_named_nodes_stmt& msg) {
        PosFromToken(msg.GetRule_bind_parameter_list1().GetRule_bind_parameter1().GetToken1());
        NewLine();
        Visit(msg.GetRule_bind_parameter_list1());
        Visit(msg.GetToken2());
        switch (msg.GetBlock3().Alt_case()) {
        case TRule_named_nodes_stmt::TBlock3::kAlt1: {
            const auto& alt = msg.GetBlock3().GetAlt1();
            Visit(alt);
            break;
        }

        case TRule_named_nodes_stmt::TBlock3::kAlt2: {
            const auto& alt = msg.GetBlock3().GetAlt2();
            const auto& subselect = alt.GetRule_subselect_stmt1();
            switch (subselect.GetBlock1().Alt_case()) {
            case TRule_subselect_stmt::TBlock1::kAlt1: {
                const auto& alt = subselect.GetBlock1().GetAlt1();
                Visit(alt.GetToken1());
                NewLine();
                PushCurrentIndent();
                Visit(alt.GetRule_select_stmt2());
                PopCurrentIndent();
                NewLine();
                Visit(alt.GetToken3());
                break;
            }

            case TRule_subselect_stmt::TBlock1::kAlt2: {
                const auto& alt = subselect.GetBlock1().GetAlt2();
                NewLine();
                PushCurrentIndent();
                Visit(alt);
                PopCurrentIndent();
                break;
            }

            default:
                ythrow yexception() << "Alt is not supported";
            }

            break;
        }

        default:
            ythrow yexception() << "Alt is not supported";
        }
    }

    void VisitCreateTable(const TRule_create_table_stmt& msg) {
        PosFromToken(msg.GetToken1());
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
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_drop_table_stmt::GetDescriptor(), msg);
    }

    void VisitUse(const TRule_use_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_use_stmt::GetDescriptor(), msg);
    }

    void VisitIntoTable(const TRule_into_table_stmt& msg) {
        switch (msg.GetBlock1().Alt_case()) {
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt1:
            PosFromToken(msg.GetBlock1().GetAlt1().GetToken1());
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt2:
            PosFromToken(msg.GetBlock1().GetAlt2().GetToken1());
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt3:
            PosFromToken(msg.GetBlock1().GetAlt3().GetToken1());
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt4:
            PosFromToken(msg.GetBlock1().GetAlt4().GetToken1());
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt5:
            PosFromToken(msg.GetBlock1().GetAlt5().GetToken1());
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt6:
            PosFromToken(msg.GetBlock1().GetAlt6().GetToken1());
            break;
        default:
            ythrow yexception() << "Alt is not supported";
        }

        NewLine();
        VisitAllFields(TRule_into_table_stmt::GetDescriptor(), msg);
    }

    void VisitCommit(const TRule_commit_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_commit_stmt::GetDescriptor(), msg);
    }

    void VisitUpdate(const TRule_update_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        Visit(msg.GetToken1());
        Visit(msg.GetRule_simple_table_ref2());
        switch (msg.GetBlock3().Alt_case()) {
        case TRule_update_stmt_TBlock3::kAlt1: {
            const auto& alt = msg.GetBlock3().GetAlt1();
            NewLine();
            Visit(alt.GetToken1());
            const auto& choice = alt.GetRule_set_clause_choice2();
            NewLine();

            switch (choice.Alt_case()) {
            case TRule_set_clause_choice::kAltSetClauseChoice1: {
                const auto& clauses = choice.GetAlt_set_clause_choice1().GetRule_set_clause_list1();
                PushCurrentIndent();
                Visit(clauses.GetRule_set_clause1());
                for (auto& block : clauses.GetBlock2()) {
                    Visit(block.GetToken1());
                    NewLine();
                    Visit(block.GetRule_set_clause2());
                }

                PopCurrentIndent();
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
                Visit(multiColumn.GetToken3());
                NewLine();
                const auto& simpleValues = multiColumn.GetRule_simple_values_source4();
                switch (simpleValues.Alt_case()) {
                case TRule_simple_values_source::kAltSimpleValuesSource1: {
                    const auto& exprs = simpleValues.GetAlt_simple_values_source1().GetRule_expr_list1();
                    PushCurrentIndent();
                    Visit(exprs.GetRule_expr1());
                    for (const auto& block : exprs.GetBlock2()) {
                        Visit(block.GetToken1());
                        NewLine();
                        Visit(block.GetRule_expr2());
                    }

                    PopCurrentIndent();
                    break;
                }
                case TRule_simple_values_source::kAltSimpleValuesSource2: {
                    PushCurrentIndent();
                    Visit(simpleValues.GetAlt_simple_values_source2());
                    PopCurrentIndent();
                    break;
                }
                default:
                    ythrow yexception() << "Alt is not supported";
                }

                NewLine();
                Visit(multiColumn.GetToken5());
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
        case TRule_update_stmt_TBlock3::kAlt2: {
            const auto& alt = msg.GetBlock3().GetAlt2();
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
        PosFromToken(msg.GetToken1());
        NewLine();
        Visit(msg.GetToken1());
        Visit(msg.GetToken2());
        Visit(msg.GetRule_simple_table_ref3());
        if (msg.HasBlock4()) {
            switch (msg.GetBlock4().Alt_case()) {
            case TRule_delete_stmt_TBlock4::kAlt1: {
                const auto& alt = msg.GetBlock4().GetAlt1();
                NewLine();
                Visit(alt);
                break;
            }
            case TRule_delete_stmt_TBlock4::kAlt2: {
                const auto& alt = msg.GetBlock4().GetAlt2();
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
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_rollback_stmt::GetDescriptor(), msg);
    }

    void VisitDeclare(const TRule_declare_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_declare_stmt::GetDescriptor(), msg);
    }

    void VisitImport(const TRule_import_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_import_stmt::GetDescriptor(), msg);
    }

    void VisitExport(const TRule_export_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_export_stmt::GetDescriptor(), msg);
    }

    void VisitAlterTable(const TRule_alter_table_stmt& msg) {
        PosFromToken(msg.GetToken1());
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
    }

    void VisitAlterTableStore(const TRule_alter_table_store_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_alter_table_store_stmt::GetDescriptor(), msg);
    }

    void VisitAlterExternalTable(const TRule_alter_external_table_stmt& msg) {
        PosFromToken(msg.GetToken1());
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
    }

    void VisitDo(const TRule_do_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitKeyword(msg.GetToken1());
        switch (msg.GetBlock2().Alt_case()) {
        case TRule_do_stmt_TBlock2::kAlt1: { // CALL
            const auto& alt = msg.GetBlock2().GetAlt1().GetRule_call_action1();
            Visit(alt.GetBlock1());
            AfterInvokeExpr = true;
            Visit(alt.GetToken2());
            if (alt.HasBlock3()) {
                Visit(alt.GetBlock3());
            }

            Visit(alt.GetToken4());
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
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitKeyword(msg.GetToken1());
        VisitKeyword(msg.GetToken2());
        Visit(msg.GetRule_bind_parameter3());
        AfterInvokeExpr = true;
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
        if (msg.HasBlock1()) {
            PosFromToken(msg.GetBlock1().GetToken1());
        } else {
            PosFromToken(msg.GetToken2());
        }

        NewLine();
        if (msg.HasBlock1()) {
            Visit(msg.GetBlock1());
        }

        Visit(msg.GetToken2());
        Visit(msg.GetRule_expr3());
        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_do_stmt4());
        PopCurrentIndent();
        if (msg.HasBlock5()) {
            NewLine();
            Visit(msg.GetBlock5().GetToken1());
            NewLine();
            PushCurrentIndent();
            Visit(msg.GetBlock5().GetRule_do_stmt2());
            PopCurrentIndent();
        }
    }

    void VisitFor(const TRule_for_stmt& msg) {
        if (msg.HasBlock1()) {
            PosFromToken(msg.GetBlock1().GetToken1());
        } else if (msg.HasBlock2()) {
            PosFromToken(msg.GetBlock2().GetToken1());
        } else {
            PosFromToken(msg.GetToken3());
        }

        NewLine();
        if (msg.HasBlock1()) {
            Visit(msg.GetBlock1());
        }

        if (msg.HasBlock2()) {
            Visit(msg.GetBlock2());
        }

        Visit(msg.GetToken3());
        Visit(msg.GetRule_bind_parameter4());
        Visit(msg.GetToken5());
        Visit(msg.GetRule_expr6());
        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_do_stmt7());
        PopCurrentIndent();
        if (msg.HasBlock8()) {
            NewLine();
            Visit(msg.GetBlock8().GetToken1());
            NewLine();
            PushCurrentIndent();
            Visit(msg.GetBlock8().GetRule_do_stmt2());
            PopCurrentIndent();
        }
    }

    void VisitValues(const TRule_values_stmt& msg) {
        PosFromToken(msg.GetToken1());
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

        PopCurrentIndent();
    }

    void VisitGrantPermissions(const TRule_grant_permissions_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_grant_permissions_stmt::GetDescriptor(), msg);
    }

    void VisitRevokePermissions(const TRule_revoke_permissions_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_revoke_permissions_stmt::GetDescriptor(), msg);
    }

    void VisitCreateUser(const TRule_create_user_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_create_user_stmt::GetDescriptor(), msg);
    }

    void VisitAlterUser(const TRule_alter_user_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_alter_user_stmt::GetDescriptor(), msg);
    }

    void VisitCreateGroup(const TRule_create_group_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_create_group_stmt::GetDescriptor(), msg);
    }

    void VisitAlterGroup(const TRule_alter_group_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_alter_group_stmt::GetDescriptor(), msg);
    }

    void VisitDropRole(const TRule_drop_role_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_drop_role_stmt::GetDescriptor(), msg);
    }

    void VisitUpsertObject(const TRule_upsert_object_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_upsert_object_stmt::GetDescriptor(), msg);
    }

    void VisitCreateObject(const TRule_create_object_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_create_object_stmt::GetDescriptor(), msg);
    }

    void VisitAlterObject(const TRule_alter_object_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_alter_object_stmt::GetDescriptor(), msg);
    }

    void VisitDropObject(const TRule_drop_object_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_drop_object_stmt::GetDescriptor(), msg);
    }

    void VisitCreateTopic(const TRule_create_topic_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitKeyword(msg.GetToken1());
        VisitKeyword(msg.GetToken2());
        Visit(msg.GetRule_topic_ref3());
        if (msg.HasBlock4()) {
            PushCurrentIndent();
            auto& b = msg.GetBlock4().GetRule_create_topic_entries1();
            Visit(b.GetToken1());
            NewLine();
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
        if (msg.HasBlock5()) {
            auto& b = msg.GetBlock5().GetRule_with_topic_settings1();
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
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitKeyword(msg.GetToken1());
        VisitKeyword(msg.GetToken2());
        Visit(msg.GetRule_topic_ref3());
        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_alter_topic_action4());
        for (auto& b : msg.GetBlock5()) {
            Visit(b.GetToken1());
            NewLine();
            Visit(b.GetRule_alter_topic_action2());
        }

        PopCurrentIndent();
    }

    void VisitDropTopic(const TRule_drop_topic_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_drop_topic_stmt::GetDescriptor(), msg);
    }

    void VisitCreateExternalDataSource(const TRule_create_external_data_source_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_create_external_data_source_stmt::GetDescriptor(), msg);
    }

    void VisitAlterExternalDataSource(const TRule_alter_external_data_source_stmt& msg) {
        PosFromToken(msg.GetToken1());
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
    }

    void VisitDropExternalDataSource(const TRule_drop_external_data_source_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_drop_external_data_source_stmt::GetDescriptor(), msg);
    }

    void VisitCreateView(const TRule_create_view_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_create_view_stmt::GetDescriptor(), msg);
    }

    void VisitDropView(const TRule_drop_view_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_drop_view_stmt::GetDescriptor(), msg);
    }

    void VisitCreateAsyncReplication(const TRule_create_replication_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_create_replication_stmt::GetDescriptor(), msg);
    }

    void VisitAlterAsyncReplication(const TRule_alter_replication_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_alter_replication_stmt::GetDescriptor(), msg);
    }

    void VisitDropAsyncReplication(const TRule_drop_replication_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_drop_replication_stmt::GetDescriptor(), msg);
    }

    void VisitCreateResourcePool(const TRule_create_resource_pool_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_create_resource_pool_stmt::GetDescriptor(), msg);
    }

    void VisitAlterResourcePool(const TRule_alter_resource_pool_stmt& msg) {
        PosFromToken(msg.GetToken1());
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
    }

    void VisitDropResourcePool(const TRule_drop_resource_pool_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_drop_resource_pool_stmt::GetDescriptor(), msg);
    }

    void VisitCreateBackupCollection(const TRule_create_backup_collection_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_create_backup_collection_stmt::GetDescriptor(), msg);
    }

    void VisitAlterBackupCollection(const TRule_alter_backup_collection_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitToken(msg.GetToken1());
        Visit(msg.GetRule_backup_collection2());

        NewLine();
        PushCurrentIndent();
        Visit(msg.GetRule_alter_backup_collection_actions3().GetRule_alter_backup_collection_action1());
        for (const auto& action : msg.GetRule_alter_backup_collection_actions3().GetBlock2()) {
            Visit(action.GetToken1()); // comma
            NewLine();
            Visit(action.GetRule_alter_backup_collection_action2());
        }

        PopCurrentIndent();
    }

    void VisitDropBackupCollection(const TRule_drop_backup_collection_stmt& msg) {
        PosFromToken(msg.GetToken1());
        NewLine();
        VisitAllFields(TRule_drop_backup_collection_stmt::GetDescriptor(), msg);
    }

    void VisitAllFields(const NProtoBuf::Descriptor* descr, const NProtoBuf::Message& msg) {
        VisitAllFieldsImpl<TPrettyVisitor, &TPrettyVisitor::Visit>(this, descr, msg);
    }

    void WriteComments() {
        while (LastComment < Comments.size()) {
            const auto& c = Comments[LastComment];
            if (c.Line > LastLine || c.Line == LastLine && c.LinePos > LastColumn) {
                break;
            }

            AddComment(c.Content);
            ++LastComment;
        }
    }

    void PosFromToken(const TToken& token) {
        LastLine = token.GetLine();
        LastColumn = token.GetColumn();
        WriteComments();
    }

    void PosFromParsedToken(const TParsedToken& token) {
        LastLine = token.Line;
        LastColumn = token.LinePos;
        WriteComments();
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

        //Cerr << str << "\n";
        auto currentScope = Scopes.back();
        if (!SkipSpaceAfterUnaryOp && !InMultiTokenOp) {
            if (AfterLess && str == ">") {
                Out(' ');
            } else if (AfterDigits && str == ".") {
                Out(' ');
            } else if (OutColumn && (currentScope == EScope::DoubleQuestion || str != "?")
                && str != ":" && str != "." && str != "," && str != ";" && str != ")" && str != "]"
                && str != "}" && str != "|>" && str != "::" && !AfterNamespace && !AfterBracket
                && !AfterInvokeExpr && !AfterDollarOrAt && !AfterDot && (!AfterQuestion || str != "?")
                && (!InsideType || (str != "<" && str != ">" && str != "<>"))
                && (!InsideType || !AfterLess)
                && (!AfterKeyExpr || str != "[")
                ) {
                Out(' ');
            }
        }

        SkipSpaceAfterUnaryOp = false;
        if (AfterUnaryOp) {
            if (str == "+" || str == "-" || str == "~") {
                SkipSpaceAfterUnaryOp = true;
            }

            AfterUnaryOp = false;
        }

        AfterInvokeExpr = false;
        AfterNamespace = (str == "::");
        AfterBracket = (str == "(" || str == "[" || str == "{" || str == "<|");
        AfterDot = (str == ".");
        AfterDigits = !str.Empty() && AllOf(str, [](char c) { return c >= '0' && c <= '9'; });
        AfterQuestion = (str == "?");
        AfterLess = (str == "<");
        AfterKeyExpr = false;

        if (forceKeyword) {
            str = to_upper(str);
        } else if (currentScope == EScope::Default) {
            if (auto p = StaticData.Keywords.find(to_upper(str));  p != StaticData.Keywords.end()) {
                str = *p;
            }
        }

        AfterDollarOrAt = (str == "$" || str == "@");

        const auto& markedInfo = MarkedTokens[TokenIndex];
        if (markedInfo.ClosingBracket) {
            Y_ENSURE(!MarkTokenStack.empty());
            auto beginTokenIndex = MarkTokenStack.back();
            if (markedInfo.BracketForcedExpansion || ParsedTokens[beginTokenIndex].Line != ParsedTokens[TokenIndex].Line) {
                // multiline
                PopCurrentIndent();
                NewLine();
            }

            MarkTokenStack.pop_back();
        }

        Out(str);
        if (str == ";") {
            Out('\n');
        }

        if (markedInfo.OpeningBracket) {
            MarkTokenStack.push_back(TokenIndex);
            if (markedInfo.BracketForcedExpansion || ParsedTokens[TokenIndex].Line != ParsedTokens[markedInfo.ClosingBracketIndex].Line) {
                // multiline
                PushCurrentIndent();
                NewLine();
            }
        }

        if (str == "," && !MarkTokenStack.empty()) {
            const bool addNewline =
                (TokenIndex + 1 < ParsedTokens.size() && ParsedTokens[TokenIndex].Line != ParsedTokens[TokenIndex + 1].Line)
             || (TokenIndex > 0 && ParsedTokens[TokenIndex - 1].Line != ParsedTokens[TokenIndex].Line);
            // add line for trailing comma
            if (addNewline) {
                NewLine();
            }
        }

        TokenIndex++;
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

                PopCurrentIndent();
                NewLine();
                Visit(columns.GetToken4());
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
                Visit(block5.GetBlock5());
            }

            if (block5.HasBlock6()) {
                NewLine();
                Visit(block5.GetBlock6());
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

        if (msg.HasBlock8()) {
            NewLine();
            Visit(msg.GetBlock8());
        }

        PopCurrentIndent();
        if (msg.HasBlock9()) {
            NewLine();
            Visit(msg.GetBlock9());
        }

        if (msg.HasBlock10()) {
            NewLine();
            Visit(msg.GetBlock10());
        }

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
        Visit(msg.GetRule_named_single_source1());
        if (msg.HasBlock2()) {
            PushCurrentIndent();
            NewLine();
            Visit(msg.GetBlock2());
            PopCurrentIndent();
        }
    }

    void VisitNamedSingleSource(const TRule_named_single_source& msg) {
        Visit(msg.GetRule_single_source1());
        if (msg.HasBlock2()) {
            const auto& matchRecognize = msg.GetBlock2();
            //TODO handle MATCH_RECOGNIZE block
            //https://st.yandex-team.ru/YQL-16186
            Visit(matchRecognize);
        }
        if (msg.HasBlock3()) {
            NewLine();
            PushCurrentIndent();
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

                NewLine();
                PopCurrentIndent();
                Visit(columns.GetToken4());
            }

            PopCurrentIndent();
        }

        if (msg.HasBlock4()) {
            NewLine();
            PushCurrentIndent();
            Visit(msg.GetBlock4());
            PopCurrentIndent();
        }
    }

    void VisitSimpleTableRef(const TRule_simple_table_ref& msg) {
        Visit(msg.GetRule_simple_table_ref_core1());
        if (msg.HasBlock2()) {
            NewLine();
            PushCurrentIndent();
            Visit(msg.GetBlock2());
            PopCurrentIndent();
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

            PopCurrentIndent();
            NewLine();
            Visit(columns.GetToken4());
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
            NewLine();
            PushCurrentIndent();
            Visit(alt);
            PopCurrentIndent();
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
            if (key.HasBlock2()) {
                NewLine();
                PushCurrentIndent();
                Visit(key.GetBlock2());
                PopCurrentIndent();
            }

            break;
        }
        case TRule_table_ref::TBlock3::kAlt2: {
            const auto& alt = block3.GetAlt2();
            Visit(alt.GetRule_an_id_expr1());
            AfterInvokeExpr = true;
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
                AfterInvokeExpr = true;
                Visit(alt.GetBlock2());
            }

            if (alt.HasBlock3()) {
                NewLine();
                PushCurrentIndent();
                Visit(alt.GetBlock3());
                PopCurrentIndent();
            }

            break;
        }
        default:
            ythrow yexception() << "Alt is not supported";
        }

        if (msg.HasBlock4()) {
            NewLine();
            PushCurrentIndent();
            Visit(msg.GetBlock4());
            PopCurrentIndent();
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
    }

    void VisitLambdaBody(const TRule_lambda_body& msg) {
        PushCurrentIndent();
        NewLine();
        VisitRepeated(msg.GetBlock1());
        for (const auto& block : msg.GetBlock2()) {
            Visit(block);
            NewLine();
        }

        Visit(msg.GetToken3());
        Visit(msg.GetRule_expr4());
        VisitRepeated(msg.GetBlock5());

        PopCurrentIndent();
        NewLine();
    }

    void VisitInAtomExpr(const TRule_in_atom_expr& msg) {
        if (msg.Alt_case() == TRule_in_atom_expr::kAltInAtomExpr7) {
            const auto& alt = msg.GetAlt_in_atom_expr7();
            Visit(alt.GetToken1());
            NewLine();
            PushCurrentIndent();
            Visit(alt.GetRule_select_stmt2());
            NewLine();
            PopCurrentIndent();
            Visit(alt.GetToken3());
        } else {
            VisitAllFields(TRule_in_atom_expr::GetDescriptor(), msg);
        }
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
        AfterInvokeExpr = true;
        Visit(msg.GetToken2());
        Visit(msg.GetRule_expr3());
        Visit(msg.GetToken4());
        Visit(msg.GetRule_type_name_or_bind5());
        Visit(msg.GetToken6());
    }

    void VisitBitCastExpr(const TRule_bitcast_expr& msg) {
        Visit(msg.GetToken1());
        AfterInvokeExpr = true;
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
        AfterKeyExpr = true;
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

    void VisitWhenExpr(const TRule_when_expr& msg) {
        VisitKeyword(msg.GetToken1());
        Visit(msg.GetRule_expr2());

        NewLine();
        PushCurrentIndent();
        VisitKeyword(msg.GetToken3());
        Visit(msg.GetRule_expr4());
        PopCurrentIndent();
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

        Visit(msg.GetToken5());
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
        VisitNeqSubexprImpl(msg, false);
    }

    void VisitNeqSubexprImpl(const TRule_neq_subexpr& msg, bool pushedIndent) {
        auto getExpr = [](const TRule_neq_subexpr::TBlock2& b) -> const TRule_bit_subexpr& { return b.GetRule_bit_subexpr2(); };
        auto getOp = [](const TRule_neq_subexpr::TBlock2& b) -> const TRule_neq_subexpr::TBlock2::TBlock1& { return b.GetBlock1(); };
        VisitBinaryOp(msg.GetRule_bit_subexpr1(), getOp, getExpr, msg.GetBlock2().begin(), msg.GetBlock2().end());

        if (msg.HasBlock3()) {
            const auto& b = msg.GetBlock3();
            switch (b.Alt_case()) {
            case TRule_neq_subexpr_TBlock3::kAlt1: {
                const auto& alt = b.GetAlt1();
                const bool hasFirstNewline = LastLine != ParsedTokens[TokenIndex].Line;
                // 2 is `??` size in tokens
                const bool hasSecondNewline = ParsedTokens[TokenIndex].Line != ParsedTokens[TokenIndex + 2].Line;
                const ui32 currentOutLine = OutLine;

                PosFromParsedToken(ParsedTokens[TokenIndex]);
                if (currentOutLine != OutLine || (hasFirstNewline && hasSecondNewline)) {
                    NewLine();
                    if (!pushedIndent) {
                        PushCurrentIndent();
                        pushedIndent = true;
                    }
                }

                Visit(alt.GetRule_double_question1());
                PosFromParsedToken(ParsedTokens[TokenIndex]);
                if (hasFirstNewline || hasSecondNewline) {
                    NewLine();
                    if (!pushedIndent) {
                        PushCurrentIndent();
                        pushedIndent = true;
                    }
                }

                VisitNeqSubexprImpl(alt.GetRule_neq_subexpr2(), pushedIndent);
                if (pushedIndent) {
                    PopCurrentIndent();
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
        InMultiTokenOp = true;
        VisitToken(msg.GetToken2());
        InMultiTokenOp = false;
    }

    void VisitRotRight(const TRule_rot_right& msg) {
        VisitToken(msg.GetToken1());
        InMultiTokenOp = true;
        VisitToken(msg.GetToken2());
        VisitToken(msg.GetToken3());
        InMultiTokenOp = false;
    }

    template <typename TExpr, typename TGetOp, typename TGetExpr, typename TIter>
    void VisitBinaryOp(const TExpr& expr, TGetOp getOp, TGetExpr getExpr, TIter begin, TIter end) {
        Visit(expr);
        bool pushedIndent = false;

        for (; begin != end; ++begin) {
            const auto op = getOp(*begin);
            const auto opSize = BinaryOpTokenSize(op);
            const bool hasFirstNewline = LastLine != ParsedTokens[TokenIndex].Line;
            const bool hasSecondNewline = ParsedTokens[TokenIndex].Line != ParsedTokens[TokenIndex + opSize].Line;
            const ui32 currentOutLine = OutLine;

            PosFromParsedToken(ParsedTokens[TokenIndex]);
            if (currentOutLine != OutLine || (hasFirstNewline && hasSecondNewline)) {
                NewLine();
                if (!pushedIndent) {
                    PushCurrentIndent();
                    pushedIndent = true;
                }
            }
            Visit(op);

            PosFromParsedToken(ParsedTokens[TokenIndex]);
            if (hasFirstNewline || hasSecondNewline) {
                NewLine();
                if (!pushedIndent) {
                    PushCurrentIndent();
                    pushedIndent = true;
                }
            }

            Visit(getExpr(*begin));
        }

        if (pushedIndent) {
            PopCurrentIndent();
        }
    }

    void PushCurrentIndent() {
        CurrentIndent += OneIndent;
    }

    void PopCurrentIndent() {
        CurrentIndent -= OneIndent;
    }

private:
    const TStaticData& StaticData;
    const TParsedTokenList& ParsedTokens;
    const TParsedTokenList& Comments;
    TStringBuilder SB;
    ui32 OutColumn = 0;
    ui32 OutLine = 1;
    ui32 LastLine = 0;
    ui32 LastColumn = 0;
    ui32 LastComment = 0;
    i32 CurrentIndent = 0;
    TVector<EScope> Scopes;
    TMaybe<bool> AddLine;
    ui64 InsideType = 0;
    bool AfterNamespace = false;
    bool AfterBracket = false;
    bool AfterInvokeExpr = false;
    bool AfterUnaryOp = false;
    bool SkipSpaceAfterUnaryOp = false;
    bool AfterDollarOrAt = false;
    bool AfterDot = false;
    bool AfterDigits = false;
    bool AfterQuestion = false;
    bool AfterLess = false;
    bool AfterKeyExpr = false;
    bool InMultiTokenOp = false;
    ui32 ForceExpandedLine = 0;
    ui32 ForceExpandedColumn = 0;

    ui32 TokenIndex = 0;
    TMarkTokenStack MarkTokenStack;
    TVector<TTokenInfo> MarkedTokens;
    ui64 InsideExpr = 0;
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
        {TRule_into_values_source::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitIntoValuesSource)},
        {TRule_select_kind::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectKind)},
        {TRule_process_core::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitProcessCore)},
        {TRule_reduce_core::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitReduceCore)},
        {TRule_sort_specification_list::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSortSpecificationList)},
        {TRule_select_core::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectCore)},
        {TRule_join_source::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitJoinSource)},
        {TRule_single_source::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSingleSource)},
        {TRule_flatten_source::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitFlattenSource)},
        {TRule_named_single_source::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitNamedSingleSource)},
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
        {TRule_in_atom_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitInAtomExpr)},
        {TRule_select_kind_parenthesis::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectKindParenthesis)},
        {TRule_cast_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCastExpr)},
        {TRule_bitcast_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitBitCastExpr)},
        {TRule_ext_order_by_clause::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitExtOrderByClause)},
        {TRule_key_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitKeyExpr)},
        {TRule_define_action_or_subquery_body::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitDefineActionOrSubqueryBody)},
        {TRule_exists_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitExistsExpr)},
        {TRule_case_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitCaseExpr)},
        {TRule_when_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitWhenExpr)},
        {TRule_with_table_settings::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitWithTableSettingsExpr)},

        {TRule_expr::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitExpr)},
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
        {TRule_select_unparenthesized_stmt::GetDescriptor(), MakePrettyFunctor(&TPrettyVisitor::VisitSelectUnparenthesized)},
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

class TSqlFormatter : public NSQLFormat::ISqlFormatter {
public:
    TSqlFormatter(const NSQLTranslation::TTranslationSettings& settings)
        : Settings(settings)
    {}

    bool Format(const TString& query, TString& formattedQuery, NYql::TIssues& issues, EFormatMode mode) override {
        formattedQuery = (mode == EFormatMode::Obfuscate) ? "" : query;
        auto parsedSettings = Settings;
        if (!NSQLTranslation::ParseTranslationSettings(query, parsedSettings, issues)) {
            return false;
        }

        if (parsedSettings.PgParser) {
            return mode != EFormatMode::Obfuscate;
        }

        if (mode == EFormatMode::Obfuscate) {
            auto message = NSQLTranslationV1::SqlAST(query, "Query", issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS, parsedSettings.AnsiLexer, parsedSettings.Arena);
            if (!message) {
                return false;
            }

            TObfuscatingVisitor visitor;
            return Format(visitor.Process(*message), formattedQuery, issues, EFormatMode::Pretty);
        }

        auto lexer = NSQLTranslationV1::MakeLexer(parsedSettings.AnsiLexer);
        TParsedTokenList allTokens;
        auto onNextToken = [&](NSQLTranslation::TParsedToken&& token) {
            if (token.Name != "EOF") {
                allTokens.push_back(token);
            }
        };

        if (!lexer->Tokenize(query, "Query", onNextToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
            return false;
        }

        TVector<TTokenIterator> statements;
        SplitByStatements(allTokens.begin(), allTokens.end(), statements);
        TStringBuilder finalFormattedQuery;
        for (size_t i = 1; i < statements.size(); ++i) {
            TStringBuilder currentQueryBuilder;
            for (auto it = statements[i - 1]; it != statements[i]; ++it) {
                currentQueryBuilder << it->Content;
            }

            TString currentQuery = currentQueryBuilder;
            currentQuery = StripStringLeft(currentQuery);
            bool isBlank = true;
            for (auto c : currentQuery) {
                if (c != ';') {
                    isBlank = false;
                    break;
                }
            };

            if (isBlank) {
                continue;
            }

            TVector<NSQLTranslation::TParsedToken> comments;
            TParsedTokenList parsedTokens, stmtTokens;
            bool hasTrailingComments = false;
            auto onNextRawToken = [&](NSQLTranslation::TParsedToken&& token) {
                stmtTokens.push_back(token);
                if (token.Name == "COMMENT") {
                    comments.emplace_back(std::move(token));
                    hasTrailingComments = true;
                } else if (token.Name != "WS" && token.Name != "EOF") {
                    parsedTokens.emplace_back(std::move(token));
                    hasTrailingComments = false;
                }
            };

            if (!lexer->Tokenize(currentQuery, "Query", onNextRawToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
                return false;
            }

            NYql::TIssues parserIssues;
            auto message = NSQLTranslationV1::SqlAST(currentQuery, "Query", parserIssues, NSQLTranslation::SQL_MAX_PARSER_ERRORS, parsedSettings.AnsiLexer, parsedSettings.Arena);
            if (!message) {
                finalFormattedQuery << currentQuery;
                if (!currentQuery.EndsWith("\n")) {
                    finalFormattedQuery << "\n";
                }

                continue;
            }

            TPrettyVisitor visitor(parsedTokens, comments);
            bool addLine;
            auto currentFormattedQuery = visitor.Process(*message, addLine);
            TParsedTokenList stmtFormattedTokens;
            auto onNextFormattedToken = [&](NSQLTranslation::TParsedToken&& token) {
                stmtFormattedTokens.push_back(token);
            };

            if (!lexer->Tokenize(currentFormattedQuery, "Query", onNextFormattedToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
                return false;
            }

            if (!Validate(stmtFormattedTokens, stmtTokens)) {
                issues.AddIssue(NYql::TIssue({}, TStringBuilder() << "Validation failed: " << currentQuery.Quote() << " != " << currentFormattedQuery.Quote()));
                return false;
            }

            if (addLine && !finalFormattedQuery.Empty()) {
                finalFormattedQuery << "\n";
            }

            finalFormattedQuery << currentFormattedQuery;
            if (parsedTokens.back().Name != "SEMICOLON") {
                if (hasTrailingComments
                     && !comments.back().Content.EndsWith("\n")
                     && comments.back().Content.StartsWith("--")) {
                    finalFormattedQuery << "\n";
                }
                finalFormattedQuery << ";\n";
            }
        }

        formattedQuery = finalFormattedQuery;
        return true;
    }

private:
    const NSQLTranslation::TTranslationSettings Settings;
};

}

ISqlFormatter::TPtr MakeSqlFormatter(const NSQLTranslation::TTranslationSettings& settings) {
    return ISqlFormatter::TPtr(new TSqlFormatter(settings));
}

TString MutateQuery(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    auto parsedSettings = settings;
    NYql::TIssues issues;
    if (!NSQLTranslation::ParseTranslationSettings(query, parsedSettings, issues)) {
        throw yexception() << issues.ToString();
    }

    auto lexer = NSQLTranslationV1::MakeLexer(parsedSettings.AnsiLexer);
    TVector<NSQLTranslation::TParsedToken> allTokens;
    auto onNextToken = [&](NSQLTranslation::TParsedToken&& token) {
        if (token.Name != "EOF") {
            allTokens.push_back(token);
        }
    };

    if (!lexer->Tokenize(query, "Query", onNextToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
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

bool SqlFormatSimple(const TString& query, TString& formattedQuery, TString& error) {
    try {
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;

        auto formatter = MakeSqlFormatter(settings);
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
    Y_ENSURE(NResource::FindExact("SQLv1.g.in", &grammar));
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
            //Cerr << before << "\n";
            res.insert(before);
        }
    }

    return res;
}

} // namespace NSQLFormat
