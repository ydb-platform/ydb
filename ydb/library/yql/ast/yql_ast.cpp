#include "yql_ast.h"
#include "yql_ast_escaping.h"

#include <util/string/builder.h>
#include <util/system/compiler.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <ydb/library/yql/utils/utf8.h>

#include <cstdlib>

namespace NYql {

namespace {

    inline bool IsWhitespace(char c) {
        return c == ' ' || c == '\n' || c == '\r' || c == '\t';
    }

    inline bool IsListStart(char c)      { return c == '(';  }
    inline bool IsListEnd(char c)        { return c == ')';  }
    inline bool IsCommentStart(char c)   { return c == '#';  }
    inline bool IsQuoteStart(char c)     { return c == '\''; }
    inline bool IsStringStart(char c)    { return c == '"';  }
    inline bool IsHexStringStart(char c) { return c == 'x';  }
    inline bool IsMultilineStringStart(char c) { return c == '@'; }

    inline bool NeedEscaping(const TStringBuf& str) {
        for (char ch: str) {
            if (IsWhitespace(ch) || IsListStart(ch) ||
                    IsListEnd(ch) || IsCommentStart(ch) ||
                    IsQuoteStart(ch) || IsStringStart(ch) ||
                    !isprint(ch & 0xff))
            {
                return true;
            }
        }

        return str.empty();
    }

    ///////////////////////////////////////////////////////////////////////////
    // TAstParser
    ///////////////////////////////////////////////////////////////////////////
    class TAstParserContext {
    public:
        inline TAstParserContext(const TStringBuf& str, TMemoryPool* externalPool, const TString& file)
            : Str_(str)
            , Position_(1, 1, file)
            , Offset_(0)
            , Pool_(externalPool)
        {
            if (!Pool_) {
                InnerPool_ = std::make_unique<TMemoryPool>(4096);
                Pool_ = InnerPool_.get();
            }
        }

        inline char Peek() const {
            Y_ABORT_UNLESS(!AtEnd());
            return Str_[Offset_];
        }

        inline bool AtEnd() const {
            return Str_.size() == Offset_;
        }

        inline char Next() {
            Y_ABORT_UNLESS(!AtEnd());
            char ch = Str_[Offset_];
            if (ch == '\n') {
                ++Position_.Row;
                Position_.Column = 1;
            } else {
                ++Position_.Column;
            }

            ++Offset_;
            return ch;
        }

        // stops right afetr stopChar
        inline void SeekTo(char stopChar) {
            while (!AtEnd() && Next() != stopChar) {
                // empty loop
            }
        }

        inline TStringBuf GetToken(ui32 begin, ui32 end) {
            Y_ABORT_UNLESS(end >= begin);
            return Str_.SubString(begin, end - begin);
        }

        inline bool IsAtomEnded() {
            if (AtEnd()) {
                return true;
            }
            char c = Peek();
            return IsWhitespace(c) || IsListStart(c) || IsListEnd(c);
        }

        inline const TStringBuf& Str() const { return Str_; }
        inline ui32 Offset() const { return Offset_; }
        inline const TPosition& Position() const { return Position_; }
        inline TMemoryPool& Pool() { return *Pool_; }
        inline std::unique_ptr<TMemoryPool>&& InnerPool() { return std::move(InnerPool_); }

    private:
        TStringBuf Str_;
        TPosition Position_;
        ui32 Offset_;
        TMemoryPool* Pool_;
        std::unique_ptr<TMemoryPool> InnerPool_;
    };

    ///////////////////////////////////////////////////////////////////////////
    // TAstParser
    ///////////////////////////////////////////////////////////////////////////
    class TAstParser {
    public:
        TAstParser(const TStringBuf& str, TMemoryPool* externalPool, const TString& file)
            : Ctx_(str, externalPool, file)
        {
        }

        TAstParseResult Parse() {
            TAstNode* root = nullptr;
            if (!IsUtf8(Ctx_.Str())) {
               AddError("Invalid UTF8 input");
            } else {
               root = ParseList(0U);

               SkipSpace();
               if (!Ctx_.AtEnd()) {
                  AddError("Unexpected symbols after end of root list");
               }
            }

            TAstParseResult result;
            if (!Issues_.Empty()) {
                result.Issues = std::move(Issues_);
            } else {
                result.Root = root;
                result.Pool = Ctx_.InnerPool();
            }
            return result;
        }

    private:
        inline void AddError(const TString& message) {
            Issues_.AddIssue(Ctx_.Position(), message);
        }

        inline void SkipComment() {
            Ctx_.SeekTo('\n');
        }

        void SkipSpace() {
            while (!Ctx_.AtEnd()) {
                char c = Ctx_.Peek();
                if (IsWhitespace(c)) {
                    Ctx_.Next();
                    continue;
                }

                if (IsCommentStart(c)) {
                    SkipComment();
                    continue;
                }

                break;
            }
        }

        TAstNode* ParseList(size_t level) {
            if (level >= 1000U) {
                AddError("Too deep graph!");
                return nullptr;
            }

            SkipSpace();

            if (Ctx_.AtEnd()) {
                AddError("Unexpected end");
                return nullptr;
            }

            if (!IsListStart(Ctx_.Peek())) {
                AddError("Expected (");
                return nullptr;
            }

            Ctx_.Next();

            TSmallVec<TAstNode*> children;
            auto listPos = Ctx_.Position();
            while (true) {
                SkipSpace();

                if (Ctx_.AtEnd()) {
                    AddError("Expected )");
                    return nullptr;
                }

                if (IsListEnd(Ctx_.Peek())) {
                    Ctx_.Next();
                    return TAstNode::NewList(listPos, children.data(), children.size(), Ctx_.Pool());
                }

                TAstNode* elem = ParseElement(level);
                if (!elem)
                    return nullptr;

                children.push_back(elem);
            }
        }

        TAstNode* ParseElement(size_t level) {
            if (Ctx_.AtEnd()) {
                AddError("Expected element");
                return nullptr;
            }

            char c = Ctx_.Peek();
            if (IsQuoteStart(c)) {
                auto resPosition = Ctx_.Position();
                Ctx_.Next();

                char ch = Ctx_.Peek();
                if (Ctx_.AtEnd() || IsWhitespace(ch) || IsCommentStart(ch) ||
                        IsListEnd(ch))
                {
                    AddError("Expected quotation");
                    return nullptr;
                }

                TAstNode* content = IsListStart(ch)
                        ? ParseList(++level)
                        : ParseAtom();
                if (!content)
                    return nullptr;

                return TAstNode::NewList(resPosition, Ctx_.Pool(), &TAstNode::QuoteAtom, content);
            }

            if (IsListStart(c))
                return ParseList(++level);

            return ParseAtom();
        }

        TAstNode* ParseAtom() {
            if (Ctx_.AtEnd()) {
                AddError("Expected atom");
                return nullptr;
            }

            auto resPosition = Ctx_.Position();
            ui32 atomStart = Ctx_.Offset();

            while (true) {
                char c = Ctx_.Peek();
                // special symbols = 0x20, 0x0a, 0x0d, 0x09 space
                // 0x22, 0x23, 0x28, 0x29  "#()
                // 0x27 '
                // special symbols = 0x40, 0x78 @x
                // &0x3f = 0x00,0x38
#define MASK(x) (1ull << ui64(x))
                const ui64 mask1 = MASK(0x20) | MASK(0x0a) | MASK(0x0d)
                    | MASK(0x09) | MASK(0x22) | MASK(0x23) | MASK(0x28) | MASK(0x29) | MASK(0x27);
                const ui64 mask2 = MASK(0x00) | MASK(0x38);
#undef MASK
                if (!(c & 0x80) && ((1ull << (c & 0x3f)) & (c <= 0x3f ? mask1 : mask2))) {
                    if (IsWhitespace(c) || IsListStart(c) || IsListEnd(c))
                        break;

                    if (IsCommentStart(c)) {
                        AddError("Unexpected comment");
                        return nullptr;
                    }

                    if (IsQuoteStart(c)) {
                        AddError("Unexpected quotation");
                        return nullptr;
                    }

                    // multiline starts with '@@'
                    if (IsMultilineStringStart(c)) {
                        Ctx_.Next();
                        if (Ctx_.AtEnd()) break;

                        if (!IsMultilineStringStart(Ctx_.Peek())) {
                            continue;
                        }

                        TString token;
                        if (!TryParseMultilineToken(token)) {
                            return nullptr;
                        }

                        if (!Ctx_.IsAtomEnded()) {
                            AddError("Unexpected end of @@");
                            return nullptr;
                        }

                        return TAstNode::NewAtom(resPosition, token, Ctx_.Pool(), TNodeFlags::MultilineContent);
                    }
                    // hex string starts with 'x"'
                    else if (IsHexStringStart(c)) {
                        Ctx_.Next(); // skip 'x'
                        if (Ctx_.AtEnd()) break;

                        if (!IsStringStart(Ctx_.Peek())) {
                            continue;
                        }

                        Ctx_.Next(); // skip first '"'

                        size_t readBytes = 0;
                        TStringStream ss;
                        TStringBuf atom = Ctx_.Str().SubStr(Ctx_.Offset());
                        EUnescapeResult unescapeResult = UnescapeBinaryAtom(
                                atom, '"', &ss, &readBytes);

                        // advance position
                        while (readBytes-- != 0) {
                            Ctx_.Next();
                        }

                        if (unescapeResult != EUnescapeResult::OK) {
                            AddError(TString(UnescapeResultToString(unescapeResult)));
                            return nullptr;
                        }

                        Ctx_.Next(); // skip last '"'
                        if (!Ctx_.IsAtomEnded()) {
                            AddError("Unexpected end of \"");
                            return nullptr;
                        }

                        return TAstNode::NewAtom(resPosition, ss.Str(), Ctx_.Pool(), TNodeFlags::BinaryContent);
                    }
                    else if (IsStringStart(c)) {
                        if (Ctx_.Offset() != atomStart) {
                            AddError("Unexpected \"");
                            return nullptr;
                        }

                        Ctx_.Next(); // skip first '"'

                        size_t readBytes = 0;
                        TStringStream ss;
                        TStringBuf atom = Ctx_.Str().SubStr(Ctx_.Offset());
                        EUnescapeResult unescapeResult = UnescapeArbitraryAtom(
                                atom, '"', &ss, &readBytes);

                        // advance position
                        while (readBytes-- != 0) {
                            Ctx_.Next();
                        }

                        if (unescapeResult != EUnescapeResult::OK) {
                            AddError(TString(UnescapeResultToString(unescapeResult)));
                            return nullptr;
                        }

                        if (!Ctx_.IsAtomEnded()) {
                            AddError("Unexpected end of \"");
                            return nullptr;
                        }

                        return TAstNode::NewAtom(resPosition, ss.Str(), Ctx_.Pool(), TNodeFlags::ArbitraryContent);
                    }
                }

                Ctx_.Next();
                if (Ctx_.AtEnd()) {
                    break;
                }
            }

            return TAstNode::NewAtom(resPosition, Ctx_.GetToken(atomStart, Ctx_.Offset()), Ctx_.Pool());
        }

        bool TryParseMultilineToken(TString& token) {
            Ctx_.Next(); // skip second '@'

            ui32 start = Ctx_.Offset();
            while (true) {
                Ctx_.SeekTo('@');

                if (Ctx_.AtEnd()) {
                    AddError("Unexpected multiline atom end");
                    return false;
                }

                ui32 count = 1; // we seek to first '@'
                while (!Ctx_.AtEnd() && Ctx_.Peek() == '@') {
                    count++;
                    Ctx_.Next();
                    if (count == 4) {
                        // Reduce each four '@' to two
                        token.append(Ctx_.GetToken(start, Ctx_.Offset() - 2));
                        start = Ctx_.Offset();
                        count = 0;
                    }
                }
                if (count >= 2) {
                    break;
                }
            }

            // two '@@' at the end
            token.append(Ctx_.GetToken(start, Ctx_.Offset() - 2));
            return true;
        }

    private:
        TAstParserContext Ctx_;
        TIssues Issues_;
    };

    ///////////////////////////////////////////////////////////////////////////
    // ast node printing functions
    ///////////////////////////////////////////////////////////////////////////

    inline bool IsQuoteNode(const TAstNode& node) {
        return node.GetChildrenCount() == 2
                && node.GetChild(0)->GetType() == TAstNode::Atom
                && node.GetChild(0)->GetContent() == TStringBuf("quote");
    }

    inline bool IsBlockNode(const TAstNode& node) {
        return node.GetChildrenCount() == 2
                && node.GetChild(0)->GetType() == TAstNode::Atom
                && node.GetChild(0)->GetContent() == TStringBuf("block");
    }

    Y_NO_INLINE void Indent(IOutputStream& out, ui32 indentation) {
        char* whitespaces = reinterpret_cast<char*>(alloca(indentation));
        memset(whitespaces, ' ', indentation);
        out.Write(whitespaces, indentation);
    }

    void MultilineAtomPrint(IOutputStream& out, const TStringBuf& str) {
        out << TStringBuf("@@");
        size_t idx = str.find('@');
        if (idx == TString::npos) {
            out << str;
        } else {
            const char* begin = str.data();
            do {
                ui32 count = 0;
                for (; idx < str.length() && str[idx] == '@'; ++idx) {
                    ++count;
                }

                if (count % 2 == 0) {
                    out.Write(begin, idx - (begin - str.data()) - count);
                    begin = str.data() + idx;

                    while (count--) {
                        out.Write(TStringBuf("@@"));
                    }
                }
                idx = str.find('@', idx);
            } while (idx != TString::npos);
            out.Write(begin, str.end() - begin);
        }
        out << TStringBuf("@@");
    }

    void PrintNode(IOutputStream& out, const TAstNode& node) {
        if (node.GetType() == TAstNode::Atom) {
            if (node.GetFlags() & TNodeFlags::ArbitraryContent) {
                EscapeArbitraryAtom(node.GetContent(), '"', &out);
            } else if (node.GetFlags() & TNodeFlags::BinaryContent) {
                EscapeBinaryAtom(node.GetContent(), '"', &out);
            } else if (node.GetFlags() & TNodeFlags::MultilineContent) {
                MultilineAtomPrint(out, node.GetContent());
            } else if (node.GetContent().empty()) {
                EscapeArbitraryAtom(node.GetContent(), '"', &out);
            } else {
                out << node.GetContent();
            }
        } else if (node.GetType() == TAstNode::List) {
            if (!node.GetChildrenCount()) {
                out << TStringBuf("()");
            } else if (IsQuoteNode(node)) {
                out << '\'';
                PrintNode(out, *node.GetChild(1));
            } else {
                out << '(';
                ui32 index = 0;
                while (true) {
                    PrintNode(out, *node.GetChild(index));
                    ++index;
                    if (index == node.GetChildrenCount()) break;
                    out  << ' ';
                }
                out << ')';
            }
        }
    }

    void PrettyPrintNode(
            IOutputStream& out, const TAstNode& node,
            i32 indent, i32 blockLevel, i32 localIndent, ui32 flags)
    {
        if (!(flags & TAstPrintFlags::PerLine)) {
            Indent(out, indent * 2);
        } else if (localIndent == 1) {
            Indent(out, blockLevel * 2);
        }

        bool isQuote = false;
        if (node.GetType() == TAstNode::Atom) {
            if (node.GetFlags() & TNodeFlags::ArbitraryContent) {
                if ((flags & TAstPrintFlags::AdaptArbitraryContent) &&
                        !NeedEscaping(node.GetContent()))
                {
                    out << node.GetContent();
                } else {
                    EscapeArbitraryAtom(node.GetContent(), '"', &out);
                }
            } else if (node.GetFlags() & TNodeFlags::BinaryContent) {
                EscapeBinaryAtom(node.GetContent(), '"', &out);
            } else if (node.GetFlags() & TNodeFlags::MultilineContent) {
                MultilineAtomPrint(out, node.GetContent());
            } else {
                if (((flags & TAstPrintFlags::AdaptArbitraryContent) && NeedEscaping(node.GetContent())) ||
                    node.GetContent().empty())
                {
                    EscapeArbitraryAtom(node.GetContent(), '"', &out);
                } else {
                    out << node.GetContent();
                }
            }
        } else if (node.GetType() == TAstNode::List) {
            isQuote = IsQuoteNode(node);
            if (isQuote && (flags & TAstPrintFlags::ShortQuote)) {
                out << '\'';
                if (localIndent == 0 || !(flags & TAstPrintFlags::PerLine)) {
                    out << Endl;
                }

                PrettyPrintNode(out, *node.GetChild(1), indent + 1, blockLevel, localIndent + 1, flags);
            } else {
                out << '(';
                if (localIndent == 0 || !(flags & TAstPrintFlags::PerLine)) {
                    out << Endl;
                }

                bool isBlock = IsBlockNode(node);
                for (ui32 index = 0; index < node.GetChildrenCount(); ++index) {
                    if (localIndent > 0 && (index > 0) && (flags & TAstPrintFlags::PerLine)) {
                        out << ' ';
                    }

                    if (isBlock && (index > 0)) {
                        PrettyPrintNode(out, *node.GetChild(index), indent + 1, blockLevel + 1, -1, flags);
                    } else {
                        PrettyPrintNode(out, *node.GetChild(index), indent + 1, blockLevel, localIndent + 1, flags);
                    }
                }
            }

            if (!isQuote || !(flags & TAstPrintFlags::ShortQuote)) {
                if (!(flags & TAstPrintFlags::PerLine)) {
                    Indent(out, indent * 2);
                }

                if (localIndent == 0 && blockLevel > 0) {
                    Indent(out, (blockLevel - 1) * 2);
                }

                out << ')';
            }
        }

        if (!isQuote || !(flags & TAstPrintFlags::ShortQuote)) {
            if (localIndent > 0 || blockLevel == 0) {
                if (localIndent <= 1 || !(flags & TAstPrintFlags::PerLine)) {
                    out << Endl;
                }
            }
        }
    }

    void DestroyNode(TAstNode* node) {
        if (node->IsList()) {
            for (ui32 i = 0; i < node->GetChildrenCount(); ++i) {
                DestroyNode(node->GetChild(i));
            }
        }

        if (node != &TAstNode::QuoteAtom) {
            node->Destroy();
        }
    }
} // namespace

TAstParseResult::~TAstParseResult() {
    Destroy();
}

TAstParseResult::TAstParseResult(TAstParseResult&& other)
    : Pool(std::move(other.Pool))
    , Root(other.Root)
    , Issues(std::move(other.Issues))
    , PgAutoParamValues(std::move(other.PgAutoParamValues))
    , ActualSyntaxType(other.ActualSyntaxType)
{
    other.Root = nullptr;
}

TAstParseResult& TAstParseResult::operator=(TAstParseResult&& other) {
    Destroy();
    Pool = std::move(other.Pool);
    Root = other.Root;
    other.Root = nullptr;
    Issues = std::move(other.Issues);
    PgAutoParamValues = std::move(other.PgAutoParamValues);
    ActualSyntaxType = other.ActualSyntaxType;
    return *this;
}

void TAstParseResult::Destroy() {
    if (Root) {
        DestroyNode(Root);
        Root = nullptr;
    }
}

TAstParseResult ParseAst(const TStringBuf& str, TMemoryPool* externalPool, const TString& file)
{
    TAstParser parser(str, externalPool, file);
    return parser.Parse();
}

void TAstNode::PrintTo(IOutputStream& out) const {
    PrintNode(out, *this);
}

void TAstNode::PrettyPrintTo(IOutputStream& out, ui32 flags) const {
    PrettyPrintNode(out, *this, 0, 0, 0, flags);
}

TAstNode TAstNode::QuoteAtom(TPosition(0, 0), TStringBuf("quote"), TNodeFlags::Default);

} // namespace NYql

template<>
void Out<NYql::TAstNode::EType>(class IOutputStream &o, NYql::TAstNode::EType x) {
#define YQL_AST_NODE_TYPE_MAP_TO_STRING_IMPL(name, ...) \
    case ::NYql::TAstNode::name: \
        o << #name; \
        return;

    switch (x) {
        YQL_AST_NODE_TYPE_MAP(YQL_AST_NODE_TYPE_MAP_TO_STRING_IMPL)
    default:
        o << static_cast<int>(x);
        return;
    }
}
