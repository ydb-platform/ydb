
#include <util/generic/string.h>
#include <util/generic/iterator.h>

// a temporary copy-paste from: yql/tools/pgrun/pgrun.cpp?rev=r11829751#L49

class TStatementIterator final
    : public TInputRangeAdaptor<TStatementIterator>
{
    enum class State {
        InOperator,
        EndOfOperator,
        LineComment,
        BlockComment,
        QuotedIdentifier,
        StringLiteral,
        EscapedStringLiteral,
        DollarStringLiteral,
        InMetaCommand,
        InCopyFromStdin,
        InVar,
    };

public:
    TStatementIterator(const TString& program);
    static bool isInWsSignificantState(State state);
    static bool isEscapedChar(const TString& s, size_t pos);
    TString RemoveEmptyLines(const TString& s, bool inStatement);
    const TString* Next();

private:
    bool SaveDollarTag();
    bool IsCopyFromStdin(size_t startPos, size_t endPos);
    bool InOperatorParser(size_t startPos);
    bool Emit(bool atEol);
    bool EndOfOperatorParser();
    bool LineCommentParser();
    bool BlockCommentParser();
    bool QuotedIdentifierParser();
    bool StringLiteralParser();
    bool EscapedStringLiteralParser();
    bool DollarStringLiteralParser();
    bool MetaCommandParser();
    bool InCopyFromStdinParser();
    bool VarParser();
    bool CallParser(size_t startPos);
    void ApplyStateFromStatement(const TStringBuf& stmt);

    TString Program_;
    TString Cur_;
    size_t Pos_;
    State State_;
    bool AtStmtStart_;

    State Mode_;
    ui16 Depth_;
    TStringBuf Tag_;
    bool StandardConformingStrings_;
};
