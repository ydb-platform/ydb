#include "sql_parser.h"
#include <util/string/split.h>
#include <util/string/builder.h>

TStatementIterator::TStatementIterator(const TString& program)
    : Program_(program)
    , Cur_()
    , Pos_(0)
    , State_(State::InOperator)
    , AtStmtStart_(true)
    , Mode_(State::InOperator)
    , Depth_(0)
    , Tag_()
    , StandardConformingStrings_(true)
{
}

bool TStatementIterator::isInWsSignificantState(State state) {
    switch (state) {
        case State::QuotedIdentifier:
        case State::StringLiteral:
        case State::EscapedStringLiteral:
        case State::DollarStringLiteral:
            return true;
        default:
            return false;
    }
}

bool TStatementIterator::isEscapedChar(const TString& s, size_t pos) {
    bool escaped = false;
    while (s[--pos] == '\\') {
        escaped = !escaped;
    }
    return escaped;
}

TString TStatementIterator::RemoveEmptyLines(const TString& s, bool inStatement) {
    if (s.empty()) {
        return {};
    }

    TStringBuilder sb;
    auto isFirstLine = true;

    if (inStatement && s[0] == '\n') {
        sb << '\n';
    }

    for (TStringBuf line : StringSplitter(s).SplitBySet("\r\n").SkipEmpty()) {
        if (isFirstLine) {
            isFirstLine = false;
        } else {
            sb << '\n';
        }
        sb << line;
    }
    return sb;
}

const TString* TStatementIterator::Next() {
    if (TStringBuf::npos == Pos_)
        return nullptr;

    size_t startPos = Pos_;
    size_t curPos = Pos_;
    size_t endPos;
    auto prevState = State_;

    TStringBuilder stmt;
    TStringBuilder rawStmt;
    auto inStatement = false;

    while (!CallParser(startPos)) {
        endPos = (TStringBuf::npos != Pos_) ? Pos_ : Program_.length();

        TStringBuf part{&Program_[curPos], endPos - curPos};

        if (isInWsSignificantState(prevState)) {
            if (!rawStmt.empty()) {
                stmt << RemoveEmptyLines(rawStmt, inStatement);
                rawStmt.clear();
            }
            stmt << part;
            inStatement = true;
        } else {
            rawStmt << part;
        }
        curPos = endPos;
        prevState = State_;
    }
    endPos = (TStringBuf::npos != Pos_) ? Pos_ : Program_.length();

    TStringBuf part{&Program_[curPos], endPos - curPos};

    if (isInWsSignificantState(prevState)) {
        if (!rawStmt.empty()) {
            stmt << RemoveEmptyLines(rawStmt, inStatement);
            rawStmt.clear();
        }
        stmt << part;
        inStatement = true;
    } else {
        rawStmt << part;
    }

#if 0
    if (0 < Pos_ && !(Pos_ == TStringBuf::npos || Program_[Pos_-1] == '\n')) {
        Cerr << "Last char: '" << Program_[Pos_-1] << "'\n";
    }
#endif

    stmt << RemoveEmptyLines(rawStmt, inStatement);
    // inv: Pos_ is at the start of next token
    if (startPos == endPos)
        return nullptr;

    stmt << '\n';
    Cur_ = stmt;

    ApplyStateFromStatement(Cur_);

    return &Cur_;
}

// States:
//  - in-operator
//  - line comment
//  - block comment
//  - quoted identifier (U& quoted identifier is no difference)
//  - string literal (U& string literal is the same for our purpose)
//  - E string literal
//  - $ string literal
//  - end-of-operator

// Rules:
//  - in-operator
//      -- -> next: line comment
//      /* -> depth := 1, next: block comment
//      " -> next: quoted identifier
//      ' -> next: string literal
//      E' -> next: E string literal
//      $tag$, not preceded by alnum char (a bit of simplification here but sufficient) -> tag := tag, next: $ string literal
//      ; -> current_mode := end-of-operator, next: end-of-operator

//  - line comment
//      EOL -> next: current_mode

//  - block comment
//      /* -> ++depth
//      */ -> --depth, if (depth == 0) -> next: current_mode

//  - quoted identifier
//      " -> next: in-operator

//  - string literal
//      ' -> next: in-operator

//  - E string literal
//      ' -> if not preceeded by \ next: in-operator

//  - $ string literal
//      $tag$ -> next: in-operator

//  - end-of-operator
//      -- -> next: line comment, just once
//      /* -> depth := 1, next: block comment
//      non-space char -> unget, emit, current_mode := in-operator, next: in-operator

// In every state:
//    EOS -> emit if consumed part of the input is not empty

bool TStatementIterator::SaveDollarTag() {
    if (Pos_ + 1 == Program_.length())
        return false;

    auto p = Program_.cbegin() + (Pos_ + 1);

    if (std::isdigit(*p))
        return false;

    for (;p != Program_.cend(); ++p) {
        if (*p == '$') {
            auto bp = &Program_[Pos_];
            auto l = p - bp;
            Tag_ = TStringBuf(bp, l + 1);
            Pos_ += l;

            return true;
        }
        if (!(std::isalpha(*p) || std::isdigit(*p) || *p == '_'))
            return false;
    }
    return false;
}

bool TStatementIterator::IsCopyFromStdin(size_t startPos, size_t endPos) {
    TString stmt(Program_, startPos, endPos - startPos + 1);
    stmt.to_upper();
    // FROM STDOUT is used in insert.sql testcase, probably a bug
    return stmt.Contains(" FROM STDIN") || stmt.Contains(" FROM STDOUT");
}

bool TStatementIterator::InOperatorParser(size_t startPos) {
    // need \ to detect psql meta-commands
    static const TString midNextTokens{"'\";-/$\\"};
    // need : for basic psql-vars support
    static const TString initNextTokens{"'\";-/$\\:"};
    const auto& nextTokens = (AtStmtStart_) ? initNextTokens : midNextTokens;

    if (AtStmtStart_) {
        Pos_ = Program_.find_first_not_of(" \t\n\r\v", Pos_);
        if (TString::npos == Pos_) {
            return true;
        }
    }

    Pos_ = Program_.find_first_of(nextTokens, Pos_);

    if (TString::npos == Pos_) {
        return true;
    }

    switch (Program_[Pos_]) {
        case '\'':
            State_ = (!StandardConformingStrings_ || 0 < Pos_ && std::toupper(Program_[Pos_ - 1]) == 'E')
                        ? State::EscapedStringLiteral
                        : State::StringLiteral;
            break;

        case '"':
            State_ = State::QuotedIdentifier;
            break;

        case ';':
            State_ = Mode_ = IsCopyFromStdin(startPos, Pos_)
                ? State::InCopyFromStdin
                : State::EndOfOperator;
            break;

        case '-':
            if (Pos_ < Program_.length() && Program_[Pos_ + 1] == '-') {
                State_ = State::LineComment;
                ++Pos_;
            }
            break;

        case '/':
            if (Pos_ < Program_.length() && Program_[Pos_ + 1] == '*') {
                State_ = State::BlockComment;
                ++Depth_;
                ++Pos_;
            }
            break;

        case '$':
            if (Pos_ == 0 || std::isspace(Program_[Pos_ - 1])) {
                if (SaveDollarTag())
                    State_ = State::DollarStringLiteral;
            }
            break;

        case '\\':
            if (AtStmtStart_) {
                State_ = State::InMetaCommand;
            } else if (Program_.Contains("\\gexec", Pos_)) {
                Pos_ += 6;
                return Emit(Program_[Pos_] == '\n');
            }
            break;

        case ':':
            if (Pos_ == 0 || Program_[Pos_-1] == '\n') {
                State_ = State::InVar;
            }
            break;
    }
    ++Pos_;
    if (Program_.length() == Pos_) {
        Pos_ = TString::npos;
        return true;
    }

    return false;
}

bool TStatementIterator::Emit(bool atEol) {
    State_ = Mode_ = State::InOperator;
    AtStmtStart_ = true;

    if (atEol) {
        ++Pos_;
        if (Program_.length() == Pos_) {
            Pos_ = TString::npos;
            return true;
        }
    }
    // else do not consume as we're expected to be on the first char of the next statement

    return true;
}

bool TStatementIterator::EndOfOperatorParser() {
    const auto p = std::find_if_not(Program_.cbegin() + Pos_, Program_.cend(), [](const auto& c) {
        return c == ' ' || c == '\t' || c == '\r';
    });

    if (p == Program_.cend()) {
        Pos_ = TStringBuf::npos;
        return true;
    }

    Pos_ = p - Program_.cbegin();

    switch (*p) {
        case '-':
            if (Pos_ < Program_.length() && Program_[Pos_ + 1] == '-') {
                State_ = State::LineComment;
                ++Pos_;
            }
            break;

        case '/':
            if (Pos_ < Program_.length() && Program_[Pos_ + 1] == '*') {
                State_ = State::BlockComment;
                ++Depth_;
                ++Pos_;
            }
            break;

        default:
            return Emit(*p == '\n');
    }

    ++Pos_;
    if (Program_.length() == Pos_) {
        Pos_ = TString::npos;
        return true;
    }

    return false;
}

bool TStatementIterator::LineCommentParser() {
    Pos_ = Program_.find('\n', Pos_);

    if (TString::npos == Pos_)
        return true;

    ++Pos_;
    if (Program_.length() == Pos_) {
        Pos_ = TString::npos;
        return true;
    }

    if (Mode_ == State::EndOfOperator) {
        return Emit(false);
    }

    State_ = Mode_;

    return false;
}

bool TStatementIterator::BlockCommentParser() {
    Pos_ = Program_.find_first_of("*/", Pos_);

    if (TString::npos == Pos_)
        return true;

    switch(Program_[Pos_]) {
        case '/':
            if (Pos_ < Program_.length() && Program_[Pos_ + 1] == '*') {
                ++Depth_;
                ++Pos_;
            }
            break;

        case '*':
            if (Pos_ < Program_.length() && Program_[Pos_ + 1] == '/') {
                --Depth_;
                ++Pos_;

                if (0 == Depth_) {
                    State_ = Mode_;
                }
            }
            break;
    }
    ++Pos_;
    if (Program_.length() == Pos_) {
        Pos_ = TString::npos;
        return true;
    }

    return false;
}

bool TStatementIterator::QuotedIdentifierParser() {
    Pos_ = Program_.find('"', Pos_);

    if (TString::npos == Pos_)
        return true;

    ++Pos_;
    if (Program_.length() == Pos_) {
        Pos_ = TString::npos;
        return true;
    }

    State_ = State::InOperator;
    AtStmtStart_ = false;

    return false;
}

bool TStatementIterator::StringLiteralParser() {
    Pos_ = Program_.find('\'', Pos_);

    if (TString::npos == Pos_)
        return true;

    ++Pos_;
    if (Program_.length() == Pos_) {
        Pos_ = TString::npos;
        return true;
    }

    State_ = State::InOperator;
    AtStmtStart_ = false;

    return false;
}

bool TStatementIterator::EscapedStringLiteralParser() {
    Pos_ = Program_.find('\'', Pos_);

    if (TString::npos == Pos_)
        return true;

    if (isEscapedChar(Program_, Pos_)) {
        ++Pos_;
        return false;
    }

    ++Pos_;
    if (Program_.length() == Pos_) {
        Pos_ = TString::npos;
        return true;
    }

    State_ = State::InOperator;
    AtStmtStart_ = false;

    return false;
}

bool TStatementIterator::DollarStringLiteralParser() {
    //Y_ENSURE(Tag_ != nullptr && 2 <= Tag_.length());

    Pos_ = Program_.find(Tag_, Pos_);

    if (TString::npos == Pos_)
        return true;

    Pos_ += Tag_.length();
    if (Program_.length() == Pos_) {
        Pos_ = TString::npos;
        return true;
    }

    Tag_.Clear();

    State_ = State::InOperator;
    AtStmtStart_ = false;

    return false;
}

bool TStatementIterator::MetaCommandParser() {
    Pos_ = Program_.find('\n', Pos_);

    if (TString::npos == Pos_)
        return true;

    ++Pos_;
    if (Program_.length() == Pos_) {
        Pos_ = TString::npos;
        return true;
    }

    return Emit(false);
}

bool TStatementIterator::InCopyFromStdinParser() {
    Pos_ = Program_.find("\n\\.\n", Pos_);

    if (TString::npos == Pos_)
        return true;

    Pos_ += 4;
    return Emit(false);
}

// For now we support vars occupying a whole line only
bool TStatementIterator::VarParser() {
    // TODO: validate var name
    Pos_ = Program_.find('\n', Pos_);

    if (TString::npos == Pos_)
        return true;

    ++Pos_;
    if (Program_.length() == Pos_) {
        Pos_ = TString::npos;
        return true;
    }

    return Emit(false);
}

bool TStatementIterator::CallParser(size_t startPos) {
    switch (State_) {
        case State::InOperator:
            return InOperatorParser(startPos);

        case State::EndOfOperator:
            return EndOfOperatorParser();

        case State::LineComment:
            return LineCommentParser();

        case State::BlockComment:
            return BlockCommentParser();

        case State::QuotedIdentifier:
            return QuotedIdentifierParser();

        case State::StringLiteral:
            return StringLiteralParser();

        case State::EscapedStringLiteral:
            return EscapedStringLiteralParser();

        case State::DollarStringLiteral:
            return DollarStringLiteralParser();

        case State::InMetaCommand:
            return MetaCommandParser();

        case State::InCopyFromStdin:
            return InCopyFromStdinParser();

        case State::InVar:
            return VarParser();

        default:
            Y_UNREACHABLE();
    }
}

void TStatementIterator::ApplyStateFromStatement(const TStringBuf& stmt) {
    if (stmt.contains("set standard_conforming_strings = on;") ||
        stmt.contains("reset standard_conforming_strings;"))
    {
        StandardConformingStrings_ = true;
    } else if (stmt.contains("set standard_conforming_strings = off;")) {
        StandardConformingStrings_ = false;
    }
}
