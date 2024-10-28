#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include "ydb/library/yql/providers/yt/common/yql_names.h"
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/pg/provider/yql_pg_provider.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/utils.h>
#include <ydb/library/yql/providers/yt/lib/schema/schema.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/yson/public.h>
#include "library/cpp/yson/node/node_io.h"
#include <library/cpp/yt/yson_string/string.h>
#include <fmt/format.h>

#include <util/system/user.h>
#include <util/stream/file.h>
#include <util/system/fs.h>
#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/string/split.h>
#include <util/generic/yexception.h>
#include <util/generic/iterator.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/string_utils/base64/base64.h>

using namespace NYql;
using namespace NKikimr::NMiniKQL;
using namespace NNodes;
using NUdf::EDataSlot;

namespace NMiniKQL = NKikimr::NMiniKQL;

const ui32 PRETTY_FLAGS = NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote |
                          NYql::TAstPrintFlags::AdaptArbitraryContent;

enum class EByteaOutput{
    hex,
    escape,
};

TString nullRepr("");
EByteaOutput byteaOutput = EByteaOutput::hex;

bool IsEscapedChar(const TString& s, size_t pos) {
    bool escaped = false;
    while (s[--pos] == '\\') {
        escaped = !escaped;
    }
    return escaped;
}

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
    TStatementIterator(const TString&& program)
        : Program_(std::move(program))
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

    static bool IsInWsSignificantState(State state) {
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

    TString RemoveEmptyLines(const TString& s, bool inStatement) {
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

    const TString* Next()
    {
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

            if (IsInWsSignificantState(prevState)) {
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

        if (IsInWsSignificantState(prevState)) {
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

private:

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

    bool SaveDollarTag() {
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

    bool IsCopyFromStdin(size_t startPos, size_t endPos) {
        TString stmt(Program_, startPos, endPos - startPos + 1);
        stmt.to_upper();
        // FROM STDOUT is used in insert.sql testcase, probably a bug
        return stmt.Contains(" FROM STDIN") || stmt.Contains(" FROM STDOUT");
    }

    bool InOperatorParser(size_t startPos) {
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

    bool Emit(bool atEol) {
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

    bool EndOfOperatorParser() {
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

    bool LineCommentParser() {
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

    bool BlockCommentParser() {
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

    bool QuotedIdentifierParser() {
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

    bool StringLiteralParser() {
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

    bool EscapedStringLiteralParser() {
        Pos_ = Program_.find('\'', Pos_);

        if (TString::npos == Pos_)
            return true;

        if (IsEscapedChar(Program_, Pos_)) {
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

    bool DollarStringLiteralParser() {
        Y_ENSURE(Tag_ != nullptr && 2 <= Tag_.length());

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

    bool MetaCommandParser() {
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

    bool InCopyFromStdinParser() {
        Pos_ = Program_.find("\n\\.\n", Pos_);

        if (TString::npos == Pos_)
            return true;

        Pos_ += 4;
        return Emit(false);
    }

    // For now we support vars occupying a whole line only
    bool VarParser() {
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

    bool CallParser(size_t startPos) {
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

    void ApplyStateFromStatement(const TStringBuf& stmt) {
        if (stmt.contains("set standard_conforming_strings = on;") ||
            stmt.contains("reset standard_conforming_strings;"))
        {
            StandardConformingStrings_ = true;
        } else if (stmt.contains("set standard_conforming_strings = off;")) {
            StandardConformingStrings_ = false;
        }
    }

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

TString GetFormattedStmt(const TStringBuf& stmt) {
    TString result;
    result.reserve(stmt.length());
    size_t pos = 0, next_pos = TStringBuf::npos;

    while (TStringBuf::npos != (next_pos = stmt.find('\n', pos))) {
        if (0 < next_pos - pos) {
            if (!(stmt[pos] == '\r' && 1 == next_pos - pos)) {
                result += stmt.substr(pos, next_pos - pos + 1);
            }
        }
        pos = next_pos + 1;
    }
    if (pos < stmt.length())
        result += stmt.substr(pos);

    if (0 < result.length() && '\n' == result.back())
        result.pop_back();

    if (0 < result.length() && '\r' == result.back())
        result.pop_back();

    return result;
}

void PrintExprTo(const TProgramPtr& program, IOutputStream& out) {
    TStringStream baseSS;

    auto baseAst = ConvertToAst(*program->ExprRoot(), program->ExprCtx(),
                                NYql::TExprAnnotationFlags::None, true);
    baseAst.Root->PrettyPrintTo(baseSS, PRETTY_FLAGS);

    out << baseSS.Data();
}

NYT::TNode ParseYson(const TString& yson)
{
    NYT::TNode root;
    NYT::TNodeBuilder builder(&root);
    NYson::TStatelessYsonParser resultParser(&builder);
    resultParser.Parse(yson);

    return root;
}

TString GetPgErrorMessage(const TIssue& issue) {
    const TString anchor("reason: ");
    const auto& msg = issue.GetMessage();

    auto pos = msg.find(anchor);

    if (TString::npos == pos)
        return TString(msg);

    return msg.substr(pos + anchor.length());
}

void WriteErrorToStream(const TProgramPtr program)
{
    program->PrintErrorsTo(Cerr);

    for (const auto& topIssue: program->Issues()) {
        WalkThroughIssues(topIssue, true, [&](const TIssue& issue, ui16 /*level*/) {
            const auto msg = GetPgErrorMessage(issue);
            Cout << msg;
            if (msg.back() != '\n') {
                Cout << '\n';
            }
        });
    }
}

using CellFormatter = std::function<const TString(const TString&)>;
using TColumnType = TString;

inline const TString FormatBool(const TString& value)
{
    static const TString T = "t";
    static const TString F = "f";

    return (value == "true") ? T
         : (value == "false") ? F
         : (value == nullRepr) ? nullRepr
         : ythrow yexception() << "Unexpected bool literal: " << value;
}

inline const TString FormatNumeric(const TString& value)
{
    static const TString Zero = "0.0";

    return (value == "0") ? Zero : value;
}

const TString FormatFloat(const TString& value, std::function<TString(const TString&)> formatter) {
    static const TString nan = "NaN";
    static const TString inf = "Infinity";
    static const TString minf = "-Infinity";

    try {
        return (value == "") ? ""
             : (value == "nan") ? nan
             : (value == "inf") ? inf
             : (value == "-inf") ? minf
             : formatter(value);
    } catch (const std::exception& e) {
        Cerr << "Unexpected float value '" << value << "'\n";
        return "";
    }
}

inline const TString FormatFloat4(const TString& value)
{
    return FormatFloat(value,
        [] (const TString& val) { return TString(fmt::format("{:.8g}", std::stof(val))); });
}

inline const TString FormatFloat8(const TString& value)
{
    return FormatFloat(value,
        [] (const TString& val) { return TString(fmt::format("{:.15g}", std::stod(val))); });
}

inline const TString FormatTransparent(const TString& value)
{
    return value;
}

static const THashMap<TColumnType, CellFormatter> ColumnFormatters {
    { "bool", FormatBool },
    { "numeric", FormatNumeric },
    { "float4", FormatFloat4 },
    { "float8", FormatFloat8 },
};

static const THashSet<TColumnType> RightAlignedTypes {
    "int2",
    "int4",
    "int8",
    "float4",
    "float8",
    "numeric",
    "oid",
};

struct TColumn {
    TString Name;
    TString Type;
    size_t Width;
    CellFormatter Formatter;
    bool RightAligned;
};

std::string FormatCell(const TString& data, const TColumn& column, size_t index, size_t numberOfColumns) {
    const auto delim = (index == 0) ? " " : " | ";

    if (column.RightAligned)
        return fmt::format("{0}{1:>{2}}", delim, data, column.Width);

    if (index == numberOfColumns - 1)
        return fmt::format("{0}{1}", delim, data);

    return fmt::format("{0}{1:<{2}}", delim, data, column.Width);
}

TString GetCellData(const NYT::TNode& cell, const TColumn& column) {
    if (column.Type == "bytea") {
        const auto rawValue = (cell.IsList())
            ? Base64Decode(cell.AsList()[0].AsString())
            : cell.AsString();

        switch (byteaOutput) {
            case EByteaOutput::hex: {
                TString result;

                const auto expectedSize = rawValue.size() * 2 + 2;
                result.resize(expectedSize);
                result[0] = '\\';
                result[1] = 'x';
                const auto cnt = HexEncode(rawValue.data(), rawValue.size(), result.begin() + 2);

                Y_ASSERT(cnt + 2 == expectedSize);

                return result;
            }
            case EByteaOutput::escape: {
                TString result;

                ui64 expectedSize = std::accumulate(rawValue.cbegin(), rawValue.cend(), 0U,
                    [] (ui64 acc, char c) {
                        return acc + ((c == '\\')
                                        ? 2
                                        : ((ui8)c < 0x20 || 0x7e < (ui8)c)
                                        ? 4
                                        : 1);
                    });
                result.resize(expectedSize);
                auto p = result.begin();
                for (const auto c : rawValue) {
                    if (c == '\\') {
                        *p++ = '\\';
                        *p++ = '\\';
                    } else if ((ui8)c < 0x20 || 0x7e < (ui8)c) {
                        auto val = (ui8)c;

                        *p++ = '\\';
                        *p++ = ((val >> 6) & 03) + '0';
                        *p++ = ((val >> 3) & 07) + '0';
                        *p++ =  (val & 07) + '0';
                    } else {
                        *p++ = c;
                    }
                }

                return result;
            }
            default:
                throw yexception() << "Unhandled EByteaOutput value";
        }
    }
    return cell.AsString();
}

void WriteTableToStream(IOutputStream& stream, const NYT::TNode::TListType& cols, const NYT::TNode::TListType& rows)
{
    TVector<TColumn> columns;
    TList<TVector<TString>> formattedData;

    for (const auto& col: cols) {
        const auto& colName = col[0].AsString();
        const auto& colType = col[1][1].AsString();

        auto& c = columns.emplace_back();

        c.Name = colName;
        c.Type = colType;
        c.Width = colName.length();
        c.Formatter = ColumnFormatters.Value(colType, FormatTransparent);
        c.RightAligned = RightAlignedTypes.contains(colType);
    }

    for (const auto& row : rows) {
        auto& rowData = formattedData.emplace_back();

        { int i = 0;
        for (const auto& cell : row.AsList()) {
            auto& c = columns[i];

            const auto cellData = cell.HasValue() ? GetCellData(cell, c) : nullRepr;

            rowData.emplace_back(c.Formatter(cellData));
            c.Width = std::max(c.Width, rowData.back().length());

            ++i;
        }}
    }

    if (columns.empty()) {
        stream << "--";
    } else {
        const auto totalTableWidth =
            std::accumulate(columns.cbegin(), columns.cend(), std::size_t{0},
            [] (const auto& sum, const auto& elem) { return sum + elem.Width; }) + columns.size() * 3 - 1;
        TString filler(totalTableWidth, '-');
        stream << fmt::format(" {0:^{1}} ", columns[0].Name, columns[0].Width);
        for (size_t i = 1, pos = columns[0].Width + 2; i < columns.size(); ++i) {
            const auto& c = columns[i];

            stream << fmt::format("| {0:^{1}} ", c.Name, c.Width);
            filler[pos] = '+';
            pos += c.Width + 3;
        }
        stream << '\n' << filler;
    }

    for (const auto& row : formattedData) {
        stream  << '\n';

        for (size_t i = 0; i < row.size(); ++i) {
            stream << FormatCell(row[i], columns[i], i, columns.size());
        }
    }
    stream << fmt::format("\n({} {})\n", formattedData.size(), (formattedData.size() == 1) ? "row" : "rows");
}

std::pair<TString, TString> GetYtTableDataPaths(const TFsPath& dataDir, const TString tableName) {
    const auto dataFileName = dataDir / tableName;
    const auto attrFileName = dataDir / (tableName + ".attr");
    return {dataFileName, attrFileName};
}

void CreateYtFileTable(const TFsPath& dataDir, const TString tableName, const TExprNode::TPtr columnsNode,
    THashMap<TString, TString>& tablesMapping, TExprContext& ctx, const TPosition& pos) {
  const auto [dataFilePath, attrFilePath] =
      GetYtTableDataPaths(dataDir, tableName);

    TFile dataFile{dataFilePath, CreateNew};
    TFile attrFile{attrFilePath, CreateNew};

    auto rowSpec = MakeIntrusive<TYqlRowSpecInfo>();

    TColumnOrder columnOrder;
    columnOrder.Reserve(columnsNode->ChildrenSize());

    TStringBuilder ysonType;
    ysonType << "[\"StructType\";[";

    for (const auto &columnNode : columnsNode->Children()) {
      const auto &colName = columnNode->Child(0)->Content();
      const auto &colTypeNode = columnNode->Child(1);

      columnOrder.AddColumn(TString(colName));

      ysonType << fmt::format("[\"{0}\";[\"{1}\";\"{2}\";];];",
                          colName, colTypeNode->Content(),
                          colTypeNode->Child(0)->Content());
    }
    ysonType << "];]";
    const auto *typeNode = NCommon::ParseTypeFromYson(TStringBuf(ysonType), ctx, pos);

    rowSpec->SetType(typeNode->Cast<TStructExprType>());
    rowSpec->SetColumnOrder(std::move(columnOrder));

    NYT::TNode attrs = NYT::TNode::CreateMap();
    rowSpec->FillAttrNode(attrs[YqlRowSpecAttribute], 0, false);

    NYT::TNode spec;
    rowSpec->FillCodecNode(spec[YqlRowSpecAttribute]);

    attrs["schema"] = RowSpecToYTSchema(spec[YqlRowSpecAttribute], 0).ToNode();

    TOFStream of(attrFile.GetName());
    of.Write(NYT::NodeToYsonString(attrs, NYson::EYsonFormat::Pretty));

    tablesMapping[TString("yt.plato.") + tableName] = dataFile.GetName();
}

bool RemoveFile(const TString& fileName) {
    if (NFs::Remove(fileName)) {
        return true;
    }

    switch (errno) {
        case ENOENT:
            return false;
        default:
            throw yexception() << "Cannot remove existing table file \"" << fileName << "\"\n";
    }
}

void DeleteYtFileTable(const TFsPath& dataDir, const TString tableName, THashMap<TString, TString>& tablesMapping) {
    const auto [dataFilePath, attrFilePath] = GetYtTableDataPaths(dataDir, tableName);

    if (!RemoveFile(dataFilePath)) {
        Cout << "table \"" << tableName << "\" does not exist\n";
    }
    RemoveFile(attrFilePath);

    tablesMapping.erase(TString("yt.plato.") + tableName);
}

int SplitStatements(int argc, char* argv[]) {
    Y_UNUSED(argc);
    Y_UNUSED(argv);

    const TString delimiter{"===a738dc70-2d81-45b4-88f2-738d09b186b7===\n"};

    for (const auto& stmt : TStatementIterator{Cin.ReadAll()}) {
        Cout << delimiter << stmt;
    }
    return 0;
}

void WriteToYtTableScheme(
    const NYql::TExprNode& writeNode,
    const TTempDir& tempDir,
    const TIntrusivePtr<class NYql::NFile::TYtFileServices> yqlNativeServices,
    TExprContext& ctx) {
    const auto* keyNode = writeNode.Child(2);

    const auto* tableNameNode = keyNode->Child(0)->Child(1);
    Y_ENSURE(tableNameNode->IsCallable("String"));

    const auto& tableName = tableNameNode->Child(0)->Content();
    Y_ENSURE(!tableName.empty());

    const auto* optionsNode = writeNode.Child(4);
    Y_ENSURE(optionsNode);

    const auto modeNode = GetSetting(*optionsNode, "mode");
    Y_ENSURE(modeNode);
    const auto mode = modeNode->Child(1)->Content();

    if (mode == "create") {
        const auto columnsNode = GetSetting(*optionsNode, "columns");
        Y_ENSURE(columnsNode);

        CreateYtFileTable(tempDir.Path(), TString(tableName), columnsNode->ChildPtr(1),
                        yqlNativeServices->GetTablesMapping(), ctx, writeNode.Pos(ctx));
    }
    else if (mode == "drop") {
        DeleteYtFileTable(tempDir.Path(), TString(tableName), yqlNativeServices->GetTablesMapping());
    }
}

void ProcessMetaCmd(const TStringBuf& cmd) {
    const TStringBuf pset_null("\\pset null ");

    if (cmd.starts_with(pset_null)) {
        const auto secondArgPos = cmd.find_first_not_of(" ", pset_null.length());
        if (secondArgPos != std::string_view::npos) {
            TStringBuf newNullRepr(cmd, secondArgPos);

            if (newNullRepr.front() == '\'') {
                newNullRepr.remove_prefix(1);

                if (newNullRepr.back() == '\'') {
                    newNullRepr.remove_suffix(1);
                }
            }
            nullRepr = newNullRepr;

            return;
        }
    }
    Cerr << "Metacommand " << cmd << " is not supported\n";
}

void ShowFinalAst(TProgramPtr& program, IOutputStream& stream) {
    Cerr << "Final AST:\n";
    PrintExprTo(program, stream);
}

void FillTablesMapping(const TFsPath& dataDir, THashMap<TString, TString>& tablesMapping) {
    TVector<TFsPath> children;

    dataDir.List(children);

    bool regMsgLogged = false;
    for (const auto& f: children) {
        if (f.GetExtension() != "attr") {
            continue;
        }
        auto tableName = f.Basename();
        tableName.resize(tableName.length() - 5);

        if (tableName.EndsWith(".tmp")) {
            continue;
        }
        if (!regMsgLogged) {
            regMsgLogged = true;

            Cerr << "Registering pre-existing tables\n";
        }
        const auto fullTableName = f.Parent() / tableName;
        Cerr << '\t' << tableName << '\n';
        tablesMapping[TString("yt.plato.") + tableName] = f.Parent() / tableName;
    }
}

int Main(int argc, char* argv[])
{
    using namespace NLastGetopt;
    TOpts opts = TOpts::Default();

    const TString runnerName{"pgrun"};
    TVector<TString> udfsPaths;

    TString rawDataDir;
    THashMap<TString, TString> clusterMapping;

    static const TString DefaultCluster{"plato"};
    clusterMapping[DefaultCluster] = YtProviderName;
    clusterMapping["pg_catalog"] = PgProviderName;
    clusterMapping["information_schema"] = PgProviderName;

    opts.AddHelpOption();
    opts.AddLongOption("print-ast", "print initial & final ASTs to stderr").NoArgument();
    opts.AddLongOption("print-result", "print program execution result to stderr").NoArgument();
    opts.AddLongOption("datadir", "directory for tables").StoreResult<TString>(&rawDataDir);
    opts.AddLongOption('u', "udf", "Load shared library with UDF by given path").AppendTo(&udfsPaths);
    opts.SetFreeArgsMax(0);

    TOptsParseResult res(&opts, argc, argv);

    const auto needPrintAst = res.Has("print-ast");
    const auto needPrintResult = res.Has("print-result");

    const bool tempDirExists = !rawDataDir.empty() && NFs::Exists(rawDataDir);
    TTempDir tempDir{rawDataDir.empty() ? TTempDir{} : TTempDir{rawDataDir}};
    if (tempDirExists) {
        tempDir.DoNotRemove();
    }

    auto fsConfig = MakeHolder<TFileStorageConfig>();

    THolder<TGatewaysConfig> gatewaysConfig;

    auto fileStorage = CreateFileStorage(*fsConfig);
    fileStorage = WithAsync(fileStorage);

    auto funcRegistry = CreateFunctionRegistry(&NYql::NBacktrace::KikimrBackTrace, CreateBuiltinRegistry(), false, udfsPaths);
    IUdfResolver::TPtr udfResolver = NCommon::CreateSimpleUdfResolver(funcRegistry.Get(), fileStorage, true);;

    bool keepTempFiles = true;
    bool emulateOutputForMultirun = false;

    auto yqlNativeServices = NFile::TYtFileServices::Make(funcRegistry.Get(), {}, fileStorage, tempDir.Path(), keepTempFiles);
    auto ytNativeGateway = CreateYtFileGateway(yqlNativeServices, &emulateOutputForMultirun);
    if (tempDirExists) {
        FillTablesMapping(tempDir.Path(), yqlNativeServices->GetTablesMapping());
    }

    TVector<TDataProviderInitializer> dataProvidersInit;
    dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytNativeGateway));
    dataProvidersInit.push_back(GetPgDataProviderInitializer());

    TExprContext ctx;

    IModuleResolver::TPtr moduleResolver;
    if (!GetYqlDefaultModuleResolver(ctx, moduleResolver, clusterMapping, true)) {
        Cerr << "Errors loading default YQL libraries:" << Endl;
        ctx.IssueManager.GetIssues().PrintTo(Cerr);
        return -1;
    }

    TExprContext::TFreezeGuard freezeGuard(ctx);

    TProgramFactory factory(true, funcRegistry.Get(), ctx.NextUniqueId, dataProvidersInit, runnerName);
    factory.SetModules(moduleResolver);
    factory.SetUdfResolver(udfResolver);
    factory.SetGatewaysConfig(gatewaysConfig.Get());

    const TString username = GetUsername();
    THashSet<TString> sqlFlags;

    NSQLTranslation::TTranslationSettings settings;
    settings.ClusterMapping = clusterMapping;
    settings.DefaultCluster = DefaultCluster;
    settings.Flags = sqlFlags;
    settings.SyntaxVersion = 1;
    settings.AnsiLexer = false;
    settings.V0Behavior = NSQLTranslation::EV0Behavior::Report;
    settings.AssumeYdbOnClusterWithSlash = false;
    settings.PgParser = true;

    for (const auto& raw_stmt : TStatementIterator{Cin.ReadAll()}) {
        const auto stmt = GetFormattedStmt(raw_stmt);
        Cout << stmt << '\n';

        Cerr << "<sql-statement>\n" << stmt << "\n</sql-statement>\n";

        if (stmt[0] == '\\') {
            ProcessMetaCmd(stmt);
            continue;
        }

        {
            const auto metaCmdStart = stmt.find("\n\\");
            if (TString::npos != metaCmdStart) {
                const auto metaCmdEnd = stmt.find_first_of("\r\n", metaCmdStart + 2);
                ProcessMetaCmd(stmt.substr(metaCmdStart + 1, metaCmdEnd));
                continue;
            }
        }

        if (TString::npos != stmt.find("SET bytea_output TO hex")) {
            byteaOutput = EByteaOutput::hex;
            continue;
        }

        if (TString::npos != stmt.find("SET bytea_output TO escape")) {
            byteaOutput = EByteaOutput::escape;
            continue;
        }

        google::protobuf::Arena arena;
        settings.Arena = &arena;

        auto program = factory.Create("-stdin-", stmt);

        if (!program->ParseSql(settings)) {
            WriteErrorToStream(program);

            continue;
        }

        if (!program->Compile(username)) {
            WriteErrorToStream(program);

            continue;
        }

#if 0
        auto validate_status = program->Validate(username, &Cout, true);
        program->PrintErrorsTo(Cerr);
        if (validate_status == TProgram::TStatus::Error) {
            return 1;
        }

        auto optimize_status = program->Optimize(username, nullptr, &Cerr, &Cout, true);
        program->PrintErrorsTo(Cerr);
        if (optimize_status == TProgram::TStatus::Error) {
            return 1;
        }
#endif

        if (needPrintAst) {
            Cerr << "Initial AST:\n";
            PrintExprTo(program, Cerr);
        }

        static const THashSet<TString> ignoredNodes{"CommitAll!", "Commit!" };
        const auto opNode = NYql::FindNode(program->ExprRoot(),
                                           [] (const TExprNode::TPtr& node) { return !ignoredNodes.contains(node->Content()); });
        if (opNode->IsCallable("Write!")) {
            Y_ENSURE(opNode->ChildrenSize() == 5);

            const auto* keyNode = opNode->Child(2);
            const bool isWriteToTableSchemeNode = keyNode->IsCallable("Key") && 0 < keyNode->ChildrenSize() &&
                keyNode->Child(0)->Child(0)->IsAtom("tablescheme");

            if (isWriteToTableSchemeNode) {
                try {
                    WriteToYtTableScheme(*opNode, tempDir, yqlNativeServices, program->ExprCtx());
                } catch (const yexception& e) {
                    program->Issues().AddIssue(e.what());

                    WriteErrorToStream(program);

                    continue;
                }

                if (needPrintAst) {
                    program->Optimize(username);

                    ShowFinalAst(program, Cerr);
                }

                continue;
            }
        }

        auto status = program->Run(username, nullptr, nullptr, nullptr, true);
        program->ConfigureYsonResultFormat(NYson::EYsonFormat::Text);

        if (status == TProgram::TStatus::Error) {
            WriteErrorToStream(program);
            continue;
        }
        if (needPrintAst) {
            ShowFinalAst(program, Cerr);
        }

        if (program->HasResults()) {
             if (needPrintResult) {
                Cerr << program->ResultsAsString() << Endl;
             }

            const auto root = ParseYson(program->ResultsAsString());

            const auto& cols = root[0]["Write"][0]["Type"][1][1].AsList();
            const auto& data = root[0]["Write"][0]["Data"].AsList();

            WriteTableToStream(Cout, cols, data);
            Cout << Endl;
        }
    }

    return 0;
}

int main(int argc, char* argv[])
{
    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    try {
        if (1 < argc) {
            if (TString(argv[1]) == "split-statements") {
                return SplitStatements(argc, argv);
            }
        }
        return Main(argc, argv);
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}

