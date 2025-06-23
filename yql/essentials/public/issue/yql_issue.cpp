#include "yql_issue.h"
#include "yql_issue_id.h"

#include <yql/essentials/utils/utf8.h>

#include <library/cpp/colorizer/output.h>

#include <util/charset/utf8.h>
#include <util/string/ascii.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/string/subst.h>
#include <util/system/compiler.h>
#include <util/generic/map.h>
#include <util/generic/stack.h>
#include <cstdlib>


namespace NYql {

void SanitizeNonAscii(TString& s) {
    if (!NYql::IsUtf8(s)) {
        TString escaped;
        escaped.reserve(s.size());
        const unsigned char* i = reinterpret_cast<const unsigned char*>(s.data());
        const unsigned char* end = i + s.size();
        while (i < end) {
            wchar32 rune;
            size_t runeLen;
            const RECODE_RESULT result = SafeReadUTF8Char(rune, runeLen, i, end);
            if (result == RECODE_OK) {
                escaped.insert(escaped.end(), reinterpret_cast<const char*>(i), reinterpret_cast<const char*>(i + runeLen));
                i += runeLen;
            } else {
                escaped.push_back('?');
                ++i;
            }
        }
        s = escaped;
    }
}

TTextWalker& TTextWalker::Advance(char c) {
    if (c == '\n') {
        HaveCr_ = false;
        ++LfCount_;
        return *this;
    }


    if (c == '\r' && !HaveCr_) {
        HaveCr_ = true;
        return *this;
    }

    ui32 charDistance = 1;
    if (Utf8Aware_ && IsUTF8ContinuationByte(c)) {
        charDistance = 0;
    }

    // either not '\r' or second '\r'
    if (LfCount_) {
        Position_.Row += LfCount_;
        Position_.Column = charDistance;
        LfCount_ = 0;
    } else {
        Position_.Column += charDistance + (HaveCr_ && c != '\r');
    }
    HaveCr_ = (c == '\r');
    return *this;
}

void TIssue::PrintTo(IOutputStream& out, bool oneLine) const {
    out << Range() << ": " << SeverityToString(GetSeverity()) << ": ";
    if (oneLine) {
        TString message = StripString(Message_);
        SubstGlobal(message, '\n', ' ');
        out << message;
    } else {
        out << Message_;
    }
    if (GetCode()) {
        out << ", code: " << GetCode();
    }
}

void WalkThroughIssues(const TIssue& topIssue, bool leafOnly, std::function<void(const TIssue&, ui16 level)> fn, std::function<void(const TIssue&, ui16 level)> afterChildrenFn) {
    enum class EFnType {
        Main,
        AfterChildren,
    };

    const bool hasAfterChildrenFn = bool(afterChildrenFn);
    TStack<std::tuple<ui16, const TIssue*, EFnType>> issuesStack;
    if (hasAfterChildrenFn) {
        issuesStack.push(std::make_tuple(0, &topIssue, EFnType::AfterChildren));
    }
    issuesStack.push(std::make_tuple(0, &topIssue, EFnType::Main));
    while (!issuesStack.empty()) {
        auto level = std::get<0>(issuesStack.top());
        const auto& curIssue = *std::get<1>(issuesStack.top());
        const EFnType fnType = std::get<2>(issuesStack.top());
        issuesStack.pop();
        if (!leafOnly || curIssue.GetSubIssues().empty()) {
            if (fnType == EFnType::Main) {
                fn(curIssue, level);
            } else {
                afterChildrenFn(curIssue, level);
            }
        }
        if (fnType == EFnType::Main) {
            level++;
            const auto& subIssues = curIssue.GetSubIssues();
            for (int i = subIssues.size() - 1; i >= 0; i--) {
                if (hasAfterChildrenFn) {
                    issuesStack.push(std::make_tuple(level, subIssues[i].Get(), EFnType::AfterChildren));
                }
                issuesStack.push(std::make_tuple(level, subIssues[i].Get(), EFnType::Main));
            }
        }
    }
}

namespace {

Y_NO_INLINE void Indent(IOutputStream& out, ui32 indentation) {
    char* whitespaces = reinterpret_cast<char*>(alloca(indentation));
    memset(whitespaces, ' ', indentation);
    out.Write(whitespaces, indentation);
}

void ProgramLinesWithErrors(
        const TString& programText,
        const TVector<TIssue>& errors,
        TMap<ui32, TStringBuf>& lines)
{
    TVector<ui32> rows;
    for (const auto& topIssue: errors) {
        WalkThroughIssues(topIssue, false, [&](const TIssue& issue, ui16 /*level*/) {
            for (ui32 row = issue.Position.Row; row <= issue.EndPosition.Row; row++) {
                rows.push_back(row);
            }
        });
    }
    std::sort(rows.begin(), rows.end());

    auto prog = StringSplitter(programText).Split('\n');
    auto progIt = prog.begin(), progEnd = prog.end();
    ui32 progRow = 1;

    for (ui32 row: rows) {
        while (progRow < row && progIt != progEnd) {
            ++progRow;
            ++progIt;
        }
        if (progIt != progEnd) {
            lines[row] = progIt->Token();
        }
    }
}

} // namspace

void TIssues::PrintTo(IOutputStream& out, bool oneLine) const
{
    if (oneLine) {
        bool printWithSpace = false;
        if (Issues_.size() > 1) {
            printWithSpace = true;
            out << "[";
        }
        for (const auto& topIssue: Issues_) {
            WalkThroughIssues(topIssue, false, [&](const TIssue& issue, ui16 level) {
                if (level > 0) {
                    out << " subissue: { ";
                } else {
                    out << (printWithSpace ? " { " : "{ ");
                }
                issue.PrintTo(out, true);
            },
            [&](const TIssue&, ui16) {
                out << " }";
            });
        }
        if (Issues_.size() > 1) {
            out << " ]";
        }
    } else {
        for (const auto& topIssue: Issues_) {
            WalkThroughIssues(topIssue, false, [&](const TIssue& issue, ui16 level) {
                auto shift = level * 4;
                Indent(out, shift);
                out << issue << Endl;
            });
        }
    }
}

void TIssues::PrintWithProgramTo(
        IOutputStream& out,
        const TString& programFilename,
        const TString& programText,
        bool colorize) const
{
    using namespace NColorizer;

    TMap<ui32, TStringBuf> lines;
    ProgramLinesWithErrors(programText, Issues_, lines);

    for (const TIssue& topIssue: Issues_) {
        WalkThroughIssues(topIssue, false, [&](const TIssue& issue, ui16 level) {
            auto shift = level * 4;
            Indent(out, shift);
            if (colorize) {
                out << DarkGray();
            }
            out << programFilename;
            if (colorize) {
                out << Old();
            }

            out << ':';
            if (colorize) {
                out << Purple();
            }

            out << issue.Range();
            if (colorize) {
                out << Old();
            }

            auto severityName = SeverityToString(issue.GetSeverity());
            if (colorize) {
                if (issue.GetSeverity() == TSeverityIds::S_INFO) {
                    out << LightGreen();
                } else if (issue.GetSeverity() == TSeverityIds::S_WARNING) {
                    out << Yellow();
                } else {
                    out << LightRed();
                }
            }

            out << ": "<< severityName << ": " << issue.GetMessage();
            if (colorize) {
                out << Old();
            }

            out << '\n';
            Indent(out, shift);
            if (issue.Position.HasValue()) {
                out << '\t' << lines[issue.Position.Row] << '\n';
                out << '\t';
                if (issue.Position.Column > 0) {
                    Indent(out, issue.Position.Column - 1);
                }
                out << '^';
            }
            out << Endl;
        });
    }
}

TIssue ExceptionToIssue(const std::exception& e, const TPosition& pos) {
    TStringBuf messageBuf = e.what();
    auto parsedPos = TryParseTerminationMessage(messageBuf);
    auto issue = TIssue(parsedPos.GetOrElse(pos), messageBuf);
    const TErrorException* errorException = dynamic_cast<const TErrorException*>(&e);
    if (errorException) {
        issue.SetCode(errorException->GetCode(), ESeverity::TSeverityIds_ESeverityId_S_ERROR);
    } else {
        issue.SetCode(UNEXPECTED_ERROR, ESeverity::TSeverityIds_ESeverityId_S_FATAL);
    }
    return issue;
}

static constexpr TStringBuf TerminationMessageMarker = "Terminate was called, reason(";

TMaybe<TPosition> TryParseTerminationMessage(TStringBuf& message) {
    size_t len = 0;
    size_t startPos = message.find(TerminationMessageMarker);
    size_t endPos = 0;
    if (startPos != TString::npos) {
        endPos = message.find(')', startPos + TerminationMessageMarker.size());
        if (endPos != TString::npos) {
            TStringBuf lenText = message.Tail(startPos + TerminationMessageMarker.size())
                .Trunc(endPos - startPos - TerminationMessageMarker.size());
            try {
                len = FromString<size_t>(lenText);
            } catch (const TFromStringException&) {
                len = 0;
            }
        }
    }

    if (len) {
        message = message.Tail(endPos + 3).Trunc(len);
        auto s = message;
        TMaybe<TStringBuf> file;
        TMaybe<TStringBuf> row;
        TMaybe<TStringBuf> column;
        GetNext(s, ':', file);
        GetNext(s, ':', row);
        GetNext(s, ':', column);
        ui32 rowValue, columnValue;
        if (file && row && column && TryFromString(*row, rowValue) && TryFromString(*column, columnValue)) {
            message = StripStringLeft(s);
            return TPosition(columnValue, rowValue, TString(*file));
        }
    }

    return Nothing();
}

} // namspace NYql

template <>
void Out<NYql::TPosition>(IOutputStream& out, const NYql::TPosition& pos) {
    out << (pos.File ? pos.File : "<main>");
    if (pos) {
        out << ":" << pos.Row << ':' << pos.Column;
    }
}

template<>
void Out<NYql::TRange>(IOutputStream & out, const NYql::TRange & range) {
    if (range.IsRange()) {
        out << '[' << range.Position << '-' << range.EndPosition << ']';
    } else {
        out << range.Position;
    }
}

template <>
void Out<NYql::TIssue>(IOutputStream& out, const NYql::TIssue& error) {
    error.PrintTo(out);
}

template <>
void Out<NYql::TIssues>(IOutputStream& out, const NYql::TIssues& error) {
    error.PrintTo(out);
}
