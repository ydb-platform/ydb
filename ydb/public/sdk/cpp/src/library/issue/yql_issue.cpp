#include "utf8.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/issue/yql_issue.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/string_utils/misc/misc.h>

#include <library/cpp/colorizer/output.h>

#include <util/charset/utf8.h>
#include <util/string/ascii.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/string/subst.h>
#include <util/system/compiler.h>
#include <util/generic/stack.h>

#include <cstdlib>
#include <charconv>

namespace NYdb {
inline namespace Dev {
namespace NIssue {

std::string SeverityToString(ESeverity severity) {
    switch (severity) {
        case ESeverity::Fatal:
            return "Fatal";
        case ESeverity::Error:
            return "Error";
        case ESeverity::Warning:
            return "Warning";
        case ESeverity::Info:
            return "Info";
    }
}

void SanitizeNonAscii(std::string& s) {
    if (!NYdb::NIssue::IsUtf8(s)) {
        std::string escaped;
        escaped.reserve(s.size());
        const unsigned char* i = reinterpret_cast<const unsigned char*>(s.data());
        const unsigned char* end = i + s.size();
        while (i < end) {
            char32_t rune;
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

uint64_t TIssue::Hash() const noexcept {
    return CombineHashes(
        CombineHashes(
            (size_t)CombineHashes(IntHash(Position.Row), IntHash(Position.Column)),
            std::hash<std::string>{}(Position.File)
        ),
        (size_t)CombineHashes((size_t)IntHash(static_cast<int>(IssueCode)), std::hash<std::string>{}(Message)));
}

void TIssue::PrintTo(IOutputStream& out, bool oneLine) const {
    out << Range() << ": " << SeverityToString(GetSeverity()) << ": ";
    if (oneLine) {
        std::string message = StripString(Message);
        SubstGlobal(message, '\n', ' ');
        out << message;
    } else {
        out << Message;
    }
    if (GetCode()) {
        out << ", code: " << GetCode();
    }
}

void WalkThroughIssues(const TIssue& topIssue, bool leafOnly, std::function<void(const TIssue&, uint16_t level)> fn, std::function<void(const TIssue&, uint16_t level)> afterChildrenFn) {
    enum class EFnType {
        Main,
        AfterChildren,
    };

    const bool hasAfterChildrenFn = bool(afterChildrenFn);
    TStack<std::tuple<uint16_t, const TIssue*, EFnType>> issuesStack;
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

Y_NO_INLINE void Indent(IOutputStream& out, uint32_t indentation) {
    char* whitespaces = reinterpret_cast<char*>(alloca(indentation));
    memset(whitespaces, ' ', indentation);
    out.Write(whitespaces, indentation);
}

void ProgramLinesWithErrors(
        const std::string& programText,
        const std::vector<TIssue>& errors,
        std::map<uint32_t, std::string_view>& lines)
{
    std::vector<uint32_t> rows;
    for (const auto& topIssue: errors) {
        WalkThroughIssues(topIssue, false, [&](const TIssue& issue, uint16_t /*level*/) {
            for (uint32_t row = issue.Position.Row; row <= issue.EndPosition.Row; row++) {
                rows.push_back(row);
            }
        });
    }
    std::sort(rows.begin(), rows.end());

    auto prog = StringSplitter(programText).Split('\n');
    auto progIt = prog.begin(), progEnd = prog.end();
    uint32_t progRow = 1;

    for (uint32_t row: rows) {
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
            WalkThroughIssues(topIssue, false, [&](const TIssue& issue, uint16_t level) {
                if (level > 0) {
                    out << " subissue: { ";
                } else {
                    out << (printWithSpace ? " { " : "{ ");
                }
                issue.PrintTo(out, true);
            },
            [&](const TIssue&, uint16_t) {
                out << " }";
            });
        }
        if (Issues_.size() > 1) {
            out << " ]";
        }
    } else {
        for (const auto& topIssue: Issues_) {
            WalkThroughIssues(topIssue, false, [&](const TIssue& issue, uint16_t level) {
                auto shift = level * 4;
                Indent(out, shift);
                out << issue << Endl;
            });
        }
    }
}

void TIssues::PrintWithProgramTo(
        IOutputStream& out,
        const std::string& programFilename,
        const std::string& programText) const
{
    using namespace NColorizer;

    std::map<uint32_t, std::string_view> lines;
    ProgramLinesWithErrors(programText, Issues_, lines);

    for (const TIssue& topIssue: Issues_) {
        WalkThroughIssues(topIssue, false, [&](const TIssue& issue, uint16_t level) {
            auto shift = level * 4;
            Indent(out, shift);
            out << DarkGray() << programFilename << Old() << ':';
            out << Purple() << issue.Range() << Old();
            auto color = (issue.GetSeverity() ==  ESeverity::Warning) ? Yellow() : LightRed();
            auto severityName = SeverityToString(issue.GetSeverity());
            out << color << ": "<< severityName << ": " << issue.GetMessage() << Old() << '\n';
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
    std::string_view messageBuf = e.what();
    auto parsedPos = TryParseTerminationMessage(messageBuf);
    auto issue = TIssue(parsedPos.value_or(pos), messageBuf);
    const TErrorException* errorException = dynamic_cast<const TErrorException*>(&e);
    if (errorException) {
        issue.SetCode(errorException->GetCode(), ESeverity::Error);
    } else {
        issue.SetCode(UNEXPECTED_ERROR, ESeverity::Fatal);
    }
    return issue;
}

static constexpr std::string_view TerminationMessageMarker = "Terminate was called, reason(";

std::optional<TPosition> TryParseTerminationMessage(std::string_view& message) {
    size_t len = 0;
    size_t startPos = message.find(TerminationMessageMarker);
    size_t endPos = 0;
    if (startPos != std::string::npos) {
        endPos = message.find(')', startPos + TerminationMessageMarker.size());
        if (endPos != std::string::npos) {
            std::string_view lenText = message.substr(startPos + TerminationMessageMarker.size())
                .substr(0, endPos - startPos - TerminationMessageMarker.size());
            auto result = std::from_chars(lenText.data(), lenText.data() + lenText.size(), len);
            if (result.ec != std::errc{} || result.ptr != lenText.data() + lenText.size()) {
                len = 0;
            }
        }
    }

    if (len) {
        message = message.substr(endPos + 3).substr(0, len);
        auto s = message;
        std::optional<std::string_view> file;
        std::optional<std::string_view> row;
        std::optional<std::string_view> column;
        NUtils::GetNext(s, ':', file);
        NUtils::GetNext(s, ':', row);
        NUtils::GetNext(s, ':', column);
        uint32_t rowValue, columnValue;
        if (file && row && column && TryFromString(*row, rowValue) && TryFromString(*column, columnValue)) {
            message = StripStringLeft(s);
            return TPosition(columnValue, rowValue, std::string(*file));
        }
    }

    return std::nullopt;
}

}
}
}

template <>
void Out<NYdb::NIssue::TPosition>(IOutputStream& out, const NYdb::NIssue::TPosition& pos) {
    out << (pos.File.empty() ? "<main>" : pos.File);
    if (pos) {
        out << ":" << pos.Row << ':' << pos.Column;
    }
}

template<>
void Out<NYdb::NIssue::TRange>(IOutputStream & out, const NYdb::NIssue::TRange & range) {
    if (range.IsRange()) {
        out << '[' << range.Position << '-' << range.EndPosition << ']';
    } else {
        out << range.Position;
    }
}

template <>
void Out<NYdb::NIssue::TIssue>(IOutputStream& out, const NYdb::NIssue::TIssue& error) {
    error.PrintTo(out);
}

template <>
void Out<NYdb::NIssue::TIssues>(IOutputStream& out, const NYdb::NIssue::TIssues& error) {
    error.PrintTo(out);
}
