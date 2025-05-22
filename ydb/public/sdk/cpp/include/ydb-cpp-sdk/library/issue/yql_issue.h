#pragma once

#include <util/generic/ptr.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/system/types.h>
#include <util/str_stl.h>

#include <google/protobuf/message.h>

#include <optional>
#include <vector>
#include <string>
#include <string_view>

namespace NYdb {
inline namespace Dev {
namespace NIssue {

using TIssueCode = uint32_t;
constexpr TIssueCode DEFAULT_ERROR = 0;
constexpr TIssueCode UNEXPECTED_ERROR = 1;

enum class ESeverity : uint32_t {
    Fatal = 0,
    Error = 1,
    Warning = 2,
    Info = 3,
};

std::string SeverityToString(ESeverity severity);

void SanitizeNonAscii(std::string& s);

///////////////////////////////////////////////////////////////////////////////
// TPosition
///////////////////////////////////////////////////////////////////////////////
struct TPosition {
    uint32_t Column = 0U;
    uint32_t Row = 0U;
    std::string File;

    TPosition() = default;

    TPosition(uint32_t column, uint32_t row, const std::string& file = {})
        : Column(column)
        , Row(row)
        , File(file)
    {
        SanitizeNonAscii(File);
    }

    explicit operator bool() const {
        return HasValue();
    }

    inline bool HasValue() const {
        return Row | Column;
    }

    inline bool operator==(const TPosition& other) const {
        return Column == other.Column && Row == other.Row && File == other.File;
    }

    inline bool operator<(const TPosition& other) const {
        return std::tie(Row, Column, File) < std::tie(other.Row, other.Column, other.File);
    }
};

struct TRange {
    TPosition Position;
    TPosition EndPosition;

    TRange() = default;

    TRange(TPosition position)
        : Position(position)
        , EndPosition(position)
    {
    }

    TRange(TPosition position, TPosition endPosition)
        : Position(position)
        , EndPosition(endPosition)
    {
    }

    inline bool IsRange() const {
        return !(Position == EndPosition);
    }
};

///////////////////////////////////////////////////////////////////////////////
// TIssue
///////////////////////////////////////////////////////////////////////////////

class TIssue;
using TIssuePtr = TIntrusivePtr<TIssue>;
class TIssue: public TThrRefBase {
    std::vector<TIntrusivePtr<TIssue>> Children_;
    std::string Message;
public:
    TPosition Position;
    TPosition EndPosition;
    TIssueCode IssueCode = 0U;
    ESeverity Severity = ESeverity::Error;

    TIssue() = default;

    template <typename T>
    explicit TIssue(const T& message)
        : Message(message)
        , Position(TPosition())
        , EndPosition(TPosition())
    {
        SanitizeNonAscii(Message);
    }

    template <typename T>
    TIssue(TPosition position, const T& message)
        : Message(message)
        , Position(position)
        , EndPosition(position)
    {
        SanitizeNonAscii(Message);
    }

    inline TRange Range() const {
        return {Position, EndPosition};
    }

    template <typename T>
    TIssue(TPosition position, TPosition endPosition, const T& message)
        : Message(message)
        , Position(position)
        , EndPosition(endPosition)
    {
        SanitizeNonAscii(Message);
    }

    inline bool operator==(const TIssue& other) const {
        return Position == other.Position && Message == other.Message
            && IssueCode == other.IssueCode;
    }

    uint64_t Hash() const noexcept;

    TIssue& SetCode(TIssueCode id, ESeverity severity) {
        IssueCode = id;
        Severity = severity;
        return *this;
    }

    TIssue& SetMessage(const std::string& msg) {
        Message = msg;
        SanitizeNonAscii(Message);
        return *this;
    }

    ESeverity GetSeverity() const {
        return Severity;
    }

    TIssueCode GetCode() const {
        return IssueCode;
    }

    const std::string& GetMessage() const {
        return Message;
    }

    TIssue& AddSubIssue(TIntrusivePtr<TIssue> issue) {
        Severity = static_cast<ESeverity>(std::min(static_cast<uint32_t>(issue->GetSeverity()),
                                                   static_cast<uint32_t>(Severity)));
        Children_.push_back(issue);
        return *this;
    }

    const std::vector<TIntrusivePtr<TIssue>>& GetSubIssues() const {
        return Children_;
    }

    void PrintTo(IOutputStream& out, bool oneLine = false) const;

    std::string ToString(bool oneLine = false) const {
        TStringStream out;
        PrintTo(out, oneLine);
        return out.Str();
    }

    // Unsafe method. Doesn't call SanitizeNonAscii(Message)
    std::string* MutableMessage() {
        return &Message;
    }

    TIssue& CopyWithoutSubIssues(const TIssue& src) {
        Message = src.Message;
        IssueCode = src.IssueCode;
        Severity = src.Severity;
        Position = src.Position;
        EndPosition = src.EndPosition;
        return *this;
    }
};

void WalkThroughIssues(const TIssue& topIssue, bool leafOnly, std::function<void(const TIssue&, uint16_t level)> fn, std::function<void(const TIssue&, uint16_t level)> afterChildrenFn = {});

///////////////////////////////////////////////////////////////////////////////
// TIssues
///////////////////////////////////////////////////////////////////////////////
class TIssues {
public:
    TIssues() = default;

    inline TIssues(const std::vector<TIssue>& issues)
        : Issues_(issues)
    {
    }

    inline TIssues(const std::initializer_list<TIssue>& issues)
        : TIssues(std::vector<TIssue>(issues))
    {
    }

    inline TIssues(const TIssues& rhs)
        : Issues_(rhs.Issues_)
    {
    }

    inline TIssues& operator=(const TIssues& rhs) {
        Issues_ = rhs.Issues_;
        return *this;
    }

    inline TIssues(TIssues&& rhs) : Issues_(std::move(rhs.Issues_))
    {
    }

    inline TIssues& operator=(TIssues&& rhs) {
        Issues_ = std::move(rhs.Issues_);
        return *this;
    }

    template <typename ... Args> void AddIssue(Args&& ... args) {
        Issues_.emplace_back(std::forward<Args>(args)...);
    }

    inline void AddIssues(const TIssues& errors) {
        Issues_.insert(Issues_.end(),
                       errors.Issues_.begin(), errors.Issues_.end());
    }

    inline void AddIssues(const TPosition& pos, const TIssues& errors) {
        Issues_.reserve(Issues_.size() + errors.Size());
        for (const auto& e: errors) {
            TIssue& issue = Issues_.emplace_back();
            *issue.MutableMessage() = e.GetMessage(); // No need to sanitize message, it has already been sanitized.
            issue.Position = pos;
            issue.SetCode(e.IssueCode, e.Severity);
        }
    }

    inline const TIssue* begin() const {
        return Issues_.data();
    }

    inline const TIssue* end() const {
        return Issues_.data() + Issues_.size();
    }

    inline TIssue& back() {
        return Issues_.back();
    }

    inline const TIssue& back() const {
        return Issues_.back();
    }

    inline bool Empty() const {
        return Issues_.empty();
    }

    explicit operator bool() const noexcept {
        return !Issues_.empty();
    }

    inline size_t Size() const {
        return Issues_.size();
    }

    void PrintTo(IOutputStream& out, bool oneLine = false) const;
    void PrintWithProgramTo(
            IOutputStream& out,
            const std::string& programFilename,
            const std::string& programText) const;

    inline std::string ToString(bool oneLine = false) const {
        TStringStream out;
        PrintTo(out, oneLine);
        return out.Str();
    }

    std::string ToOneLineString() const {
        return ToString(true);
    }

    inline void Clear() {
        Issues_.clear();
    }

    void Reserve(size_t capacity) {
        Issues_.reserve(capacity);
    }

private:
    std::vector<TIssue> Issues_;
};

class TErrorException : public yexception {
    const TIssueCode Code_;
public:
    explicit TErrorException(TIssueCode code)
        : Code_(code)
    {}
    TIssueCode GetCode() const {
        return Code_;
    }
};

TIssue ExceptionToIssue(const std::exception& e, const TPosition& pos = TPosition());
std::optional<TPosition> TryParseTerminationMessage(std::string_view& message);

}
}
}

template <>
void Out<NYdb::NIssue::TPosition>(IOutputStream& out, const NYdb::NIssue::TPosition& pos);

template <>
void Out<NYdb::NIssue::TRange>(IOutputStream& out, const NYdb::NIssue::TRange& pos);

template <>
void Out<NYdb::NIssue::TIssue>(IOutputStream& out, const NYdb::NIssue::TIssue& error);

template <>
void Out<NYdb::NIssue::TIssues>(IOutputStream& out, const NYdb::NIssue::TIssues& error);

template <>
struct THash<NYdb::NIssue::TIssue> {
    inline size_t operator()(const NYdb::NIssue::TIssue& err) const {
        return err.Hash();
    }
};
