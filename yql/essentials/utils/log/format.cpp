#include "format.h"

#include "context.h"
#include "fwd_backend.h"

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/logger/record.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/builder.h>
#include <util/generic/algorithm.h>

#include <ranges>

namespace NYql::NLog {

namespace {

constexpr size_t MaxRequiredContextKey = static_cast<size_t>(EContextKey::Line);

auto RequiredContextAccessor(const TLogRecord& rec) {
    return [&](EContextKey key) -> TStringBuf {
        return rec.MetaFlags.at(static_cast<size_t>(key)).second;
    };
}

auto OptionalContextAccessor(const TLogRecord& rec) {
    return [&](TStringBuf key) -> TMaybe<TStringBuf> {
        const auto isContextKeyPath = [&](const auto& pair) {
            return pair.first == key;
        };

        const auto* path = FindIfPtr(
            rec.MetaFlags.begin() + MaxRequiredContextKey + 1,
            rec.MetaFlags.end(),
            isContextKeyPath);

        if (!path) {
            return Nothing();
        }

        return path->second;
    };
}

void PrintBody(TStringBuilder& out, const TLogRecord& rec, size_t flagBegin) {
    out << TStringBuf(rec.Data, rec.Len);

    if (flagBegin < rec.MetaFlags.size()) {
        out << ". Extra context: ";
    }

    for (size_t i = flagBegin; i < rec.MetaFlags.size(); ++i) {
        const auto& [key, value] = rec.MetaFlags[i];
        out << key << " = " << value;
        if (i + 1 != rec.MetaFlags.size()) {
            out << ", ";
        }
    }
}

TString FallbackFormat(const TLogRecord& rec) {
    TStringBuilder out;
    PrintBody(out, rec, /*flagBegin=*/0);
    return out;
}

TString GetBasicLoggingFormatFromRecord(const TLogRecord& rec) {
    auto priority = rec.Priority;
    switch (priority) {
        case TLOG_EMERG:
        case TLOG_ALERT:
        case TLOG_CRIT:
        case TLOG_ERR:
            return "ERROR";

        case TLOG_WARNING:
            return "WARN";

        case TLOG_NOTICE:
        case TLOG_INFO:
            return "INFO";

        case TLOG_DEBUG:
        case TLOG_RESOURCES:
            return "DEBUG";
        default:
            return "DEBUG";
    }
}

class TFormattingLogBackend final: public TForwardingLogBackend {
public:
    explicit TFormattingLogBackend(TFormatter formatter, bool isStrict, TAutoPtr<TLogBackend> child)
        : TForwardingLogBackend(std::move(child))
        , Formatter_(std::move(formatter))
        , IsStrict_(isStrict)
    {
    }

    void WriteData(const TLogRecord& rec) final {
        if (rec.MetaFlags.empty()) {
            // NB. For signal handler.
            return TForwardingLogBackend::WriteData(rec);
        }

        TString message;
        if (IsSupported(rec.MetaFlags)) {
            message = Formatter_(rec);
        } else if (IsStrict_) {
            TStringBuilder message;
            message << "LogRecord is not supported: ";
            PrintBody(message, rec, /* flagBegin = */ 0);
            ythrow yexception() << std::move(message);
        } else {
            message = FallbackFormat(rec);
        }
        message.append('\n');

        const TLogRecord formatted(rec.Priority, message.data(), message.size());
        return TForwardingLogBackend::WriteData(formatted);
    }

protected:
    static bool IsSupported(const TLogRecord::TMetaFlags& flags) {
        const auto isSupported = [&](size_t i) -> bool {
            const EContextKey key = static_cast<EContextKey>(i);

            const TStringBuf expected = ToStringBuf(key);
            if (flags.size() <= i) {
                return false;
            }

            const TStringBuf actual = flags[i].first;
            if (actual != expected) {
                return false;
            }

            return true;
        };

        return AllOf(std::views::iota(Min<size_t>(), MaxRequiredContextKey), isSupported);
    }

private:
    TFormatter Formatter_;
    bool IsStrict_;
};

} // namespace

TString LegacyFormat(const TLogRecord& rec) {
    const auto get = RequiredContextAccessor(rec);
    const auto opt = OptionalContextAccessor(rec);

    TStringBuilder out;
    out << get(EContextKey::DateTime) << ' '
        << get(EContextKey::Level) << ' '
        << get(EContextKey::ProcessName)
        << "(pid=" << get(EContextKey::ProcessID)
        << ", tid=" << get(EContextKey::ThreadID)
        << ") [" << get(EContextKey::Component) << "] "
        << get(EContextKey::FileName)
        << ':' << get(EContextKey::Line) << ": ";

    size_t unknownContextBegin = MaxRequiredContextKey + 1;
    if (auto path = opt(ToStringBuf(EContextKey::Path))) {
        out << "{" << *path << "} ";
        unknownContextBegin += 1;
    }

    PrintBody(out, rec, unknownContextBegin);
    return out;
}

TString JsonFormat(const TLogRecord& rec) {
    TStringStream out;
    NJsonWriter::TBuf buf(NJsonWriter::HEM_DONT_ESCAPE_HTML, &out);
    buf.BeginObject();
    buf.WriteKey("message");
    buf.WriteString(TStringBuf(rec.Data, rec.Len));
    buf.WriteKey("levelStr");
    buf.WriteString(GetBasicLoggingFormatFromRecord(rec));
    buf.WriteKey("@fields");
    buf.BeginObject();
    for (const auto& [key, value] : rec.MetaFlags) {
        buf.WriteKey(key);
        buf.WriteString(value);
    }
    buf.EndObject();
    buf.EndObject();
    return std::move(out.Str());
}

TAutoPtr<TLogBackend> MakeFormattingLogBackend(TFormatter formatter, bool isStrict, TAutoPtr<TLogBackend> child) {
    return new TFormattingLogBackend(std::move(formatter), isStrict, std::move(child));
}

} // namespace NYql::NLog
