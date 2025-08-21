#include "format.h"

#include "context.h"
#include "fwd_backend.h"

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/logger/record.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/builder.h>

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

        template <std::invocable<EContextKey> Action>
        void ForEachRequiredContextKey(Action&& action) {
            const size_t min = static_cast<size_t>(EContextKey::DateTime);
            for (size_t i = min; i <= MaxRequiredContextKey; ++i) {
                action(static_cast<EContextKey>(i));
            }
        }

        class TFormattingLogBackend final: public TForwardingLogBackend {
        public:
            explicit TFormattingLogBackend(TFormatter formatter, TAutoPtr<TLogBackend> child)
                : TForwardingLogBackend(std::move(child))
                , Formatter_(std::move(formatter))
            {
            }

            void WriteData(const TLogRecord& rec) final {
                Validate(rec);

                TString message = Formatter_(rec);
                message.append('\n');

                const TLogRecord formatted(rec.Priority, message.data(), message.size());

                return TForwardingLogBackend::WriteData(formatted);
            }

        protected:
            void Validate(const TLogRecord& rec) const {
                const TLogRecord::TMetaFlags& flags = rec.MetaFlags;

                ForEachRequiredContextKey([&](EContextKey key) {
                    size_t i = static_cast<int>(key);

                    const TStringBuf expected = ToStringBuf(key);
                    YQL_ENSURE(
                        i < flags.size(),
                        "ContextKey #" << i << " named'" << expected << "' is out of range " << flags.size());

                    const TStringBuf actual = rec.MetaFlags[i].first;
                    YQL_ENSURE(
                        actual == expected,
                        "MetaFlag #" << i << " key was expected to be '" << expected << "', but got '" << actual << "'");
                });
            }

        private:
            TFormatter Formatter_;
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

        if (auto path = opt(ToStringBuf(EContextKey::Path))) {
            out << "{" << *path << "} ";
        }

        return out << TStringBuf(rec.Data, rec.Len);
    }

    TString JsonFormat(const TLogRecord& rec) {
        TStringStream out;
        NJsonWriter::TBuf buf(NJsonWriter::HEM_DONT_ESCAPE_HTML, &out);
        buf.BeginObject();
        buf.WriteKey("message");
        buf.WriteString(TStringBuf(rec.Data, rec.Len));
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

    TAutoPtr<TLogBackend> MakeFormattingLogBackend(TFormatter formatter, TAutoPtr<TLogBackend> child) {
        return new TFormattingLogBackend(std::move(formatter), std::move(child));
    }

} // namespace NYql::NLog
