#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/text_writer.h>
#include <ydb/library/services/services.pb.h>

#include <type_traits>

#define NPQ_LOG_PREFIX ::NKikimr::NPQ::MakeRuntimeLogPrefix(*this)
#define LOG(level, T, ...) YDB_LOG_COMP(level, this->Service, T, NPQ_LOG_PREFIX, ##__VA_ARGS__)
#define LOG_T(T, ...) YDB_LOG_TRACE_COMP(this->Service, T, NPQ_LOG_PREFIX, ##__VA_ARGS__)
#define LOG_D(T, ...) YDB_LOG_DEBUG_COMP(this->Service, T, NPQ_LOG_PREFIX, ##__VA_ARGS__)
#define LOG_I(T, ...) YDB_LOG_INFO_COMP(this->Service, T, NPQ_LOG_PREFIX, ##__VA_ARGS__)
#define LOG_N(T, ...) YDB_LOG_NOTICE_COMP(this->Service, T, NPQ_LOG_PREFIX, ##__VA_ARGS__)
#define LOG_W(T, ...) YDB_LOG_WARN_COMP(this->Service, T, NPQ_LOG_PREFIX, ##__VA_ARGS__)
#define LOG_E(T, ...) YDB_LOG_ERROR_COMP(this->Service, T, NPQ_LOG_PREFIX, ##__VA_ARGS__)
#define LOG_C(T, ...) YDB_LOG_CRIT_COMP(this->Service, T, NPQ_LOG_PREFIX, ##__VA_ARGS__)
#define LOG_A(T, ...) YDB_LOG_ALERT_COMP(this->Service, T, NPQ_LOG_PREFIX, ##__VA_ARGS__)

namespace NKikimr::NPQ {

using TStructuredLogPrefix = NActors::NStructuredLog::TStructuredMessage;

class TLogPrefix {
public:
    explicit TLogPrefix(NKikimrServices::EServiceKikimr service = NKikimrServices::PERSQUEUE)
        : Service(service)
    {
    }

    virtual ~TLogPrefix() = default;

    virtual TStructuredLogPrefix LogPrefix() const = 0;

    NKikimrServices::EServiceKikimr Service;
};

template <typename T>
TStructuredLogPrefix MakeRuntimeLogPrefix(const T& self) {
    TStructuredLogPrefix prefix;
    if constexpr (requires { self.TabletId; }) {
        prefix.AppendMessage(YDB_LOG_CREATE_MESSAGE({"tabletId", self.TabletId}));
    } else if constexpr (requires { self.TabletID(); }) {
        prefix.AppendMessage(YDB_LOG_CREATE_MESSAGE({"tabletId", self.TabletID()}));
    }
    if constexpr (requires { self.SelfId(); }) {
        prefix.AppendMessage(YDB_LOG_CREATE_MESSAGE({"selfId", self.SelfId()}));
    } else if constexpr (requires { self.SelfID; }) {
        prefix.AppendMessage(YDB_LOG_CREATE_MESSAGE({"selfId", self.SelfID}));
    }
    if constexpr (std::is_base_of_v<TLogPrefix, T>) {
        prefix.AppendMessage(static_cast<const TLogPrefix&>(self).LogPrefix());
    } else {
        prefix.AppendMessage(self.LogPrefix());
    }
    return prefix;
}

inline TString StructuredLogPrefixText(const TStructuredLogPrefix& prefix) {
    TStringBuilder out;
    NActors::NStructuredLog::TTextWriter writer;
    writer.Write(out, prefix);
    return out;
}

} // namespace NKikimr::NPQ
