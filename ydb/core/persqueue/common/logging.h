#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/text_writer.h>
#include <ydb/library/services/services.pb.h>

#define NPQ_LOG_PREFIX this->LogPrefix()
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

inline TStructuredLogPrefix MakeNpqLogPrefix(TStructuredLogPrefix builder, const TStructuredLogPrefix& prefix) {
    builder.AppendMessage(prefix);
    return builder;
}

inline TString StructuredLogPrefixText(const TStructuredLogPrefix& prefix) {
    TStringBuilder out;
    NActors::NStructuredLog::TTextWriter writer;
    writer.Write(out, prefix);
    return out;
}

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

} // namespace NKikimr::NPQ
