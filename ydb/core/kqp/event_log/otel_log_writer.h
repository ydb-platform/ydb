#pragma once

#include "base_event_log_writer.h"
#include "log_column.h"

#include <ydb/library/actors/struct_log/log_sink.h>
#include <ydb/library/actors/struct_log/otel/full_otel_log_sink.h>
#include <ydb/library/actors/struct_log/otel/simple_otel_log_sink.h>

#include <set>
#include <utility>
#include <vector>

namespace NKikimr::NKqp::NEventLog {

template <typename TLogClass, typename TBase>
class TBaseOtelLogWriter : public TBase {
public:
    template <typename TArg>
    TBaseOtelLogWriter(TArg&& arg)
        : TBase(std::forward<TArg>(arg))
        , Filter(TLogClass::CreateFilter())
        , RequiredKeys(MakeRequiredKeys())
    {
    }

    TBaseOtelLogWriter(NActors::NStructuredLog::TBaseOtelLogSink::TOtelSettings settings, TString logType)
        : TBase(std::move(settings), std::move(logType))
        , Filter(TLogClass::CreateFilter())
        , RequiredKeys(MakeRequiredKeys())
    {
    }

    void Write(const NActors::NStructuredLog::TLogMessage& message) override {
        if (!Filter(message)) {
            return;
        }
        NActors::NStructuredLog::TLogMessage filteredMessage = message;
        filteredMessage.StructuredMessage =
            message.StructuredMessage.SelectAttachedValues(RequiredKeys);
        TBase::Write(filteredMessage);
    }

private:
    static std::set<std::vector<NActors::NStructuredLog::TKeyName>> MakeRequiredKeys() {
        std::set<std::vector<NActors::NStructuredLog::TKeyName>> result;
        for (const auto& column : TLogClass::GetColumns()) {
            if (const auto* keyName = column->GetStructuredKeyName()) {
                result.insert(*keyName);
            }
        }
        return result;
    }

    const TBaseEventLogWriter::TFilterFunction Filter;
    const std::set<std::vector<NActors::NStructuredLog::TKeyName>> RequiredKeys;
};

template <typename TLogClass>
class TSimpleOtelLogWriter : public TBaseOtelLogWriter<TLogClass, NActors::NStructuredLog::TSimpleOtelLogSink> {
public:
    explicit TSimpleOtelLogWriter(const NActors::NStructuredLog::TBaseOtelLogSink::TOtelSettings& settings)
        : TBaseOtelLogWriter<TLogClass, NActors::NStructuredLog::TSimpleOtelLogSink>(settings)
    {
    }
};

template <typename TLogClass>
class TFullOtelLogWriter : public TBaseOtelLogWriter<TLogClass, NActors::NStructuredLog::TFullOtelLogSink> {
public:

    explicit TFullOtelLogWriter(const NActors::NStructuredLog::TBaseOtelLogSink::TOtelSettings& settings)
        : TBaseOtelLogWriter<TLogClass, NActors::NStructuredLog::TFullOtelLogSink>(
              std::move(settings), TString{TLogClass::GetSinkSourceName()})
    {
    }
};

} // namespace NKikimr::NKqp::NEventLog