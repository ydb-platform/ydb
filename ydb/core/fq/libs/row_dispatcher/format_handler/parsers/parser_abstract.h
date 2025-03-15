#pragma once

#include <ydb/core/fq/libs/row_dispatcher/events/topic_session_stats.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/common/common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_events.h>

#include <yql/essentials/public/udf/udf_value.h>

namespace NFq::NRowDispatcher {

class IParsedDataConsumer : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IParsedDataConsumer>;

public:
    virtual const TVector<TSchemaColumn>& GetColumns() const = 0;

    virtual void OnParsingError(TStatus status) = 0;
    virtual void OnParsedData(ui64 numberRows) = 0;
};

class ITopicParser : public TThrRefBase, public TNonCopyable {
public:
    using TPtr = TIntrusivePtr<ITopicParser>;

public:
    virtual void ParseMessages(const std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages) = 0;
    virtual void Refresh(bool force = false) = 0;

    virtual const TVector<ui64>& GetOffsets() const = 0;
    virtual TValueStatus<const TVector<NYql::NUdf::TUnboxedValue>*> GetParsedColumn(ui64 columnId) const = 0;

    virtual void FillStatistics(TFormatHandlerStatistic& statistic) = 0;
};

}  // namespace NFq::NRowDispatcher
