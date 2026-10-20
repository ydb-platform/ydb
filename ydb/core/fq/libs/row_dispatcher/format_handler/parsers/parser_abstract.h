#pragma once

#include <ydb/core/fq/libs/row_dispatcher/events/topic_session_stats.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/common/common.h>

#include <ydb/library/yql/providers/abstract/message_stream/message_stream_session.h>

#include <yql/essentials/public/udf/udf_value.h>

#include <optional>
#include <span>

namespace NFq::NRowDispatcher {

class IParsedDataConsumer: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IParsedDataConsumer>;

public:
    virtual const TVector<TSchemaColumn>& GetColumns() const = 0;

    virtual void OnParsingError(TStatus status) = 0;
    virtual void OnParsedData(ui64 numberRows) = 0;
};

class ITopicParser: public TThrRefBase, public TNonCopyable {
public:
    using TPtr = TIntrusivePtr<ITopicParser>;

public:
    virtual void ParseRecords(const std::vector<TMessageStreamRecord>& records) = 0;
    virtual void Refresh(bool force = false) = 0;
    virtual TStatus ChangeConsumer(IParsedDataConsumer::TPtr consumer) = 0;

    virtual std::span<const ui64> GetOffsets() const = 0;
    // Progress of the input batch, including messages that produced no rows.
    // Available during OnParsedData(), like GetOffsets().
    virtual std::optional<ui64> GetLastMessageOffset() const {
        const auto offsets = GetOffsets();
        return offsets.empty() ? std::nullopt : std::optional<ui64>(offsets.back());
    }
    virtual TValueStatus<std::span<NYql::NUdf::TUnboxedValue>> GetParsedColumn(ui64 columnId) = 0;

    virtual void FillStatistics(TFormatHandlerStatistic& statistic) = 0;
};

}   // namespace NFq::NRowDispatcher
