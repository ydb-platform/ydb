#pragma once

#include <ydb/core/fq/libs/row_dispatcher/format_handler/filters/filters_set.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/parsers/json_parser.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/util/rope.h>

namespace NFq::NRowDispatcher {

static constexpr ui64 MAX_BATCH_SIZE = 10_MB;

class IClientDataConsumer : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IClientDataConsumer>;

public:
    virtual const TVector<TSchemaColumn>& GetColumns() const = 0;
    virtual const TString& GetWhereFilter() const = 0;
    virtual TPurecalcCompileSettings GetPurecalcSettings() const = 0;
    virtual NActors::TActorId GetClientId() const = 0;
    virtual std::optional<ui64> GetNextMessageOffset() const = 0;

    virtual void OnClientError(TStatus status) = 0;

    virtual void StartClientSession() = 0;
    virtual void AddDataToClient(ui64 offset, ui64 rowSize) = 0;
    virtual void UpdateClientOffset(ui64 offset) = 0;
};

class ITopicFormatHandler : public TNonCopyable {
private:
    class TDestroy {
    public:
        static void Destroy(ITopicFormatHandler* handler);
    };

public:
    using TPtr = THolder<ITopicFormatHandler, TDestroy>;

    struct TSettings {
        TString ParsingFormat = "raw";

        std::strong_ordering operator<=>(const TSettings& other) const = default;
    };

public:
    virtual void ParseMessages(const std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages) = 0;

    // vector of (messages batch, [offsets])
    virtual TQueue<std::pair<TRope, TVector<ui64>>> ExtractClientData(NActors::TActorId clientId) = 0;

    virtual TStatus AddClient(IClientDataConsumer::TPtr client) = 0;
    virtual void RemoveClient(NActors::TActorId clientId) = 0;
    virtual bool HasClients() const = 0;

    virtual TFormatHandlerStatistic GetStatistics() = 0;
    virtual void ForceRefresh() = 0;

protected:
    virtual NActors::TActorId GetSelfId() const = 0;
};

// Static properties for all format handlers
struct TFormatHandlerConfig {
    TJsonParserConfig JsonParserConfig;
    TTopicFiltersConfig FiltersConfig;
};

ITopicFormatHandler::TPtr CreateTopicFormatHandler(const NActors::TActorContext& owner, const TFormatHandlerConfig& config, const ITopicFormatHandler::TSettings& settings, const TCountersDesc& counters);
TFormatHandlerConfig CreateFormatHandlerConfig(const NConfig::TRowDispatcherConfig& rowDispatcherConfig, NActors::TActorId compileServiceId);

namespace NTests {

ITopicFormatHandler::TPtr CreateTestFormatHandler(const TFormatHandlerConfig& config, const ITopicFormatHandler::TSettings& settings);

}  // namespace NTests

}  // namespace NFq::NRowDispatcher
