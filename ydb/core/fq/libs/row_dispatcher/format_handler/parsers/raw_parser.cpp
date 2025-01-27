#include "raw_parser.h"

#include "parser_base.h"

#include <ydb/core/fq/libs/actors/logging/log.h>

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_ops.h>

namespace NFq::NRowDispatcher {

namespace {

class TRawParser : public TTopicParserBase {
public:
    using TBase = TTopicParserBase;
    using TPtr = TIntrusivePtr<TRawParser>;

public:
    TRawParser(IParsedDataConsumer::TPtr consumer, const TSchemaColumn& schema, const TCountersDesc& counters)
        : TBase(std::move(consumer), __LOCATION__, counters)
        , Schema(schema)
        , LogPrefix("TRawParser: ")
    {}

    TStatus InitColumnParser() {
        auto typeStatus = ParseTypeYson(Schema.TypeYson);
        if (typeStatus.IsFail()) {
            return typeStatus;
        }

        for (NKikimr::NMiniKQL::TType* type = typeStatus.DetachResult(); true; type = AS_TYPE(NKikimr::NMiniKQL::TOptionalType, type)->GetItemType()) {
            if (type->GetKind() == NKikimr::NMiniKQL::TTypeBase::EKind::Data) {
                auto slotStatus = GetDataSlot(type);
                if (slotStatus.IsFail()) {
                    return slotStatus;
                }
                DataSlot = slotStatus.DetachResult();
                return TStatus::Success();
            }

            if (type->GetKind() != NKikimr::NMiniKQL::TTypeBase::EKind::Optional) {
                return TStatus::Fail(EStatusId::UNSUPPORTED, TStringBuilder() << "Unsupported type kind for raw format: " << type->GetKindAsStr());
            }

            NumberOptionals++;
        }

        return TStatus::Success();
    }

public:
    void ParseMessages(const std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages) override {
        LOG_ROW_DISPATCHER_TRACE("Add " << messages.size() << " messages to parse");

        for (const auto& message : messages) {
            CurrentMessage = message.GetData();
            Offsets.emplace_back(message.GetOffset());
            ParseBuffer();
        }
    }

    const TVector<ui64>& GetOffsets() const override {
        return Offsets;
    }

    TValueStatus<const TVector<NYql::NUdf::TUnboxedValue>*> GetParsedColumn(ui64 columnId) const override {
        Y_ENSURE(columnId == 0, "Invalid column id for raw parser");
        return &ParsedColumn;
    }

protected:
    TStatus DoParsing() override {
        LOG_ROW_DISPATCHER_TRACE("Do parsing, first offset: " << Offsets.front() << ", value: " << CurrentMessage);

        NYql::NUdf::TUnboxedValue value = LockObject(NKikimr::NMiniKQL::ValueFromString(DataSlot, CurrentMessage));
        if (value) {
            for (size_t i = 0; i < NumberOptionals; ++i) {
                value = value.MakeOptional();
            }
        } else if (!NumberOptionals) {
            return TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse massege at offset " << Offsets.back() << ", can't parse data type " << NYql::NUdf::GetDataTypeInfo(DataSlot).Name << " from string: '" << TruncateString(CurrentMessage) << "'");
        }

        ParsedColumn.emplace_back(std::move(value));
        return TStatus::Success();
    }

    void ClearBuffer() override {
        for (auto& parsedValue : ParsedColumn) {
            ClearObject(parsedValue);
        }
        ParsedColumn.clear();
        Offsets.clear();
    }

private:
    const TSchemaColumn Schema;
    const TString LogPrefix;

    NYql::NUdf::EDataSlot DataSlot;
    ui64 NumberOptionals = 0;

    TString CurrentMessage;
    TVector<ui64> Offsets;
    TVector<NYql::NUdf::TUnboxedValue> ParsedColumn;
};

}  // anonymous namespace

TValueStatus<ITopicParser::TPtr> CreateRawParser(IParsedDataConsumer::TPtr consumer, const TCountersDesc& counters) {
    const auto& columns = consumer->GetColumns();
    if (columns.size() != 1) {
        return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Expected only one column for raw format, but got " << columns.size());
    }

    TRawParser::TPtr parser = MakeIntrusive<TRawParser>(consumer, columns[0], counters);
    if (auto status = parser->InitColumnParser(); status.IsFail()) {
        return status.AddParentIssue(TStringBuilder() << "Failed to create raw parser for column " << columns[0].ToString());
    }

    return ITopicParser::TPtr(parser);
}

}  // namespace NFq::NRowDispatcher
