#include "parser_base.h"

#include <util/generic/scope.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>

namespace NFq::NRowDispatcher {

//// TTypeParser

TTypeParser::TTypeParser(const TSourceLocation& location, const TCountersDesc& counters)
    : Alloc(location, NKikimr::TAlignedPagePoolCounters(counters.CountersRoot, counters.MkqlCountersName), true, false)
    , FunctionRegistry(NKikimr::NMiniKQL::CreateFunctionRegistry(&PrintBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, {}))
    , TypeEnv(std::make_unique<NKikimr::NMiniKQL::TTypeEnvironment>(Alloc))
    , ProgramBuilder(std::make_unique<NKikimr::NMiniKQL::TProgramBuilder>(*TypeEnv, *FunctionRegistry))
{}

TTypeParser::~TTypeParser() {
    with_lock (Alloc) {
        FunctionRegistry.Reset();
        TypeEnv.reset();
        ProgramBuilder.reset();
    }
}

TValueStatus<NKikimr::NMiniKQL::TType*> TTypeParser::ParseTypeYson(const TString& typeYson) const {
    TString parseTypeError;
    TStringOutput errorStream(parseTypeError);
    NKikimr::NMiniKQL::TType* typeMkql = nullptr;

    with_lock (Alloc) {
        typeMkql = NYql::NCommon::ParseTypeFromYson(TStringBuf(typeYson), *ProgramBuilder, errorStream); 
    }

    if (!typeMkql) {
        return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to parse type from yson: " << parseTypeError);
    }
    return typeMkql;
}

//// TTopicParserBase::TStats

void TTopicParserBase::TStats::AddParserLatency(TDuration parserLatency) {
    ParserLatency = std::max(ParserLatency, parserLatency);
}

void TTopicParserBase::TStats::AddParseAndFilterLatency(TDuration parseAndFilterLatency) {
    ParseAndFilterLatency = std::max(ParseAndFilterLatency, parseAndFilterLatency);
}

void TTopicParserBase::TStats::Clear() {
    ParserLatency = TDuration::Zero();
    ParseAndFilterLatency = TDuration::Zero();
}

//// TTopicParserBase

TTopicParserBase::TTopicParserBase(IParsedDataConsumer::TPtr consumer, const TSourceLocation& location, const TCountersDesc& counters)
    : TTypeParser(location, counters)
    , Consumer(std::move(consumer))
{}

void TTopicParserBase::Refresh(bool force) {
    Y_UNUSED(force);
}

void TTopicParserBase::FillStatistics(TFormatHandlerStatistic& statistic) {
    statistic.ParseAndFilterLatency = Stats.ParseAndFilterLatency;
    statistic.ParserStats.ParserLatency = Stats.ParserLatency;

    Stats.Clear();
}

void TTopicParserBase::ParseBuffer() {
    const TInstant startParseAndFilter = TInstant::Now();
    try {
        Y_DEFER {
            with_lock(Alloc) {
                ClearBuffer();
            }
        };

        TStatus status = TStatus::Success();
        const TInstant startParse = TInstant::Now();
        with_lock(Alloc) {
            status = DoParsing();
        }
        Stats.AddParserLatency(TInstant::Now() - startParse);

        if (status.IsSuccess()) {
            Consumer->OnParsedData(GetOffsets().size());
        } else {
            Consumer->OnParsingError(status);
        }
    } catch (...) {
        auto error = TStringBuilder() << "Failed to parse messages";
        if (const auto& offsets = GetOffsets()) {
            error << " from offset " << offsets.front();
        }
        error << ", got unexpected exception: " << CurrentExceptionMessage();
        Consumer->OnParsingError(TStatus::Fail(EStatusId::INTERNAL_ERROR, error));
    }
    Stats.AddParseAndFilterLatency(TInstant::Now() - startParseAndFilter);
}

//// Functions

NYql::NUdf::TUnboxedValue LockObject(NYql::NUdf::TUnboxedValue&& value) {
    // All UnboxedValue's with type Boxed or String should be locked
    // because after parsing they will be used under another MKQL allocator in purecalc filters

    const i32 numberRefs = value.LockRef();

    // -1 - value is embbeded or empty, otherwise value should have exactly one ref
    Y_ENSURE(numberRefs == -1 || numberRefs == 1);

    return value;
}

void ClearObject(NYql::NUdf::TUnboxedValue& value) {
    // Value should be unlocked with same number of refs
    value.UnlockRef(1);
    value.Clear();
}

TValueStatus<NYql::NUdf::EDataSlot> GetDataSlot(const NKikimr::NMiniKQL::TType* type) {
    Y_ENSURE(type->GetKind() == NKikimr::NMiniKQL::TTypeBase::EKind::Data, "Expected data type");

    const auto* dataType = AS_TYPE(NKikimr::NMiniKQL::TDataType, type);
    if (const auto dataSlot = dataType->GetDataSlot()) {
        return *dataSlot;
    }
    return TStatus::Fail(EStatusId::UNSUPPORTED, TStringBuilder() << "Unsupported data type with id: " << dataType->GetSchemeType());
}

TString TruncateString(std::string_view rawString, size_t maxSize) {
    if (rawString.size() <= maxSize) {
        return TString(rawString);
    }
    return TStringBuilder() << rawString.substr(0, maxSize) << " truncated (full length was " << rawString.size() << ")...";
}

}  // namespace NFq::NRowDispatcher
