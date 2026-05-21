#include "parser_base.h"

#include <util/generic/scope.h>

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>

namespace NFq::NRowDispatcher {

//// TTypeParser

TTypeParser::TTypeParser(const TSourceLocation& location, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, const TCountersDesc& counters)
    : Alloc(location, NKikimr::TAlignedPagePoolCounters(counters.CountersRoot, counters.MkqlCountersName), true, false)
    , FunctionRegistry(functionRegistry)
    , TypeEnv(std::make_unique<NKikimr::NMiniKQL::TTypeEnvironment>(Alloc))
    , ProgramBuilder(std::make_unique<NKikimr::NMiniKQL::TProgramBuilder>(*TypeEnv, *FunctionRegistry))
    , MemInfo("SharedReadingParser")
    , HolderFactory(std::make_unique<NKikimr::NMiniKQL::THolderFactory>(Alloc.Ref(), MemInfo, functionRegistry))
{}

TTypeParser::~TTypeParser() {
    with_lock (Alloc) {
        HolderFactory.reset();
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

TTopicParserBase::TTopicParserBase(IParsedDataConsumer::TPtr consumer, const TSourceLocation& location, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, const TCountersDesc& counters)
    : TTypeParser(location, functionRegistry, counters)
    , Consumer(std::move(consumer))
{}

void TTopicParserBase::Refresh(bool force) {
    Y_UNUSED(force);
}

TStatus TTopicParserBase::ChangeConsumer(IParsedDataConsumer::TPtr consumer) {
    Consumer = std::move(consumer);
    return TStatus::Success();
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
    // Object must be one of:
    // 1) null (refs = -1)
    // 2) embedded string/POD (refs = -1)
    // 3) large string (refs = 1)
    // 4) (struct | tuple | list) as boxed -> direct array holder (refs = 1, except for special case: zero-length direct array holder points to special shared zero-length container)
    Y_ABORT_UNLESS(value.RefCount() == -1 || value.RefCount() == 1 || (value.IsBoxed() && value.GetListLength() == 0));
    return std::move(value);
}

void ClearObject(NYql::NUdf::TUnboxedValue& value) {
    // Every other reference to value must be cleared (except for special case - zero-length list)
    Y_ABORT_UNLESS(value.RefCount() == -1 || value.RefCount() == 1 || (value.IsBoxed() && value.GetListLength() == 0));
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
