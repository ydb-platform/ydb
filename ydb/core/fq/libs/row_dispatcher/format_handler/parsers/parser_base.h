#pragma once

#include "parser_abstract.h"

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NFq::NRowDispatcher {

class TTypeParser {
public:
    explicit TTypeParser(const TSourceLocation& location, const TCountersDesc& counters);
    virtual ~TTypeParser();

    TValueStatus<NKikimr::NMiniKQL::TType*> ParseTypeYson(const TString& typeYson) const;

protected:
    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    NKikimr::NMiniKQL::IFunctionRegistry::TPtr FunctionRegistry;
    std::unique_ptr<NKikimr::NMiniKQL::TTypeEnvironment> TypeEnv;
    std::unique_ptr<NKikimr::NMiniKQL::TProgramBuilder> ProgramBuilder;
};

class TTopicParserBase : public ITopicParser, public TTypeParser {
private:
    struct TStats {
        void AddParserLatency(TDuration parserLatency);
        void AddParseAndFilterLatency(TDuration parseAndFilterLatency);
        void Clear();

        TDuration ParserLatency;
        TDuration ParseAndFilterLatency;
    };

public:
    using TPtr = TIntrusivePtr<TTopicParserBase>;

public:
    TTopicParserBase(IParsedDataConsumer::TPtr consumer, const TSourceLocation& location, const TCountersDesc& counters);
    virtual ~TTopicParserBase() = default;

public:
    virtual void Refresh(bool force = false) override;
    virtual void FillStatistics(TFormatHandlerStatistic& statistic) override;

protected:
    // Called with binded alloc
    virtual TStatus DoParsing() = 0;
    virtual void ClearBuffer() = 0;

protected:
    void ParseBuffer();

protected:
    const IParsedDataConsumer::TPtr Consumer;

private:
    TStats Stats;
};

NYql::NUdf::TUnboxedValue LockObject(NYql::NUdf::TUnboxedValue&& value);
void ClearObject(NYql::NUdf::TUnboxedValue& value);

TValueStatus<NYql::NUdf::EDataSlot> GetDataSlot(const NKikimr::NMiniKQL::TType* type);

TString TruncateString(std::string_view rawString, size_t maxSize = 1_KB);

}  // namespace NFq::NRowDispatcher
