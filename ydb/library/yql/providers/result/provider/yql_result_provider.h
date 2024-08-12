#pragma once

#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/yql_execution.h>

namespace Nkikimr::NMiniKQL {
    class IFunctionRegistry;
}

namespace NYql {

class IResultWriter : public TThrRefBase {
public:
    virtual void Init(bool discard, const TString& label, TMaybe<TPosition> pos, bool unordered) = 0;
    virtual void Write(const TStringBuf& resultData) = 0;
    virtual void Commit(bool overflow) = 0;
    virtual bool IsDiscard() const = 0;
    virtual TStringBuf Str() = 0;
    virtual ui64 Size() = 0;
};

TIntrusivePtr<IResultWriter> CreateYsonResultWriter(NYson::EYsonFormat format);

struct TResultProviderConfig : TThrRefBase {
    using TResultWriterFactory = std::function<TIntrusivePtr<IResultWriter>()>;

    TResultProviderConfig(TTypeAnnotationContext& types,
        const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
        IDataProvider::EResultFormat format, const TString& formatDetails, TResultWriterFactory writerFactory)
        : Types(types)
        , FunctionRegistry(functionRegistry)
        , WriterFactory(writerFactory)
    {
        FillSettings.Format = format;
        FillSettings.FormatDetails = formatDetails;
    }

    TTypeAnnotationContext& Types;
    const NKikimr::NMiniKQL::IFunctionRegistry& FunctionRegistry;
    IDataProvider::TFillSettings FillSettings;
    TResultWriterFactory WriterFactory;
    TVector<TString> CommittedResults;
    bool SupportsResultPosition = false;
};

TIntrusivePtr<IDataProvider> CreateResultProvider(const TIntrusivePtr<TResultProviderConfig>& config);
const THashSet<TStringBuf>& ResultProviderFunctions();

}
