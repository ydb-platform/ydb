#include "yql_udf_resolver_logger.h"

#include <yql/essentials/core/yql_user_data_storage.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/datetime/cputimer.h>

namespace {
using namespace NYql;

class TUdfResolverWithLoggerDecorator : public IUdfResolver {
public:
    TUdfResolverWithLoggerDecorator(IUdfResolver::TPtr underlying, const TString& path, const TString& sessionId)
        : Underlying_(underlying), Out_(TFile(path, WrOnly | ForAppend)), SessionId_(sessionId) {}

    TMaybe<TFilePathWithMd5> GetSystemModulePath(const TStringBuf& moduleName) const override {
        return Underlying_->GetSystemModulePath(moduleName);
    }

    bool LoadMetadata(
        const TVector<TImport*>& imports, const TVector<TFunction*>& functions,
        TExprContext& ctx, NUdf::ELogLevel logLevel, THoldingFileStorage& storage) const override
    {
        TSimpleTimer t;
        auto result = Underlying_->LoadMetadata(imports, functions, ctx, logLevel, storage);
        auto runningTime = t.Get().MilliSeconds();

        TStringBuilder sb;
        sb << SessionId_ << " LoadMetadata with imports (";
        for (auto& e: imports) {
            if (!e || !e->Block) {
                continue;
            }
            auto frozen = e->Block->Type != EUserDataType::URL ? e->Block->FrozenFile : storage.GetFrozenBlock(*e->Block);
            if (!frozen) {
                continue;
            }
            sb << " " << frozen->GetMd5() << ":" << frozen->GetSize();
        }
        sb << ") took " << runningTime << " ms\n";
        Out_ << TString(sb);
        return result;
    }

    TResolveResult LoadRichMetadata(const TVector<TImport>& imports, NUdf::ELogLevel logLevel, THoldingFileStorage& storage) const override {
        TSimpleTimer t;
        auto result = Underlying_->LoadRichMetadata(imports, logLevel, storage);
        auto runningTime = t.Get().MilliSeconds();

        TStringBuilder sb;
        sb << SessionId_ << " LoadRichMetadata with imports (";
        for (auto& e: imports) {
            if (!e.Block) {
                continue;
            }
            auto frozen = e.Block->Type != EUserDataType::URL ? e.Block->FrozenFile : storage.GetFrozenBlock(*e.Block);
            if (!frozen) {
                continue;
            }
            sb << " " << frozen->GetMd5() << ":" << frozen->GetSize();
        }
        sb << ") took " << runningTime << " ms\n";
        Out_ << TString(sb);
        return result;
    }

    bool ContainsModule(const TStringBuf& moduleName) const override {
        return Underlying_->ContainsModule(moduleName);
    }
private:
    IUdfResolver::TPtr Underlying_;
    mutable TUnbufferedFileOutput Out_;
    TString SessionId_;
};

}

namespace NYql::NCommon {
IUdfResolver::TPtr CreateUdfResolverDecoratorWithLogger(IUdfResolver::TPtr underlying, const TString& path, const TString& sessionId) {
    return new TUdfResolverWithLoggerDecorator(underlying, path, sessionId);
}
}
