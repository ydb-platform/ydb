#include "yql_udf_resolver_logger.h"

#include <yql/essentials/core/yql_user_data_storage.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/datetime/cputimer.h>

namespace {
using namespace NYql;

class TUdfResolverWithLoggerDecorator : public IUdfResolver {
public:
    TUdfResolverWithLoggerDecorator(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
                        IUdfResolver::TPtr underlying, const TString& path, const TString& sessionId)
        : FunctionRegistry_(functionRegistry)
        , Underlying_(underlying)
        , Out_(TFile(path, WrOnly | ForAppend | OpenAlways))
        , SessionId_(sessionId) {}

    TMaybe<TFilePathWithMd5> GetSystemModulePath(const TStringBuf& moduleName) const override {
        return Underlying_->GetSystemModulePath(moduleName);
    }

    void LogImport(TStringBuilder& sb, const TImport& import) const {
        sb << " ";
        switch (import.Block->Type) {
        case NYql::EUserDataType::PATH:             sb << "PATH"; break;
        case NYql::EUserDataType::URL:              sb << "URL"; break;
        case NYql::EUserDataType::RAW_INLINE_DATA:  sb << "RAW_INLINE_DATA"; break;
        };
        sb << ":" << import.FileAlias << ":";
        bool isTrusted = false;
        if (import.Modules) {
            bool was = false;
            for (auto& e: *import.Modules) {
                sb << (was ? "," : "") << e;
                isTrusted |= FunctionRegistry_->IsLoadedUdfModule(e);
                was = true;
            }
        }
        sb << ":" << isTrusted << ":";
        auto frozen = import.Block->FrozenFile;
        Y_ENSURE(frozen);
        sb << frozen->GetMd5() << ":" << frozen->GetSize();
    }

    bool LoadMetadata(
        const TVector<TImport*>& imports, const TVector<TFunction*>& functions,
        TExprContext& ctx, NUdf::ELogLevel logLevel, THoldingFileStorage& storage) const override
    {
        TSimpleTimer t;
        auto result = Underlying_->LoadMetadata(imports, functions, ctx, logLevel, storage);
        auto runningTime = t.Get().MilliSeconds();
        if (imports.empty()) {
            return result;
        }

        TStringBuilder sb;
        sb << TInstant::Now() << " " << SessionId_ << " LoadMetadata with imports (";
        for (auto& e: imports) {
            if (!e || !e->Block) {
                continue;
            }
            LogImport(sb, *e);
        }
        sb << ") took " << runningTime << " ms\n";
        Out_ << TString(sb);
        return result;
    }

    TResolveResult LoadRichMetadata(const TVector<TImport>& imports, NUdf::ELogLevel logLevel, THoldingFileStorage& storage) const override {
        TSimpleTimer t;
        auto result = Underlying_->LoadRichMetadata(imports, logLevel, storage);
        auto runningTime = t.Get().MilliSeconds();
        if (imports.empty()) {
            return result;
        }

        TStringBuilder sb;
        sb << TInstant::Now() << " " << SessionId_ << " LoadRichMetadata with imports (";
        for (auto& e: imports) {
            if (!e.Block) {
                continue;
            }
            LogImport(sb, e);
        }
        sb << ") took " << runningTime << " ms\n";
        Out_ << TString(sb);
        return result;
    }

    bool ContainsModule(const TStringBuf& moduleName) const override {
        return Underlying_->ContainsModule(moduleName);
    }
private:
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry_;
    IUdfResolver::TPtr Underlying_;
    mutable TUnbufferedFileOutput Out_;
    TString SessionId_;
};

}

namespace NYql::NCommon {
IUdfResolver::TPtr CreateUdfResolverDecoratorWithLogger(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, IUdfResolver::TPtr underlying, const TString& path, const TString& sessionId) {
    return new TUdfResolverWithLoggerDecorator(functionRegistry, underlying, path, sessionId);
}
}
