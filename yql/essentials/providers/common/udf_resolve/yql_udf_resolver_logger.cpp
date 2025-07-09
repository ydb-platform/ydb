#include "yql_udf_resolver_logger.h"

#include <yql/essentials/core/yql_user_data_storage.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/datetime/cputimer.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_value.h>

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

    void LogImport(NJson::TJsonArray& result, const TImport& import) const {
        auto currImport = NJson::TJsonMap();
        switch (import.Block->Type) {
        case NYql::EUserDataType::PATH:             currImport["type"] = "PATH"; break;
        case NYql::EUserDataType::URL:              currImport["type"] = "URL"; break;
        case NYql::EUserDataType::RAW_INLINE_DATA:  currImport["type"] = "RAW_INLINE_DATA"; break;
        };
        currImport["alias"] = import.FileAlias;
        auto modulesJson = NJson::TJsonArray();
        bool isTrusted = false;
        if (import.Modules) {
            TSet<TString> modules(import.Modules->begin(), import.Modules->end());
            for (auto& e: modules) {
                modulesJson.AppendValue(import.Block->CustomUdfPrefix + e);
                isTrusted |= FunctionRegistry_->IsLoadedUdfModule(e);
            }
        }
        currImport["modules"] = std::move(modulesJson);
        currImport["trusted"] = isTrusted;
        auto frozen = import.Block->FrozenFile;
        Y_ENSURE(frozen);
        currImport["md5"] = frozen->GetMd5();
        currImport["size"] = frozen->GetSize();
        result.AppendValue(std::move(currImport));
    }

    void LogFunction(NJson::TJsonArray& result, const TFunction& fn) const {
        auto currFn = NJson::TJsonMap();
        currFn["name"] = fn.Name;
        currFn["normalized_name"] = fn.NormalizedName;
        result.AppendValue(std::move(currFn));
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
        auto logEntry = NJson::TJsonMap();
        logEntry["timestamp"] = TInstant::Now().ToString();
        logEntry["query_id"] = SessionId_;
        logEntry["method"] = "LoadMetadata";
        logEntry["duration"] = runningTime;
        auto importsJson = NJson::TJsonArray();
        for (auto& e: imports) {
            if (!e || !e->Block) {
                continue;
            }
            LogImport(importsJson, *e);
        }
        auto fnsJson = NJson::TJsonArray();
        for (auto& e: functions) {
            if (!e) {
                continue;
            }
            LogFunction(fnsJson, *e);
        }
        logEntry["imports"] = std::move(importsJson);
        logEntry["functions"] = std::move(fnsJson);
        sb << NJson::WriteJson(logEntry, false) << "\n";
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
        auto logEntry = NJson::TJsonMap();
        logEntry["timestamp"] = TInstant::Now().ToString();
        logEntry["query_id"] = SessionId_;
        logEntry["method"] = "LoadRichMetadata";
        logEntry["duration"] = runningTime;
        auto importsJson = NJson::TJsonArray();
        for (auto& e: imports) {
            if (!e.Block) {
                continue;
            }
            LogImport(importsJson, e);
        }
        logEntry["imports"] = std::move(importsJson);
        sb << NJson::WriteJson(logEntry, false) << "\n";
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
