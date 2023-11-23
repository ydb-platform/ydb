#include "discover.h"

#include <ydb/library/yql/utils/backtrace/backtrace.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_utils.h>

#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/providers/common/proto/udf_resolver.pb.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace NUdfResolver {
namespace {
NYql::TResolveResult DoDiscover(const NYql::TResolve& inMsg, IMutableFunctionRegistry& functionRegistry) {
    NYql::TResolveResult outMsg;

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);

    NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper());
    THashMap<std::pair<TString, TString>, THashSet<TString>> cachedModules;
    for (auto& import : inMsg.GetImports()) {
        auto importRes = outMsg.AddImports();
        importRes->SetFileAlias(import.GetFileAlias());
        importRes->SetCustomUdfPrefix(import.GetCustomUdfPrefix());

        auto [it, inserted] = cachedModules.emplace(std::make_pair(import.GetPath(), import.GetCustomUdfPrefix()), THashSet<TString>());
        if (inserted) {
            THashSet<TString> modules;
            functionRegistry.LoadUdfs(import.GetPath(),
                                {},
                                NUdf::IRegistrator::TFlags::TypesOnly,
                                import.GetCustomUdfPrefix(),
                                &modules);
            FillImportResultModules(modules, *importRes);
            it->second = modules;
        } else {
            FillImportResultModules(it->second, *importRes);
        }
    }

    for (const auto& module : functionRegistry.GetAllModuleNames()) {
        const auto& functions = functionRegistry.GetModuleFunctions(module);
        for (auto& f : functions) {
            const TString funcName = NKikimr::NMiniKQL::FullName(module, f.first);
            auto udfRes = outMsg.AddUdfs();
            udfRes->SetName(funcName);
            udfRes->SetIsTypeAwareness(f.second.IsTypeAwareness);

            TFunctionTypeInfo funcInfo;
            if (!f.second.IsTypeAwareness) {
                auto status = functionRegistry.FindFunctionTypeInfo(env, typeInfoHelper,
                    nullptr, funcName, nullptr, nullptr, NUdf::IUdfModule::TFlags::TypesOnly, {}, nullptr, &funcInfo);

                if (!status.IsOk()) {
                    udfRes->SetError("Failed to resolve signature, error: " + status.GetError());
                }
            }

            // nullptr for polymorphic functions
            if (funcInfo.FunctionType) {
                udfRes->SetCallableType(NYql::NCommon::WriteTypeToYson(funcInfo.FunctionType, NYT::NYson::EYsonFormat::Text));
                udfRes->SetArgCount(funcInfo.FunctionType->GetArgumentsCount());
                udfRes->SetOptionalArgCount(funcInfo.FunctionType->GetOptionalArgumentsCount());

                if (funcInfo.RunConfigType) {
                    udfRes->SetRunConfigType(NYql::NCommon::WriteTypeToYson(funcInfo.RunConfigType, NYT::NYson::EYsonFormat::Text));
                }

                udfRes->SetSupportsBlocks(funcInfo.SupportsBlocks);
                udfRes->SetIsStrict(funcInfo.IsStrict);
            }
        }
    }

    return outMsg;
}

void Print(const NYql::TResolveResult& result, IOutputStream& out, bool printAsProto) {
    if (printAsProto) {
        result.SerializeToArcadiaStream(&out);
        return;
    }

    SerializeToTextFormat(result, out);
    out << "UDF count: " << result.UdfsSize() << Endl;
}

void DiscoverInFiles(const TVector<TString>& udfPaths, IOutputStream& out, bool printAsProto) {
    NYql::TResolve inMsg;
    for (auto& path : udfPaths) {
        auto import = inMsg.AddImports();
        import->SetPath(path);
        import->SetFileAlias(path);
    }

    auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
    auto newRegistry = functionRegistry->Clone();
    newRegistry->SetBackTraceCallback(&NYql::NBacktrace::KikimrBackTrace);

    NYql::TResolveResult result = DoDiscover(inMsg, *newRegistry);
    Print(result, out, printAsProto);
}

}

void DiscoverInDir(const TString& dir, IOutputStream& out, bool printAsProto) {
    TVector<TString> udfPaths;
    NMiniKQL::FindUdfsInDir(dir, &udfPaths);
    DiscoverInFiles(udfPaths, out, printAsProto);
}

void DiscoverInFile(const TString& filePath, IOutputStream& out, bool printAsProto) {
    DiscoverInFiles({ filePath }, out, printAsProto);
}

void Discover(IInputStream& in, IOutputStream& out, bool printAsProto) {
    NYql::TResolve inMsg;
    if (!inMsg.ParseFromArcadiaStream(&in)) {
        ythrow yexception() << "Bad input TResolve proto message";
    }

    auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
    auto newRegistry = functionRegistry->Clone();
    newRegistry->SetBackTraceCallback(&NYql::NBacktrace::KikimrBackTrace);

    NYql::TResolveResult result = DoDiscover(inMsg, *newRegistry);
    Print(result, out, printAsProto);
}

void FillImportResultModules(const THashSet<TString>& modules, NYql::TImportResult& importRes) {
    for (auto& m : modules) {
        importRes.AddModules(m);
    }
}
}
