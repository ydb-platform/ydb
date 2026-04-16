#include "discover.h"

#include <yql/essentials/utils/backtrace/backtrace.h>

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/utils/time_provider.h>
#include <yql/essentials/core/poly_args/yql_poly_args.h>

#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>
#include <yql/essentials/providers/common/proto/udf_resolver.pb.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/yson/node/node_io.h>

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
    TProgramBuilder pgmBuilder(env, functionRegistry);

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

    NYql::TFunctionResult* udfRes = nullptr;

    auto logProvider = NUdf::MakeLogProvider(
        [&](const NUdf::TStringRef& component, NUdf::ELogLevel level, const NUdf::TStringRef& message) {
            udfRes->AddMessages(TStringBuilder() << NYql::GetTimeProvider()->Now() << " " << component << " [" << level << "] " << message);
        },
        static_cast<NUdf::ELogLevel>(inMsg.GetRuntimeLogLevel()));

    for (const auto& module : functionRegistry.GetAllModuleNames()) {
        const auto& functions = functionRegistry.GetModuleFunctions(module);
        for (auto& f : functions) {
            const TString funcName = NKikimr::NMiniKQL::FullName(module, f.first);
            udfRes = outMsg.AddUdfs();
            udfRes->SetName(funcName);
            udfRes->SetIsTypeAwareness(f.second.IsTypeAwareness);

            TFunctionTypeInfo funcInfo;
            if (!f.second.IsTypeAwareness) {
                auto status = functionRegistry.FindFunctionTypeInfo(NYql::UnknownLangVersion, env, typeInfoHelper,
                                                                    nullptr, funcName, nullptr, nullptr, NUdf::IUdfModule::TFlags::TypesOnly, NUdf::TSourcePosition(), nullptr, logProvider.Get(), &funcInfo);

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
                udfRes->SetMinLangVer(funcInfo.MinLangVer);
                udfRes->SetMaxLangVer(funcInfo.MaxLangVer);
            }

            if (f.second.PolyArgs) {
                NYT::TNode polyArgsNode;
                std::unique_ptr<NYql::IPolyArgs> polyArgs;
                try {
                    polyArgsNode = NYT::NodeFromYsonString(*f.second.PolyArgs);
                    polyArgs = NYql::ParsePolyArgs(polyArgsNode);
                } catch (...) {
                    udfRes->SetError("Failed to parse PolyArgs, error: " + CurrentExceptionMessage());
                    continue;
                }
                udfRes->SetPolyArgs(NYT::NodeToCanonicalYsonString(polyArgsNode));
                NYT::TNode resolvedCallableTypes = NYT::TNode::CreateList();
                bool hasError = false;
                for (ui32 i = 0; i < polyArgs->GetPredicatesCount(); ++i) {
                    auto resolvedInput = polyArgs->GetUnresolvedInput(i);
                    if (!resolvedInput) {
                        resolvedCallableTypes.Add(NYT::TNode::CreateEntity());
                        continue;
                    }

                    TType* mkqlUserType = nullptr;
                    if (resolvedInput->UserTypeArgs) {
                        TVector<TType*> args(Reserve(resolvedInput->UserTypeArgs->size()));
                        for (const auto& arg : *resolvedInput->UserTypeArgs) {
                            TStringStream err;
                            auto type = NYql::NCommon::ParseTypeFromYson(arg, pgmBuilder, err);
                            if (!type) {
                                udfRes->SetError(err.Str());
                                hasError = true;
                                break;
                            }

                            args.push_back(type);
                        }

                        if (hasError) {
                            break;
                        }

                        std::array<TType*, 3> topElements;
                        topElements[0] = pgmBuilder.NewTupleType(args);
                        topElements[1] = pgmBuilder.NewEmptyStructType();
                        topElements[2] = pgmBuilder.NewEmptyTupleType();
                        mkqlUserType = pgmBuilder.NewTupleType(topElements);
                    }

                    auto status = functionRegistry.FindFunctionTypeInfo(resolvedInput->LangVer, env, typeInfoHelper,
                                                                        nullptr, funcName, mkqlUserType, nullptr, NUdf::IUdfModule::TFlags::TypesOnly, NUdf::TSourcePosition(), nullptr, logProvider.Get(), &funcInfo);

                    if (!status.IsOk()) {
                        hasError = true;
                        udfRes->SetError("Failed to resolve signature, error: " + status.GetError());
                        break;
                    }

                    resolvedCallableTypes.Add(NYql::NCommon::TypeToYsonNode(funcInfo.FunctionType));
                }

                if (hasError) {
                    continue;
                }

                udfRes->SetResolvedCallableTypes(NYT::NodeToCanonicalYsonString(resolvedCallableTypes));
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

void DiscoverInFiles(const TVector<TString>& udfPaths, IOutputStream& out, bool printAsProto,
                     NYql::NUdf::ELogLevel logLevel) {
    NYql::TResolve inMsg;
    inMsg.SetRuntimeLogLevel(static_cast<ui32>(logLevel));
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

} // namespace

void DiscoverInDir(const TString& dir, IOutputStream& out, bool printAsProto, NYql::NUdf::ELogLevel logLevel) {
    TVector<TString> udfPaths;
    NMiniKQL::FindUdfsInDir(dir, &udfPaths);
    DiscoverInFiles(udfPaths, out, printAsProto, logLevel);
}

void DiscoverInFile(const TString& filePath, IOutputStream& out, bool printAsProto, NYql::NUdf::ELogLevel logLevel) {
    DiscoverInFiles({filePath}, out, printAsProto, logLevel);
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
} // namespace NUdfResolver
