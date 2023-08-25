#pragma once

#include "server.h"
#include "assets_servlet.h"
#include "yql_servlet.h"
#include "yql_functions_servlet.h"

#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/core/yql_csv.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <util/stream/file.h>
#include <util/system/user.h>
#include <util/system/tempfile.h>


namespace NYql {
namespace NHttp {

enum class EDataSource {
    FILE,
    YAMR,
    YT
};

///////////////////////////////////////////////////////////////////////////////
// TYqlServer
///////////////////////////////////////////////////////////////////////////////
class TYqlServer: private TNonCopyable
{
public:
    inline TYqlServer(
            const TServerConfig& config,
            const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
            TUdfIndex::TPtr udfIndex,
            ui64 nextUniqueId,
            TUserDataTable filesMapping,
            THolder<TGatewaysConfig>&& gatewaysConfig,
            IModuleResolver::TPtr modules,
            IUdfResolver::TPtr udfResolver,
            TFileStoragePtr fileStorage)
        : HttpServer(config)
        , FunctionRegistry(functionRegistry)
        , UdfIndex(udfIndex)
        , NextUniqueId(nextUniqueId)
        , FilesMapping(std::move(filesMapping))
        , GatewaysConfig(std::move(gatewaysConfig))
        , Modules(modules)
        , UdfResolver(udfResolver)
        , FileStorage(fileStorage)
    {
    }

    template <typename TAction>
    void RegisterAction(const TString& path) {
        RegisterServlet(path, new TYqlServlet<TAction>(*this));
    }

    void RegisterServlet(const TString& path, TAutoPtr<IServlet> sp) {
        HttpServer.RegisterServlet(path, sp);
    }

    void ShutdownOn(int signal);
    void Start();
    void Wait();

public:
    TServer HttpServer;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    TUdfIndex::TPtr UdfIndex;
    ui64 NextUniqueId;
    TUserDataTable FilesMapping;
    const THolder<TGatewaysConfig> GatewaysConfig;
    IModuleResolver::TPtr Modules;
    IUdfResolver::TPtr UdfResolver;
    TFileStoragePtr FileStorage;
};

TAutoPtr<TYqlServer> CreateYqlServer(
        TServerConfig config,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TUdfIndex::TPtr udfIndex,
        ui64 nextUniqueId,
        TUserDataTable filesMapping,
        THolder<TGatewaysConfig>&& gatewaysConfig,
        IModuleResolver::TPtr modules = nullptr,
        IUdfResolver::TPtr udfResolver = nullptr,
        TFileStoragePtr fileStorage = nullptr);

} // namspace NHttp
} // namspace NYql
