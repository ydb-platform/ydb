#pragma once

#include <ydb/library/yql/yt/bridge/interface.h>

#include <library/cpp/logger/log.h>

#include <library/cpp/yt/string/guid.h>

#include <library/cpp/yt/yson_string/string.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

using TQueryId = TGuid;

////////////////////////////////////////////////////////////////////////////////

class TYqlPluginOptions
{
public:
    NYson::TYsonString SingletonsConfig;
    NYson::TYsonString GatewayConfig;
    NYson::TYsonString DqGatewayConfig;
    NYson::TYsonString DqManagerConfig;
    NYson::TYsonString FileStorageConfig;
    NYson::TYsonString OperationAttributes;

    TString YTTokenPath;

    THolder<TLogBackend> LogBackend;

    std::optional<TString> YqlPluginSharedLibrary;
};

struct TQueryResult
{
    std::optional<TString> YsonResult;
    std::optional<TString> Plan;
    std::optional<TString> Statistics;
    std::optional<TString> Progress;
    std::optional<TString> TaskInfo;

    //! YSON representation of a YT error.
    std::optional<TString> YsonError;
};

struct TClustersResult
{
    std::vector<TString> Clusters;

    //! YSON representation of a YT error.
    std::optional<TString> YsonError;
};

struct TQueryFile
{
    TStringBuf Name;
    TStringBuf Content;
    EQueryFileContentType Type;
};

struct TAbortResult
{
    //! YSON representation of a YT error.
    std::optional<TString> YsonError;
};

//! This interface encapsulates YT <-> YQL integration.
//! There are two major implementation: one of them is based
//! on YQL code and another wraps the pure C bridge interface, which
//! is implemented by a dynamic library.
/*!
*  \note Thread affinity: any
*/
struct IYqlPlugin
{
    virtual void Start() = 0;

    virtual TClustersResult GetUsedClusters(
        TString queryText,
        NYson::TYsonString settings,
        std::vector<TQueryFile> files) noexcept = 0;

    virtual TQueryResult Run(
        TQueryId queryId,
        TString user,
        TString token,
        TString queryText,
        NYson::TYsonString settings,
        std::vector<TQueryFile> files,
        int executeMode) noexcept = 0;

    virtual TQueryResult GetProgress(TQueryId queryId) noexcept = 0;

    virtual TAbortResult Abort(TQueryId queryId) noexcept = 0;

    virtual ~IYqlPlugin() = default;
};

////////////////////////////////////////////////////////////////////////////////

Y_WEAK std::unique_ptr<IYqlPlugin> CreateYqlPlugin(TYqlPluginOptions& options) noexcept;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
