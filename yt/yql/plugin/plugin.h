#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <library/cpp/logger/log.h>

#include <library/cpp/yt/yson_string/string.h>

#include <optional>

namespace NYT::NYqlPlugin {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TYqlPluginOptions
{
public:
    TString MRJobBinary = "./mrjob";
    TString UdfDirectory;

    //! Mapping cluster name -> proxy address.
    THashMap<TString, TString> Clusters;
    std::optional<TString> DefaultCluster;

    TYsonString OperationAttributes;

    TString YTTokenPath;

    THolder<TLogBackend> LogBackend;

    std::optional<TString> YqlPluginSharedLibrary;
};

struct TQueryResult
{
    std::optional<TString> YsonResult;
    std::optional<TString> Plan;
    std::optional<TString> Statistics;
    std::optional<TString> TaskInfo;

    //! YSON representation of a YT error.
    std::optional<TString> YsonError;
};

//! This interface encapsulates YT <-> YQL integration.
//! There are two major implementation: one of them is based
//! on YQL code and another wraps the pure C bridge interface, which
//! is implemented by a dynamic library.
struct IYqlPlugin
{
    virtual TQueryResult Run(TString impersonationUser, TString queryText, TYsonString settings) noexcept = 0;

    virtual ~IYqlPlugin() = default;
};

////////////////////////////////////////////////////////////////////////////////

Y_WEAK std::unique_ptr<IYqlPlugin> CreateYqlPlugin(TYqlPluginOptions& options) noexcept;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
