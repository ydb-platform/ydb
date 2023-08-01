#pragma once

#include <ydb/library/yql/core/user_data/yql_user_data.h>
#include <ydb/library/yql/core/file_storage/defs/downloader.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <library/cpp/logger/priority.h>
#include <library/cpp/yson/public.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {
    class TFileStorageConfig;

    namespace NEmbedded {
        class IOperation {
        public:
            virtual ~IOperation() = default;
            virtual const TString& YsonResult() const = 0;
            virtual const TString& Plan() const = 0;
            virtual const TString& Statistics() const = 0;
            virtual const TString& TaskInfo() const = 0;
        };

        enum class EExecuteMode {
            Validate,
            Optimize,
            Run,
            Lineage
        };

        struct TOperationOptions {
            TMaybe<TString> Title; // title for YT transactions and operations, should contain 'YQL' somewhere
            ui16 SyntaxVersion = 1;
            TMaybe<TString> Attributes; // yson map with additional attributes
            TMaybe<TString> Parameters; // in yson format
            EExecuteMode Mode = EExecuteMode::Run;
        };

        // must be allocated only once per process
        class IOperationFactory {
        public:
            virtual ~IOperationFactory() = default;
            virtual THolder<IOperation> Run(const TString& queryText, const TOperationOptions& options) const = 0;
            virtual void Save(const TString& queryText, const TOperationOptions& options, const TString& destinationFolder) const = 0;
        };

        struct TYtClusterOptions {
            TString Name_;
            TString Cluster_;
        };

        struct TOperationFactoryOptions {
            TString MrJobBinary_; // assume static linking (including UDFs) if empty
            TString UdfResolverBinary_;
            TString UdfsDir_;
            bool PreloadUdfs_ = false; // used when UdfResolverBinary_ is specified, if UdfResolverBinary_ is empty it is considered equal to true
            TVector<NUserData::TUserData> UserData_;

            ELogPriority LogLevel_ = TLOG_ERR;
            NYT::ILogger::ELevel YtLogLevel_ = NYT::ILogger::ERROR;
            NYson::EYsonFormat ResultFormat_ = NYson::EYsonFormat::Pretty;

            TVector<TYtClusterOptions> YtClusters_;
            TString YtToken_;
            TString YtOwners_;
            TString StatToken_;
            bool LocalChainTest_ = false;
            TString LocalChainFile_;
            THashMap<TString, TString> CustomTokens_;
        };

        THolder<IOperationFactory> MakeOperationFactory(
            const TOperationFactoryOptions& options,
            const TString& configData, 
            std::function<NFS::IDownloaderPtr(const TFileStorageConfig&)> arcDownloaderFactory);
    }
}
