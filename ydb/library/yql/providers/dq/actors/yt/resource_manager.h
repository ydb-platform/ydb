#pragma once

#include <util/system/file.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/providers/dq/config/config.pb.h>

#include <ydb/library/yql/providers/dq/task_runner/file_cache.h>
#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>

#include <library/cpp/threading/future/future.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/yt/yson_string/string.h>

namespace NYql {
    namespace NCommonJobVars {
        extern const TString ACTOR_PORT;
        extern const TString ACTOR_NODE_ID;
        extern const TString ADDRESS_RESOLVER_CONFIG;
        extern const TString UDFS_PATH;
        extern const TString OPERATION_SIZE;
        extern const TString YT_COORDINATOR;
        extern const TString YT_BACKEND;
        extern const TString YT_FORCE_IPV4;
    }

    class ICoordinationHelper;

    struct TResourceFile
    {
        TString ObjectId;
        TString LocalFileName;
        TMaybe<TString> RemoteFileName;
        THashMap<TString, TString> Attributes;
        TFile File;

        TResourceFile() { }
        TResourceFile(const TString& localFileName)
            : LocalFileName(localFileName)
            , File(LocalFileName, RdOnly | OpenExisting)
        { }

        TString GetRemoteFileName() const {
            if (RemoteFileName) {
                return *RemoteFileName;
            } else {
                auto pos = LocalFileName.rfind('/');
                return LocalFileName.substr(pos+1);
            }
        }
    };

    struct TResourceManagerOptions {
        NYql::NProto::TDqConfig::TYtBackend YtBackend;

        TString UploadPrefix;
        TString LockName;
        TString LogFile;

        TString TmpDir;
        IFileCache::TPtr FileCache;

        TVector<TResourceFile> Files;

        TMaybe<NThreading::TPromise<void>> Uploaded;

        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();

        bool ExitOnPingFail = false;

        int Capabilities = 0;
        int MaxRetries = -1;

        bool ForceIPv4 = false;

        std::optional<NYT::NYson::TYsonString> AddressResolverConfig;

        // Pinger
        TString DieOnFileAbsence; // see YQL-14099

        // Cleaner
        TMaybe<int> KeepFirst;
        TDuration DropBefore;
        TMaybe<TString> KeepFilter;

        TMaybe<TString> AnnounceClusterName;
    };

    NActors::IActor* CreateResourceManager(const TResourceManagerOptions& options, const TIntrusivePtr<ICoordinationHelper>& coordinator);

    NActors::IActor* CreateResourceUploader(const TResourceManagerOptions& options, const TIntrusivePtr<ICoordinationHelper>& coordinator);

    NActors::IActor* CreateResourceDownloader(const TResourceManagerOptions& options, const TIntrusivePtr<ICoordinationHelper>& coordinator);

    NActors::IActor* CreateResourceCleaner(const TResourceManagerOptions& options, const TIntrusivePtr<ICoordinationHelper>& coordinator);

} // namespace NYql
