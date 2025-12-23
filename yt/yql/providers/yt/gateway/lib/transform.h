#pragma once

#include "exec_ctx.h"

#include "downloader.h"
#include "user_files.h"

#include <yt/yql/providers/yt/common/yql_yt_settings.h>
#include <yt/yql/providers/yt/job/yql_job_base.h>

#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/yql_udf_resolver.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <yt/cpp/mapreduce/interface/operation.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NYql {

struct TLocalFileInfo {
    TString Hash;
    bool BypassArtifactCache = false;
};

// Helper getter struct for TGatewayTransfomer fields to use in FMR gateway.
struct TTransformerFiles {
    TVector<NYT::TRichYPath> RemoteFiles;
    TVector<std::pair<TString, TLocalFileInfo>> LocalFiles;
    TVector<std::pair<TString, TLocalFileInfo>> DeferredUdfFiles;
    THashMap<TString, TString> JobUdfs;
};

template<typename TExecContextPtr>
class TGatewayTransformer {
public:
    TGatewayTransformer(TExecContextPtr execCtx, TYtSettings::TConstPtr settings, const TString& optLLVM,
        TUdfModulesTable udfModules, IUdfResolver::TPtr udfResolver, ITableDownloaderFunc downloader,
        NKikimr::NMiniKQL::TProgramBuilder& builder, TMaybe<ui32> publicId);

    TGatewayTransformer(TExecContextPtr execCtx, ITableDownloaderFunc downloader,
        NKikimr::NMiniKQL::TProgramBuilder& builder)
        : TGatewayTransformer(execCtx, execCtx->Options_.Config(), execCtx->Options_.OptLLVM(),
            execCtx->Options_.UdfModules(), execCtx->Options_.UdfResolver(), downloader,
            builder, execCtx->Options_.PublicId())
    {
    }

    virtual ~TGatewayTransformer() = default;

    enum class EPhase {
        Content,
        Other,
        All
    };

    NKikimr::NMiniKQL::TCallableVisitFunc operator()(NKikimr::NMiniKQL::TInternName name);

    void SetTwoPhaseTransform() {
        Phase_ = EPhase::Other;
    }

    bool HasSecondPhase();

    inline bool CanExecuteInternally() const {
        return !*RemoteExecutionFlag_ && !*UntrustedUdfFlag_;
    }

    inline bool CanExecuteLocally() const {
        return !*RemoteExecutionFlag_;
    }

    inline ui64 GetUsedMemory() const {
        return *UsedMem_;
    }

    void ApplyJobProps(TYqlJobBase& job);
    TTransformerFiles GetTransformerFiles();

protected:
    void AddFile(TString alias, TUserFiles::TFileInfo fileInfo, const TString& udfPrefix = {});
    TString FindUdfPath(const TStringBuf moduleName) const;
    TString FindUdfPrefix(const TStringBuf moduleName) const;

    virtual void CalculateRemoteMemoryUsage(double remoteMemoryFactor, const NYT::TRichYPath& remoteFilePath);

    virtual void HandleQContextCapture(TUserFiles::TFileInfo& fileInfo, const TString& alias);
    virtual void ProcessLocalFileForDump(const TString& alias, const TString& localPath);
    virtual void ProcessRemoteFileForDump(const TString& alias, const NYT::TRichYPath& remoteFile);

protected:
    TExecContextPtr ExecCtx_;
    TYtSettings::TConstPtr Settings_;
    TUdfModulesTable UdfModules_;
    IUdfResolver::TPtr UdfResolver_;

    ITableDownloaderFunc Downloader_;
    NKikimr::NMiniKQL::TProgramBuilder& PgmBuilder_;
    TMaybe<ui32> PublicId_;
    EPhase Phase_ = EPhase::All;
    bool ForceLocalTableContent_;

    // Wrap to shared ptr because TGatewayTransformer is passed by value
    std::shared_ptr<bool> TableContentFlag_;
    std::shared_ptr<bool> RemoteExecutionFlag_;
    std::shared_ptr<bool> UntrustedUdfFlag_;
    std::shared_ptr<ui64> UsedMem_;
    std::shared_ptr<THashMap<TString, TString>> JobFileAliases_;
    std::shared_ptr<THashMap<TString, TString>> JobUdfs_;
    std::shared_ptr<THashMap<TString, TString>> UniqFiles_;
    std::shared_ptr<TVector<NYT::TRichYPath>> RemoteFiles_;
    std::shared_ptr<TVector<std::pair<TString, TLocalFileInfo>>> LocalFiles_;
    std::shared_ptr<TVector<std::pair<TString, TLocalFileInfo>>> DeferredUdfFiles_;
};

} // NYql

#include "transform-inl.h"
