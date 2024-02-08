#pragma once

#include "yql_yt_exec_ctx.h"

#include <ydb/library/yql/providers/yt/gateway/lib/temp_files.h>
#include <ydb/library/yql/providers/yt/gateway/lib/transaction_cache.h>
#include <ydb/library/yql/providers/yt/gateway/lib/user_files.h>

#include <ydb/library/yql/providers/yt/common/yql_yt_settings.h>
#include <ydb/library/yql/providers/yt/job/yql_job_base.h>

#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/yql_udf_resolver.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <yt/cpp/mapreduce/interface/operation.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NYql {

namespace NNative {

class TGatewayTransformer {
public:
    TGatewayTransformer(const TExecContextBase& execCtx, TYtSettings::TConstPtr settings, const TString& optLLVM,
        TUdfModulesTable udfModules, IUdfResolver::TPtr udfResolver, TTransactionCache::TEntry::TPtr entry,
        NKikimr::NMiniKQL::TProgramBuilder& builder, TTempFiles& tmpFiles, TMaybe<ui32> publicId);

    template <class TExecContextPtr>
    TGatewayTransformer(const TExecContextPtr& execCtx, TTransactionCache::TEntry::TPtr entry,
        NKikimr::NMiniKQL::TProgramBuilder& builder, TTempFiles& tmpFiles)
        : TGatewayTransformer(*execCtx, execCtx->Options_.Config(), execCtx->Options_.OptLLVM(),
            execCtx->Options_.UdfModules(), execCtx->Options_.UdfResolver(), std::move(entry),
            builder, tmpFiles, execCtx->Options_.PublicId())
    {
    }

    enum class EPhase {
        Content,
        Other,
        All
    };

    struct TLocalFileInfo {
        TString Hash;
        bool BypassArtifactCache;
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
    void ApplyUserJobSpec(NYT::TUserJobSpec& spec, bool localRun);

private:
    NYT::ITransactionPtr GetTx();
    TTransactionCache::TEntry::TPtr GetEntry();
    void AddFile(TString alias, const TUserFiles::TFileInfo& fileInfo, const TString& udfPrefix = {});
    TString FindUdfPath(const TStringBuf moduleName) const;
    TString FindUdfPrefix(const TStringBuf moduleName) const;

private:
    const TExecContextBase& ExecCtx_;
    TYtSettings::TConstPtr Settings_;
    TUdfModulesTable UdfModules_;
    IUdfResolver::TPtr UdfResolver_;

    TTransactionCache::TEntry::TPtr Entry_;
    NKikimr::NMiniKQL::TProgramBuilder& PgmBuilder_;
    TTempFiles& TmpFiles_;
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

} // NNative

} // NYql
