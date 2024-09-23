#pragma once

#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_key.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/threading/future/core/future.h>

namespace NYql {
NNodes::TExprBase
BuildFolderListItemExpr(TExprContext &ctx, NYql::TPositionHandle pos,
                        const TString &path, const TString &type,
                        const TString &attributesYson);

NNodes::TCoList
BuildFolderListExpr(TExprContext& ctx, NYql::TPositionHandle pos,
                    const TVector<NNodes::TExprBase>& folderItems);

NNodes::TCoStructType
BuildFolderItemStructType(TExprContext& ctx, NYql::TPositionHandle pos);

class TWalkFoldersImpl {
public:
    TWalkFoldersImpl(const TString& sessionId, const TString& cluster, TYtSettings::TConstPtr config, 
                     TPosition pos, const TYtKey::TWalkFoldersArgs& args, const IYtGateway::TPtr gateway);

    IGraphTransformer::TStatus GetNextStateExpr(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args, TExprNode::TPtr& state);

    enum EProcessingState {
        WaitingListFolderOp,
        PreHandling,
        ResolveHandling,
        AfterResolveHandling,
        WaitingResolveLinkOp,
        DiveHandling,
        AfterDiveHandling,
        PostHandling,
        FinishingHandling,
        FinishedHandling
    };

    EProcessingState GetProcessingState() const {
        return ProcessingState_;
    }
    
    bool IsFinished() const {
        return ProcessingState_ == FinishedHandling;
    }

    NThreading::TFuture<void> GetAnyOpFuture() const {
        TVector<NThreading::TFuture<void>> futures;
        if (BatchFolderListFuture_ && BatchFolderListFuture_->Initialized()) {
            futures.push_back(BatchFolderListFuture_->IgnoreResult());
        }
        if (BatchResolveFuture_&& BatchResolveFuture_->Initialized()) {
            futures.push_back(BatchResolveFuture_->IgnoreResult());
        }
        return NThreading::WaitAny(futures);
    }

    TWalkFoldersImpl& operator=(const TWalkFoldersImpl&) = delete;
    TWalkFoldersImpl(const TWalkFoldersImpl&) = delete;

    TWalkFoldersImpl(TWalkFoldersImpl&&) = default;
    TWalkFoldersImpl& operator=(TWalkFoldersImpl&&) = default;

private:
    static constexpr size_t LARGE_USER_STATE = 8192;

    TPosition Pos_;
    TPositionHandle PosHandle_;

    TMaybe<TExprNode::TPtr> PreHandler_;
    TMaybe<TExprNode::TPtr> ResolveHandler_;
    TMaybe<TExprNode::TPtr> DiveHandler_;
    TMaybe<TExprNode::TPtr> PostHandler_;

    struct TFolderQueueItem {
        TYtKey::TFolderList Folder;

        bool PreHandleItemsFetched = false;
        
        TVector<IYtGateway::TBatchFolderResult::TFolderItem> ItemsToPreHandle;
        TVector<IYtGateway::TBatchFolderResult::TFolderItem> LinksToResolveHandle;
        TVector<IYtGateway::TBatchFolderResult::TFolderItem> ItemsToDiveHandle;
        TVector<IYtGateway::TBatchFolderResult::TFolderItem> ItemsToPostHandle;
        
        ui64 Level = 0;
    };
    TDeque<TFolderQueueItem> ProcessFoldersQueue_;

    EProcessingState ProcessingState_ = WaitingListFolderOp;
    
    TString SessionId_;
    TString Cluster_;
    TYtSettings::TConstPtr Config_;

    IYtGateway::TPtr Gateway_;

    TMaybe<NThreading::TFuture<IYtGateway::TBatchFolderResult>> BatchFolderListFuture_;
    TMaybe<NThreading::TFuture<IYtGateway::TBatchFolderResult>> BatchResolveFuture_;

    void DoFolderListOperation(TVector<IYtGateway::TBatchFolderOptions::TFolderPrefixAttrs>&& folders);

    IGraphTransformer::TStatus EvaluateNextUserStateExpr(TExprContext& ctx, const TExprNode::TPtr& userStateType, const TExprNode::TPtr userStateExpr, std::function<TExprNode::TPtr(const NNodes::TExprBase&)> nextStateFunc, TExprNode::TPtr& state);

    IGraphTransformer::TStatus AfterListFolderOp(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args, TExprNode::TPtr& state);

    IGraphTransformer::TStatus PreHandleVisitedInSingleFolder(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args, TFolderQueueItem& folder, TExprNode::TPtr& state);

    IGraphTransformer::TStatus ResolveHandleInSingleFolder(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args,  TFolderQueueItem& folder, TExprNode::TPtr& state);

    IGraphTransformer::TStatus BuildDiveOrResolveHandlerEval(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args, TExprNode::TPtr& handler,
                                                  const TVector<IYtGateway::TBatchFolderResult::TFolderItem>& res, const TVector<TString>& attributes, ui64 level, TExprNode::TPtr& state);

    IGraphTransformer::TStatus AfterResolveHandle(TExprContext& ctx, TYtKey::TWalkFoldersImplArgs args, TFolderQueueItem& folder, TExprNode::TPtr& state);

    IGraphTransformer::TStatus HandleAfterResolveFuture(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args, TFolderQueueItem& folder, TExprNode::TPtr& state);

    IGraphTransformer::TStatus DiveHandleInSingleFolder(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args,  TFolderQueueItem& folder, TExprNode::TPtr& state);

    IGraphTransformer::TStatus AfterDiveHandle(TExprContext& ctx, TYtKey::TWalkFoldersImplArgs args, TFolderQueueItem& folder, TExprNode::TPtr& state);

    IGraphTransformer::TStatus PostHandleVisitedInSingleFolder(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args, TFolderQueueItem& folder, TExprNode::TPtr& state);

    IGraphTransformer::TStatus BuildFinishedState(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args, TExprNode::TPtr& state);
};
} // namespace NYql
