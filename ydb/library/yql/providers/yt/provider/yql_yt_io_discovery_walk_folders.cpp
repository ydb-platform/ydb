#include "yql_yt_io_discovery_walk_folders.h"

#include <ydb/library/yql/providers/yt/gateway/native/yql_yt_native_folders.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/string/split.h>

namespace NYql {
using namespace NNodes;


NNodes::TCoStructType
BuildFolderItemStructType(TExprContext& ctx, NYql::TPositionHandle pos) {
    return Build<TCoStructType>(ctx, pos)
        .Add<TExprList>()
            .Add<TCoAtom>()
                .Value("Path")
            .Build()
            .Add<TCoDataType>()
                .Type()
                    .Value("String")
                .Build()
            .Build()
        .Build()
        .Add<TExprList>()
            .Add<TCoAtom>()
                .Value("Type")
            .Build()
            .Add<TCoDataType>()
                .Type()
                    .Value("String")
                .Build()
            .Build()
        .Build()
        .Add<TExprList>()
            .Add<TCoAtom>()
                .Value("Attributes")
            .Build()
            .Add<TCoDataType>()
                .Type()
                    .Value("Yson")
                .Build()
            .Build()
        .Build()
        .Done();
}

TCoList 
BuildFolderListExpr(TExprContext& ctx, NYql::TPositionHandle pos, const TVector<NNodes::TExprBase>& folderItems) {
    return Build<TCoList>(ctx, pos)
        .ListType<TCoListType>()
            .ItemType<TCoStructType>()
                .InitFrom(BuildFolderItemStructType(ctx, pos))
            .Build()
        .Build()
        .FreeArgs()
            .Add(folderItems)
        .Build()
    .Build()
    .Value();
}

TExprBase
BuildFolderListItemExpr(TExprContext &ctx, NYql::TPositionHandle pos,
                        const TString &path, const TString &type,
                        const TString &attributesYson) {
    return Build<TCoAsStruct>(ctx, pos)
        .Add()
            .Add<TCoAtom>()
                .Value("Path")
            .Build()
            .Add<TCoString>()
                .Literal()
                    .Value(path)
                .Build()
            .Build()
        .Build()
        .Add()
            .Add<TCoAtom>()
                .Value("Type")
            .Build()
            .Add<TCoString>()
                .Literal()
                    .Value(type)
                .Build()
            .Build()
        .Build()
        .Add()
            .Add<TCoAtom>()
                .Value("Attributes")
            .Build()
            .Add<TCoYson>()
                .Literal()
                    .Value(attributesYson)
                .Build()
            .Build()
        .Build()
        .Done();
}

TWalkFoldersImpl::TWalkFoldersImpl(const TString& sessionId, const TString& cluster, TYtSettings::TConstPtr config, 
    TPosition pos, TYtKey::TWalkFoldersArgs&& args, const IYtGateway::TPtr gateway):
    Pos_(pos), SessionId_(sessionId), Cluster_(cluster), Config_(config), Gateway_(gateway) {
    
    PreHandler_ = args.PreHandler->IsCallable("Void") ? Nothing() : MakeMaybe(args.PreHandler);
    ResolveHandler_ = args.ResolveHandler;
    DiveHandler_ = args.DiveHandler;
    PostHandler_ = args.PostHandler->IsCallable("Void") ? Nothing() : MakeMaybe(args.PostHandler);
    
    ProcessFoldersQueue_.emplace_back(TFolderQueueItem {
        .Folder = args.InitialFolder,
    });
    IYtGateway::TBatchFolderOptions::TFolderPrefixAttrs folder {
        std::move(args.InitialFolder.Prefix), 
        TSet<TString>(args.InitialFolder.Attributes.begin(), args.InitialFolder.Attributes.end())
    };
    DoFolderListOperation({folder});
}

TExprNode::TPtr TWalkFoldersImpl::GetNextStateExpr(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args) {
    YQL_CLOG(INFO, ProviderYt) << "Current processing state: " << int(ProcessingState_);
    switch (ProcessingState_) {
        case WaitingListFolderOp: {
            return AfterListFolderOp(ctx, args);
        }
        case PreHandling: {
            return PreHandleVisitedInSingleFolder(ctx, args, ProcessFoldersQueue_.front());
        }
        case ResolveHandling: {
            return ResolveHandleInSingleFolder(ctx, args, ProcessFoldersQueue_.front());
        }
        case AfterResolveHandling: {
            return AfterResolveHandle(ctx, args, ProcessFoldersQueue_.front());
        }
        case WaitingResolveLinkOp: {
            return HandleAfterResolveFuture(ctx, args, ProcessFoldersQueue_.front());
        }
        case DiveHandling: {
            return DiveHandleInSingleFolder(ctx, args, ProcessFoldersQueue_.front());
        }
        case AfterDiveHandling: {
            return AfterDiveHandle(ctx, args, ProcessFoldersQueue_.front());
        }
        case PostHandling: {
            return PostHandleVisitedInSingleFolder(ctx, args, ProcessFoldersQueue_.front());
        }
        case FinishingHandling: {
            return BuildFinishedState(ctx, args);
        }
        case FinishedHandling: {
        }
    }
    return args.UserStateExpr;
}

void TWalkFoldersImpl::DoFolderListOperation(TVector<IYtGateway::TBatchFolderOptions::TFolderPrefixAttrs>&& folders) {
    YQL_CLOG(INFO, ProviderYt) << "Sending folder list batch with " << folders.size() << " items";
    auto options = IYtGateway::TBatchFolderOptions(SessionId_)
        .Pos(Pos_)
        .Cluster(Cluster_)
        .Config(Config_)
        .Folders(folders);
    BatchFolderListFuture_ = Gateway_->GetFolders(std::move(options));
}

TExprNode::TPtr TWalkFoldersImpl::EvaluateNextUserStateExpr(TExprContext& ctx, const TExprNode::TPtr& userStateType, const TExprNode::TPtr userStateExpr, std::function<TExprNode::TPtr(const NNodes::TExprBase&)> nextStateFunc) {

    const auto userStateUnpickled = Build<TCoUnpickle>(ctx, PosHandle_)
        .Type(userStateType)
        .Buffer(userStateExpr)
    .Build();

    const auto nextUserStatePickled = Build<TCoPickle>(ctx, PosHandle_)
        .Value(nextStateFunc(userStateUnpickled.Value()))
    .Build()
    .Value()
    .Ptr();
    
    ctx.Step.Repeat(TExprStep::ExprEval);

    YQL_CLOG(TRACE, ProviderYt) << "WalkFolders - next evaluate ast: " << ConvertToAst(*nextUserStatePickled, ctx, {}).Root->ToString();

    return ctx.Builder(PosHandle_)
        .Callable("EvaluateExpr")
            .Add(0, nextUserStatePickled)
        .Seal()
        .Build();
}

TExprNode::TPtr TWalkFoldersImpl::AfterListFolderOp(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args) {
    if (!BatchFolderListFuture_) {
        YQL_CLOG(INFO, ProviderYt) << "Folder queue is empty, finishing WalkFolders with key: " << args.StateKey;
        ProcessingState_ = FinishingHandling;
        return GetNextStateExpr(ctx, args);
    } else {
        if (!BatchFolderListFuture_->HasValue()) {
            YQL_CLOG(INFO, ProviderYt) << "Batch list future is not ready";
            return args.UserStateExpr;
        }

        Y_ENSURE(!ProcessFoldersQueue_.empty(), "Got future result for Yt List but no folder in queue");
        auto folderListVal = BatchFolderListFuture_->ExtractValue();
        if (folderListVal.Success()) {
            auto& folder = ProcessFoldersQueue_.front();
            YQL_CLOG(INFO, ProviderYt) << "Got " << folderListVal.Items.size() << " results for list op at `" << folder.Folder.Prefix << "`";
            folder.ItemsToPreHandle = std::move(folderListVal.Items);
            folder.PreHandleItemsFetched = true;
            ProcessingState_ = PreHandling;
        } else {
            folderListVal.ReportIssues(ctx.IssueManager);
        }

        BatchFolderListFuture_ = Nothing();
    }
    return PreHandleVisitedInSingleFolder(ctx, args, ProcessFoldersQueue_.front());
}

TExprNode::TPtr TWalkFoldersImpl::PreHandleVisitedInSingleFolder(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args, TFolderQueueItem& folder) {
    YQL_CLOG(INFO, ProviderYt) << "Processing preHandler at " << folder.Folder.Prefix
                               << " for WalkFolders with key: " << args.StateKey;

    if (!folder.PreHandleItemsFetched) {
        YQL_CLOG(INFO, ProviderYt) << "Waiting for folder list: `" << folder.Folder.Prefix << "`";
        ProcessingState_ = WaitingListFolderOp;
        return GetNextStateExpr(ctx, args);
    }

    if (folder.ItemsToPreHandle.empty()) {
        YQL_CLOG(INFO, ProviderYt) << "Items to preHandle are empty, skipping " << folder.Folder.Prefix;
        ProcessFoldersQueue_.pop_front();
        ProcessingState_ = WaitingListFolderOp;
        return GetNextStateExpr(ctx, args);
    }

    TVector<TExprBase> folderListItems;
    for (auto&& item : folder.ItemsToPreHandle) {
        if (PreHandler_) {
            folderListItems.push_back(
                BuildFolderListItemExpr(ctx, PosHandle_, item.Path,item.Type,
                                        NYT::NodeToYsonString(item.Attributes)));
        }

        if (item.Type == "link") {
            folder.LinksToResolveHandle.emplace_back(std::move(item));
        } else if (item.Type == "map_node") {
            folder.ItemsToDiveHandle.emplace_back(std::move(item));
        } else {
            folder.ItemsToPostHandle.emplace_back(std::move(item));
        }
    }

    if (!PreHandler_) {
        YQL_CLOG(INFO, ProviderYt) << "No preHandler defined, skipping for WalkFolders with key: " << args.StateKey;
        ProcessingState_ = ResolveHandling;
        return GetNextStateExpr(ctx, args);
    }

    const auto folderListExpr = BuildFolderListExpr(ctx, PosHandle_, folderListItems);

    const auto makeNextUserState = [&] (const TExprBase& userStateUnpickled) { 
        return Build<TCoApply>(ctx, PosHandle_)
            .Callable(PreHandler_.GetRef())
            .FreeArgs()
                .Add(folderListExpr)
                .Add(userStateUnpickled)
                .Add<TCoInt64>()
                    .Literal()
                        .Value(ToString(folder.Level))
                    .Build()
                .Build()
            .Build()
        .Build()
        .Value()
        .Ptr();
    };

    ProcessingState_ = ResolveHandling;
    return EvaluateNextUserStateExpr(ctx, args.UserStateType, args.UserStateExpr, makeNextUserState);
}

TExprNode::TPtr TWalkFoldersImpl::ResolveHandleInSingleFolder(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args,  TFolderQueueItem& folder) {
    YQL_CLOG(INFO, ProviderYt) << "Processing resolveHandler at " << folder.Folder.Prefix
                               << "for WalkFolders with key: " << args.StateKey;
    ProcessingState_ = AfterResolveHandling;
    return BuildDiveOrResolveHandlerEval(ctx, args, ResolveHandler_.GetRef(), folder.LinksToResolveHandle, folder.Folder.Attributes, folder.Level);
}

TExprNode::TPtr TWalkFoldersImpl::BuildDiveOrResolveHandlerEval(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args, TExprNode::TPtr& handler,
                                              const TVector<IYtGateway::TBatchFolderResult::TFolderItem>& res, const TVector<TString>& attributes, ui64 level) {
    using namespace NNodes;

    TVector<TExprBase> items;
    items.reserve(res.size());
    for (auto& link : res) {
        auto itemsExpr = 
            BuildFolderListItemExpr(ctx, PosHandle_, 
                link.Path, link.Type, NYT::NodeToYsonString(link.Attributes));
        items.push_back(itemsExpr);
    }
    const auto itemsNode = BuildFolderListExpr(ctx, PosHandle_, items);

    TVector<TExprBase> attributeExprs;
    for (auto& attr : attributes) {
        const auto  attributeExpr = Build<TCoString>(ctx, PosHandle_)
            .Literal()
                .Value(attr)
            .Build()
        .Build()
        .Value();
        attributeExprs.push_back(attributeExpr);
    }

    const auto userStateUnpickled = Build<TCoUnpickle>(ctx, PosHandle_)
        .Type(args.UserStateType)
        .Buffer(args.UserStateExpr)
    .DoBuild();

    const auto handlerResult = Build<TCoApply>(ctx, PosHandle_)
        .Callable(handler)
        .FreeArgs()
            .Add(itemsNode)
            .Add(userStateUnpickled)
            .Add<TCoAsList>()
                .Add(attributeExprs)
            .Build()
            .Add<TCoInt64>()
                .Literal()
                    .Value(ToString(level))
                .Build()
            .Build()
        .Build()
    .Build()
    .Value()
    .Ptr();
    
    auto resolveHandlerResPickled = ctx.Builder(PosHandle_)
        .Callable("StaticMap")
            .Add(0, handlerResult)
            .Lambda(1)
                .Param("item")
                .Callable("Pickle")
                    .Arg(0, "item")
                .Seal()
            .Seal()
        .Seal()
        .Build();

    ctx.Step.Repeat(TExprStep::ExprEval);
    return ctx.Builder(PosHandle_)
        .Callable("EvaluateExpr")
            .Add(0, resolveHandlerResPickled)
        .Seal()
        .Build();
}

void ParseNameAttributesPickledList(TStringBuf pickledTupleList, std::function<void(TString&&, TSet<TString>)> handleParsedNameAndAttrs) {
    using namespace NKikimr::NMiniKQL;

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TMemoryUsageInfo memInfo("Yt WalkFolders");
    THolderFactory holderFactory(alloc.Ref(), memInfo);

    TSmallVec<TType*> nodeToResolveWithAttrListTypes;
    auto stringType = TDataType::Create(NUdf::TDataType<char*>::Id, env);
    nodeToResolveWithAttrListTypes.push_back(stringType);
    nodeToResolveWithAttrListTypes.push_back(TListType::Create(stringType, env));

    auto nodeToResolveTuple = TTupleType::Create(2, nodeToResolveWithAttrListTypes.data(), env);
    TValuePacker packer(false, TListType::Create(nodeToResolveTuple, env));
    auto parsedList = packer.Unpack(pickledTupleList, holderFactory);

    YQL_CLOG(INFO, ProviderYt) << "Parsing list with length: " << parsedList.GetListLength();

    for (size_t i = 0; i < parsedList.GetListLength(); ++i) {
        const auto requestedTuple = parsedList.GetElement(i);
        const auto nameEl = requestedTuple.GetElement(0);
        const auto name = nameEl.AsStringRef();
        YQL_CLOG(INFO, ProviderYt) << "Parsed dive or resolve item name: " << name;

        auto requestedAttrsVal = requestedTuple.GetElement(1);
        TSet<TString> attrs;
        for (size_t j = 0; j < requestedAttrsVal.GetListLength(); ++j) {
            const auto attrEl = requestedAttrsVal.GetElement(j);
            YQL_CLOG(INFO, ProviderYt) << "Parsed requested attribute: " << attrEl.AsStringRef();
            attrs.insert(TString(attrEl.AsStringRef()));
        }
        handleParsedNameAndAttrs(TString(name), attrs);
    }
}

TExprNode::TPtr TWalkFoldersImpl::AfterResolveHandle(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args, TFolderQueueItem& folder) {
    EnsureTupleSize(*args.UserStateExpr, 2, ctx);
    YQL_CLOG(INFO, ProviderYt) << "After resolveHandler EvaluateExpr";

    TCoString pickledLinksToResolve(args.UserStateExpr->Child(0));
    THashMap<TString, TSet<TString>> nameAndRequestedAttrs;
    ParseNameAttributesPickledList(pickledLinksToResolve.Literal().StringValue(),
        [&nameAndRequestedAttrs] (TString name, TSet<TString> attrs) {
        nameAndRequestedAttrs[name] = std::move(attrs);
    });

    TVector<IYtGateway::TResolveOptions::TItemWithReqAttrs> links;
    links.reserve(nameAndRequestedAttrs.size());
    for (auto&& linkToResolve : folder.LinksToResolveHandle) { 
        auto it = nameAndRequestedAttrs.find(linkToResolve.Path);
        if (it == nameAndRequestedAttrs.end()) {
            continue;
        }

        IYtGateway::TResolveOptions::TItemWithReqAttrs link {
            .Item = std::move(linkToResolve),
            .AttrKeys = std::move(it->second),
        };
        links.emplace_back(std::move(link));
    }

    if (links.empty()) {
        YQL_CLOG(INFO, ProviderYt) << "Links to visit are empty";
        ProcessingState_ = DiveHandling;
        return GetNextStateExpr(ctx, {.UserStateExpr = args.UserStateExpr->Child(1), .UserStateType = args.UserStateType, .StateKey = args.StateKey});
    }

    ProcessingState_ = WaitingResolveLinkOp;
    auto options = IYtGateway::TResolveOptions(SessionId_)
        .Pos(Pos_)
        .Cluster(Cluster_)
        .Config(Config_)
        .Items(links);
    BatchResolveFuture_ = Gateway_->ResolveLinks(std::move(options));

    return args.UserStateExpr->Child(1);
}

TExprNode::TPtr TWalkFoldersImpl::HandleAfterResolveFuture(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args, TFolderQueueItem& folder) {
    YQL_CLOG(INFO, ProviderYt) << "After resolve future result";

    if (!BatchResolveFuture_) {
        YQL_CLOG(WARN, ProviderYt) << "Resolve future not set";
        return nullptr;
    }
    if (!BatchResolveFuture_->HasValue() && !BatchResolveFuture_->HasException()) {
        YQL_CLOG(INFO, ProviderYt) << "Batch resolve future is not ready";
        return args.UserStateExpr;
    }

    auto res = BatchResolveFuture_->ExtractValue();
    BatchResolveFuture_ = Nothing();
    YQL_CLOG(INFO, ProviderYt) << "Added items to handle after batch resolve future completion";

    for (auto&& node : res.Items) {
        if (node.Type == "map_node") {
            folder.ItemsToDiveHandle.emplace_back(std::move(node));
        } else {
            folder.ItemsToPostHandle.emplace_back(std::move(node));
        }
    }

    ProcessingState_ = DiveHandling;
    return GetNextStateExpr(ctx, args);
}

TExprNode::TPtr TWalkFoldersImpl::DiveHandleInSingleFolder(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args,  TFolderQueueItem& folder) {
    YQL_CLOG(INFO, ProviderYt) << "Processing diveHandler at " << folder.Folder.Prefix
                               << " for WalkFolders with key: " << args.StateKey;
    ProcessingState_ = AfterDiveHandling;
    return BuildDiveOrResolveHandlerEval(ctx, args, DiveHandler_.GetRef(), folder.ItemsToDiveHandle, folder.Folder.Attributes, folder.Level);
}

TExprNode::TPtr TWalkFoldersImpl::AfterDiveHandle(TExprContext& ctx, TYtKey::TWalkFoldersImplArgs args, TFolderQueueItem& folder) {
    using namespace NKikimr::NMiniKQL;

    EnsureTupleSize(*args.UserStateExpr, 2, ctx);
    YQL_CLOG(INFO, ProviderYt) << "After diveHandler EvaluateExpr";

    TVector<IYtGateway::TBatchFolderOptions::TFolderPrefixAttrs> diveItems;
    TCoString pickledLinksToResolve(args.UserStateExpr->Child(0));
    THashMap<TString, TSet<TString>> nameAndRequestedAttrs;
    ParseNameAttributesPickledList(pickledLinksToResolve.Literal().StringValue(),
        [&queue=ProcessFoldersQueue_, &diveItems, nextLevel = folder.Level + 1] (TString path, TSet<TString> attrs) {
        diveItems.push_back({.Prefix = path, .AttrKeys = attrs});
        queue.push_back({
            .Folder = {.Prefix = std::move(path), .Attributes = TVector<TString>(attrs.begin(), attrs.end())},
            .Level = nextLevel,
        });
    });

    folder.ItemsToPostHandle.insert(folder.ItemsToPostHandle.end(), 
        std::make_move_iterator(folder.ItemsToDiveHandle.begin()),
        std::make_move_iterator(folder.ItemsToDiveHandle.end()));
    folder.ItemsToDiveHandle.clear();
    
    args.UserStateExpr = args.UserStateExpr->Child(1);
    ProcessingState_ = PostHandling;

    if (diveItems.empty()) {
        YQL_CLOG(INFO, ProviderYt) << "Nodes to dive are empty";
        return GetNextStateExpr(ctx, args);
    }

    auto options = IYtGateway::TBatchFolderOptions(SessionId_)
        .Pos(Pos_)
        .Cluster(Cluster_)
        .Config(Config_)
        .Folders(diveItems);
    Y_ENSURE(!BatchFolderListFuture_, "Single inflight batch folder request allowed");
    BatchFolderListFuture_ = Gateway_->GetFolders(std::move(options));

    return GetNextStateExpr(ctx, args);
}

TExprNode::TPtr TWalkFoldersImpl::PostHandleVisitedInSingleFolder(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args, TFolderQueueItem& folder) {
    if (!PostHandler_) {
        YQL_CLOG(INFO, ProviderYt) << "No postHandler defined, skipping for WalkFolders with key: " << args.StateKey;
        ProcessingState_ = WaitingListFolderOp;
        return args.UserStateExpr;
    }

    YQL_CLOG(INFO, ProviderYt) << "Processing postHandler at " << folder.Folder.Prefix
                                << " for WalkFolders with key: " << args.StateKey;

    TVector<TExprBase> folderListItems;
    for (auto&& item : folder.ItemsToPostHandle) {
        folderListItems.push_back(
            BuildFolderListItemExpr(ctx, 
                                    PosHandle_, 
                                    item.Path,
                                    item.Type,
                                    NYT::NodeToYsonString(item.Attributes)));

    }

    const auto folderListExpr = BuildFolderListExpr(ctx, PosHandle_, folderListItems);

    const auto makeNextUserState = [&] (const TExprBase& userStateUnpickled) { 
        return Build<TCoApply>(ctx, PosHandle_)
            .Callable(PostHandler_.GetRef())
            .FreeArgs()
                .Add(folderListExpr)
                .Add(userStateUnpickled)
                .Add<TCoInt64>()
                    .Literal()
                        .Value(ToString(folder.Level))
                    .Build()
                .Build()
            .Build()
        .Build()
        .Value()
        .Ptr();
    };

    ProcessingState_ = WaitingListFolderOp;

    ProcessFoldersQueue_.pop_front();
    return EvaluateNextUserStateExpr(ctx, args.UserStateType, args.UserStateExpr, makeNextUserState);
}


TExprNode::TPtr TWalkFoldersImpl::BuildFinishedState(TExprContext& ctx, const TYtKey::TWalkFoldersImplArgs& args) {
    // TODO: Dump large user state to file
    // const auto dataLen = args.UserStateExpr->IsCallable("String") 
    //     ? TCoString(args.UserStateExpr).Literal().StringValue().Size()
    //     : 0;

    const auto userStateUnpickled = Build<TCoUnpickle>(ctx, PosHandle_)
        .Type(args.UserStateType)
        .Buffer(args.UserStateExpr)
    .DoBuild()
    .Ptr();

    ctx.Step.Repeat(TExprStep::ExprEval);
    ProcessingState_ = FinishedHandling;

    return ctx.Builder(PosHandle_)
        .Callable("EvaluateExpr")
            .Add(0, userStateUnpickled)
        .Seal()
        .Build();
}
} // namespace NYql
