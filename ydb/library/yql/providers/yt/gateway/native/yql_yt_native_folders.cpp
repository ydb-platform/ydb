#include "yql_yt_native_folders.h"

#include <yt/cpp/mapreduce/interface/error_codes.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/yt/gateway/lib/yt_helpers.h>

namespace NYql::NNative {

using namespace NYT;
using namespace NCommon;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NNodes;
using namespace NThreading;

TString GetTypeFromAttributes(const NYT::TNode& attributes, bool getDocumentType) {
    if (!attributes.HasKey("type")) {
        return "unknown";
    }
    const auto type = attributes["type"].AsString();
    if (getDocumentType && "document" == type) {
        if (!attributes.HasKey("_yql_type")) {
            return "unknown";
        }
        return attributes["_yql_type"].AsString();
    } else {
        return type;
    }
}

TString GetTypeFromNode(const NYT::TNode& node, bool getDocumentType) {
    if (!node.HasAttributes()) {
        return "unknown";
    }
    return GetTypeFromAttributes(node.GetAttributes(), getDocumentType);
}

TMaybe<TVector<IYtGateway::TBatchFolderResult::TFolderItem>> MaybeGetFolderFromCache(TTransactionCache::TEntry::TPtr entry, TStringBuf cacheKey) {
    TVector<IYtGateway::TBatchFolderResult::TFolderItem> items;
    with_lock(entry->Lock_) {
        const auto listPtr = entry->FolderCache.FindPtr(cacheKey);
        if (listPtr) {
            YQL_CLOG(INFO, ProviderYt) << "Found folder in cache with key ('" << cacheKey << "') with " << listPtr->size() << " items";
            for (auto& el : *listPtr) {
                IYtGateway::TBatchFolderResult::TFolderItem item;
                std::tie(item.Type, item.Path, item.Attributes) = el;
                items.emplace_back(std::move(item));
            }
            return items;
        }
    }
    return {};
}

TMaybe<TFileLinkPtr> MaybeGetFilePtrFromCache(TTransactionCache::TEntry::TPtr entry, const IYtGateway::TBatchFolderOptions::TFolderPrefixAttrs& folder) {
    const auto cacheKey = std::accumulate(folder.AttrKeys.begin(), folder.AttrKeys.end(), folder.Prefix,
        [] (TString&& str, const TString& arg) {
        return str + "&" + arg;
    });
    with_lock(entry->Lock_) {
        const auto filePtr = entry->FolderFilePtrCache.FindPtr(cacheKey);
        if (filePtr) {
            return filePtr->Get();
        }
    }
    return {};
}

TAttributeFilter MakeAttrFilter(const TSet<TString>& attributes, bool isResolvingLink) {
    NYT::TAttributeFilter filter;
    for (const auto& attr : attributes) {
        filter.AddAttribute(attr);
    }
    if (!isResolvingLink) {
        filter.AddAttribute("target_path");
        filter.AddAttribute("broken");
    }
    filter.AddAttribute("type");
    return filter;
}

IYtGateway::TBatchFolderResult::TFolderItem MakeFolderItem(const NYT::TNode& node, const TString& path) {
    IYtGateway::TBatchFolderResult::TFolderItem item;
    item.Attributes = NYT::TNode::CreateMap();
    for (const auto& attr: node.GetAttributes().AsMap()) {
        if (attr.first == "type") {
            continue;
        }
        item.Attributes[attr.first] = attr.second;
    }
    item.Type = GetTypeFromNode(node, false);
    item.Path = path.StartsWith(NYT::TConfig::Get()->Prefix)
        ? path.substr(NYT::TConfig::Get()->Prefix.size())
        : path;
    return item;
}

const TTransactionCache::TEntry::TFolderCache::value_type& StoreResInCache(const TTransactionCache::TEntry::TPtr& entry, TVector<IYtGateway::TBatchFolderResult::TFolderItem>&& items, const TString& cacheKey) {
    std::vector<std::tuple<TString, TString, NYT::TNode>> cache;
    for (const auto& item : items) {
        cache.emplace_back(std::move(item.Type), std::move(item.Path), std::move(item.Attributes));
    }
    with_lock(entry->Lock_) {
        const auto [it, _] = entry->FolderCache.insert_or_assign(cacheKey, std::move(cache));
        return *it;
    }
}

TFileLinkPtr SaveItemsToTempFile(const TExecContext<IYtGateway::TBatchFolderOptions>::TPtr& execCtx, const TVector<IYtGateway::TFolderResult::TFolderItem>& folderItems) {
    const TString file = execCtx->FileStorage_->GetTemp() / GetGuidAsString(execCtx->Session_->RandomProvider_->GenGuid());
    YQL_CLOG(INFO, ProviderYt) << "Folder limit exceeded. Writing items to file " << file;

    auto out = MakeHolder<TOFStream>(file);
    for (auto& item: folderItems) {
        ::SaveMany(out.Get(), item.Type, item.Path, item.Attributes);
    }
    ::SaveSize(out.Get(), 0);
    out.Destroy();
    return CreateFakeFileLink(file, "", true);
}

IYtGateway::TBatchFolderResult ExecResolveLinks(const TExecContext<IYtGateway::TResolveOptions>::TPtr& execCtx) {
    try {
        auto batchGet = execCtx->GetEntry()->Tx->CreateBatchRequest();
        TVector<TFuture<IYtGateway::TBatchFolderResult::TFolderItem>> batchRes;

        for (const auto& [item, reqAttributes]: execCtx->Options_.Items()) {
            if (item.Type != "link") {
                batchRes.push_back(MakeFuture<IYtGateway::TBatchFolderResult::TFolderItem>(std::move(item)));
                continue;
            }
            if (item.Attributes["broken"].AsBool()) {
                continue;
            }
            const auto& targetPath = item.Attributes["target_path"].AsString();
            const auto& path = item.Path;
            const auto attrFilter = MakeAttrFilter(reqAttributes, /* isResolvingLink */ true);

            batchRes.push_back(
                batchGet->Get(targetPath, TGetOptions().AttributeFilter(attrFilter))
                    .Apply([path, pos = execCtx->Options_.Pos()] (const auto& f) {
                        try {
                            const auto linkNode = f.GetValue();
                            return MakeFolderItem(linkNode, path);
                        } catch (const NYT::TErrorResponse& e) {
                            return MakeFolderItem(NYT::TNode::CreateMap(), path);
                        }
                    })
            );
        }
        IYtGateway::TBatchFolderResult res;
        res.SetSuccess();
        if (batchRes.empty()) {
            YQL_CLOG(INFO, ProviderYt) << "Batch get result is empty";
            return res;
        }

        res.Items.reserve(batchRes.size());

        batchGet->ExecuteBatch();
        WaitAll(batchRes).Wait();
        for (auto& f : batchRes) {
            res.Items.push_back(f.ExtractValue());
        }
        return res;
    }
    catch (...) {
        return ResultFromCurrentException<IYtGateway::TBatchFolderResult>(execCtx->Options_.Pos());
    }
}

IYtGateway::TBatchFolderResult ExecGetFolder(const TExecContext<IYtGateway::TBatchFolderOptions>::TPtr& execCtx) {
    const auto entry = execCtx->GetOrCreateEntry();
    auto batchList = entry->Tx->CreateBatchRequest();
    TVector<TFuture<TVector<IYtGateway::TBatchFolderResult::TFolderItem>>> batchRes;

    IYtGateway::TBatchFolderResult folderResult;
    folderResult.SetSuccess();

    for (const auto& folder : execCtx->Options_.Folders()) {
        const auto cacheKey = std::accumulate(folder.AttrKeys.begin(), folder.AttrKeys.end(), folder.Prefix,
            [] (TString&& str, const TString& arg) {
            return str + "&" + arg;
        });
        YQL_CLOG(INFO, ProviderYt) << "Executing list command with prefix: " << folder.Prefix << " , cacheKey = " << cacheKey;

        auto maybeCached = MaybeGetFolderFromCache(entry, cacheKey);
        if (maybeCached) {
            batchRes.push_back(MakeFuture<TVector<IYtGateway::TBatchFolderResult::TFolderItem>>(std::move(*maybeCached)));
            continue;
        }

        const auto attrFilter = MakeAttrFilter(folder.AttrKeys, /* isResolvingLink */ false);
        batchRes.push_back(
            batchList->List(folder.Prefix, TListOptions().AttributeFilter(attrFilter))
            .Apply([&folder, cacheKey = std::move(cacheKey), &entry] (const TFuture<NYT::TNode::TListType>& f)
                -> TFuture<TVector<IYtGateway::TBatchFolderResult::TFolderItem>> {
                TVector<IYtGateway::TBatchFolderResult::TFolderItem> folderItems;
                try {
                    auto nodeList = f.GetValue();
                    folderItems.reserve(nodeList.size());
                    for (const auto& node : nodeList) {
                        TStringBuilder path;
                        if (!folder.Prefix.Empty()) {
                            path << folder.Prefix << "/";
                        }
                        path << node.AsString();
                        folderItems.push_back(MakeFolderItem(node, path));
                    }
                    StoreResInCache(entry, std::move(folderItems), cacheKey);
                    return MakeFuture(std::move(folderItems));
                }
                catch (const NYT::TErrorResponse& e) {
                    if (e.GetError().ContainsErrorCode(NYT::NClusterErrorCodes::NYTree::ResolveError)) {
                        // Return empty list on missing path
                        YQL_CLOG(INFO, ProviderYt) << "Storing empty folder in cache with key ('" << cacheKey << "')";
                        StoreResInCache(entry, {}, cacheKey);
                        return MakeFuture<TVector<IYtGateway::TBatchFolderResult::TFolderItem>>({});
                    }
                    return MakeErrorFuture<TVector<IYtGateway::TBatchFolderResult::TFolderItem>>(std::current_exception());
                }
                catch (...) {
                    return MakeErrorFuture<TVector<IYtGateway::TBatchFolderResult::TFolderItem>>(std::current_exception());
                }
            })
        );
    }

    TExecuteBatchOptions batchOptions;
    if (batchRes.size() > 1) {
        const size_t concurrency = execCtx->Options_.Config()->BatchListFolderConcurrency
            .Get().GetOrElse(DEFAULT_BATCH_LIST_FOLDER_CONCURRENCY);
        batchOptions.Concurrency(concurrency);
    }

    try {
        batchList->ExecuteBatch(batchOptions);
        WaitExceptionOrAll(batchRes).Wait();
        for (auto& res : batchRes) {
            const auto items = res.ExtractValue();
            folderResult.Items.reserve(folderResult.Items.size() + items.size());
            for (const auto& item : items) {
                folderResult.Items.push_back(std::move(item));
            }
        }
    }
    catch (...) {
        return ResultFromCurrentException<IYtGateway::TBatchFolderResult>(execCtx->Options_.Pos());
    }
    return folderResult;
}

} // NYql::NNative
