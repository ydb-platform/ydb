#include "yql_op_hash.h"
#include "yql_hash_builder.h"

#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {

using namespace NNodes;

void TNodeHashCalculator::UpdateFileHash(THashBuilder& builder, TStringBuf alias) const {
    auto block = Types.UserDataStorage->FindUserDataBlock(alias);
    YQL_ENSURE(block, "File " << alias << " not found");
    if (block->FrozenFile || block->Type != EUserDataType::URL) {
        YQL_ENSURE(block->FrozenFile, "File " << alias << " is not frozen");
        YQL_ENSURE(block->FrozenFile->GetMd5(), "MD5 for file " << alias << " is empty");

        builder << alias << (ui32)block->Type << block->FrozenFile->GetMd5();
        return;
    }

    // temporary approach: for YT remote files we support URL hashing rather than file content hashing
    // todo: rework it and use file metadata
    builder << alias << (ui32)block->Type << block->Data;
}

bool TNodeHashCalculator::UpdateChildrenHash(THashBuilder& builder, const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel, size_t fromIndex) const {
    for (size_t i = fromIndex; i < node.ChildrenSize(); ++i) {
        auto childHash = GetHashImpl(*node.Child(i), argIndex, frameLevel);
        if (childHash.empty()) {
            return false;
        }

        builder << childHash;
    }
    return true;
}

TString TNodeHashCalculator::GetHashImpl(const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) const {
    auto it = NodeHash.find(node.UniqueId());
    if (it != NodeHash.end()) {
        return it->second;
    }

    bool isHashable = true;
    TString myHash;
    ui32 typeNum = node.Type();
    THashBuilder builder;
    builder << Salt << typeNum;
    switch (node.Type()) {
    case TExprNode::List: {
        if (!UpdateChildrenHash(builder, node, argIndex, frameLevel)) {
            isHashable = false;
        }
        break;
    }
    case TExprNode::Atom: {
        builder << node.Content();
        break;
    }
    case TExprNode::Callable: {
        if (auto p = Hashers.FindPtr(node.Content())) {
            auto callableHash = (*p)(node, argIndex, frameLevel);
            if (callableHash.empty()) {
                isHashable = false;
            }
            else {
                builder << callableHash;
            }
        }
        else {
            builder << node.Content();
            if (node.ChildrenSize() > 0 && node.Child(0)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::World) {
                YQL_CLOG(ERROR, ProviderYt) << "Cannot calculate hash for " << node.Content();
                isHashable = false;
            }
            else {
                if (!UpdateChildrenHash(builder, node, argIndex, frameLevel)) {
                    isHashable = false;
                }
                else {
                    if (TCoUdf::Match(&node) && node.ChildrenSize() > TCoUdf::idx_FileAlias && !node.Child(TCoUdf::idx_FileAlias)->Content().empty()) {
                        // an udf from imported file, use hash of file
                        auto alias = node.Child(TCoUdf::idx_FileAlias)->Content();
                        UpdateFileHash(builder, alias);
                    } else if (node.Content() == "FilePath" || node.Content() == "FileContent") {
                        auto alias = node.Child(0)->Content();
                        UpdateFileHash(builder, alias);
                    } else if (node.Content() == "FolderPath") {
                        auto alias = node.Child(0)->Content();
                        auto blocks = Types.UserDataStorage->FindUserDataFolder(alias);
                        YQL_ENSURE(blocks, "Folder" << alias << " not found");
                        // keys for blocks must be ordered (not a hashmap)
                        for (const auto& x : *blocks) {
                            UpdateFileHash(builder, x.first.Alias());
                        }
                    }
                }
            }
        }
        break;
    }
    case TExprNode::Lambda: {
        ui32 pos = 0;
        for (const auto& arg : node.Child(0)->Children()) {
            // argument is described by it's frame level (starting from 1) and position
            YQL_ENSURE(argIndex.insert(std::make_pair(arg.Get(), std::make_pair(frameLevel + 1, pos++ ))).second);
        }

        if (!UpdateChildrenHash(builder, node, argIndex, frameLevel + 1, 1)) {
            isHashable = false;
        }
        break;
    }

    case TExprNode::Argument: {
        auto it = argIndex.find(&node);
        YQL_ENSURE(it != argIndex.end());
        builder << (frameLevel - it->second.first) << it->second.second;
        break;
    }

    case TExprNode::World:
        isHashable = false;
        break;
    default:
        YQL_ENSURE(false, "unexpected");
    }

    if (isHashable) {
        myHash = builder.Finish();
    }

    NodeHash.emplace(node.UniqueId(), myHash);
    return myHash;
}

} // NYql
