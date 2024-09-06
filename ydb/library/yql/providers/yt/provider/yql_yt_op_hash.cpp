#include "yql_yt_helpers.h"
#include "yql_yt_op_hash.h"
#include "yql_yt_op_settings.h"

#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/hash/yql_hash_builder.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/string/cast.h>
#include <util/string/hex.h>

namespace NYql {

using namespace NNodes;

TYtNodeHashCalculator::TYtNodeHashCalculator(const TYtState::TPtr& state, const TString& cluster, const TYtSettings::TConstPtr& config)
    : TNodeHashCalculator(*state->Types, state->NodeHash, MakeSalt(config, cluster))
    , DontFailOnMissingParentOpHash(state->Types->EvaluationInProgress)
    , State(state)
    , Cluster(cluster)
    , Configuration(config)
{
    auto opHasher = [this] (const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) {
        return GetOperationHash(node, argIndex, frameLevel);
    };
    Hashers[TYtReadTable::CallableName()] = opHasher;
    Hashers[TYtFill::CallableName()] = opHasher;
    Hashers[TYtSort::CallableName()] = opHasher;
    Hashers[TYtMerge::CallableName()] = opHasher;
    Hashers[TYtMap::CallableName()] = opHasher;
    Hashers[TYtReduce::CallableName()] = opHasher;
    Hashers[TYtMapReduce::CallableName()] = opHasher;
    Hashers[TYtCopy::CallableName()] = opHasher;
    Hashers[TYtTouch::CallableName()] = opHasher;
    Hashers[TYtDqProcessWrite::CallableName()] = [this] (const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) {
        if (const auto flags = TYtDqProcessWrite(&node).Flags()) {
            // Only for hybrid for now.
            for (const auto& atom : flags.Cast()) {
                if (atom.Value() == "FallbackOnError") {
                    return GetOperationHash(node, argIndex, frameLevel);
                }
            }
        }
        return TString();
    };

    Hashers[TDqStage::CallableName()] = [this] (const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) {
        THashBuilder builder;
        builder << node.Content();
        for (size_t i = 0; i < node.ChildrenSize(); ++i) {
            // skip _logical_id setting from hashing
            if (i == TDqStageBase::idx_Settings) {
                for (size_t j = 0; j < node.Child(i)->ChildrenSize(); ++j) {
                    if((node.Child(i)->Child(j)->Type() == TExprNode::List)
                        && node.Child(i)->Child(j)->ChildrenSize() > 0
                        && (node.Child(i)->Child(j)->Child(0)->Content() = NDq::TDqStageSettings::LogicalIdSettingName)) {
                        continue;
                    }
                    if (auto partHash = GetHashImpl(*node.Child(i)->Child(j), argIndex, frameLevel)) {
                        builder << partHash;
                    }
                    else {
                        return TString();
                    }
                }
            } else {
                if (auto partHash = GetHashImpl(*node.Child(i), argIndex, frameLevel)) {
                    builder << partHash;
                }
                else {
                    return TString();
                }
            }
        }
        return builder.Finish();
    };

    Hashers[TYtOutput::CallableName()] = [this] (const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) {
        return GetOutputHash(node, argIndex, frameLevel);
    };
    Hashers[TYtOutTable::CallableName()] = [this] (const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) {
        return GetOutTableHash(node, argIndex, frameLevel);
    };
    Hashers[TYtTable::CallableName()] = [this] (const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) {
        return GetTableHash(node, argIndex, frameLevel);
    };

    Hashers[TYtStat::CallableName()] = [this] (const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) {
        Y_UNUSED(argIndex);
        Y_UNUSED(frameLevel);

        TYtTableStatInfo stat{TExprBase(&node)};

        const bool ignoreRevision = Configuration->QueryCacheIgnoreTableRevision.Get().GetOrElse(false);

        THashBuilder builder;
        builder << stat.Id;
        if (!ignoreRevision) {
            builder << stat.Revision;
            // TODO remove after https://st.yandex-team.ru/YT-9914
            builder << stat.ChunkCount;
            builder << stat.DataSize;
            builder << stat.RecordsCount;
        }

        return builder.Finish();
    };
}

TString TYtNodeHashCalculator::MakeSalt(const TYtSettings::TConstPtr& config, const TString& cluster) {
    auto salt = config->QueryCacheSalt.Get().GetOrElse(TString());
    THashBuilder builder;
    bool update = false;
    if (auto val = config->LayerPaths.Get(cluster)) {
        update = true;
        for (const auto& path : *val) {
            builder << path;
        }
    }
    if (auto val = config->DockerImage.Get(cluster)) {
        update = true;
        builder << *val;
    }
    if (auto val = config->NativeYtTypeCompatibility.Get(cluster)) {
        update = true;
        builder << *val;
    }
    if (auto val = config->OptimizeFor.Get(cluster)) {
        update = true;
        builder << *val;
    }
    if (update) {
        builder << salt;
        salt = builder.Finish();
    }
    return salt;
}

TString TYtNodeHashCalculator::GetOutputHash(const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) const {
    Y_UNUSED(argIndex);
    Y_UNUSED(frameLevel);

    auto output = TYtOutput(&node);
    auto opUniqueId = GetOutputOp(output).Ref().UniqueId();
    auto it = NodeHash.find(opUniqueId);
    if (it == NodeHash.end()) {
        if (DontFailOnMissingParentOpHash) {
            return TString();
        }
        YQL_ENSURE(false, "Cannot find hash for operation " << GetOutputOp(output).Ref().Content()
            << ", #" << opUniqueId);
    }
    if (it->second.empty()) {
        return TString();
    }
    return (THashBuilder() << it->second << FromString<size_t>(output.OutIndex().Value())).Finish();
}

TString TYtNodeHashCalculator::GetOutTableHash(const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) const {
    auto opHash = NYql::GetSetting(*node.Child(TYtOutTable::idx_Settings), EYtSettingType::OpHash);
    if (opHash && opHash->Child(1)->Content().empty()) {
        return TString();
    }
    THashBuilder builder;
    builder << node.Content();
    for (size_t i = 0; i < node.ChildrenSize(); ++i) {
        if (i != TYtOutTable::idx_Name && i != TYtOutTable::idx_Stat && i != TYtOutTable::idx_Settings) {
            if (auto partHash = GetHashImpl(*node.Child(i), argIndex, frameLevel)) {
                builder << partHash;
            }
            else {
                return TString();
            }
        }
    }

    for (auto& setting : node.Child(TYtOutTable::idx_Settings)->Children()) {
        if (auto partHash = GetHashImpl(*setting, argIndex, frameLevel)) {
            builder << partHash;
        }
        else {
            return TString();
        }
    }

    // OpHash is present for substitutions YtOutput->YtOutTable. It already includes publish-related hashes from the parent operation
    if (!opHash) {
        if (auto compressionCodec = Configuration->TemporaryCompressionCodec.Get(Cluster)) {
            builder << *compressionCodec;
        }
        if (auto erasureCodec = Configuration->TemporaryErasureCodec.Get(Cluster)) {
            builder << ToString(*erasureCodec);
        }
        if (auto media = Configuration->TemporaryMedia.Get(Cluster)) {
            builder << NYT::NodeToYsonString(*media);
        }
        if (auto primaryMedium = Configuration->TemporaryPrimaryMedium.Get(Cluster)) {
            builder << *primaryMedium;
        }
        if (auto replicationFactor = Configuration->TemporaryReplicationFactor.Get(Cluster)) {
            builder << *replicationFactor;
        }
        if (auto optimizeFor = Configuration->OptimizeFor.Get(Cluster)) {
            builder << *optimizeFor;
        }
    }

    return builder.Finish();
}

TString TYtNodeHashCalculator::GetTableHash(const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) const {
    TYtTable ytTable{ &node };
    auto epoch = TEpochInfo::Parse(ytTable.Epoch().Ref()).GetOrElse(0);
    const auto tableLabel = TString(TYtTableInfo::GetTableLabel(ytTable));
    const auto& tableDescription = State->TablesData->GetTable(TString(ytTable.Cluster().Value()), tableLabel, epoch);
    if (tableDescription.Hash) {
        if (!tableDescription.Hash.Empty()) {
            YQL_CLOG(INFO, ProviderYt) << "Use hash " << HexEncode(*tableDescription.Hash) << " for table " <<
                ytTable.Cluster().Value() << "." << tableLabel << "#" << epoch;
        }

        return *tableDescription.Hash;
    }

    const bool ignoreRevision = Configuration->QueryCacheIgnoreTableRevision.Get().GetOrElse(false);
    if (!ignoreRevision && TYtTableMetaInfo(ytTable.Meta()).IsDynamic) {
        return TString();
    }

    THashBuilder builder;
    builder << node.Content();
    builder << tableLabel;
    for (size_t i = 0; i < node.ChildrenSize(); ++i) {
        if (i != TYtTable::idx_Name && i != TYtTable::idx_Meta) {
            if (auto partHash = GetHashImpl(*node.Child(i), argIndex, frameLevel)) {
                builder << partHash;
            }
            else {
                return TString();
            }
        }
    }
    return builder.Finish();
}

TString TYtNodeHashCalculator::GetOperationHash(const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) const {
    THashBuilder builder;
    builder << node.Content();
    for (size_t i = 0; i < node.ChildrenSize(); ++i) {
        if (i != TYtReadTable::idx_World) {
            if (auto partHash = GetHashImpl(*node.Child(i), argIndex, frameLevel)) {
                builder << partHash;
            }
            else {
                return TString();
            }
        }
    }
    return builder.Finish();
}

} // NYql
