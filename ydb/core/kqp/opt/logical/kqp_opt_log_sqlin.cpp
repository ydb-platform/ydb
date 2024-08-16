#include "kqp_opt_log_impl.h"
#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/common_opt/yql_co_sqlin.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

bool CanRewriteSqlInToEquiJoin(const TTypeAnnotationNode* lookupType, const TTypeAnnotationNode* collectionType) {
    // SqlIn in Dict
    if (collectionType->GetKind() == ETypeAnnotationKind::Dict) {
        return IsDataOrOptionalOfData(lookupType);
    }

    // SqlIn in List<DataType> or List<Tuple<DataType...>>
    if (collectionType->GetKind() == ETypeAnnotationKind::List) {
        auto collectionItemType = collectionType->Cast<TListExprType>()->GetItemType();

        if (collectionItemType->GetKind() == ETypeAnnotationKind::Tuple) {
            if (lookupType->GetKind() != ETypeAnnotationKind::Tuple) {
                return false;
            }
            auto lookupItems = lookupType->Cast<TTupleExprType>()->GetItems();
            auto collectionItems = collectionItemType->Cast<TTupleExprType>()->GetItems();
            if (lookupItems.size() != collectionItems.size()) {
                return false;
            }
            return AllOf(collectionItems, [](const auto& item) { return IsDataOrOptionalOfData(item); });
        }

        return IsDataOrOptionalOfData(collectionItemType);
    }

    return false;
}

} // namespace

TExprBase KqpRewriteSqlInToEquiJoin(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    const TKikimrConfiguration::TPtr& config)
{
    if (kqpCtx.IsScanQuery() && !kqpCtx.Config->EnableKqpScanQueryStreamLookup) {
        return node;
    }

    if (config->HasOptDisableSqlInToJoin()) {
        return node;
    }

    if (!node.Maybe<TCoFlatMap>()) {
        return node;
    }

    const auto flatMap = node.Cast<TCoFlatMap>();
    const auto lambdaBody = flatMap.Lambda().Body();

    // SqlIn expected to be rewritten to (FlatMap <in> (OptionalIf ...)) or (FlatMap <in> (FlatListIf ...))
    if (!lambdaBody.Maybe<TCoOptionalIf>() && !lambdaBody.Maybe<TCoFlatListIf>()) {
        return node;
    }

    if (!FindNode(lambdaBody.Ptr(), [](const TExprNode::TPtr& x) { return TCoSqlIn::Match(x.Get()); })) {
        return node;
    }

    TVector<TStringBuf> keys; // remaining key parts, that can be used in SqlIn (only in asc order)

    const NYql::TKikimrTableDescription* tableDesc;

    auto readMatch = MatchRead(flatMap.Input(), [](TExprBase node) {
            return node.Maybe<TKqlReadTableBase>() || node.Maybe<TKqlReadTableRangesBase>();
        });

    if (!readMatch) {
        return node;
    }

    ui64 fixedPrefixLen;
    auto pointSelection = RewriteReadToPrefixLookup(readMatch->Read, ctx, kqpCtx, kqpCtx.Config->IdxLookupJoinsPrefixPointLimit);
    if (!pointSelection) {
        return node;
    }

    fixedPrefixLen = pointSelection->PrefixSize;
    tableDesc = &kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, pointSelection->LookupTableName);

    i64 keySuffixLen = (i64) tableDesc->Metadata->KeyColumnNames.size() - (i64) fixedPrefixLen;
    if (keySuffixLen <= 0) {
        return node;
    }

    keys.reserve(keySuffixLen);
    for (ui64 idx = fixedPrefixLen; idx < tableDesc->Metadata->KeyColumnNames.size(); ++idx) {
        keys.emplace_back(TStringBuf(tableDesc->Metadata->KeyColumnNames[idx]));
    }


    auto flatMapLambdaArg = flatMap.Lambda().Args().Arg(0);

    auto findMemberIndexInKeys = [&keys](const TCoArgument& flatMapLambdaArg, const TCoMember& member) {
        if (member.Struct().Raw() != flatMapLambdaArg.Raw()) {
            return -1;
        }
        for (size_t i = 0; i < keys.size(); ++i) {
            if (member.Name().Value() == keys[i]) {
                return (int) i;
            }
        }
        return -1;
    };

    auto shouldConvertSqlInToJoin = [&](const TCoSqlIn& sqlIn, bool negated) {
        if (negated) {
            // negated can't be rewritten to the index-lookup, so skip it
            return false;
        }

        // validate key prefix
        if (sqlIn.Lookup().Maybe<TCoMember>()) {
            if (findMemberIndexInKeys(flatMapLambdaArg, sqlIn.Lookup().Cast<TCoMember>()) != 0) {
                return false;
            }
        } else if (sqlIn.Lookup().Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
            auto children = sqlIn.Lookup().Ref().ChildrenList();
            TVector<int> usedKeyIndexes{Reserve(children.size())};
            for (const auto& itemPtr : children) {
                TExprBase item{itemPtr};
                if (!item.Maybe<TCoMember>()) {
                    return false;
                }
                int keyIndex = findMemberIndexInKeys(flatMapLambdaArg, item.Cast<TCoMember>());
                if (keyIndex >= 0) {
                    usedKeyIndexes.push_back(keyIndex);
                } else {
                    return false;
                }
            }
            if (usedKeyIndexes.empty()) {
                return false;
            }
            ::Sort(usedKeyIndexes);
            for (size_t i = 0; i < usedKeyIndexes.size(); ++i) {
                if (usedKeyIndexes[i] != (int) i) {
                    return false;
                }
            }
        } else {
            return false;
        }

        return CanRewriteSqlInToEquiJoin(sqlIn.Lookup().Ref().GetTypeAnn(), sqlIn.Collection().Ref().GetTypeAnn());
    };

    const bool prefixOnly = true;
    if (auto ret = TryConvertSqlInPredicatesToJoins(flatMap, shouldConvertSqlInToJoin, ctx, prefixOnly)) {
        return TExprBase(ret);
    }

    return node;
}

} // namespace NKikimr::NKqp::NOpt
