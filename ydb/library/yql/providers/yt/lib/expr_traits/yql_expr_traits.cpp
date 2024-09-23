#include "yql_expr_traits.h"

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_yt_settings.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/string/cast.h>

namespace NYql {
    namespace {
        // Mapping of comparison op to its normalized form
        const THashSet<TStringBuf> RANGE_COMPARISON_OPS = {
            NNodes::TCoCmpLess::CallableName(),
            NNodes::TCoCmpLessOrEqual::CallableName(),
            NNodes::TCoCmpEqual::CallableName(),
            NNodes::TCoCmpGreater::CallableName(),
            NNodes::TCoCmpGreaterOrEqual::CallableName(),
            NNodes::TCoCmpStartsWith::CallableName(),
        };

    }

    bool IsRangeComparison(const TStringBuf& operation) {
        return RANGE_COMPARISON_OPS.contains(operation);
    }

    void ScanResourceUsage(const TExprNode& input, const TYtSettings& config, const TTypeAnnotationContext* types,
        TMap<TStringBuf, ui64>* memoryUsage, TMap<TStringBuf, double>* cpuUsage, size_t* files)
    {
        VisitExpr(input, [&](const TExprNode& node) {
            if (NNodes::TYtOutput::Match(&node)) {
                // Stop traversing dependent operations
                return false;
            }

            if (memoryUsage) {
                if (node.IsCallable("CommonJoinCore")) {
                    if (auto memLimitSetting = GetSetting(*node.Child(5), "memLimit")) {
                        (*memoryUsage)["CommonJoinCore"] += FromString<ui64>(memLimitSetting->Child(1)->Content());
                    }
                } else if (node.IsCallable("WideCombiner")) {
                    (*memoryUsage)["WideCombiner"] += FromString<ui64>(node.Child(1U)->Content());
                } else if (NNodes::TCoCombineCore::Match(&node)) {
                    (*memoryUsage)["CombineCore"] += FromString<ui64>(node.Child(NNodes::TCoCombineCore::idx_MemLimit)->Content());
                }
            }

            if (node.IsCallable("Udf")) { // TODO: use YQL-369 when it's ready
                if (files) {
                    ++*files;
                }
                if (node.Child(0)->Content().StartsWith("Geo.")) {
                    if (files) {
                        ++*files; // geobase
                    }
                    if (memoryUsage) {
                        (*memoryUsage)["Geo module"] = 2_GB; // Take into account only once
                    }
                }
                if (memoryUsage && node.Child(0)->Content().StartsWith("UserSessions.")) {
                    (*memoryUsage)["UserSessions module"] = 512_MB; // Take into account only once
                }
            }

            if (NNodes::TYtTableContent::Match(&node)) {
                if (files) {
                    auto content = NNodes::TYtTableContent(&node);
                    if (auto read = content.Input().Maybe<NNodes::TYtReadTable>()) {
                        for (auto section: read.Cast().Input()) {
                            *files += section.Paths().Size();
                        }
                    } else {
                        // YtOutput
                        ++*files;
                    }
                }
                if (memoryUsage) {
                    if (auto setting = NYql::GetSetting(*node.Child(NNodes::TYtTableContent::idx_Settings), "memUsage")) {
                        (*memoryUsage)["YtTableContent"] += FromString<ui64>(setting->Child(1)->Content());
                    }
                }
                // Increase CPU only for CROSS JOIN. Check "rowFactor" as CROSS JOIN flag
                if (cpuUsage && HasSetting(*node.Child(NNodes::TYtTableContent::idx_Settings), "rowFactor")) {
                    if (auto setting = NYql::GetSetting(*node.Child(NNodes::TYtTableContent::idx_Settings), "itemsCount")) {
                        (*cpuUsage)["YtTableContent"] = double(FromString<ui64>(setting->Child(1)->Content()));
                    }
                }
            }

            if (NNodes::TCoSwitch::Match(&node)) {
                if (memoryUsage) {
                    (*memoryUsage)["Switch"] += FromString<ui64>(node.Child(1)->Content());
                }
                if (cpuUsage) {
                    const auto& inputItemType = GetSeqItemType(*node.Head().GetTypeAnn());
                    ui32 inputStreamsCount = 1;
                    if (inputItemType.GetKind() == ETypeAnnotationKind::Variant) {
                        auto underlyingType = inputItemType.Cast<TVariantExprType>()->GetUnderlyingType();
                        inputStreamsCount = underlyingType->Cast<TTupleExprType>()->GetSize();
                    }

                    TVector<ui32> streamUsage(inputStreamsCount, 0);
                    for (ui32 i = 2; i < node.ChildrenSize(); i += 2) {
                        for (auto& child : node.Child(i)->Children()) {
                            ++streamUsage[FromString<ui32>(child->Content())];
                        }
                    }
                    auto maxStreamUsage = *MaxElement(streamUsage.begin(), streamUsage.end());
                    if (maxStreamUsage > 1) {
                        double usage = maxStreamUsage;
                        if (auto prev = cpuUsage->FindPtr("Switch")) {
                            usage *= *prev;
                        }
                        (*cpuUsage)["Switch"] = usage;
                    }
                }
            }

            if (node.IsCallable(TStringBuf("ScriptUdf"))) {
                if (files) {
                    ++*files;
                }
                if (cpuUsage) {
                    if (auto cpu = config.ScriptCpu.Get(NCommon::ALL_CLUSTERS)) {
                        (*cpuUsage)["ScriptUdf"] = *cpu; // Take into account only once
                    }
                    if (node.Child(0)->Content().EndsWith("Javascript")) {
                        if (auto cpu = config.JavascriptCpu.Get(NCommon::ALL_CLUSTERS)) {
                            (*cpuUsage)["Javascript module"] = *cpu; // Take into account only once
                        }
                    }
                }
                if (node.Child(0)->Content().Contains("Python")) {
                    if (memoryUsage) {
                        (*memoryUsage)["Python module"] = 512_MB; // Take into account only once
                    }
                    if (cpuUsage) {
                        if (auto cpu = config.PythonCpu.Get(NCommon::ALL_CLUSTERS)) {
                            (*cpuUsage)["Python module"] = *cpu; // Take into account only once
                        }
                    }
                }
                if (node.ChildrenSize() >= 5) {
                    if (cpuUsage) {
                        if (auto cpuSetting = GetSetting(*node.Child(4), "cpu")) {
                            double usage = FromString<double>(cpuSetting->Child(1)->Content());
                            if (auto prev = cpuUsage->FindPtr("ScriptUdf from settings arg")) {
                                usage *= *prev;
                            }
                            (*cpuUsage)["ScriptUdf from settings arg"] = usage;
                        }
                    }
                    if (memoryUsage) {
                        if (auto extraMemSetting = GetSetting(*node.Child(4), "extraMem")) {
                            (*memoryUsage)["ScriptUdf from settings arg"] += FromString<ui64>(extraMemSetting->Child(1)->Content());
                        }
                    }
                }
            }
            if (files) {
                if (node.IsCallable("FilePath") || node.IsCallable("FileContent")) {
                    ++*files;
                }
                else if (node.IsCallable("FolderPath")) {
                    YQL_ENSURE(types);
                    const auto& name = node.Head().Content();
                    if (auto blocks = types->UserDataStorage->FindUserDataFolder(name)) {
                        *files += blocks->size();
                    }
                }
            }
            return true;
        });
    }

    TExpressionResorceUsage ScanExtraResourceUsage(const TExprNode& input, const TYtSettings& config) {
        TExpressionResorceUsage ret;
        TMap<TStringBuf, ui64> memory;
        TMap<TStringBuf, double> cpu;
        ScanResourceUsage(input, config, nullptr, &memory, &cpu, nullptr);

        for (auto& m: memory) {
            ret.Memory += m.second;
            YQL_CLOG(DEBUG, ProviderYt) << "Increased extraMemUsage for " << m.first << " to " << ret.Memory << " (by " << m.second << ")";
        }
        for (auto& c: cpu) {
            ret.Cpu *= c.second;
            YQL_CLOG(DEBUG, ProviderYt) << "Increased cpu for " << c.first << " to " << ret.Cpu;
        }
        return ret;
    }

    namespace {
        bool IsPrimitiveType(const TTypeAnnotationNode* type) {
            if (ETypeAnnotationKind::Data != type->GetKind()) {
                return false;
            }
            using namespace NKikimr::NUdf;
            switch (type->Cast<TDataExprType>()->GetSlot()) {
                case EDataSlot::Bool:
                case EDataSlot::Uint8:
                case EDataSlot::Int8:
                case EDataSlot::Uint16:
                case EDataSlot::Int16:
                case EDataSlot::Uint32:
                case EDataSlot::Int32:
                case EDataSlot::Uint64:
                case EDataSlot::Int64:
                case EDataSlot::Float:
                case EDataSlot::Double:
                case EDataSlot::Date:
                case EDataSlot::Datetime:
                case EDataSlot::Timestamp:
                case EDataSlot::Interval:
                case EDataSlot::Date32:
                case EDataSlot::Datetime64:
                case EDataSlot::Timestamp64:
                case EDataSlot::Interval64:
                    return true;
                case EDataSlot::String:
                case EDataSlot::Utf8:
                case EDataSlot::Yson:
                case EDataSlot::Json:
                case EDataSlot::Decimal:
                case EDataSlot::Uuid:
                case EDataSlot::TzDate:
                case EDataSlot::TzDatetime:
                case EDataSlot::TzTimestamp:
                case EDataSlot::TzDate32:
                case EDataSlot::TzDatetime64:
                case EDataSlot::TzTimestamp64:
                case EDataSlot::DyNumber:
                case EDataSlot::JsonDocument:
                    break;
            }
            return false;
        }

        bool IsSmallType(const TTypeAnnotationNode* type) {
            if (!IsPrimitiveType(type)) {
                return false;
            }
            using namespace NKikimr::NUdf;
            switch (type->Cast<TDataExprType>()->GetSlot()) {
                case EDataSlot::Uint64: // Doesn't fit to 7 bytes in common case
                case EDataSlot::Int64:  // Doesn't fit to 7 bytes in common case
                case EDataSlot::Double: // Doesn't fit to 7 bytes
                case EDataSlot::Timestamp:
                case EDataSlot::Interval:
                    return false;
                default:
                    break;
            }
            return true;
        }

    }

    void CalcToDictFactors(
        const TTypeAnnotationNode* keyType,
        const TTypeAnnotationNode* payloadType,
        EDictType type, bool many, bool compact,
        double& sizeFactor, ui64& rowFactor) {
        sizeFactor = 1.;
        rowFactor = 0ULL;
        type = SelectDictType(type, keyType);

        // See https://st.yandex-team.ru/YQL-848#1473441162000 for measures
        if (compact) {
            if (!many && ETypeAnnotationKind::Void == payloadType->GetKind()) {
                if (IsPrimitiveType(keyType)) {
                    // THashedDictSetAccumulator<THashedSingleFixedCompactSetTraits>
                    sizeFactor = 0.;
                    rowFactor = 16;
                } else {
                    // THashedDictSetAccumulator<THashedCompactSetTraits>
                    sizeFactor = 3.0;
                    rowFactor = 26;
                }
            } else {
                if (IsPrimitiveType(keyType)) {
                    // THashedDictMapAccumulator<THashedSingleFixedCompactMapTraits>
                    sizeFactor = (!IsSmallType(payloadType)) * 3.3;
                    rowFactor = 28;
                } else {
                    // THashedDictMapAccumulator<THashedCompactMapTraits>
                    sizeFactor = (!IsSmallType(keyType) + !IsSmallType(payloadType)) * 3.3;
                    rowFactor = 28;
                }
            }
        } else if (type == EDictType::Hashed) {
            // Not tuned. Should not be used with large dicts
            sizeFactor = 1.5;
            rowFactor = 46;
        } else {
            sizeFactor = 1.5;
            rowFactor = sizeof(NKikimr::NMiniKQL::TKeyPayloadPair);
        }
    }

    TMaybe<TIssue> CalcToDictFactors(const TExprNode& toDictNode, TExprContext& ctx, double& sizeFactor, ui64& rowFactor) {
        YQL_ENSURE(toDictNode.IsCallable({"ToDict", "SqueezeToDict", "SqlIn"}));

        TMaybe<bool> isMany;
        TMaybe<EDictType> type;
        bool isCompact = false;
        TMaybe<ui64> itemsCount;
        if (toDictNode.IsCallable("SqlIn")) {
            if (auto typeAnn = toDictNode.Head().GetTypeAnn(); typeAnn->GetKind() == ETypeAnnotationKind::List
                && typeAnn->Cast<TListExprType>()->GetItemType()->GetKind() == ETypeAnnotationKind::Struct) {
                isMany = false;
                type = EDictType::Auto;
                isCompact = true;

                auto structType = typeAnn->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                YQL_ENSURE(structType->GetSize() == 1);
                auto dictKeyType = structType->GetItems()[0]->GetItemType();

                CalcToDictFactors(
                    dictKeyType,
                    ctx.MakeType<TVoidExprType>(),
                    *type, false/*isMany*/, true/*isCompact*/,
                    sizeFactor, rowFactor);
            }
            return {};
        } else if (auto err = ParseToDictSettings(toDictNode, ctx, type, isMany, itemsCount, isCompact)) {
            return err;
        }

        CalcToDictFactors(
            toDictNode.Child(1)->GetTypeAnn(),
            toDictNode.Child(2)->GetTypeAnn(),
            *type, *isMany, isCompact,
            sizeFactor, rowFactor);

        return {};
    }

    namespace {

        THashSet<TStringBuf> TABLE_CONTENT_CONSUMER = {
            TStringBuf("Fold"),
            TStringBuf("Fold1"),
            TStringBuf("Apply"),
            TStringBuf("ToOptional"),
            TStringBuf("Head"),
            TStringBuf("Last"),
            TStringBuf("ToDict"),
            TStringBuf("SqueezeToDict"),
            TStringBuf("Iterator"), //  Why?
            TStringBuf("Collect"),
            TStringBuf("Length"),
            TStringBuf("HasItems"),
            TStringBuf("SqlIn"),
        };

        bool HasExternalArgsImpl(const TExprNode& root, TNodeSet& visited, TNodeSet& activeArgs) {
            if (!visited.emplace(&root).second) {
                return false;
            }

            if (root.Type() == TExprNode::Argument) {
                if (activeArgs.find(&root) == activeArgs.cend()) {
                    return true;
                }
            }

            if (root.Type() == TExprNode::Lambda) {
                root.Child(0)->ForEachChild([&](const TExprNode& arg) {
                    activeArgs.emplace(&arg);
                });
            }

            bool res = false;
            root.ForEachChild([&](const TExprNode& child) {
                res = res || HasExternalArgsImpl(child, visited, activeArgs);
            });
            return res;
        }

        bool HasExternalArgs(const TExprNode& node) {
            bool res = false;
            node.ForEachChild([&](const TExprNode& child) {
                if (!res && child.Type() == TExprNode::Lambda) {
                    TNodeSet visited;
                    TNodeSet activeArgs;
                    res = res || HasExternalArgsImpl(child, visited, activeArgs);
                }
            });
            return res;
        }
    }

    bool GetTableContentConsumerNodes(const TExprNode& node, const TExprNode& rootNode,
        const TParentsMap& parentsMap, TNodeSet& consumers)
    {
        const auto parents = parentsMap.find(&node);
        if (parents == parentsMap.cend()) {
            return true;
        }

        for (const auto& parent : parents->second) {
            if (parent == &rootNode) {
                continue;
            }
            else if (parent->Type() == TExprNode::Arguments) {
                continue;
            }
            else if (parent->IsCallable("DependsOn")) {
                continue;
            }
            else if (parent->IsCallable(TABLE_CONTENT_CONSUMER)) {
                if (HasExternalArgs(*parent)) {
                    return false;
                }
                consumers.insert(parent);
            }
            else if (const auto kind = parent->GetTypeAnn()->GetKind(); ETypeAnnotationKind::Flow == kind || ETypeAnnotationKind::List == kind || ETypeAnnotationKind::Stream == kind) {
                if (HasExternalArgs(*parent)) {
                    return false;
                }
                if (!GetTableContentConsumerNodes(*parent, rootNode, parentsMap, consumers)) {
                    return false;
                }
            }
            else {
                return false;
            }
        }

        return true;
    }
}
