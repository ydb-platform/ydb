#include "yql_yt_provider_impl.h"
#include "yql_yt_helpers.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_table.h"
#include "yql_yt_join_impl.h"
#include "yql_yt_optimize.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/lib/key_filter/yql_key_filter.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/providers/stat/expr_nodes/yql_stat_expr_nodes.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.gen.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/extract_predicate/extract_predicate.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/string/cast.h>
#include <util/string/type.h>
#include <util/generic/xrange.h>
#include <util/generic/maybe.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/size_literals.h>

#include <algorithm>
#include <type_traits>

namespace NYql {

namespace {

using namespace NNodes;
using namespace NDq;

class TYtPhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TYtPhysicalOptProposalTransformer(TYtState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYt, state->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>()))
        , State_(state)
    {
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TYtPhysicalOptProposalTransformer::name)
        AddHandler(0, &TCoMux::Match, HNDL(Mux));
        AddHandler(0, &TYtWriteTable::Match, HNDL(Write));
        AddHandler(0, &TYtWriteTable::Match, HNDL(DqWrite));
        AddHandler(0, Names({TCoLength::CallableName(), TCoHasItems::CallableName()}), HNDL(Length));
        AddHandler(0, &TCoSort::Match, HNDL(Sort<false>));
        AddHandler(0, &TCoTopSort::Match, HNDL(Sort<true>));
        AddHandler(0, &TCoTop::Match, HNDL(Sort<true>));
        AddHandler(0, &TYtSort::Match, HNDL(YtSortOverAlreadySorted));
        AddHandler(0, &TCoPartitionByKeyBase::Match, HNDL(PartitionByKey));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(FlatMap));
        AddHandler(0, &TCoCombineByKey::Match, HNDL(CombineByKey));
        AddHandler(0, &TCoLMap::Match, HNDL(LMap<TCoLMap>));
        AddHandler(0, &TCoOrderedLMap::Match, HNDL(LMap<TCoOrderedLMap>));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(EquiJoin));
        AddHandler(0, &TCoCountBase::Match, HNDL(TakeOrSkip));
        AddHandler(0, &TYtWriteTable::Match, HNDL(Fill));
        AddHandler(0, &TResPull::Match, HNDL(ResPull));
        if (State_->Configuration->UseNewPredicateExtraction.Get().GetOrElse(DEFAULT_USE_NEW_PREDICATE_EXTRACTION)) {
            AddHandler(0, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(ExtractKeyRange));
            AddHandler(0, &TCoFlatMapBase::Match, HNDL(ExtractKeyRangeDqReadWrap));
        } else {
            AddHandler(0, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(ExtractKeyRangeLegacy));
        }
        AddHandler(0, &TCoExtendBase::Match, HNDL(Extend));
        AddHandler(0, &TCoAssumeSorted::Match, HNDL(AssumeSorted));
        AddHandler(0, &TYtMapReduce::Match, HNDL(AddTrivialMapperForNativeYtTypes));
        AddHandler(0, &TYtDqWrite::Match, HNDL(YtDqWrite));
        AddHandler(0, &TYtDqProcessWrite::Match, HNDL(YtDqProcessWrite));
        AddHandler(0, &TYtEquiJoin::Match, HNDL(EarlyMergeJoin));

        AddHandler(1, &TYtMap::Match, HNDL(FuseInnerMap));
        AddHandler(1, &TYtMap::Match, HNDL(FuseOuterMap));
        AddHandler(1, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(MapFieldsSubset));
        AddHandler(1, Names({TYtMapReduce::CallableName(), TYtReduce::CallableName()}), HNDL(ReduceFieldsSubset));
        AddHandler(1, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(MultiMapFieldsSubset));
        AddHandler(1, Names({TYtMapReduce::CallableName(), TYtReduce::CallableName()}), HNDL(MultiReduceFieldsSubset));
        AddHandler(1, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(WeakFields));
        AddHandler(1, &TYtTransientOpBase::Match, HNDL(BypassMerge));
        AddHandler(1, &TYtPublish::Match, HNDL(BypassMergeBeforePublish));
        AddHandler(1, &TYtOutputOpBase::Match, HNDL(TableContentWithSettings));
        AddHandler(1, &TYtOutputOpBase::Match, HNDL(NonOptimalTableContent));
        AddHandler(1, &TCoRight::Match, HNDL(ReadWithSettings));
        AddHandler(1, &TYtTransientOpBase::Match, HNDL(PushDownKeyExtract));
        AddHandler(1, &TYtTransientOpBase::Match, HNDL(TransientOpWithSettings));
        AddHandler(1, &TYtSort::Match, HNDL(TopSort));
        AddHandler(1, &TYtWithUserJobsOpBase::Match, HNDL(EmbedLimit));
        AddHandler(1, &TYtMerge::Match, HNDL(PushMergeLimitToInput));
        AddHandler(1, &TYtReduce::Match, HNDL(FuseReduce));

        AddHandler(2, &TYtEquiJoin::Match, HNDL(RuntimeEquiJoin));
        AddHandler(2, &TStatWriteTable::Match, HNDL(ReplaceStatWriteTable));
        AddHandler(2, &TYtMap::Match, HNDL(MapToMerge));
        AddHandler(2, &TYtPublish::Match, HNDL(UnorderedPublishTarget));
        AddHandler(2, &TYtMap::Match, HNDL(PushDownYtMapOverSortedMerge));
        AddHandler(2, &TYtMerge::Match, HNDL(MergeToCopy));
#undef HNDL
    }
private:
    static TYtDSink GetDataSink(TExprBase input, TExprContext& ctx) {
        if (auto read = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
            return TYtDSink(ctx.RenameNode(read.Cast().DataSource().Ref(), "DataSink"));
        } else if (auto out = input.Maybe<TYtOutput>()) {
            return GetOutputOp(out.Cast()).DataSink();
        } else {
            YQL_ENSURE(false, "Unknown operation input");
        }
    }

    static TExprBase GetWorld(TExprBase input, TMaybeNode<TExprBase> main, TExprContext& ctx) {
        if (!main) {
            main = ctx.NewWorld(input.Pos());
        }
        TSyncMap syncList;
        if (auto maybeRead = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
            auto read = maybeRead.Cast();
            if (read.World().Ref().Type() != TExprNode::World) {
                syncList.emplace(read.World().Ptr(), syncList.size());
            }
        } else {
            YQL_ENSURE(input.Maybe<TYtOutput>(), "Unknown operation input: " << input.Ref().Content());
        }
        return TExprBase(ApplySyncListToWorld(main.Cast().Ptr(), syncList, ctx));
    }

    template <class TExpr>
    TMaybeNode<TExpr> CleanupWorld(TExpr node, TExprContext& ctx) const {
        return TMaybeNode<TExpr>(YtCleanupWorld(node.Ptr(), ctx, State_));
    }

    struct TConvertInputOpts {
        TMaybeNode<TCoNameValueTupleList> Settings_;
        TMaybeNode<TCoAtomList> CustomFields_;
        bool KeepDirecRead_;
        bool MakeUnordered_;
        bool ClearUnordered_;

        TConvertInputOpts()
            : KeepDirecRead_(false)
            , MakeUnordered_(false)
            , ClearUnordered_(false)
        {
        }

        TConvertInputOpts& Settings(const TMaybeNode<TCoNameValueTupleList>& settings) {
            Settings_ = settings;
            return *this;
        }

        TConvertInputOpts& CustomFields(const TMaybeNode<TCoAtomList>& customFields) {
            CustomFields_ = customFields;
            return *this;
        }

        TConvertInputOpts& ExplicitFields(const TYqlRowSpecInfo& rowSpec, TPositionHandle pos, TExprContext& ctx) {
            TVector<TString> columns;
            for (auto item: rowSpec.GetType()->GetItems()) {
                columns.emplace_back(item->GetName());
            }
            for (auto item: rowSpec.GetAuxColumns()) {
                columns.emplace_back(item.first);
            }
            CustomFields_ = ToAtomList(columns, pos, ctx);
            return *this;
        }

        TConvertInputOpts& ExplicitFields(const TStructExprType& type, TPositionHandle pos, TExprContext& ctx) {
            TVector<TString> columns;
            for (auto item: type.GetItems()) {
                columns.emplace_back(item->GetName());
            }
            CustomFields_ = ToAtomList(columns, pos, ctx);
            return *this;
        }

        TConvertInputOpts& KeepDirecRead(bool keepDirecRead = true) {
            KeepDirecRead_ = keepDirecRead;
            return *this;
        }

        TConvertInputOpts& MakeUnordered(bool makeUnordered = true) {
            YQL_ENSURE(!makeUnordered || !ClearUnordered_);
            MakeUnordered_ = makeUnordered;
            return *this;
        }

        TConvertInputOpts& ClearUnordered() {
            YQL_ENSURE(!MakeUnordered_);
            ClearUnordered_ = true;
            return *this;
        }
    };

    static TYtSectionList ConvertInputTable(TExprBase input, TExprContext& ctx, const TConvertInputOpts& opts = {}) {
        TVector<TYtSection> sections;
        TExprBase columns = opts.CustomFields_ ? TExprBase(opts.CustomFields_.Cast()) : TExprBase(Build<TCoVoid>(ctx, input.Pos()).Done());
        if (auto out = input.Maybe<TYtOutput>()) {
            auto settings = opts.Settings_;
            if (!settings) {
                settings = Build<TCoNameValueTupleList>(ctx, input.Pos()).Done();
            }
            TMaybeNode<TCoAtom> mode = out.Mode();
            if (opts.MakeUnordered_) {
                mode = Build<TCoAtom>(ctx, out.Cast().Pos()).Value(ToString(EYtSettingType::Unordered)).Done();
            } else if (opts.ClearUnordered_) {
                mode = {};
            }
            sections.push_back(Build<TYtSection>(ctx, input.Pos())
                .Paths()
                    .Add()
                        .Table<TYtOutput>()
                            .InitFrom(out.Cast())
                            .Mode(mode)
                        .Build()
                        .Columns(columns)
                        .Ranges<TCoVoid>().Build()
                        .Stat<TCoVoid>().Build()
                    .Build()
                .Build()
                .Settings(settings.Cast())
                .Done());
        }
        else {
            auto read = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>();
            YQL_ENSURE(read, "Unknown operation input");

            for (auto section: read.Cast().Input()) {
                bool makeUnordered = opts.MakeUnordered_;

                auto mergedSettings = section.Settings().Ptr();
                if (NYql::HasSetting(*mergedSettings, EYtSettingType::Unordered)) {
                    mergedSettings = NYql::RemoveSetting(*mergedSettings, EYtSettingType::Unordered, ctx);
                    makeUnordered = false;
                }
                if (!opts.KeepDirecRead_) {
                    mergedSettings = NYql::RemoveSetting(*mergedSettings, EYtSettingType::DirectRead, ctx);
                }
                if (opts.Settings_) {
                    mergedSettings = MergeSettings(*mergedSettings, opts.Settings_.Cast().Ref(), ctx);
                }

                section = Build<TYtSection>(ctx, section.Pos())
                    .InitFrom(section)
                    .Settings(mergedSettings)
                    .Done();

                if (makeUnordered) {
                    section = MakeUnorderedSection(section, ctx);
                } else if (opts.ClearUnordered_) {
                    section = ClearUnorderedSection(section, ctx);
                }

                if (opts.CustomFields_) {
                    section = UpdateInputFields(section, opts.CustomFields_.Cast(), ctx);
                }

                sections.push_back(section);
            }
        }

        return Build<TYtSectionList>(ctx, input.Pos())
            .Add(sections)
            .Done();
    }

    static bool CollectMemberPaths(TExprBase row, const TExprNode::TPtr& lookupItem,
        TMap<TStringBuf, TVector<ui32>>& memberPaths, TVector<ui32>& currPath)
    {
        if (lookupItem->IsCallable("Member") && lookupItem->Child(0) == row.Raw()) {
            TStringBuf memberName = lookupItem->Child(1)->Content();
            memberPaths.insert({ memberName, currPath });
            return true;
        }

        if (lookupItem->IsList()) {
            for (ui32 i = 0; i < lookupItem->ChildrenSize(); ++i) {
                currPath.push_back(i);
                auto res = CollectMemberPaths(row, lookupItem->ChildPtr(i), memberPaths, currPath);
                currPath.pop_back();
                if (!res) {
                    return false;
                }
            }
            return true;
        }

        return !IsDepended(*lookupItem, row.Ref());
    }

    static bool CollectMemberPaths(TExprBase row, const TExprNode::TPtr& lookupItem,
        TMap<TStringBuf, TVector<ui32>>& memberPaths)
    {
        TVector<ui32> currPath;
        return CollectMemberPaths(row, lookupItem, memberPaths, currPath);
    }

    static bool CollectKeyPredicatesFromLookup(TExprBase row, TCoLookupBase lookup, TVector<TKeyFilterPredicates>& ranges,
        size_t maxTables)
    {
        TExprNode::TPtr collection;
        if (lookup.Collection().Ref().IsList()) {
            collection = lookup.Collection().Ptr();
        } else if (auto maybeAsList = lookup.Collection().Maybe<TCoAsList>()) {
            collection = maybeAsList.Cast().Ptr();
        } else if (auto maybeDictFromKeys = lookup.Collection().Maybe<TCoDictFromKeys>()) {
            collection = maybeDictFromKeys.Cast().Keys().Ptr();
        } else {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": unsupported collection " << lookup.Collection().Ref().Content();
            return false;
        }

        auto size = collection->ChildrenSize();
        if (!size) {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": empty keys collection";
            return false;
        }

        if (size + ranges.size() > maxTables) {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": too many dict keys - " << size;
            return false;
        }

        if (IsDepended(*collection, row.Ref())) {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": depends on lambda arg";
            return false;
        }

        TExprNode::TPtr lookupItem = lookup.Lookup().Ptr();
        TMap<TStringBuf, TVector<ui32>> memberPaths;
        if (!CollectMemberPaths(row, lookupItem, memberPaths)) {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": unsupported lookup item";
            return false;
        }

        if (memberPaths.empty()) {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": no key predicates in lookup item";
            return false;
        }

        for (auto& collectionItem : collection->Children()) {
            ranges.emplace_back();
            for (auto& memberAndPath : memberPaths) {
                auto member = memberAndPath.first;
                auto& path = memberAndPath.second;
                auto value = collectionItem;
                for (auto idx : path) {
                    if (!value->IsList() || idx >= value->ChildrenSize()) {
                        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": unexpected literal structure";
                        return false;
                    }
                    value = value->ChildPtr(idx);
                }
                ranges.back().emplace(member, std::make_pair(TString{TCoCmpEqual::CallableName()}, value));
            }
        }

        return true;
    }

    static bool CollectKeyPredicatesAnd(TExprBase row, const std::vector<TExprBase>& predicates, TVector<TKeyFilterPredicates>& ranges, size_t maxTables)
    {
        TVector<TKeyFilterPredicates> leftRanges;
        for (const auto& predicate : predicates) {
            TVector<TKeyFilterPredicates> rightRanges;
            if (CollectKeyPredicates(row, predicate, rightRanges, maxTables)) {
                if (leftRanges.empty()) {
                    leftRanges = std::move(rightRanges);
                } else {
                    const auto total = leftRanges.size() * rightRanges.size();
                    if (total + ranges.size() > maxTables) {
                        YQL_CLOG(DEBUG, ProviderYt) << __func__  << ": too many tables - " << (total + ranges.size());
                        return false;
                    }

                    if (1U == total) {
                        leftRanges.front().insert(rightRanges.front().cbegin(), rightRanges.front().cend());
                    } else {
                        TVector<TKeyFilterPredicates> temp;
                        temp.reserve(total);
                        for (const auto& left : leftRanges) {
                            for (const auto& right : rightRanges) {
                                temp.emplace_back(left);
                                temp.back().insert(right.cbegin(), right.cend());
                            }
                        }
                        leftRanges = std::move(temp);
                    }
                }
            }
        }

        if (leftRanges.empty()) {
            return false;
        }

        std::move(leftRanges.begin(), leftRanges.end(), std::back_inserter(ranges));
        return true;
    }

    static bool CollectKeyPredicatesOr(TExprBase row, const std::vector<TExprBase>& predicates, TVector<TKeyFilterPredicates>& ranges, size_t maxTables)
    {
        for (const auto& predicate : predicates) {
            if (!CollectKeyPredicates(row, predicate, ranges, maxTables)) {
                return false;
            }
        }
        return true;
    }

    static bool CollectKeyPredicates(TExprBase row, TExprBase predicate, TVector<TKeyFilterPredicates>& ranges, size_t maxTables)
    {
        if (const auto maybeAnd = predicate.Maybe<TCoAnd>()) {
            const auto size = maybeAnd.Cast().Args().size();
            std::vector<TExprBase> predicates;
            predicates.reserve(size);
            for (auto i = 0U; i < size; ++i) {
                predicates.emplace_back(maybeAnd.Cast().Arg(i));
            }
            return CollectKeyPredicatesAnd(row, predicates, ranges, maxTables);
        }

        if (const auto maybeOr = predicate.Maybe<TCoOr>()) {
            const auto size = maybeOr.Cast().Args().size();
            std::vector<TExprBase> predicates;
            predicates.reserve(size);
            for (auto i = 0U; i < size; ++i) {
                predicates.emplace_back(maybeOr.Cast().Arg(i));
            }
            return CollectKeyPredicatesOr(row, predicates, ranges, maxTables);
        }

        TMaybeNode<TCoCompare> maybeCompare = predicate.Maybe<TCoCompare>();
        if (auto maybeLiteral = predicate.Maybe<TCoCoalesce>().Value().Maybe<TCoBool>().Literal()) {
            if (maybeLiteral.Cast().Value() == "false") {
                maybeCompare = predicate.Cast<TCoCoalesce>().Predicate().Maybe<TCoCompare>();
            }
        }

        auto getRowMember = [row] (TExprBase expr) {
            if (auto maybeMember = expr.Maybe<TCoMember>()) {
                if (maybeMember.Cast().Struct().Raw() == row.Raw()) {
                    return maybeMember;
                }
            }

            return TMaybeNode<TCoMember>();
        };

        if (maybeCompare) {
            auto left = maybeCompare.Cast().Left();
            auto right = maybeCompare.Cast().Right();

            TMaybeNode<TCoMember> maybeMember = getRowMember(left);
            TMaybeNode<TExprBase> maybeValue = right;
            bool invert = false;
            if (!maybeMember) {
                maybeMember = getRowMember(right);
                maybeValue = left;
                invert = true;
            }

            if (!maybeMember) {
                YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": comparison with non-member";
                return false;
            }

            const auto value = maybeValue.Cast();
            if (IsDepended(value.Ref(), row.Ref())) {
                YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": depends on lambda arg";
                return false;
            }

            auto column = maybeMember.Cast().Name().Value();
            TString cmpOp = TString{maybeCompare.Cast().Ref().Content()};
            if (!IsRangeComparison(cmpOp)) {
                YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": non-range comparison " << cmpOp;
                return false;
            }

            if (invert) {
                if (TCoCmpStartsWith::CallableName() == cmpOp)
                    return false;

                switch (cmpOp.front()) {
                    case '<': cmpOp.replace(0, 1, 1, '>'); break;
                    case '>': cmpOp.replace(0, 1, 1, '<'); break;
                    default: break;
                }
            }

            ranges.emplace_back();
            ranges.back().emplace(column, std::make_pair(cmpOp, value.Ptr()));
            return true;
        }

        if (auto maybeLookup = predicate.Maybe<TCoLookupBase>()) {
            return CollectKeyPredicatesFromLookup(row, maybeLookup.Cast(), ranges, maxTables);
        }

        if (auto maybeLiteral = predicate.Maybe<TCoCoalesce>().Value().Maybe<TCoBool>().Literal()) {
            if (maybeLiteral.Cast().Value() == "false") {
                if (auto maybeLookup = predicate.Maybe<TCoCoalesce>().Predicate().Maybe<TCoLookupBase>()) {
                    return CollectKeyPredicatesFromLookup(row, maybeLookup.Cast(), ranges, maxTables);
                }
            }
        }

        if (auto maybeNotExists = predicate.Maybe<TCoNot>().Value().Maybe<TCoExists>().Optional()) {
            TMaybeNode<TCoMember> maybeMember = getRowMember(maybeNotExists.Cast());
            if (!maybeMember) {
                YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": Not Exists for non-member";
                return false;
            }
            auto column = maybeMember.Cast().Name().Value();
            ranges.emplace_back();
            ranges.back().emplace(column, std::make_pair(TString{TCoCmpEqual::CallableName()}, TExprNode::TPtr()));
            return true;
        }

        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": unsupported predicate " << predicate.Ref().Content();
        return false;
    }

    static TVector<TYtOutTable> ConvertMultiOutTables(TPositionHandle pos, const TTypeAnnotationNode* outItemType, TExprContext& ctx,
        const TYtState::TPtr& state, const TMultiConstraintNode* multi = nullptr) {
        TVector<TYtOutTable> outTables;
        YQL_ENSURE(outItemType->GetKind() == ETypeAnnotationKind::Variant);
        const TTupleExprType* tupleType = outItemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>();
        size_t ndx = 0;
        const ui64 nativeTypeFlags = state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE;
        for (auto tupleItemType: tupleType->GetItems()) {
            TYtOutTableInfo outTableInfo(tupleItemType->Cast<TStructExprType>(), nativeTypeFlags);
            const TConstraintSet* constraints = multi ? multi->GetItem(ndx) : nullptr;
            if (constraints)
                outTableInfo.RowSpec->SetConstraints(*constraints);
            outTables.push_back(outTableInfo.SetUnique(constraints ? constraints->GetConstraint<TDistinctConstraintNode>() : nullptr, pos, ctx).ToExprNode(ctx, pos).Cast<TYtOutTable>());
            ++ndx;
        }
        return outTables;
    }

    static TVector<TYtOutTable> ConvertOutTables(TPositionHandle pos, const TTypeAnnotationNode* outItemType, TExprContext& ctx,
        const TYtState::TPtr& state, const TConstraintSet* constraint = nullptr) {
        if (outItemType->GetKind() == ETypeAnnotationKind::Variant) {
            return ConvertMultiOutTables(pos, outItemType, ctx, state, constraint ? constraint->GetConstraint<TMultiConstraintNode>() : nullptr);
        }

        TYtOutTableInfo outTableInfo(outItemType->Cast<TStructExprType>(), state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
        if (constraint)
            outTableInfo.RowSpec->SetConstraints(*constraint);
        return TVector<TYtOutTable>{outTableInfo.SetUnique(constraint ? constraint->GetConstraint<TDistinctConstraintNode>() : nullptr, pos, ctx).ToExprNode(ctx, pos).Cast<TYtOutTable>()};
    }

    static TVector<TYtOutTable> ConvertMultiOutTablesWithSortAware(TExprNode::TPtr& lambda, bool& ordered, TPositionHandle pos,
        const TTypeAnnotationNode* outItemType, TExprContext& ctx, const TYtState::TPtr& state, const TConstraintSet& constraints) {

        YQL_ENSURE(outItemType->GetKind() == ETypeAnnotationKind::Variant);

        const ui64 nativeTypeFlags = state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE;
        const bool useNativeDescSort = state->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
        const auto multi = constraints.GetConstraint<TMultiConstraintNode>();
        const TTupleExprType* tupleType = outItemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>();

        ordered = false;
        TVector<TYtOutTable> outTables;
        size_t ndx = 0;
        TVector<TExprBase> switchArgs;
        for (auto tupleItemType: tupleType->GetItems()) {
            const TConstraintSet* itemConstraints = multi ? multi->GetItem(ndx) : nullptr;
            TYtOutTableInfo outTable(tupleItemType->Cast<TStructExprType>(), nativeTypeFlags);
            TExprNode::TPtr remapper;
            if (auto sorted = itemConstraints ? itemConstraints->GetConstraint<TSortedConstraintNode>() : nullptr) {
                TKeySelectorBuilder builder(pos, ctx, useNativeDescSort, tupleItemType->Cast<TStructExprType>());
                builder.ProcessConstraint(*sorted);
                builder.FillRowSpecSort(*outTable.RowSpec);
                if (builder.NeedMap()) {
                    remapper = builder.MakeRemapLambda(true);
                }
                ordered = true;
            }
            if (remapper) {
                if (ndx > 0 && switchArgs.empty()) {
                    for (size_t i = 0; i < ndx; ++i) {
                        switchArgs.push_back(
                            Build<TCoAtomList>(ctx, pos)
                                .Add()
                                    .Value(i)
                                .Build()
                            .Done());
                        switchArgs.push_back(Build<TCoLambda>(ctx, pos).Args({"stream"}).Body("stream").Done());
                    }
                }
                switchArgs.push_back(
                    Build<TCoAtomList>(ctx, pos)
                        .Add()
                            .Value(ndx)
                        .Build()
                    .Done());
                switchArgs.push_back(TExprBase(remapper));
            } else if (!switchArgs.empty()) {
                switchArgs.push_back(
                    Build<TCoAtomList>(ctx, pos)
                        .Add()
                            .Value(ndx)
                        .Build()
                    .Done());
                switchArgs.push_back(Build<TCoLambda>(ctx, pos).Args({"stream"}).Body("stream").Done());
            }
            if (itemConstraints)
                outTable.RowSpec->SetConstraints(*itemConstraints);
            outTables.push_back(outTable
                .SetUnique(itemConstraints ? itemConstraints->GetConstraint<TDistinctConstraintNode>() : nullptr, pos, ctx)
                .ToExprNode(ctx, pos).Cast<TYtOutTable>()
            );
            ++ndx;
        }
        if (!switchArgs.empty()) {
            lambda = Build<TCoLambda>(ctx, pos)
                .Args({"stream"})
                .Body<TCoSwitch>()
                    .Input<TExprApplier>()
                        .Apply(TCoLambda(lambda))
                        .With(0, "stream")
                    .Build()
                    .BufferBytes()
                        .Value(state->Configuration->SwitchLimit.Get().GetOrElse(DEFAULT_SWITCH_MEMORY_LIMIT))
                    .Build()
                    .FreeArgs()
                        .Add(switchArgs)
                    .Build()
                .Build()
                .Done().Ptr();
        }
        return outTables;
    }

    static TYtOutTable ConvertSingleOutTableWithSortAware(TExprNode::TPtr& lambda, bool& ordered, TPositionHandle pos,
        const TTypeAnnotationNode* outItemType, TExprContext& ctx, const TYtState::TPtr& state, const TConstraintSet& constraints) {

        const ui64 nativeTypeFlags = state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE;
        const bool useNativeDescSort = state->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
        const auto outStructType = outItemType->Cast<TStructExprType>();

        ordered = false;
        TYtOutTableInfo outTable(outStructType, nativeTypeFlags);
        if (auto sorted = constraints.GetConstraint<TSortedConstraintNode>()) {
            TKeySelectorBuilder builder(pos, ctx, useNativeDescSort, outStructType);
            builder.ProcessConstraint(*sorted);
            builder.FillRowSpecSort(*outTable.RowSpec);

            if (builder.NeedMap()) {
                lambda = ctx.Builder(pos)
                    .Lambda()
                        .Param("stream")
                        .Apply(builder.MakeRemapLambda(true))
                            .With(0)
                                .Apply(*lambda)
                                    .With(0, "stream")
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                    .Build();
            }
            ordered = true;
        }
        outTable.RowSpec->SetConstraints(constraints);
        outTable.SetUnique(constraints.GetConstraint<TDistinctConstraintNode>(), pos, ctx);
        return outTable.ToExprNode(ctx, pos).Cast<TYtOutTable>();
    }

    static TVector<TYtOutTable> ConvertOutTablesWithSortAware(TExprNode::TPtr& lambda, bool& ordered, TPositionHandle pos,
        const TTypeAnnotationNode* outItemType, TExprContext& ctx, const TYtState::TPtr& state, const TConstraintSet& constraints) {
        TVector<TYtOutTable> outTables;
        if (outItemType->GetKind() == ETypeAnnotationKind::Variant) {
            return ConvertMultiOutTablesWithSortAware(lambda, ordered, pos, outItemType, ctx, state, constraints);
        }

        return TVector<TYtOutTable>{ConvertSingleOutTableWithSortAware(lambda, ordered, pos, outItemType, ctx, state, constraints)};
    }

    static TExprBase WrapOp(TYtOutputOpBase op, TExprContext& ctx) {
        if (op.Output().Size() > 1) {
            return Build<TCoRight>(ctx, op.Pos())
                .Input(op)
                .Done();
        }

        return Build<TYtOutput>(ctx, op.Pos())
            .Operation(op)
            .OutIndex().Value("0").Build()
            .Done();
    }

    static TCoLambda MapEmbedInputFieldsFilter(TCoLambda lambda, bool ordered, TCoAtomList fields, TExprContext& ctx) {
        auto filter = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            ui32 index = 0;
            for (const auto& x : fields) {
                parent
                    .List(index++)
                        .Add(0, x.Ptr())
                        .Callable(1, "Member")
                            .Arg(0, "row")
                            .Add(1, x.Ptr())
                        .Seal()
                    .Seal();
            }

            return parent;
        };

        return TCoLambda(ctx.Builder(lambda.Pos())
            .Lambda()
                .Param("stream")
                .Apply(lambda.Ptr()).With(0)
                    .Callable(ordered ? "OrderedFlatMap" : "FlatMap")
                        .Arg(0, "stream")
                        .Lambda(1)
                            .Param("row")
                            .Callable("Just")
                                .Callable(0, "AsStruct")
                                    .Do(filter)
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Done()
                .Seal()
            .Seal()
            .Build());
    }


    class TYtSortMembersCollection: public TSortMembersCollection {
    public:
        void BuildKeyFilters(TPositionHandle pos, size_t tableCount, size_t orGroupCount, TExprNode::TListType& result, TExprContext& ctx) {
            TExprNode::TListType prefix;
            for (size_t orGroup = 0; orGroup < orGroupCount; ++orGroup) {
                BuildKeyFiltersImpl(pos, tableCount, orGroup, prefix, Members, result, ctx);
            }
        }

    private:
        void BuildKeyFiltersImpl(TPositionHandle pos, size_t tableCount, size_t orGroup, TExprNode::TListType& prefix,
            const TMemberDescrMap& members, TExprNode::TListType& result, TExprContext& ctx)
        {
            for (auto& item: members) {
                size_t prefixLen = prefix.size();

                auto keyPredicateBuilder = Build<TCoNameValueTupleList>(ctx, pos);
                auto iterRange = item.second->Ranges.equal_range(orGroup);
                if (iterRange.first != iterRange.second) {
                    for (auto it = iterRange.first; it != iterRange.second; ++it) {
                        TString cmpOp;
                        TExprNode::TPtr value;
                        const TDataExprType* dataType = nullptr;
                        std::tie(cmpOp, value, dataType) = it->second;

                        if (!value) {
                            keyPredicateBuilder
                                .Add()
                                    .Name()
                                        .Value(cmpOp)
                                    .Build()
                                    .Value<TCoNull>()
                                    .Build()
                                .Build();
                        } else {
                            keyPredicateBuilder
                                .Add()
                                    .Name()
                                        .Value(cmpOp)
                                    .Build()
                                    .Value(value)
                                .Build();
                        }
                    }

                    prefix.push_back(
                        Build<TCoNameValueTuple>(ctx, pos)
                            .Name()
                                .Value(item.first)
                            .Build()
                            .Value(keyPredicateBuilder.Done())
                            .Done().Ptr()
                    );
                }

                if (!item.second->Tables.empty()) {
                    YQL_ENSURE(!prefix.empty());
                    if (item.second->Tables.size() == tableCount) {
                        result.push_back(Build<TCoNameValueTuple>(ctx, pos)
                            .Name()
                                .Value(ToString(EYtSettingType::KeyFilter))
                            .Build()
                            .Value<TExprList>()
                                .Add<TExprList>()
                                    .Add(prefix)
                                .Build()
                            .Build()
                            .Done().Ptr()
                        );
                    }
                    else {
                        for (auto tableNdx: item.second->Tables) {
                            result.push_back(Build<TCoNameValueTuple>(ctx, pos)
                                .Name()
                                    .Value(ToString(EYtSettingType::KeyFilter))
                                .Build()
                                .Value<TExprList>()
                                    .Add<TExprList>()
                                        .Add(prefix)
                                    .Build()
                                    .Add<TCoAtom>()
                                        .Value(ToString(tableNdx))
                                    .Build()
                                .Build()
                                .Done().Ptr()
                            );
                        }
                    }
                }
                YQL_ENSURE(item.second->Tables.size() != tableCount || item.second->NextMembers.empty());
                BuildKeyFiltersImpl(pos, tableCount, orGroup, prefix, item.second->NextMembers, result, ctx);

                prefix.erase(prefix.begin() + prefixLen, prefix.end());
            }
        }
    };

    static bool IsAscending(const TExprNode& node) {
        TMaybe<bool> ascending;
        if (node.IsCallable("Bool")) {
            ascending = IsTrue(node.Child(0)->Content());
        }
        return ascending.Defined() && *ascending;
    }

    static bool CollectSortSet(const TExprNode& sortNode, TSet<TVector<TStringBuf>>& sortSets) {
        if (sortNode.IsCallable("Sort")) {
            auto directions = sortNode.ChildPtr(1);

            auto lambdaArg = sortNode.Child(2)->Child(0)->Child(0);
            auto lambdaBody = sortNode.Child(2)->ChildPtr(1);

            TExprNode::TListType directionItems;
            if (directions->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
                directionItems = directions->ChildrenList();
            } else {
                directionItems.push_back(directions);
            }

            if (AnyOf(directionItems, [](const TExprNode::TPtr& direction) { return !IsAscending(*direction); })) {
                return false;
            }

            TExprNode::TListType lambdaBodyItems;
            if (directions->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
                lambdaBodyItems = lambdaBody->ChildrenList();
            } else {
                lambdaBodyItems.push_back(lambdaBody);
            }

            TVector<TStringBuf> sortBy;
            for (auto& item : lambdaBodyItems) {
                if (!item->IsCallable("Member") || item->Child(0) != lambdaArg) {
                    return false;
                }
                YQL_ENSURE(item->Child(1)->IsAtom());
                sortBy.push_back(item->Child(1)->Content());
            }

            return sortSets.insert(sortBy).second;
        } else if (sortNode.IsCallable("Aggregate")) {
            if (!HasSetting(TCoAggregate(&sortNode).Settings().Ref(), "compact")) {
                return false;
            }
            auto keys = sortNode.Child(1);
            const auto keyNum = keys->ChildrenSize();
            if (keyNum == 0) {
                return false;
            }

            TVector<TStringBuf> keyList;
            keyList.reserve(keys->ChildrenSize());

            for (const auto& key : keys->ChildrenList()) {
                keyList.push_back(key->Content());
            }

            do {
                TVector<TStringBuf> sortBy;
                sortBy.reserve(keyNum);
                copy(keyList.begin(), keyList.end(), std::back_inserter(sortBy));
                sortSets.insert(sortBy);
                if (sortSets.size() > 20) {
                    YQL_CLOG(WARN, ProviderYt) << __FUNCTION__ << ": join's preferred_sort can't have more than 20 key combinations";
                    return true;
                }
            } while(next_permutation(keyList.begin(), keyList.end()));
            sortSets.insert(keyList);

            return true;
        } else {
            return false;
        }
    }

    static TExprNode::TPtr CollectPreferredSortsForEquiJoinOutput(TExprBase join, const TExprNode::TPtr& options,
                                                                  TExprContext& ctx, const TParentsMap& parentsMap)
    {
        auto parentsIt = parentsMap.find(join.Raw());
        if (parentsIt == parentsMap.end()) {
            return options;
        }

        TSet<TVector<TStringBuf>> sortSets = LoadJoinSortSets(*options);
        size_t collected = 0;
        for (auto& parent : parentsIt->second) {
            if (CollectSortSet(*parent, sortSets)) {
                ++collected;
            }
        }

        if (!collected) {
            return options;
        }

        YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": Collected " << collected << " new sorts";

        auto removedOptions = RemoveSetting(*options, "preferred_sort", ctx);
        TExprNode::TListType optionsNodes = removedOptions->ChildrenList();
        AppendEquiJoinSortSets(options->Pos(), sortSets, optionsNodes, ctx);

        return ctx.NewList(options->Pos(), std::move(optionsNodes));
    }

    static bool CanExtraColumnBePulledIntoEquiJoin(const TTypeAnnotationNode* type) {
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }

        switch (type->GetKind()) {
            case ETypeAnnotationKind::Data:
                return IsFixedSizeData(type);
            case ETypeAnnotationKind::Null:
            case ETypeAnnotationKind::Void:
                return true;
            default:
                return false;
        }
    }

    static bool IsYtOrPlainTablePropsDependent(NNodes::TExprBase input) {
        bool found = false;
        VisitExpr(input.Ref(), [&found](const TExprNode& n) {
            found = found || TYtTablePropBase::Match(&n) || TCoTablePropBase::Match(&n);
            return !found;
        });
        return found;
    }

    static bool IsLambdaSuitableForPullingIntoEquiJoin(const TCoFlatMapBase& flatMap, const TExprNode& label,
                                                       const THashMap<TStringBuf, THashSet<TStringBuf>>& tableKeysMap,
                                                       const TExprNode* extractedMembers)
    {
        if (!label.IsAtom()) {
            return false;
        }

        auto inputSeqItem = SilentGetSequenceItemType(flatMap.Input().Ref(), false);
        if (!inputSeqItem || !inputSeqItem->IsPersistable()) {
            return false;
        }

        auto outputSeqItem = SilentGetSequenceItemType(flatMap.Ref(), false);
        if (!outputSeqItem || !outputSeqItem->IsPersistable()) {
            return false;
        }

        if (IsYtOrPlainTablePropsDependent(flatMap.Lambda().Body())) {
            return false;
        }

        // allow only projective FlatMaps
        if (!IsJustOrSingleAsList(flatMap.Lambda().Body().Ref())) {
            return false;
        }

        // all input column should be either renamed, removed or passed as is
        // all join keys should be passed as-is
        // only fixed-size data type can be added
        auto arg = flatMap.Lambda().Args().Arg(0).Raw();
        auto outItem = flatMap.Lambda().Body().Ref().Child(0);

        if (outItem->IsCallable("AsStruct")) {
            size_t joinKeysPassed = 0;
            auto it = tableKeysMap.find(label.Content());
            const size_t joinKeysCount = (it == tableKeysMap.end()) ? 0 : it->second.size();

            TMaybe<THashSet<TStringBuf>> filteredMemberSet;
            if (extractedMembers) {
                filteredMemberSet.ConstructInPlace();
                for (auto member : extractedMembers->ChildrenList()) {
                    YQL_ENSURE(member->IsAtom());
                    filteredMemberSet->insert(member->Content());
                }
            }
            for (auto& item : outItem->Children()) {
                TStringBuf outMemberName = item->Child(0)->Content();
                if (filteredMemberSet && !filteredMemberSet->contains(outMemberName)) {
                    // member will be filtered out by parent ExtractMembers
                    continue;
                }
                if (item->Child(1)->IsCallable("Member") && item->Child(1)->Child(0) == arg) {
                    TStringBuf inMemberName = item->Child(1)->Child(1)->Content();
                    bool isJoinKey = joinKeysCount && it->second.contains(outMemberName);
                    if (isJoinKey && inMemberName != outMemberName) {
                        return false;
                    }
                    joinKeysPassed += isJoinKey;
                } else if (!CanExtraColumnBePulledIntoEquiJoin(item->Child(1)->GetTypeAnn())) {
                    return false;
                }
            }

            YQL_ENSURE(joinKeysPassed <= joinKeysCount);
            return joinKeysPassed == joinKeysCount;
        } else {
            return outItem == arg;
        }
    }

    static bool CanBePulledIntoParentEquiJoin(const TCoFlatMapBase& flatMap, const TGetParents& getParents) {
        const TParentsMap* parents = getParents();
        YQL_ENSURE(parents);

        auto equiJoinParents = CollectEquiJoinOnlyParents(flatMap, *parents);
        if (equiJoinParents.empty()) {
            return false;
        }

        bool suitable = true;
        for (auto it = equiJoinParents.begin(); it != equiJoinParents.end() && suitable; ++it) {
            TCoEquiJoin equiJoin(it->Node);
            auto inputIndex = it->Index;

            auto equiJoinTree = equiJoin.Arg(equiJoin.ArgCount() - 2);
            THashMap<TStringBuf, THashSet<TStringBuf>> tableKeysMap =
                CollectEquiJoinKeyColumnsByLabel(equiJoinTree.Ref());

            auto input = equiJoin.Arg(inputIndex).Cast<TCoEquiJoinInput>();

            suitable = suitable && IsLambdaSuitableForPullingIntoEquiJoin(flatMap, input.Scope().Ref(), tableKeysMap,
                                                                          it->ExtractedMembers);
        }

        return suitable;
    }

    static TExprNode::TPtr BuildYtEquiJoinPremap(TExprBase list, TMaybeNode<TCoLambda> premapLambda, TExprContext& ctx) {
        if (auto type = GetSequenceItemType(list, false, ctx)) {
            if (!EnsurePersistableType(list.Pos(), *type, ctx)) {
                return {};
            }
            if (premapLambda) {
                return premapLambda.Cast().Ptr();
            }
            return Build<TCoVoid>(ctx, list.Pos()).Done().Ptr();
        }
        return {};
    }

    TMaybe<bool> CanFuseLambdas(const TCoLambda& innerLambda, const TCoLambda& outerLambda, TExprContext& ctx) const {
        auto maxJobMemoryLimit = State_->Configuration->MaxExtraJobMemoryToFuseOperations.Get();
        auto maxOperationFiles = State_->Configuration->MaxOperationFiles.Get().GetOrElse(DEFAULT_MAX_OPERATION_FILES);
        TMap<TStringBuf, ui64> memUsage;

        TExprNode::TPtr updatedBody = innerLambda.Body().Ptr();
        if (maxJobMemoryLimit) {
            auto status = UpdateTableContentMemoryUsage(innerLambda.Body().Ptr(), updatedBody, State_, ctx);
            if (status.Level != TStatus::Ok) {
                return {};
            }
        }
        size_t innerFiles = 1; // jobstate. Take into account only once
        ScanResourceUsage(*updatedBody, *State_->Configuration, State_->Types, maxJobMemoryLimit ? &memUsage : nullptr, nullptr, &innerFiles);

        auto prevMemory = Accumulate(memUsage.begin(), memUsage.end(), 0ul,
            [](ui64 sum, const std::pair<const TStringBuf, ui64>& val) { return sum + val.second; });

        updatedBody = outerLambda.Body().Ptr();
        if (maxJobMemoryLimit) {
            auto status = UpdateTableContentMemoryUsage(outerLambda.Body().Ptr(), updatedBody, State_, ctx);
            if (status.Level != TStatus::Ok) {
                return {};
            }
        }
        size_t outerFiles = 0;
        ScanResourceUsage(*updatedBody, *State_->Configuration, State_->Types, maxJobMemoryLimit ? &memUsage : nullptr, nullptr, &outerFiles);

        auto currMemory = Accumulate(memUsage.begin(), memUsage.end(), 0ul,
            [](ui64 sum, const std::pair<const TStringBuf, ui64>& val) { return sum + val.second; });

        if (maxJobMemoryLimit && currMemory != prevMemory && currMemory > *maxJobMemoryLimit) {
            YQL_CLOG(DEBUG, ProviderYt) << "Memory usage: innerLambda=" << prevMemory
                << ", joinedLambda=" << currMemory << ", MaxJobMemoryLimit=" << *maxJobMemoryLimit;
            return false;
        }
        if (innerFiles + outerFiles > maxOperationFiles) {
            YQL_CLOG(DEBUG, ProviderYt) << "Files usage: innerLambda=" << innerFiles
                << ", outerLambda=" << outerFiles << ", MaxOperationFiles=" << maxOperationFiles;
            return false;
        }

        if (auto maxReplcationFactor = State_->Configuration->MaxReplicationFactorToFuseOperations.Get()) {
            double replicationFactor1 = NCommon::GetDataReplicationFactor(innerLambda.Ref(), ctx);
            double replicationFactor2 = NCommon::GetDataReplicationFactor(outerLambda.Ref(), ctx);
            YQL_CLOG(DEBUG, ProviderYt) << "Replication factors: innerLambda=" << replicationFactor1
                << ", outerLambda=" << replicationFactor2 << ", MaxReplicationFactorToFuseOperations=" << *maxReplcationFactor;

            if (replicationFactor1 > 1.0 && replicationFactor2 > 1.0 && replicationFactor1 * replicationFactor2 > *maxReplcationFactor) {
                return false;
            }
        }
        return true;
    }

    // label -> pair(<asc sort keys>, <inputs matched by keys>)
    static THashMap<TStringBuf, std::pair<TVector<TStringBuf>, ui32>> CollectTableSortKeysUsage(const TYtState::TPtr& state, const TCoEquiJoin& equiJoin) {
        THashMap<TStringBuf, std::pair<TVector<TStringBuf>, ui32>> tableSortKeys;
        for (size_t i = 0; i + 2 < equiJoin.ArgCount(); ++i) {
            auto joinInput = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
            auto list = joinInput.List();
            if (joinInput.Scope().Ref().IsAtom()) {
                TVector<TStringBuf> sortKeys;
                if (const auto sorted = list.Ref().GetConstraint<TSortedConstraintNode>()) {
                    for (const auto& key : sorted->GetContent()) {
                        bool appropriate = false;
                        if (key.second && !key.first.empty()) {
                            for (auto& alt: key.first) {
                                if (alt.size() == 1U) {
                                    sortKeys.emplace_back(alt.front());
                                    appropriate = true;
                                    break;
                                }
                            }
                        }
                        if (!appropriate) {
                            break;
                        }
                    }
                }
                tableSortKeys[joinInput.Scope().Ref().Content()] = std::make_pair(std::move(sortKeys), 0);
            }
        }

        // Only Lookup, Merge, and Star strategies use a table sort
        if (!state->Configuration->JoinMergeTablesLimit.Get().GetOrElse(0)
            && !(state->Configuration->LookupJoinLimit.Get().GetOrElse(0) && state->Configuration->LookupJoinMaxRows.Get().GetOrElse(0))
            && !(state->Configuration->JoinEnableStarJoin.Get().GetOrElse(false) && state->Configuration->JoinAllowColumnRenames.Get().GetOrElse(true))
        ) {
            return tableSortKeys;
        }

        TVector<const TExprNode*> joinTreeNodes;
        joinTreeNodes.push_back(equiJoin.Arg(equiJoin.ArgCount() - 2).Raw());
        while (!joinTreeNodes.empty()) {
            const TExprNode* joinTree = joinTreeNodes.back();
            joinTreeNodes.pop_back();

            if (!joinTree->Child(1)->IsAtom()) {
                joinTreeNodes.push_back(joinTree->Child(1));
            }

            if (!joinTree->Child(2)->IsAtom()) {
                joinTreeNodes.push_back(joinTree->Child(2));
            }

            if (joinTree->Child(0)->Content() != "Cross") {
                THashMap<TStringBuf, THashSet<TStringBuf>> tableJoinKeys;
                for (auto keys: {joinTree->Child(3), joinTree->Child(4)}) {
                    for (ui32 i = 0; i < keys->ChildrenSize(); i += 2) {
                        auto tableName = keys->Child(i)->Content();
                        auto column = keys->Child(i + 1)->Content();
                        tableJoinKeys[tableName].insert(column);
                    }
                }

                for (auto& [label, joinKeys]: tableJoinKeys) {
                    if (auto sortKeys = tableSortKeys.FindPtr(label)) {
                        if (joinKeys.size() <= sortKeys->first.size()) {
                            bool matched = true;
                            for (size_t i = 0; i < joinKeys.size(); ++i) {
                                if (!joinKeys.contains(sortKeys->first[i])) {
                                    matched = false;
                                    break;
                                }
                            }
                            if (matched) {
                                ++sortKeys->second;
                            }
                        }
                    }
                }
            }
        }

        return tableSortKeys;
    }

    static TCoLambda FallbackLambdaInput(TCoLambda lambda, TExprContext& ctx) {
        if (const auto narrow = FindNode(lambda.Ref().TailPtr(), [&](const TExprNode::TPtr& node) { return node->IsCallable("NarrowMap") && &lambda.Args().Arg(0).Ref() == &node->Head(); })) {
            return TCoLambda(ctx.DeepCopyLambda(lambda.Ref(), ctx.ReplaceNodes(lambda.Ref().TailPtr(), {{narrow.Get(), narrow->HeadPtr()}})));
        }

        return lambda;
    }

    static TCoLambda FallbackLambdaOutput(TCoLambda lambda, TExprContext& ctx) {
        if (auto body = lambda.Ref().TailPtr(); body->IsCallable("ExpandMap")) {
            return TCoLambda(ctx.DeepCopyLambda(lambda.Ref(), body->HeadPtr()));
        }

        return lambda;
    }

    TCoLambda MakeJobLambdaNoArg(TExprBase content, TExprContext& ctx) const {
        if (State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
            content = Build<TCoToFlow>(ctx, content.Pos()).Input(content).Done();
        } else {
            content = Build<TCoToStream>(ctx, content.Pos()).Input(content).Done();
        }

        return Build<TCoLambda>(ctx, content.Pos()).Args({}).Body(content).Done();
    }

    template<bool WithList>
    TCoLambda MakeJobLambda(TCoLambda lambda, bool useFlow, TExprContext& ctx) const {
        if (useFlow) {
            if constexpr (WithList) {
                return Build<TCoLambda>(ctx, lambda.Pos())
                    .Args({"flow"})
                    .Body<TCoToFlow>()
                        .Input<TExprApplier>()
                            .Apply(lambda)
                            .With<TCoForwardList>(0)
                                .Stream("flow")
                            .Build()
                        .Build()
                    .Build()
                .Done();
            } else {
                return Build<TCoLambda>(ctx, lambda.Pos())
                    .Args({"flow"})
                    .Body<TCoToFlow>()
                        .Input<TExprApplier>()
                            .Apply(lambda)
                            .With<TCoFromFlow>(0)
                                .Input("flow")
                            .Build()
                        .Build()
                    .Build()
                .Done();
            }
        } else {
            if constexpr (WithList) {
                return Build<TCoLambda>(ctx, lambda.Pos())
                    .Args({"stream"})
                    .Body<TCoToStream>()
                        .Input<TExprApplier>()
                            .Apply(lambda)
                            .With<TCoForwardList>(0)
                                .Stream("stream")
                            .Build()
                        .Build()
                    .Build()
                .Done();
            } else {
                return Build<TCoLambda>(ctx, lambda.Pos())
                    .Args({"stream"})
                    .Body<TCoToStream>()
                        .Input<TExprApplier>()
                            .Apply(lambda)
                            .With(0, "stream")
                        .Build()
                    .Build()
                .Done();
            }
        }
    }
private:
    TMaybeNode<TExprBase> Mux(TExprBase node, TExprContext& ctx) const {
        auto mux = node.Cast<TCoMux>();
        const TTypeAnnotationNode* muxItemTypeNode = GetSeqItemType(mux.Ref().GetTypeAnn());
        if (!muxItemTypeNode) {
            return node;
        }
        auto muxItemType = muxItemTypeNode->Cast<TVariantExprType>();
        if (muxItemType->GetUnderlyingType()->GetKind() != ETypeAnnotationKind::Tuple) {
            return node;
        }

        const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
        bool allAreTables = true;
        bool hasTables = false;
        bool allAreTableContents = true;
        bool hasContents = false;
        TString resultCluster;
        TMaybeNode<TYtDSource> dataSource;
        for (auto child: mux.Input().Cast<TExprList>()) {
            bool isTable = IsYtProviderInput(child);
            bool isContent = child.Maybe<TYtTableContent>().IsValid();
            if (!isTable && !isContent) {
                // Don't match foreign provider input
                if (child.Maybe<TCoRight>()) {
                    return node;
                }
            } else {
                if (!dataSource) {
                    dataSource = GetDataSource(child, ctx);
                }

                if (!resultCluster) {
                    resultCluster = TString{dataSource.Cast().Cluster().Value()};
                }
                else if (resultCluster != dataSource.Cast().Cluster().Value()) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                        << "Different source clusters in Mux: " << resultCluster
                        << " and " << dataSource.Cast().Cluster().Value()));
                    return {};
                }
            }
            allAreTables = allAreTables && isTable;
            hasTables = hasTables || isTable;
            allAreTableContents = allAreTableContents && isContent;
            hasContents = hasContents || isContent;
        }

        if (!hasTables && !hasContents) {
            return node;
        }

        auto dataSink = TYtDSink(ctx.RenameNode(dataSource.Ref(), "DataSink"));
        if (allAreTables || allAreTableContents) {
            TVector<TExprBase> worlds;
            TVector<TYtSection> sections;
            for (auto child: mux.Input().Cast<TExprList>()) {
                auto read = child.Maybe<TCoRight>().Input().Maybe<TYtReadTable>();
                if (!read) {
                    read = child.Maybe<TYtTableContent>().Input().Maybe<TYtReadTable>();
                }
                if (read) {
                    YQL_ENSURE(read.Cast().Input().Size() == 1);
                    auto section = read.Cast().Input().Item(0);
                    sections.push_back(section);
                    if (allAreTables) {
                        worlds.push_back(GetWorld(child, {}, ctx));
                    }
                } else {
                    YQL_ENSURE(child.Maybe<TYtOutput>(), "Unknown Mux element: " << child.Ref().Content());
                    sections.push_back(
                        Build<TYtSection>(ctx, child.Pos())
                            .Paths()
                                .Add()
                                    .Table(child) // child is TYtOutput
                                    .Columns<TCoVoid>().Build()
                                    .Ranges<TCoVoid>().Build()
                                    .Stat<TCoVoid>().Build()
                                .Build()
                            .Build()
                            .Settings()
                            .Build()
                            .Done()
                        );
                }
            }

            auto world = worlds.empty()
                ? TExprBase(ctx.NewWorld(mux.Pos()))
                : worlds.size() == 1
                    ? worlds.front()
                    : Build<TCoSync>(ctx, mux.Pos()).Add(worlds).Done();

            auto resRead = Build<TYtReadTable>(ctx, mux.Pos())
                .World(world)
                .DataSource(dataSource.Cast())
                .Input()
                    .Add(sections)
                .Build()
                .Done();

            return allAreTables
                ? Build<TCoRight>(ctx, mux.Pos())
                    .Input(resRead)
                    .Done().Cast<TExprBase>()
                : Build<TYtTableContent>(ctx, mux.Pos())
                    .Input(resRead)
                    .Settings().Build()
                    .Done().Cast<TExprBase>();
        }

        if (!hasTables) {
            return node;
        }

        TVector<TExprBase> newMuxParts;
        for (auto child: mux.Input().Cast<TExprList>()) {
            if (!IsYtProviderInput(child)) {
                if (State_->Types->EvaluationInProgress) {
                    return node;
                }
                TSyncMap syncList;
                if (!IsYtCompleteIsolatedLambda(child.Ref(), syncList, resultCluster, true, false)) {
                    return node;
                }

                const TStructExprType* outItemType = nullptr;
                if (auto type = GetSequenceItemType(child, false, ctx)) {
                    if (!EnsurePersistableType(child.Pos(), *type, ctx)) {
                        return {};
                    }
                    outItemType = type->Cast<TStructExprType>();
                } else {
                    return {};
                }

                TYtOutTableInfo outTable(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
                auto content = child;
                if (auto sorted = child.Ref().GetConstraint<TSortedConstraintNode>()) {
                    TKeySelectorBuilder builder(child.Pos(), ctx, useNativeDescSort, outItemType);
                    builder.ProcessConstraint(*sorted);
                    builder.FillRowSpecSort(*outTable.RowSpec);

                    if (builder.NeedMap()) {
                        content = Build<TExprApplier>(ctx, child.Pos())
                            .Apply(TCoLambda(builder.MakeRemapLambda(true)))
                            .With(0, content)
                            .Done();
                    }

                } else if (auto unordered = content.Maybe<TCoUnorderedBase>()) {
                    content = unordered.Cast().Input();
                }
                outTable.RowSpec->SetConstraints(child.Ref().GetConstraintSet());
                outTable.SetUnique(child.Ref().GetConstraint<TDistinctConstraintNode>(), child.Pos(), ctx);

                auto cleanup = CleanupWorld(content, ctx);
                if (!cleanup) {
                    return {};
                }

                newMuxParts.push_back(
                    Build<TYtOutput>(ctx, child.Pos())
                        .Operation<TYtFill>()
                            .World(ApplySyncListToWorld(ctx.NewWorld(child.Pos()), syncList, ctx))
                            .DataSink(dataSink)
                            .Content(MakeJobLambdaNoArg(cleanup.Cast(), ctx))
                            .Output()
                                .Add(outTable.ToExprNode(ctx, child.Pos()).Cast<TYtOutTable>())
                            .Build()
                            .Settings(GetFlowSettings(child.Pos(), *State_, ctx))
                        .Build()
                        .OutIndex().Value(0U).Build()
                    .Done()
                );
            }
            else {
                newMuxParts.push_back(child);
            }
        }

        return Build<TCoMux>(ctx, mux.Pos())
            .Input<TExprList>()
                .Add(newMuxParts)
            .Build()
            .Done();
    }

    template<bool IsTop>
    TMaybeNode<TExprBase> Sort(TExprBase node, TExprContext& ctx) const {
        if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
            return node;
        }

        const auto sort = node.Cast<std::conditional_t<IsTop, TCoTopBase, TCoSort>>();
        if (!IsYtProviderInput(sort.Input())) {
            return node;
        }

        auto sortDirections = sort.SortDirections();
        if (!IsConstExpSortDirections(sortDirections)) {
            return node;
        }

        auto keySelectorLambda = sort.KeySelectorLambda();
        auto cluster = TString{GetClusterName(sort.Input())};
        TSyncMap syncList;
        if (!IsYtCompleteIsolatedLambda(keySelectorLambda.Ref(), syncList, cluster, true, false)) {
            return node;
        }

        const TStructExprType* outType = nullptr;
        if (auto type = GetSequenceItemType(node, false, ctx)) {
            outType = type->Cast<TStructExprType>();
        } else {
            return {};
        }

        TVector<TYtPathInfo::TPtr> inputInfos = GetInputPaths(sort.Input());

        TMaybe<NYT::TNode> firstNativeType;
        if (!inputInfos.empty()) {
            firstNativeType = inputInfos.front()->GetNativeYtType();
        }
        auto maybeReadSettings = sort.Input().template Maybe<TCoRight>().Input().template Maybe<TYtReadTable>().Input().Item(0).Settings();
        const ui64 nativeTypeFlags = State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES)
             ? GetNativeYtTypeFlags(*outType)
             : 0ul;
        const bool needMap = (maybeReadSettings && NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::SysColumns))
            || AnyOf(inputInfos, [nativeTypeFlags, firstNativeType] (const TYtPathInfo::TPtr& path) {
                return path->RequiresRemap()
                    || nativeTypeFlags != path->GetNativeYtTypeFlags()
                    || firstNativeType != path->GetNativeYtType();
            });

        bool useExplicitColumns = AnyOf(inputInfos, [] (const TYtPathInfo::TPtr& path) {
            return !path->Table->IsTemp || (path->Table->RowSpec && path->Table->RowSpec->HasAuxColumns());
        });

        const bool needMerge = maybeReadSettings && NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::Sample);

        const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);

        TKeySelectorBuilder builder(node.Pos(), ctx, useNativeDescSort, outType);
        builder.ProcessKeySelector(keySelectorLambda.Ptr(), sortDirections.Ptr());

        TYtOutTableInfo sortOut(outType, nativeTypeFlags);
        builder.FillRowSpecSort(*sortOut.RowSpec);
        sortOut.SetUnique(sort.Ref().template GetConstraint<TDistinctConstraintNode>(), node.Pos(), ctx);

        TExprBase sortInput = sort.Input();
        TExprBase world = TExprBase(ApplySyncListToWorld(GetWorld(sortInput, {}, ctx).Ptr(), syncList, ctx));
        bool unordered = ctx.IsConstraintEnabled<TSortedConstraintNode>();
        if (needMap || builder.NeedMap()) {
            auto mapper = builder.MakeRemapLambda();

            auto mapperClean = CleanupWorld(TCoLambda(mapper), ctx);
            if (!mapperClean) {
                return {};
            }

            TYtOutTableInfo mapOut(builder.MakeRemapType(), nativeTypeFlags);
            mapOut.SetUnique(sort.Ref().template GetConstraint<TDistinctConstraintNode>(), node.Pos(), ctx);

            sortInput = Build<TYtOutput>(ctx, node.Pos())
                .Operation<TYtMap>()
                    .World(world)
                    .DataSink(GetDataSink(sort.Input(), ctx))
                    .Input(ConvertInputTable(sort.Input(), ctx, TConvertInputOpts().MakeUnordered(unordered)))
                    .Output()
                        .Add(mapOut.ToExprNode(ctx, node.Pos()).Cast<TYtOutTable>())
                    .Build()
                    .Settings(GetFlowSettings(node.Pos(), *State_, ctx))
                    .Mapper(mapperClean.Cast())
                .Build()
                .OutIndex()
                    .Value(0U)
                .Build()
                .Done();
            world = TExprBase(ctx.NewWorld(node.Pos()));
            unordered = false;
        }
        else if (needMerge) {
            TYtOutTableInfo mergeOut(outType, nativeTypeFlags);
            mergeOut.SetUnique(sort.Ref().template GetConstraint<TDistinctConstraintNode>(), node.Pos(), ctx);
            if (firstNativeType) {
                mergeOut.RowSpec->CopyTypeOrders(*firstNativeType);
                sortOut.RowSpec->CopyTypeOrders(*firstNativeType);
            }

            TConvertInputOpts opts;
            if (useExplicitColumns) {
                opts.ExplicitFields(*mergeOut.RowSpec, node.Pos(), ctx);
                useExplicitColumns = false;
            }

            sortInput = Build<TYtOutput>(ctx, node.Pos())
                .Operation<TYtMerge>()
                    .World(world)
                    .DataSink(GetDataSink(sort.Input(), ctx))
                    .Input(ConvertInputTable(sort.Input(), ctx, opts.MakeUnordered(unordered)))
                    .Output()
                        .Add(mergeOut.ToExprNode(ctx, node.Pos()).Cast<TYtOutTable>())
                    .Build()
                    .Settings()
                        .Add()
                            .Name()
                                .Value(ToString(EYtSettingType::ForceTransform), TNodeFlags::Default)
                            .Build()
                        .Build()
                    .Build()
                .Build()
                .OutIndex()
                    .Value(0U)
                .Build()
                .Done();
            world = TExprBase(ctx.NewWorld(node.Pos()));
            unordered = false;
        } else if (firstNativeType) {
            sortOut.RowSpec->CopyTypeOrders(*firstNativeType);
        }

        bool canUseMerge = !needMap && !needMerge;
        if (auto maxTablesForSortedMerge = State_->Configuration->MaxInputTablesForSortedMerge.Get()) {
            if (inputInfos.size() > *maxTablesForSortedMerge) {
                canUseMerge = false;
            }
        }

        if (canUseMerge) {
            TYqlRowSpecInfo commonSorted = *sortOut.RowSpec;
            for (auto& pathInfo: inputInfos) {
                commonSorted.MakeCommonSortness(*pathInfo->Table->RowSpec);
            }
            // input is sorted at least as strictly as output
            if (!sortOut.RowSpec->CompareSortness(commonSorted)) {
                canUseMerge = false;
            }
        }

        sortOut.RowSpec->SetConstraints(sort.Ref().GetConstraintSet());

        TConvertInputOpts opts;
        if (useExplicitColumns) {
            opts.ExplicitFields(*sortOut.RowSpec, node.Pos(), ctx);
        }

        auto res = canUseMerge ?
            TExprBase(Build<TYtMerge>(ctx, node.Pos())
                .World(world)
                .DataSink(GetDataSink(sortInput, ctx))
                .Input(ConvertInputTable(sortInput, ctx, opts.ClearUnordered()))
                .Output()
                    .Add(sortOut.ToExprNode(ctx, node.Pos()).Cast<TYtOutTable>())
                .Build()
                .Settings()
                    .Add()
                        .Name()
                            .Value(ToString(EYtSettingType::KeepSorted), TNodeFlags::Default)
                        .Build()
                    .Build()
                .Build()
            .Done()):
            TExprBase(Build<TYtSort>(ctx, node.Pos())
                .World(world)
                .DataSink(GetDataSink(sortInput, ctx))
                .Input(ConvertInputTable(sortInput, ctx, opts.MakeUnordered(unordered)))
                .Output()
                    .Add(sortOut.ToExprNode(ctx, node.Pos()).Cast<TYtOutTable>())
                .Build()
                .Settings().Build()
            .Done());

        res = Build<TYtOutput>(ctx, node.Pos())
            .Operation(res)
            .OutIndex().Value(0U).Build()
            .Done();


        if constexpr (IsTop) {
            res = Build<TCoTake>(ctx, node.Pos())
                .Input(res)
                .Count(sort.Count())
                .Done();
        }

        return res;
    }

    TMaybeNode<TExprBase> YtSortOverAlreadySorted(TExprBase node, TExprContext& ctx) const {
        auto sort = node.Cast<TYtSort>();

        if (auto maxTablesForSortedMerge = State_->Configuration->MaxInputTablesForSortedMerge.Get()) {
            if (sort.Input().Item(0).Paths().Size() > *maxTablesForSortedMerge) {
                return node;
            }
        }

        const TYqlRowSpecInfo outRowSpec(sort.Output().Item(0).RowSpec());

        TYqlRowSpecInfo commonSorted = outRowSpec;
        auto section = sort.Input().Item(0);
        for (auto path: section.Paths()) {
            commonSorted.MakeCommonSortness(*TYtTableBaseInfo::GetRowSpec(path.Table()));
        }

        if (outRowSpec.CompareSortness(commonSorted)) {
            // input is sorted at least as strictly as output
            auto res = ctx.RenameNode(sort.Ref(), TYtMerge::CallableName());
            res = ctx.ChangeChild(*res, TYtMerge::idx_Settings,
                Build<TCoNameValueTupleList>(ctx, sort.Pos())
                    .Add()
                        .Name()
                            .Value(ToString(EYtSettingType::KeepSorted))
                        .Build()
                    .Build()
                .Done().Ptr()
            );
            section = ClearUnorderedSection(section, ctx);
            if (section.Ptr() != sort.Input().Item(0).Ptr()) {
                res = ctx.ChangeChild(*res, TYtMerge::idx_Input, Build<TYtSectionList>(ctx, sort.Input().Pos()).Add(section).Done().Ptr());
            }
            return TExprBase(res);
        }

        return node;
    }

    TMaybeNode<TExprBase> PartitionByKey(TExprBase node, TExprContext& ctx) const {
        if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
            return node;
        }

        auto partByKey = node.Cast<TCoPartitionByKeyBase>();

        TExprBase input = partByKey.Input();
        TCoLambda keySelectorLambda = partByKey.KeySelectorLambda();
        TCoLambda handlerLambda = partByKey.ListHandlerLambda();

        if (!IsYtProviderInput(input, true)) {
            return node;
        }

        auto outItemType = SilentGetSequenceItemType(handlerLambda.Body().Ref(), true);
        if (!outItemType || !outItemType->IsPersistable()) {
            return node;
        }

        auto cluster = TString{GetClusterName(input)};
        TSyncMap syncList;
        if (!IsYtCompleteIsolatedLambda(keySelectorLambda.Ref(), syncList, cluster, true, false)
            || !IsYtCompleteIsolatedLambda(handlerLambda.Ref(), syncList, cluster, true, false)) {
            return node;
        }

        const auto inputItemType = GetSequenceItemType(input, true, ctx);
        if (!inputItemType) {
            return {};
        }
        const bool multiInput = (inputItemType->GetKind() == ETypeAnnotationKind::Variant);
        bool useSystemColumns = State_->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS);
        const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);

        TVector<TYtPathInfo::TPtr> inputPaths = GetInputPaths(input);
        bool needMap = AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) {
            return path->RequiresRemap();
        });

        bool forceMapper = false;
        if (auto maybeRead = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
            forceMapper = AnyOf(maybeRead.Cast().Input(), [] (const TYtSection& section) {
                return NYql::HasSetting(section.Settings().Ref(), EYtSettingType::SysColumns);
            });
        }

        if (!multiInput) {
            const ui64 nativeTypeFlags = State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES)
                ? GetNativeYtTypeFlags(*inputItemType->Cast<TStructExprType>())
                : 0ul;

            TMaybe<NYT::TNode> firstNativeType;
            if (!inputPaths.empty()) {
                firstNativeType = inputPaths.front()->GetNativeYtType();
            }

            forceMapper = forceMapper || AnyOf(inputPaths, [nativeTypeFlags, firstNativeType] (const TYtPathInfo::TPtr& path) {
                return nativeTypeFlags != path->GetNativeYtTypeFlags()
                    || firstNativeType != path->GetNativeYtType();
            });
        }

        bool useExplicitColumns = AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) {
            return !path->Table->IsTemp
                || (path->Table->RowSpec && path->Table->RowSpec->HasAuxColumns());
        });

        TKeySelectorBuilder builder(node.Pos(), ctx, useNativeDescSort, inputItemType);
        builder.ProcessKeySelector(keySelectorLambda.Ptr(), {}, true);

        TVector<std::pair<TString, bool>> reduceByColumns = builder.ForeignSortColumns();
        TVector<std::pair<TString, bool>> sortByColumns;

        if (!partByKey.SortDirections().Maybe<TCoVoid>()) {
            TExprBase sortDirections = partByKey.SortDirections();
            if (!IsConstExpSortDirections(sortDirections)) {
                return node;
            }

            TCoLambda sortKeySelectorLambda = partByKey.SortKeySelectorLambda().Cast<TCoLambda>();
            if (!IsYtCompleteIsolatedLambda(sortKeySelectorLambda.Ref(), syncList, cluster, true, false)) {
                return node;
            }

            builder.ProcessKeySelector(sortKeySelectorLambda.Ptr(), sortDirections.Ptr());
            sortByColumns = builder.ForeignSortColumns();
        }

        TExprBase mapper = Build<TCoVoid>(ctx, node.Pos()).Done();
        needMap = needMap || builder.NeedMap();

        bool hasInputSampling = false;
        // Read sampling settings from the first section only, because all sections should have the same sampling settings
        if (auto maybeReadSettings = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>().Input().Item(0).Settings()) {
            hasInputSampling = NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::Sample);
        }

        bool canUseReduce = !needMap;
        if (canUseReduce) {
            TVector<std::pair<TString, bool>> sortPrefix;
            for (auto& pathInfo: inputPaths) {
                if (pathInfo->Table->IsUnordered
                    || !pathInfo->Table->RowSpec
                    || !pathInfo->Table->RowSpec->IsSorted()
                    || pathInfo->Table->RowSpec->SortedBy.size() < builder.Columns().size())
                {
                    canUseReduce = false;
                    break;
                }
                if (sortPrefix.empty()) {
                    // reduceBy columns can be in any order, with any ascending
                    THashMap<TString, bool> partColumnSet(reduceByColumns.begin(), reduceByColumns.end());
                    auto sortedBy = pathInfo->Table->RowSpec->GetForeignSort();
                    const bool equalReduceByPrefix = AllOf(sortedBy.begin(), sortedBy.begin() + reduceByColumns.size(),
                        [&partColumnSet](const std::pair<TString, bool>& c) {
                            return partColumnSet.contains(c.first);
                        });

                    // sortBy suffix should exactly match
                    const bool equalSortBySuffix = equalReduceByPrefix && (sortByColumns.empty()
                        || std::equal(sortByColumns.begin() + reduceByColumns.size(), sortByColumns.end(), sortedBy.begin() + reduceByColumns.size()));

                    if (equalSortBySuffix) {
                        sortPrefix.assign(sortedBy.begin(), sortedBy.begin() + builder.Columns().size());
                        // All other tables should have the same sort order as the first one
                    } else {
                        canUseReduce = false;
                        break;
                    }
                } else {
                    auto sortedBy = pathInfo->Table->RowSpec->GetForeignSort();
                    if (!std::equal(sortPrefix.begin(), sortPrefix.end(), sortedBy.begin())) {
                        canUseReduce = false;
                        break;
                    }
                }
            }
            if (canUseReduce) {
                const auto reduceBySize = reduceByColumns.size();
                reduceByColumns.assign(sortPrefix.begin(), sortPrefix.begin() + reduceBySize);
                if (!sortByColumns.empty()) {
                    sortByColumns = std::move(sortPrefix);
                }
            }
        }

        const bool canUseMapInsteadOfReduce = keySelectorLambda.Body().Ref().IsComplete() &&
            partByKey.SortDirections().Maybe<TCoVoid>() &&
            State_->Configuration->PartitionByConstantKeysViaMap.Get().GetOrElse(DEFAULT_PARTITION_BY_CONSTANT_KEYS_VIA_MAP);

        if (canUseMapInsteadOfReduce) {
            YQL_ENSURE(!canUseReduce);
            YQL_ENSURE(sortByColumns.empty());
            useSystemColumns = false;
        }

        auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, node.Pos());
        if (!canUseMapInsteadOfReduce) {
            settingsBuilder
                .Add()
                    .Name().Value(ToString(EYtSettingType::ReduceBy)).Build()
                    .Value(TExprBase(ToColumnPairList(reduceByColumns, node.Pos(), ctx)))
                .Build();
        }

        if (!sortByColumns.empty()) {
            settingsBuilder
                .Add()
                    .Name().Value(ToString(EYtSettingType::SortBy)).Build()
                    .Value(TExprBase(ToColumnPairList(sortByColumns, node.Pos(), ctx)))
                .Build();
        }
        if (!canUseReduce && multiInput) {
            needMap = true; // YtMapReduce with empty mapper doesn't support table indices
        }

        bool useReduceFlow = State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW);
        bool useMapFlow = useReduceFlow;

        const bool newPartsByKeys = bool(partByKey.Maybe<TCoPartitionsByKeys>());

        // Convert reduce output to stream
        if (newPartsByKeys) {
            if (useSystemColumns) {
                TNodeSet nodesToOptimize;
                TProcessedNodesSet processedNodes;
                auto arg = handlerLambda.Args().Arg(0).Ptr();
                VisitExpr(handlerLambda.Body().Ptr(), [&nodesToOptimize, &processedNodes, arg](const TExprNode::TPtr& node) {
                    if (TMaybeNode<TCoChopper>(node).GroupSwitch().Body().Maybe<TCoIsKeySwitch>()) {
                        if (IsDepended(node->Head(), *arg)) {
                            nodesToOptimize.insert(node.Get());
                        }
                    }
                    else if (TMaybeNode<TCoCondense1>(node).SwitchHandler().Body().Maybe<TCoIsKeySwitch>()) {
                        if (IsDepended(node->Head(), *arg)) {
                            nodesToOptimize.insert(node.Get());
                        }
                    }
                    else if (TMaybeNode<TCoCondense>(node).SwitchHandler().Body().Maybe<TCoIsKeySwitch>()) {
                        if (IsDepended(node->Head(), *arg)) {
                            nodesToOptimize.insert(node.Get());
                        }
                    }
                    else if (TYtOutput::Match(node.Get())) {
                        // Stop traversing dependent operations
                        processedNodes.insert(node->UniqueId());
                        return false;
                    }
                    return true;
                });

                if (!nodesToOptimize.empty()) {
                    TOptimizeExprSettings settings(State_->Types);
                    settings.ProcessedNodes = &processedNodes; // Prevent optimizer to go deeper than current operation
                    TExprNode::TPtr newBody = handlerLambda.Body().Ptr();
                    auto status = OptimizeExpr(newBody, newBody, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                        if (nodesToOptimize.find(node.Get()) == nodesToOptimize.end()) {
                            return node;
                        }

                        if (auto maybeChopper = TMaybeNode<TCoChopper>(node)) {
                            auto chopper = maybeChopper.Cast();

                            auto chopperSwitch = ctx.Builder(chopper.GroupSwitch().Pos())
                                .Lambda()
                                    .Param("key")
                                    .Param("item")
                                    .Callable("SqlExtractKey")
                                        .Arg(0, "item")
                                        .Lambda(1)
                                            .Param("row")
                                            .Callable("Member")
                                                .Arg(0, "row")
                                                .Atom(1, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                .Seal()
                                .Build();

                            TExprNode::TPtr chopperHandler;
                            TExprNode::TPtr chopperKeyExtract;
                            if (!canUseReduce && multiInput) {
                                chopperKeyExtract = ctx.Builder(chopper.Handler().Pos())
                                    .Lambda()
                                        .Param("item")
                                        .Apply(chopper.KeyExtractor().Ptr())
                                            .With(0)
                                                .Callable("Member")
                                                    .Arg(0, "item")
                                                    .Atom(1, "_yql_original_row", TNodeFlags::Default)
                                                .Seal()
                                            .Done()
                                        .Seal()
                                    .Seal()
                                    .Build();

                                chopperHandler = ctx.Builder(chopper.Handler().Pos())
                                    .Lambda()
                                        .Param("key")
                                        .Param("group")
                                        .Apply(chopper.Handler().Ptr())
                                            .With(0, "key")
                                            .With(1)
                                                .Callable("Map")
                                                    .Arg(0, "group")
                                                    .Lambda(1)
                                                        .Param("row")
                                                        .Callable("Member")
                                                            .Arg(0, "row")
                                                            .Atom(1, "_yql_original_row", TNodeFlags::Default)
                                                        .Seal()
                                                    .Seal()
                                                .Seal()
                                            .Done()
                                        .Seal()
                                    .Seal()
                                    .Build();
                            } else {
                                chopperKeyExtract = ctx.Builder(chopper.Handler().Pos())
                                    .Lambda()
                                        .Param("item")
                                        .Apply(chopper.KeyExtractor().Ptr())
                                            .With(0)
                                                .Callable("RemovePrefixMembers")
                                                    .Arg(0, "item")
                                                    .List(1)
                                                        .Atom(0, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                                    .Seal()
                                                .Seal()
                                            .Done()
                                        .Seal()
                                    .Seal()
                                    .Build();

                                chopperHandler = ctx.Builder(chopper.Handler().Pos())
                                    .Lambda()
                                        .Param("key")
                                        .Param("group")
                                        .Apply(chopper.Handler().Ptr())
                                            .With(0, "key")
                                            .With(1)
                                                .Callable("RemovePrefixMembers")
                                                    .Arg(0, "group")
                                                    .List(1)
                                                        .Atom(0, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                                    .Seal()
                                                .Seal()
                                            .Done()
                                        .Seal()
                                    .Seal()
                                    .Build();
                            }

                            TNodeOnNodeOwnedMap deepClones;
                            return Build<TCoChopper>(ctx, chopper.Pos())
                                .Input(chopper.Input())
                                .KeyExtractor(chopperKeyExtract)
                                .GroupSwitch(chopperSwitch)
                                .Handler(chopperHandler)
                                .Done().Ptr();
                        }
                        else if (auto maybeCondense = TMaybeNode<TCoCondense1>(node)) {
                            auto condense = maybeCondense.Cast();
                            auto switchHandler = ctx.Builder(condense.SwitchHandler().Pos())
                                .Lambda()
                                    .Param("item")
                                    .Param("state")
                                    .Callable("SqlExtractKey")
                                        .Arg(0, "item")
                                        .Lambda(1)
                                            .Param("row")
                                            .Callable("Member")
                                                .Arg(0, "row")
                                                .Atom(1, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                .Seal()
                                .Build();

                            TExprNode::TPtr initHandler;
                            TExprNode::TPtr updateHandler;

                            if (!canUseReduce && multiInput) {
                                initHandler = ctx.Builder(condense.InitHandler().Pos())
                                    .Lambda()
                                        .Param("item")
                                        .Apply(condense.InitHandler().Ptr())
                                            .With(0)
                                                .Callable("Member")
                                                    .Arg(0, "item")
                                                    .Atom(1, "_yql_original_row", TNodeFlags::Default)
                                                .Seal()
                                            .Done()
                                        .Seal()
                                    .Seal()
                                    .Build();

                                updateHandler = ctx.Builder(condense.UpdateHandler().Pos())
                                    .Lambda()
                                        .Param("item")
                                        .Param("state")
                                        .Apply(condense.UpdateHandler().Ptr())
                                            .With(0)
                                                .Callable("Member")
                                                    .Arg(0, "item")
                                                    .Atom(1, "_yql_original_row", TNodeFlags::Default)
                                                .Seal()
                                            .Done()
                                            .With(1, "state")
                                        .Seal()
                                    .Seal()
                                    .Build();
                            } else {
                                initHandler = ctx.Builder(condense.InitHandler().Pos())
                                    .Lambda()
                                        .Param("item")
                                        .Apply(condense.InitHandler().Ptr())
                                            .With(0)
                                                .Callable("RemovePrefixMembers")
                                                    .Arg(0, "item")
                                                    .List(1)
                                                        .Atom(0, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                                    .Seal()
                                                .Seal()
                                            .Done()
                                        .Seal()
                                    .Seal()
                                    .Build();

                                updateHandler = ctx.Builder(condense.UpdateHandler().Pos())
                                    .Lambda()
                                        .Param("item")
                                        .Param("state")
                                        .Apply(condense.UpdateHandler().Ptr())
                                            .With(0)
                                                .Callable("RemovePrefixMembers")
                                                    .Arg(0, "item")
                                                    .List(1)
                                                        .Atom(0, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                                    .Seal()
                                                .Seal()
                                            .Done()
                                            .With(1, "state")
                                        .Seal()
                                    .Seal()
                                    .Build();
                            }

                            return Build<TCoCondense1>(ctx, condense.Pos())
                                .Input(condense.Input())
                                .InitHandler(initHandler)
                                .SwitchHandler(switchHandler)
                                .UpdateHandler(updateHandler)
                                .Done().Ptr();
                        }
                        else if (auto maybeCondense = TMaybeNode<TCoCondense>(node)) {
                            auto condense = maybeCondense.Cast();

                            auto switchHandler = ctx.Builder(condense.SwitchHandler().Pos())
                                .Lambda()
                                    .Param("item")
                                    .Param("state")
                                    .Callable("SqlExtractKey")
                                        .Arg(0, "item")
                                        .Lambda(1)
                                            .Param("row")
                                            .Callable("Member")
                                                .Arg(0, "row")
                                                .Atom(1, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                .Seal()
                                .Build();

                            TExprNode::TPtr updateHandler;
                            if (!canUseReduce && multiInput) {
                                updateHandler = ctx.Builder(condense.UpdateHandler().Pos())
                                    .Lambda()
                                        .Param("item")
                                        .Param("state")
                                        .Apply(condense.UpdateHandler().Ptr())
                                            .With(0)
                                                .Callable("Member")
                                                    .Arg(0, "item")
                                                    .Atom(1, "_yql_original_row", TNodeFlags::Default)
                                                .Seal()
                                            .Done()
                                            .With(1, "state")
                                        .Seal()
                                    .Seal()
                                    .Build();
                            } else {
                                updateHandler = ctx.Builder(condense.UpdateHandler().Pos())
                                    .Lambda()
                                        .Param("item")
                                        .Param("state")
                                        .Apply(condense.UpdateHandler().Ptr())
                                            .With(0)
                                                .Callable("RemovePrefixMembers")
                                                    .Arg(0, "item")
                                                    .List(1)
                                                        .Atom(0, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                                    .Seal()
                                                .Seal()
                                            .Done()
                                            .With(1, "state")
                                        .Seal()
                                    .Seal()
                                    .Build();
                            }

                            return Build<TCoCondense>(ctx, condense.Pos())
                                .Input(condense.Input())
                                .State(condense.State())
                                .SwitchHandler(switchHandler)
                                .UpdateHandler(updateHandler)
                                .Done().Ptr();
                        }

                        return node;
                    }, ctx, settings);

                    if (status.Level == TStatus::Error) {
                        return {};
                    }

                    if (status.Level == TStatus::Ok) {
                        useSystemColumns = false;
                    }
                    else {
                        handlerLambda = Build<TCoLambda>(ctx, handlerLambda.Pos())
                            .Args({TStringBuf("stream")})
                            .Body<TExprApplier>()
                                .Apply(TExprBase(newBody))
                                .With(handlerLambda.Args().Arg(0), TStringBuf("stream"))
                            .Build()
                            .Done();
                    }
                }
                else {
                    useSystemColumns = false;
                }
            }

            if (!useSystemColumns) {
                auto preReduceLambda = Build<TCoLambda>(ctx, handlerLambda.Pos())
                    .Args({"stream"})
                    .Body("stream")
                    .Done();

                if (!canUseReduce && multiInput) {
                    preReduceLambda = Build<TCoLambda>(ctx, handlerLambda.Pos())
                        .Args({"stream"})
                        .Body<TCoFlatMap>()
                            .Input("stream")
                            .Lambda()
                                .Args({"item"})
                                .Body<TCoJust>()
                                    .Input<TCoMember>()
                                        .Struct("item")
                                        .Name()
                                            .Value("_yql_original_row")
                                        .Build()
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                        .Done();
                }

                handlerLambda = Build<TCoLambda>(ctx, handlerLambda.Pos())
                    .Args({"stream"})
                    .Body<TExprApplier>()
                        .Apply(handlerLambda)
                        .With<TExprApplier>(0)
                            .Apply(preReduceLambda)
                            .With(0, "stream")
                        .Build()
                    .Build()
                    .Done();
            }
        }
        else {
            TExprNode::TPtr groupSwitch;
            TExprNode::TPtr keyExtractor;
            TExprNode::TPtr handler;

            if (canUseMapInsteadOfReduce) {
                groupSwitch = Build<TCoLambda>(ctx, handlerLambda.Pos())
                    .Args({"key", "item"})
                    .Body<TCoBool>()
                        .Literal().Build("false")
                    .Build()
                    .Done().Ptr();
            } else if (useSystemColumns) {
                groupSwitch = Build<TCoLambda>(ctx, handlerLambda.Pos())
                    .Args({"key", "item"})
                    .Body<TCoSqlExtractKey>()
                        .Item("item")
                        .Extractor()
                            .Args({"row"})
                            .Body<TCoMember>()
                                .Struct("row")
                                .Name()
                                    .Value(YqlSysColumnKeySwitch, TNodeFlags::Default)
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
            } else {
                groupSwitch = Build<TCoLambda>(ctx, handlerLambda.Pos())
                    .Args({"key", "item"})
                    .Body<TYtIsKeySwitch>()
                        .DependsOn()
                            .Input("item")
                        .Build()
                    .Build()
                    .Done().Ptr();
            }

            if (!canUseReduce && multiInput) {
                keyExtractor = Build<TCoLambda>(ctx, handlerLambda.Pos())
                    .Args({"item"})
                    .Body<TExprApplier>()
                        .Apply(keySelectorLambda)
                        .With<TCoMember>(0)
                            .Struct("item")
                            .Name()
                                .Value("_yql_original_row")
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();

                handler = Build<TCoLambda>(ctx, handlerLambda.Pos())
                    .Args({"item"})
                    .Body<TCoMember>()
                        .Struct("item")
                        .Name()
                            .Value("_yql_original_row")
                        .Build()
                    .Build()
                    .Done().Ptr();
            } else {
                keyExtractor = Build<TCoLambda>(ctx, handlerLambda.Pos())
                    .Args({"item"})
                    .Body<TExprApplier>()
                        .Apply(keySelectorLambda)
                        .With(0, "item")
                    .Build()
                    .Done().Ptr();

                if (canUseMapInsteadOfReduce) {
                    handler = BuildIdentityLambda(handlerLambda.Pos(), ctx).Ptr();
                } else {
                    handler = Build<TCoLambda>(ctx, handlerLambda.Pos())
                        .Args({"item"})
                        .Body<TCoRemovePrefixMembers>()
                            .Input("item")
                            .Prefixes()
                                .Add()
                                    .Value(YqlSysColumnKeySwitch)
                                .Build()
                            .Build()
                        .Build()
                        .Done().Ptr();
                }
            }

            handlerLambda = Build<TCoLambda>(ctx, handlerLambda.Pos())
                .Args({"stream"})
                .Body<TExprApplier>()
                    .Apply(handlerLambda)
                    .With<TCoGroupingCore>(0)
                        .Input("stream")
                        .GroupSwitch(groupSwitch)
                        .KeyExtractor(keyExtractor)
                        .ConvertHandler(handler)
                    .Build()
                .Build()
                .Done();
        }

        auto handlerLambdaCleanup = CleanupWorld(handlerLambda, ctx);
        if (!handlerLambdaCleanup) {
            return {};
        }

        const auto mapOutputType = needMap ? builder.MakeRemapType() : nullptr;
        auto reduceInputType = mapOutputType ? mapOutputType : inputItemType;
        if (useSystemColumns) {
            settingsBuilder
                .Add()
                    .Name().Value(ToString(EYtSettingType::KeySwitch)).Build()
                .Build();

            if (ETypeAnnotationKind::Struct == reduceInputType->GetKind()) {
                auto items = reduceInputType->Cast<TStructExprType>()->GetItems();
                items.emplace_back(ctx.MakeType<TItemExprType>(YqlSysColumnKeySwitch, ctx.MakeType<TDataExprType>(EDataSlot::Bool)));
                reduceInputType = ctx.MakeType<TStructExprType>(items);
            }
        }

        TVector<TString> filterColumns;
        if (needMap) {
            if (auto maybeMapper = CleanupWorld(TCoLambda(builder.MakeRemapLambda()), ctx)) {
                mapper = maybeMapper.Cast();
            } else {
                return {};
            }

            if (builder.NeedMap() || multiInput) {
                if (multiInput) {
                    filterColumns.emplace_back("_yql_original_row");
                }
                else {
                    for (auto& item: inputItemType->Cast<TStructExprType>()->GetItems()) {
                        filterColumns.emplace_back(item->GetName());
                    }
                }

                if (ETypeAnnotationKind::Struct == reduceInputType->GetKind()) {
                    const std::unordered_set<std::string_view> set(filterColumns.cbegin(), filterColumns.cend());
                    TVector<const TItemExprType*> items;
                    items.reserve(set.size());
                    for (const auto& item : reduceInputType->Cast<TStructExprType>()->GetItems())
                        if (const auto& name = item->GetName(); YqlSysColumnKeySwitch == name || set.cend() != set.find(name))
                            items.emplace_back(item);
                    reduceInputType = ctx.MakeType<TStructExprType>(items);
                }
            }
        }

        auto reducer = newPartsByKeys ?
            MakeJobLambda<true>(handlerLambdaCleanup.Cast(), useReduceFlow, ctx):
            MakeJobLambda<false>(handlerLambdaCleanup.Cast(), useReduceFlow, ctx);

        if (useReduceFlow) {
            settingsBuilder
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::Flow))
                    .Build()
                .Build();
        }

        if (canUseReduce) {
            auto reduce = Build<TYtReduce>(ctx, node.Pos())
                .World(ApplySyncListToWorld(GetWorld(input, {}, ctx).Ptr(), syncList, ctx))
                .DataSink(GetDataSink(input, ctx))
                .Input(ConvertInputTable(input, ctx))
                .Output()
                    .Add(ConvertOutTables(node.Pos(), outItemType, ctx, State_, &partByKey.Ref().GetConstraintSet()))
                .Build()
                .Settings(settingsBuilder.Done())
                .Reducer(reducer)
                .Done();
            return WrapOp(reduce, ctx);
        }

        if (needMap && (builder.NeedMap() || multiInput) && !canUseMapInsteadOfReduce) {
            settingsBuilder
                .Add()
                    .Name().Value(ToString(EYtSettingType::ReduceFilterBy)).Build()
                    .Value(TExprBase(ToAtomList(filterColumns, node.Pos(), ctx)))
                .Build();
        }

        if (canUseMapInsteadOfReduce && !filterColumns.empty()) {
            reducer = Build<TCoLambda>(ctx, reducer.Pos())
                .Args({"input"})
                .Body<TExprApplier>()
                    .Apply(reducer)
                    .With<TCoMap>(0)
                        .Input("input")
                        .Lambda()
                            .Args({"item"})
                            .Body<TCoSelectMembers>()
                                .Input("item")
                                .Members(ToAtomList(filterColumns, node.Pos(), ctx))
                            .Build()
                        .Build()
                    .Build()
                .Build()
                .Done();
        }

        bool unordered = ctx.IsConstraintEnabled<TSortedConstraintNode>();
        TExprBase world = GetWorld(input, {}, ctx);
        if (hasInputSampling) {

            if (forceMapper && !needMap) {
                mapper = Build<TCoLambda>(ctx, node.Pos()).Args({"stream"}).Body("stream").Done();
                needMap = true;
            }

            if (needMap) {
                input = Build<TYtOutput>(ctx, node.Pos())
                    .Operation<TYtMap>()
                        .World(world)
                        .DataSink(GetDataSink(input, ctx))
                        .Input(ConvertInputTable(input, ctx, TConvertInputOpts().MakeUnordered(unordered)))
                        .Output()
                            .Add(ConvertOutTables(node.Pos(), mapOutputType ? mapOutputType : inputItemType, ctx, State_))
                        .Build()
                        .Settings(GetFlowSettings(node.Pos(), *State_, ctx))
                        .Mapper(MakeJobLambda<false>(mapper.Cast<TCoLambda>(), useMapFlow, ctx))
                    .Build()
                    .OutIndex().Value(0U).Build()
                    .Done();

                mapper = Build<TCoVoid>(ctx, node.Pos()).Done();
                needMap = false;
                forceMapper = false;
            }
            else {
                TConvertInputOpts opts;
                if (useExplicitColumns) {
                    opts.ExplicitFields(*inputItemType->Cast<TStructExprType>(), node.Pos(), ctx);
                }

                input = Build<TYtOutput>(ctx, node.Pos())
                    .Operation<TYtMerge>()
                        .World(world)
                        .DataSink(GetDataSink(input, ctx))
                        .Input(ConvertInputTable(input, ctx, opts.MakeUnordered(unordered)))
                        .Output()
                            .Add(ConvertOutTables(node.Pos(), inputItemType, ctx, State_))
                        .Build()
                        .Settings()
                            .Add()
                                .Name()
                                    .Value(ToString(EYtSettingType::ForceTransform))
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .OutIndex().Value(0U).Build()
                    .Done();
            }
            world = TExprBase(ctx.NewWorld(node.Pos()));
            unordered = false;
        }

        if (needMap) {
            if (multiInput) {
                input = Build<TYtOutput>(ctx, node.Pos())
                    .Operation<TYtMap>()
                        .World(world)
                        .DataSink(GetDataSink(input, ctx))
                        .Input(ConvertInputTable(input, ctx, TConvertInputOpts().MakeUnordered(unordered)))
                        .Output()
                            .Add(ConvertOutTables(node.Pos(), mapOutputType, ctx, State_))
                        .Build()
                        .Settings(GetFlowSettings(node.Pos(), *State_, ctx))
                        .Mapper(MakeJobLambda<false>(mapper.Cast<TCoLambda>(), useMapFlow, ctx))
                    .Build()
                    .OutIndex().Value(0U).Build()
                    .Done();

                mapper = Build<TCoVoid>(ctx, node.Pos()).Done();
                world = TExprBase(ctx.NewWorld(node.Pos()));
                unordered = false;
            } else {
                useMapFlow = useReduceFlow;
                mapper = MakeJobLambda<false>(mapper.Cast<TCoLambda>(), useMapFlow, ctx);
            }
        }

        if (canUseMapInsteadOfReduce) {
            settingsBuilder
                .Add()
                    .Name().Value(ToString(EYtSettingType::JobCount)).Build()
                    .Value(TExprBase(ctx.NewAtom(node.Pos(), 1u)))
                .Build();

            auto result = Build<TYtMap>(ctx, node.Pos())
                .World(ApplySyncListToWorld(world.Ptr(), syncList, ctx))
                .DataSink(GetDataSink(input, ctx))
                .Input(ConvertInputTable(input, ctx, TConvertInputOpts().MakeUnordered(unordered)))
                .Output()
                    .Add(ConvertOutTables(node.Pos(), outItemType, ctx, State_, &partByKey.Ref().GetConstraintSet()))
                .Build()
                .Settings(settingsBuilder.Done())
                .Mapper(reducer)
                .Done();
            return WrapOp(result, ctx);
        }

        if (forceMapper && mapper.Maybe<TCoVoid>()) {
            mapper = MakeJobLambda<false>(Build<TCoLambda>(ctx, node.Pos()).Args({"stream"}).Body("stream").Done(), useMapFlow, ctx);
        }
        auto mapReduce = Build<TYtMapReduce>(ctx, node.Pos())
            .World(ApplySyncListToWorld(world.Ptr(), syncList, ctx))
            .DataSink(GetDataSink(input, ctx))
            .Input(ConvertInputTable(input, ctx, TConvertInputOpts().MakeUnordered(unordered)))
            .Output()
                .Add(ConvertOutTables(node.Pos(), outItemType, ctx, State_, &partByKey.Ref().GetConstraintSet()))
            .Build()
            .Settings(settingsBuilder.Done())
            .Mapper(mapper)
            .Reducer(reducer)
            .Done();
        return WrapOp(mapReduce, ctx);
    }

    TMaybeNode<TExprBase> FlatMap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
            return node;
        }

        auto flatMap = node.Cast<TCoFlatMapBase>();

        const auto disableOptimizers = State_->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>());
        if (!disableOptimizers.contains("EquiJoinPremap") && CanBePulledIntoParentEquiJoin(flatMap, getParents)) {
            YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": " << flatMap.Ref().Content() << " can be pulled into parent EquiJoin";
            return node;
        }

        auto input = flatMap.Input();
        if (!IsYtProviderInput(input, true)) {
            return node;
        }

        auto cluster = TString{GetClusterName(input)};
        TSyncMap syncList;
        if (!IsYtCompleteIsolatedLambda(flatMap.Lambda().Ref(), syncList, cluster, true, false)) {
            return node;
        }

        auto outItemType = SilentGetSequenceItemType(flatMap.Lambda().Body().Ref(), true);
        if (!outItemType || !outItemType->IsPersistable()) {
            return node;
        }

        auto cleanup = CleanupWorld(flatMap.Lambda(), ctx);
        if (!cleanup) {
            return {};
        }

        auto mapper = ctx.Builder(node.Pos())
            .Lambda()
                .Param("stream")
                .Callable(flatMap.Ref().Content())
                    .Arg(0, "stream")
                    .Lambda(1)
                        .Param("item")
                        .Apply(cleanup.Cast().Ptr())
                            .With(0, "item")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        bool sortedOutput = false;
        TVector<TYtOutTable> outTables = ConvertOutTablesWithSortAware(mapper, sortedOutput, flatMap.Pos(),
            outItemType, ctx, State_, flatMap.Ref().GetConstraintSet());

        auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, flatMap.Pos());
        if (TCoOrderedFlatMap::Match(flatMap.Raw()) || sortedOutput) {
            settingsBuilder
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::Ordered))
                    .Build()
                .Build();
        }
        if (State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
            settingsBuilder
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::Flow))
                    .Build()
                .Build();
        }

        auto ytMap = Build<TYtMap>(ctx, node.Pos())
            .World(ApplySyncListToWorld(GetWorld(input, {}, ctx).Ptr(), syncList, ctx))
            .DataSink(GetDataSink(input, ctx))
            .Input(ConvertInputTable(input, ctx))
            .Output()
                .Add(outTables)
            .Build()
            .Settings(settingsBuilder.Done())
            .Mapper(std::move(mapper))
            .Done();

        return WrapOp(ytMap, ctx);
    }

    TMaybeNode<TExprBase> CombineByKey(TExprBase node, TExprContext& ctx) const {
        if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
            return node;
        }

        auto combineByKey = node.Cast<TCoCombineByKey>();

        auto input = combineByKey.Input();
        if (!IsYtProviderInput(input)) {
            return node;
        }

        if (!GetSequenceItemType(input, false, ctx)) {
            return {};
        }

        const TStructExprType* outItemType = nullptr;
        if (auto type = SilentGetSequenceItemType(combineByKey.FinishHandlerLambda().Body().Ref(), false); type && type->IsPersistable()) {
            outItemType = type->Cast<TStructExprType>();
        } else {
            return node;
        }

        auto cluster = TString{GetClusterName(input)};
        TSyncMap syncList;
        if (!IsYtCompleteIsolatedLambda(combineByKey.PreMapLambda().Ref(), syncList, cluster, true, false)) {
            return node;
        }
        if (!IsYtCompleteIsolatedLambda(combineByKey.KeySelectorLambda().Ref(), syncList, cluster, true, false)) {
            return node;
        }
        if (!IsYtCompleteIsolatedLambda(combineByKey.InitHandlerLambda().Ref(), syncList, cluster, true, false)) {
            return node;
        }
        if (!IsYtCompleteIsolatedLambda(combineByKey.UpdateHandlerLambda().Ref(), syncList, cluster, true, false)) {
            return node;
        }
        if (!IsYtCompleteIsolatedLambda(combineByKey.FinishHandlerLambda().Ref(), syncList, cluster, true, false)) {
            return node;
        }

        auto preMapLambda = CleanupWorld(combineByKey.PreMapLambda(), ctx);
        auto keySelectorLambda = CleanupWorld(combineByKey.KeySelectorLambda(), ctx);
        auto initHandlerLambda = CleanupWorld(combineByKey.InitHandlerLambda(), ctx);
        auto updateHandlerLambda = CleanupWorld(combineByKey.UpdateHandlerLambda(), ctx);
        auto finishHandlerLambda = CleanupWorld(combineByKey.FinishHandlerLambda(), ctx);
        if (!preMapLambda || !keySelectorLambda || !initHandlerLambda || !updateHandlerLambda || !finishHandlerLambda) {
            return {};
        }

        auto lambdaBuilder = Build<TCoLambda>(ctx, combineByKey.Pos());
        TMaybe<TCoLambda> lambdaRet;
        if (!IsDepended(keySelectorLambda.Cast().Body().Ref(), keySelectorLambda.Cast().Args().Arg(0).Ref())) {
            lambdaBuilder
                .Args({TStringBuf("stream")})
                .Body<TCoFlatMap>()
                    .Input<TCoCondense1>()
                        .Input<TCoFlatMap>()
                            .Input(TStringBuf("stream"))
                            .Lambda()
                                .Args({TStringBuf("item")})
                                .Body<TExprApplier>()
                                    .Apply(preMapLambda.Cast())
                                    .With(0, TStringBuf("item"))
                                .Build()
                            .Build()
                        .Build()
                        .InitHandler()
                            .Args({TStringBuf("item")})
                            .Body<TExprApplier>()
                                .Apply(initHandlerLambda.Cast())
                                .With(0, keySelectorLambda.Cast().Body())
                                .With(1, TStringBuf("item"))
                            .Build()
                        .Build()
                        .SwitchHandler()
                            .Args({TStringBuf("item"), TStringBuf("state")})
                            .Body<TCoBool>()
                                .Literal().Build("false")
                            .Build()
                        .Build()
                        .UpdateHandler()
                            .Args({TStringBuf("item"), TStringBuf("state")})
                            .Body<TExprApplier>()
                                .Apply(updateHandlerLambda.Cast())
                                .With(0, keySelectorLambda.Cast().Body())
                                .With(1, TStringBuf("item"))
                                .With(2, TStringBuf("state"))
                            .Build()
                        .Build()
                    .Build()
                    .Lambda()
                        .Args({TStringBuf("state")})
                        .Body<TExprApplier>()
                            .Apply(finishHandlerLambda.Cast())
                            .With(0, keySelectorLambda.Cast().Body())
                            .With(1, TStringBuf("state"))
                        .Build()
                    .Build()
                .Build();

            lambdaRet = lambdaBuilder.Done();
        } else {
            lambdaBuilder
                .Args({TStringBuf("stream")})
                .Body<TCoCombineCore>()
                    .Input<TCoFlatMap>()
                        .Input(TStringBuf("stream"))
                        .Lambda()
                            .Args({TStringBuf("item")})
                            .Body<TExprApplier>()
                                .Apply(preMapLambda.Cast())
                                .With(0, TStringBuf("item"))
                            .Build()
                        .Build()
                    .Build()
                    .KeyExtractor()
                        .Args({TStringBuf("item")})
                        .Body<TExprApplier>()
                            .Apply(keySelectorLambda.Cast())
                            .With(0, TStringBuf("item"))
                        .Build()
                    .Build()
                    .InitHandler()
                        .Args({TStringBuf("key"), TStringBuf("item")})
                        .Body<TExprApplier>()
                            .Apply(initHandlerLambda.Cast())
                            .With(0, TStringBuf("key"))
                            .With(1, TStringBuf("item"))
                        .Build()
                    .Build()
                    .UpdateHandler()
                        .Args({TStringBuf("key"), TStringBuf("item"), TStringBuf("state")})
                        .Body<TExprApplier>()
                            .Apply(updateHandlerLambda.Cast())
                            .With(0, TStringBuf("key"))
                            .With(1, TStringBuf("item"))
                            .With(2, TStringBuf("state"))
                        .Build()
                    .Build()
                    .FinishHandler()
                        .Args({TStringBuf("key"), TStringBuf("state")})
                        .Body<TExprApplier>()
                            .Apply(finishHandlerLambda.Cast())
                            .With(0, TStringBuf("key"))
                            .With(1, TStringBuf("state"))
                        .Build()
                    .Build()
                    .MemLimit()
                        .Value(ToString(State_->Configuration->CombineCoreLimit.Get().GetOrElse(0)))
                    .Build()
                .Build();

            lambdaRet = lambdaBuilder.Done();
        }

        if (HasContextFuncs(*lambdaRet->Ptr())) {
            lambdaRet = Build<TCoLambda>(ctx, combineByKey.Pos())
                .Args({ TStringBuf("stream") })
                .Body<TCoWithContext>()
                    .Name()
                        .Value("Agg")
                    .Build()
                    .Input<TExprApplier>()
                        .Apply(*lambdaRet)
                        .With(0, TStringBuf("stream"))
                    .Build()
                .Build()
                .Done();
        }

        TYtOutTableInfo combineOut(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);

        return Build<TYtOutput>(ctx, combineByKey.Pos())
            .Operation<TYtMap>()
                .World(ApplySyncListToWorld(GetWorld(input, {}, ctx).Ptr(), syncList, ctx))
                .DataSink(GetDataSink(input, ctx))
                .Input(ConvertInputTable(input, ctx))
                .Output()
                    .Add(combineOut.ToExprNode(ctx, combineByKey.Pos()).Cast<TYtOutTable>())
                .Build()
                .Settings(GetFlowSettings(combineByKey.Pos(), *State_, ctx))
                .Mapper(*lambdaRet)
            .Build()
            .OutIndex().Value(0U).Build()
            .Done();
    }

    TMaybeNode<TExprBase> DqWrite(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const {
        if (State_->PassiveExecution) {
            return node;
        }

        auto write = node.Cast<TYtWriteTable>();
        if (!TDqCnUnionAll::Match(write.Content().Raw())) {
            return node;
        }

        const TStructExprType* outItemType;
        if (auto type = GetSequenceItemType(write.Content(), false, ctx)) {
            outItemType = type->Cast<TStructExprType>();
        } else {
            return node;
        }

        if (!NDq::IsSingleConsumerConnection(write.Content().Cast<TDqCnUnionAll>(), *getParents())) {
            return node;
        }

        TSyncMap syncList;
        if (!IsYtCompleteIsolatedLambda(write.Content().Ref(), syncList, true, true)) {
            return node;
        }

        const ui64 nativeTypeFlags = State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE;
        TYtOutTableInfo outTable(outItemType, nativeTypeFlags);

        const auto dqUnion = write.Content().Cast<TDqCnUnionAll>();

        TMaybeNode<TCoSort> sort;
        TMaybeNode<TCoTopSort> topSort;
        TMaybeNode<TDqCnMerge> mergeConnection;
        auto topLambdaBody = dqUnion.Output().Stage().Program().Body();

        // Look for the sort-only stage or DcCnMerge connection.
        bool sortOnlyLambda = true;
        auto& topNode = SkipCallables(topLambdaBody.Ref(), {"ToFlow", "FromFlow", "ToStream"});
        if (auto maybeSort = TMaybeNode<TCoSort>(&topNode)) {
            sort = maybeSort;
        } else if (auto maybeTopSort = TMaybeNode<TCoTopSort>(&topNode)) {
            topSort = maybeTopSort;
        } else {
            sortOnlyLambda = false;
            if (auto inputs = dqUnion.Output().Stage().Inputs(); inputs.Size() == 1 && inputs.Item(0).Maybe<TDqCnMerge>().IsValid()) {
                if (topNode.IsArgument()) {
                    mergeConnection = inputs.Item(0).Maybe<TDqCnMerge>();
                } else if (topNode.IsCallable(TDqReplicate::CallableName()) && topNode.Head().IsArgument()) {
                    auto ndx = FromString<size_t>(dqUnion.Output().Index().Value());
                    YQL_ENSURE(ndx + 1 < topNode.ChildrenSize());
                    if (&topNode.Child(ndx + 1)->Head().Head() == &topNode.Child(ndx + 1)->Tail()) { // trivial lambda
                        mergeConnection = inputs.Item(0).Maybe<TDqCnMerge>();
                    }
                }
            }
        }

        if (sortOnlyLambda) {
            auto& bottomNode = SkipCallables(topNode.Head(), {"ToFlow", "FromFlow", "ToStream"});
            sortOnlyLambda = bottomNode.IsArgument();
        }

        TCoLambda writeLambda = Build<TCoLambda>(ctx, write.Pos())
            .Args({"stream"})
            .Body<TDqWrite>()
                .Input("stream")
                .Provider().Value(YtProviderName).Build()
                .Settings().Build()
            .Build()
            .Done();

        if (sortOnlyLambda && (sort || topSort)) {
            // Add Unordered callable to kill sort in a stage. Sorting will be handled by YtSort.
            writeLambda = Build<TCoLambda>(ctx, write.Pos())
                .Args({"stream"})
                .Body<TExprApplier>()
                    .Apply(writeLambda)
                    .With<TCoUnordered>(0)
                        .Input("stream")
                    .Build()
                .Build()
                .Done();
        }

        TMaybeNode<TDqConnection> result;
        if (GetStageOutputsCount(dqUnion.Output().Stage()) > 1) {
            result = Build<TDqCnUnionAll>(ctx, write.Pos())
                .Output()
                    .Stage<TDqStage>()
                        .Inputs()
                            .Add(dqUnion)
                        .Build()
                        .Program(writeLambda)
                        .Settings(TDqStageSettings().BuildNode(ctx, write.Pos()))
                    .Build()
                    .Index().Build("0")
                .Build()
                .Done().Ptr();
        } else {
            result = NDq::DqPushLambdaToStageUnionAll(dqUnion, writeLambda, {}, ctx, optCtx);
            if (!result) {
                return {};
            }
        }

        result = CleanupWorld(result.Cast(), ctx);

        auto dqCnResult = Build<TDqCnResult>(ctx, write.Pos())
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(result.Cast())
                    .Build()
                    .Program()
                        .Args({"row"})
                        .Body("row")
                    .Build()
                    .Settings(TDqStageSettings().BuildNode(ctx, write.Pos()))
                .Build()
                .Index().Build("0")
            .Build()
            .ColumnHints() // TODO: set column hints
            .Build()
            .Done().Ptr();

        auto writeOp = Build<TYtDqProcessWrite>(ctx, write.Pos())
            .World(ApplySyncListToWorld(ctx.NewWorld(write.Pos()), syncList, ctx))
            .DataSink(write.DataSink().Ptr())
            .Output()
                .Add(outTable.ToExprNode(ctx, write.Pos()).Cast<TYtOutTable>())
            .Build()
            .Input(dqCnResult)
            .Done().Ptr();

        auto writeOutput = Build<TYtOutput>(ctx, write.Pos())
            .Operation(writeOp)
            .OutIndex().Value(0U).Build()
            .Done().Ptr();

        if (sort) {
            writeOutput = Build<TCoSort>(ctx, write.Pos())
                .Input(writeOutput)
                .SortDirections(sort.SortDirections().Cast())
                .KeySelectorLambda(sort.KeySelectorLambda().Cast())
                .Done().Ptr();
        } else if (topSort) {
            writeOutput = Build<TCoTopSort>(ctx, write.Pos())
                .Input(writeOutput)
                .Count(topSort.Count().Cast())
                .SortDirections(topSort.SortDirections().Cast())
                .KeySelectorLambda(topSort.KeySelectorLambda().Cast())
                .Done().Ptr();
        } else if (mergeConnection) {
            writeOutput = Build<TCoSort>(ctx, write.Pos())
                .Input(writeOutput)
                .SortDirections(
                    ctx.Builder(write.Pos())
                        .List()
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                size_t index = 0;
                                for (auto col: mergeConnection.Cast().SortColumns()) {
                                    parent.Callable(index++, "Bool")
                                        .Atom(0, col.SortDirection().Value() == "Asc" ? "1" : "0", TNodeFlags::Default)
                                    .Seal();
                                }
                                return parent;
                            })
                        .Seal()
                        .Build()
                )
                .KeySelectorLambda(
                    ctx.Builder(write.Pos())
                        .Lambda()
                            .Param("item")
                            .List()
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    size_t index = 0;
                                    for (auto col: mergeConnection.Cast().SortColumns()) {
                                        parent.Callable(index++, "Member")
                                            .Arg(0, "item")
                                            .Atom(1, col.Column().Value())
                                        .Seal();
                                    }
                                    return parent;
                                })
                            .Seal()
                        .Seal()
                        .Build()
                )
                .Done().Ptr();
        }

        return ctx.ChangeChild(write.Ref(), TYtWriteTable::idx_Content, std::move(writeOutput));
    }

    TMaybeNode<TExprBase> YtDqProcessWrite(TExprBase node, TExprContext& ctx) const {
        const auto& write = node.Cast<TYtDqProcessWrite>();
        if (const auto& contents = FindNodes(write.Input().Ptr(),
            [] (const TExprNode::TPtr& node) { return !TYtOutputOpBase::Match(node.Get()); },
            [] (const TExprNode::TPtr& node) { return node->IsCallable({TCoToFlow::CallableName(), TCoIterator::CallableName()}) && node->Head().IsCallable(TYtTableContent::CallableName()); });
            !contents.empty()) {
            TNodeOnNodeOwnedMap replaces(contents.size());
            const bool addToken = !State_->Configuration->Auth.Get().GetOrElse(TString()).empty();

            for (const auto& cont : contents) {
                const TYtTableContent content(cont->HeadPtr());
                auto input = content.Input();
                const auto output = input.Maybe<TYtOutput>();
                const auto structType = GetSeqItemType(output ? output.Cast().Ref().GetTypeAnn() : input.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back())->Cast<TStructExprType>();
                if (output) {
                    input = ConvertContentInputToRead(output.Cast(), {}, ctx);
                }
                TMaybeNode<TCoSecureParam> secParams;
                if (addToken) {
                    const auto cluster = input.Cast<TYtReadTable>().DataSource().Cluster();
                    secParams = Build<TCoSecureParam>(ctx, node.Pos()).Name().Build(TString("cluster:default_").append(cluster)).Done();
                }

                TExprNode::TListType flags;
                if (!NYql::HasSetting(content.Settings().Ref(), EYtSettingType::Split))
                    flags.emplace_back(ctx.NewAtom(cont->Pos(), "Solid", TNodeFlags::Default));

                const auto read = Build<TDqReadWideWrap>(ctx, cont->Pos())
                        .Input(input)
                        .Flags().Add(std::move(flags)).Build()
                        .Token(secParams)
                        .Done();

                auto narrow = ctx.Builder(cont->Pos())
                    .Callable("NarrowMap")
                        .Add(0, read.Ptr())
                        .Lambda(1)
                            .Params("fields", structType->GetSize())
                            .Callable(TCoAsStruct::CallableName())
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    ui32 i = 0U;
                                    for (const auto& item : structType->GetItems()) {
                                        parent.List(i)
                                            .Atom(0, item->GetName())
                                            .Arg(1, "fields", i)
                                        .Seal();
                                        ++i;
                                    }
                                    return parent;
                                })
                            .Seal()
                        .Seal()
                    .Seal().Build();

                narrow = ctx.WrapByCallableIf(cont->IsCallable(TCoIterator::CallableName()), TCoFromFlow::CallableName(), std::move(narrow));
                replaces.emplace(cont.Get(), std::move(narrow));
            }

            return Build<TYtDqProcessWrite>(ctx, write.Pos())
                .InitFrom(write)
                .Input(ctx.ReplaceNodes(write.Input().Ptr(), replaces))
            .Done();
        }

        return node;
    }

    TMaybeNode<TExprBase> Write(TExprBase node, TExprContext& ctx) const {
        auto write = node.Cast<TYtWriteTable>();
        if (!IsYtProviderInput(write.Content())) {
            return node;
        }

        auto cluster = TString{write.DataSink().Cluster().Value()};
        auto srcCluster = GetClusterName(write.Content());
        if (cluster != srcCluster) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                << "Result from cluster " << TString{srcCluster}.Quote()
                << " cannot be written to a different destination cluster " << cluster.Quote()));
            return {};
        }

        TVector<TYtPathInfo::TPtr> inputPaths = GetInputPaths(write.Content());
        TYtTableInfo::TPtr outTableInfo = MakeIntrusive<TYtTableInfo>(write.Table());

        const auto mode = NYql::GetSetting(write.Settings().Ref(), EYtSettingType::Mode);
        const bool renew = !mode || FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Renew;
        const bool flush = mode && FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Flush;
        const bool transactionalOverrideTarget = NYql::GetSetting(write.Settings().Ref(), EYtSettingType::Initial)
            && !flush && (renew || !outTableInfo->Meta->DoesExist);

        const TStructExprType* outItemType = nullptr;
        if (auto type = GetSequenceItemType(write.Content(), false, ctx)) {
            outItemType = type->Cast<TStructExprType>();
        } else {
            return {};
        }

        auto maybeReadSettings = write.Content().Maybe<TCoRight>().Input().Maybe<TYtReadTable>().Input().Item(0).Settings();

        const TYtTableDescription& nextDescription = State_->TablesData->GetTable(cluster, outTableInfo->Name, outTableInfo->CommitEpoch);
        const ui64 nativeTypeFlags = nextDescription.RowSpec->GetNativeYtTypeFlags();

        TMaybe<NYT::TNode> firstNativeType;
        ui64 firstNativeTypeFlags = 0;
        if (!inputPaths.empty()) {
            firstNativeType = inputPaths.front()->GetNativeYtType();
            firstNativeTypeFlags = inputPaths.front()->GetNativeYtTypeFlags();
        }

        bool requiresMap = (maybeReadSettings && NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::SysColumns))
            || AnyOf(inputPaths, [firstNativeType] (const TYtPathInfo::TPtr& path) {
                return path->RequiresRemap() || firstNativeType != path->GetNativeYtType();
            });

        const bool requiresMerge = !requiresMap && (
            AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) {
                return path->Ranges || path->HasColumns() || path->Table->Meta->IsDynamic || path->Table->FromNode.Maybe<TYtTable>();
            })
            || (maybeReadSettings && NYql::HasAnySetting(maybeReadSettings.Ref(),
                EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Sample))
            || (nextDescription.RowSpec->GetColumnOrder().Defined() && AnyOf(inputPaths, [colOrder = *nextDescription.RowSpec->GetColumnOrder()] (const TYtPathInfo::TPtr& path) {
                return path->Table->RowSpec->GetColumnOrder().Defined() && path->Table->RowSpec->GetColumnOrder() != colOrder;
            }))
        );

        TMaybeNode<TCoAtom> outMode;
        if (ctx.IsConstraintEnabled<TSortedConstraintNode>() && maybeReadSettings && NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::Unordered)) {
            outMode = Build<TCoAtom>(ctx, write.Pos()).Value(ToString(EYtSettingType::Unordered)).Done();
        }

        TVector<TYtOutput> publishInput;
        if (requiresMap || requiresMerge) {
            TExprNode::TPtr mapper;
            if (requiresMap) {
                mapper = Build<TCoLambda>(ctx, write.Pos())
                    .Args({"stream"})
                    .Body("stream")
                    .Done().Ptr();
            }

            // For YtMerge passthrough native flags as is. AlignPublishTypes optimizer will add additional remapping
            TYtOutTableInfo outTable(outItemType, requiresMerge ? firstNativeTypeFlags : nativeTypeFlags);
            if (firstNativeType) {
                outTable.RowSpec->CopyTypeOrders(*firstNativeType);
            }
            auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, write.Pos());
            bool useExplicitColumns = requiresMerge && AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) {
                return !path->Table->IsTemp;
            });
            if (requiresMap) {
                if (ctx.IsConstraintEnabled<TSortedConstraintNode>()) {
                    if (auto sorted = write.Content().Ref().GetConstraint<TSortedConstraintNode>()) {
                        const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
                        TKeySelectorBuilder builder(write.Pos(), ctx, useNativeDescSort, outItemType);
                        builder.ProcessConstraint(*sorted);
                        builder.FillRowSpecSort(*outTable.RowSpec);

                        if (builder.NeedMap()) {
                            mapper = ctx.Builder(write.Pos())
                                .Lambda()
                                    .Param("stream")
                                    .Apply(builder.MakeRemapLambda(true))
                                        .With(0)
                                            .Apply(mapper)
                                                .With(0, "stream")
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal()
                                .Build();
                        }
                    }
                } else {
                    if (inputPaths.size() == 1 && inputPaths.front()->Table->RowSpec && inputPaths.front()->Table->RowSpec->IsSorted()) {
                        outTable.RowSpec->CopySortness(*inputPaths.front()->Table->RowSpec);
                    }
                }
            }
            else { // requiresMerge
                // TODO: should we keep sort if multiple inputs?
                if (outMode || AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) { return path->Table->IsUnordered; })) {
                    useExplicitColumns = useExplicitColumns || AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) { return path->Table->RowSpec->HasAuxColumns(); });
                }
                else {
                    const bool exactCopySort = inputPaths.size() == 1 && !inputPaths.front()->HasColumns();
                    bool hasAux = inputPaths.front()->Table->RowSpec->HasAuxColumns();
                    bool sortIsChanged = inputPaths.front()->Table->IsUnordered
                        ? inputPaths.front()->Table->RowSpec->IsSorted()
                        : outTable.RowSpec->CopySortness(*inputPaths.front()->Table->RowSpec,
                            exactCopySort ? TYqlRowSpecInfo::ECopySort::Exact : TYqlRowSpecInfo::ECopySort::WithDesc);
                    useExplicitColumns = useExplicitColumns || (inputPaths.front()->HasColumns() && hasAux);

                    for (size_t i = 1; i < inputPaths.size(); ++i) {
                        sortIsChanged = outTable.RowSpec->MakeCommonSortness(*inputPaths[i]->Table->RowSpec) || sortIsChanged;
                        const bool tableHasAux = inputPaths[i]->Table->RowSpec->HasAuxColumns();
                        hasAux = hasAux || tableHasAux;
                        if (inputPaths[i]->HasColumns() && tableHasAux) {
                            useExplicitColumns = true;
                        }
                    }
                    useExplicitColumns = useExplicitColumns || (sortIsChanged && hasAux);
                }

                if (maybeReadSettings && NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::Sample)) {
                    settingsBuilder.Add()
                        .Name().Value(ToString(EYtSettingType::ForceTransform)).Build()
                    .Build();
                }
            }
            outTable.SetUnique(write.Content().Ref().GetConstraint<TDistinctConstraintNode>(), write.Pos(), ctx);
            outTable.RowSpec->SetConstraints(write.Content().Ref().GetConstraintSet());

            TMaybeNode<TYtTransientOpBase> op;
            if (requiresMap) {
                if (outTable.RowSpec->IsSorted()) {
                    settingsBuilder
                        .Add()
                            .Name()
                                .Value(ToString(EYtSettingType::Ordered))
                            .Build()
                        .Build();
                }
                if (State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
                    settingsBuilder
                        .Add()
                            .Name()
                                .Value(ToString(EYtSettingType::Flow))
                            .Build()
                        .Build();
                }

                op = Build<TYtMap>(ctx, write.Pos())
                    .World(GetWorld(write.Content(), {}, ctx))
                    .DataSink(write.DataSink())
                    .Input(ConvertInputTable(write.Content(), ctx))
                    .Output()
                        .Add(outTable.ToExprNode(ctx, write.Pos()).Cast<TYtOutTable>())
                    .Build()
                    .Settings(settingsBuilder.Done())
                    .Mapper(mapper)
                    .Done();
            }
            else {
                TConvertInputOpts opts;
                if (useExplicitColumns) {
                    opts.ExplicitFields(*outTable.RowSpec, write.Pos(), ctx);
                }
                op = Build<TYtMerge>(ctx, write.Pos())
                    .World(GetWorld(write.Content(), {}, ctx))
                    .DataSink(write.DataSink())
                    .Input(ConvertInputTable(write.Content(), ctx, opts))
                    .Output()
                        .Add(outTable.ToExprNode(ctx, write.Pos()).Cast<TYtOutTable>())
                    .Build()
                    .Settings(settingsBuilder.Done())
                    .Done();
            }

            publishInput.push_back(Build<TYtOutput>(ctx, write.Pos())
                .Operation(op.Cast())
                .OutIndex().Value(0U).Build()
                .Mode(outMode)
                .Done());
        }
        else {
            if (auto out = write.Content().Maybe<TYtOutput>()) {
                publishInput.push_back(out.Cast());
            } else {
                for (auto path: write.Content().Cast<TCoRight>().Input().Cast<TYtReadTable>().Input().Item(0).Paths()) {
                    publishInput.push_back(Build<TYtOutput>(ctx, path.Table().Pos())
                        .InitFrom(path.Table().Cast<TYtOutput>())
                        .Mode(outMode)
                        .Done());
                }
            }
        }

        auto publishSettings = write.Settings();
        if (transactionalOverrideTarget) {
            publishSettings = TCoNameValueTupleList(NYql::RemoveSetting(publishSettings.Ref(), EYtSettingType::Mode, ctx));
        }

        return Build<TYtPublish>(ctx, write.Pos())
            .World(write.World())
            .DataSink(write.DataSink())
            .Input()
                .Add(publishInput)
            .Build()
            .Publish(write.Table().Cast<TYtTable>())
            .Settings(publishSettings)
            .Done();
    }

    TMaybeNode<TExprBase> Fill(TExprBase node, TExprContext& ctx) const {
        if (State_->PassiveExecution) {
            return node;
        }

        auto write = node.Cast<TYtWriteTable>();

        auto mode = NYql::GetSetting(write.Settings().Ref(), EYtSettingType::Mode);

        if (mode && FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Drop) {
            return node;
        }

        auto cluster = TString{write.DataSink().Cluster().Value()};
        TSyncMap syncList;
        if (!IsYtCompleteIsolatedLambda(write.Content().Ref(), syncList, cluster, true, false)) {
            return node;
        }

        if (FindNode(write.Content().Ptr(),
            [] (const TExprNode::TPtr& node) { return !TMaybeNode<TYtOutputOpBase>(node).IsValid(); },
            [] (const TExprNode::TPtr& node) { return TMaybeNode<TDqConnection>(node).IsValid(); })) {
            return node;
        }

        const TStructExprType* outItemType = nullptr;
        if (auto type = GetSequenceItemType(write.Content(), false, ctx)) {
            if (!EnsurePersistableType(write.Content().Pos(), *type, ctx)) {
                return {};
            }
            outItemType = type->Cast<TStructExprType>();
        } else {
            return {};
        }
        TYtOutTableInfo outTable(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);

        {
            auto path = write.Table().Name().StringValue();
            auto commitEpoch = TEpochInfo::Parse(write.Table().CommitEpoch().Ref()).GetOrElse(0);
            auto dstRowSpec = State_->TablesData->GetTable(cluster, path, commitEpoch).RowSpec;
            outTable.RowSpec->SetColumnOrder(dstRowSpec->GetColumnOrder());
        }
        auto content = write.Content();
        if (auto sorted = content.Ref().GetConstraint<TSortedConstraintNode>()) {
            const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
            TKeySelectorBuilder builder(node.Pos(), ctx, useNativeDescSort, outItemType);
            builder.ProcessConstraint(*sorted);
            builder.FillRowSpecSort(*outTable.RowSpec);

            if (builder.NeedMap()) {
                content = Build<TExprApplier>(ctx, content.Pos())
                    .Apply(TCoLambda(builder.MakeRemapLambda(true)))
                    .With(0, content)
                    .Done();
                outItemType = builder.MakeRemapType();
            }

        } else if (auto unordered = content.Maybe<TCoUnorderedBase>()) {
            content = unordered.Cast().Input();
        }
        outTable.RowSpec->SetConstraints(write.Content().Ref().GetConstraintSet());
        outTable.SetUnique(write.Content().Ref().GetConstraint<TDistinctConstraintNode>(), node.Pos(), ctx);

        TYtTableInfo::TPtr pubTableInfo = MakeIntrusive<TYtTableInfo>(write.Table());
        const bool renew = !mode || FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Renew;
        const bool flush = mode && FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Flush;
        const bool transactionalOverrideTarget = NYql::GetSetting(write.Settings().Ref(), EYtSettingType::Initial)
            && !flush && (renew || !pubTableInfo->Meta->DoesExist);

        auto publishSettings = write.Settings();
        if (transactionalOverrideTarget) {
            publishSettings = TCoNameValueTupleList(NYql::RemoveSetting(publishSettings.Ref(), EYtSettingType::Mode, ctx));
        }

        auto cleanup = CleanupWorld(content, ctx);
        if (!cleanup) {
            return {};
        }

        return Build<TYtPublish>(ctx, write.Pos())
            .World(write.World())
            .DataSink(write.DataSink())
            .Input()
                .Add()
                    .Operation<TYtFill>()
                        .World(ApplySyncListToWorld(ctx.NewWorld(write.Pos()), syncList, ctx))
                        .DataSink(write.DataSink())
                        .Content(MakeJobLambdaNoArg(cleanup.Cast(), ctx))
                        .Output()
                            .Add(outTable.ToExprNode(ctx, write.Pos()).Cast<TYtOutTable>())
                        .Build()
                        .Settings(GetFlowSettings(write.Pos(), *State_, ctx))
                    .Build()
                    .OutIndex().Value(0U).Build()
                .Build()
            .Build()
            .Publish(write.Table())
            .Settings(publishSettings)
            .Done();
    }

    TMaybeNode<TExprBase> TakeOrSkip(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto countBase = node.Cast<TCoCountBase>();
        auto input = countBase.Input();
        if (!IsYtProviderInput(input)) {
            return node;
        }

        auto cluster = TString{GetClusterName(input)};
        TSyncMap syncList;
        if (!IsYtCompleteIsolatedLambda(countBase.Count().Ref(), syncList, cluster, true, false)) {
            return node;
        }

        auto count = State_->PassiveExecution ? countBase.Count() : CleanupWorld(countBase.Count(), ctx);
        if (!count) {
            return {};
        }

        EYtSettingType settingType = node.Maybe<TCoSkip>() ? EYtSettingType::Skip : EYtSettingType::Take;

        auto settings = Build<TCoNameValueTupleList>(ctx, countBase.Pos())
            .Add()
                .Name()
                    .Value(ToString(settingType))
                .Build()
                .Value(count.Cast())
            .Build()
            .Done();

        if (!ctx.IsConstraintEnabled<TSortedConstraintNode>()) {
            if (auto maybeMap = input.Maybe<TYtOutput>().Operation().Maybe<TYtMap>()) {
                TYtMap map = maybeMap.Cast();
                if (!IsOutputUsedMultipleTimes(map.Ref(), *getParents())) {
                    TYtOutTableInfo mapOut(map.Output().Item(0));
                    if (mapOut.RowSpec->IsSorted()) {
                        mapOut.RowSpec->ClearSortness();
                        input = Build<TYtOutput>(ctx, input.Pos())
                            .InitFrom(input.Cast<TYtOutput>())
                            .Operation<TYtMap>()
                                .InitFrom(map)
                                .Output()
                                    .Add(mapOut.ToExprNode(ctx, map.Output().Item(0).Pos()).Cast<TYtOutTable>())
                                .Build()
                            .Build()
                            .Done();
                    }
                }
            }
        }

        auto res = Build<TCoRight>(ctx, countBase.Pos())
            .Input<TYtReadTable>()
                .World(ApplySyncListToWorld(GetWorld(input, {}, ctx).Ptr(), syncList, ctx))
                .DataSource(GetDataSource(input, ctx))
                .Input(ConvertInputTable(input, ctx, TConvertInputOpts().KeepDirecRead(true).Settings(settings)))
            .Build()
            .Done();
        return KeepColumnOrder(res.Ptr(), node.Ref(), ctx, *State_->Types);
    }

    TMaybeNode<TExprBase> Extend(TExprBase node, TExprContext& ctx) const {
        if (State_->PassiveExecution) {
            return node;
        }

        auto extend = node.Cast<TCoExtendBase>();

        bool allAreTables = true;
        bool hasTables = false;
        bool allAreTableContents = true;
        bool hasContents = false;
        bool keepSort = !ctx.IsConstraintEnabled<TSortedConstraintNode>() || (bool)extend.Ref().GetConstraint<TSortedConstraintNode>();
        TString resultCluster;
        TMaybeNode<TYtDSource> dataSource;

        for (auto child: extend) {
            bool isTable = IsYtProviderInput(child);
            bool isContent = child.Maybe<TYtTableContent>().IsValid();
            if (!isTable && !isContent) {
                // Don't match foreign provider input
                if (child.Maybe<TCoRight>()) {
                    return node;
                }
            } else {
                auto currentDataSource = GetDataSource(child, ctx);
                auto currentCluster = TString{currentDataSource.Cluster().Value()};
                if (!dataSource) {
                    dataSource = currentDataSource;
                    resultCluster = currentCluster;
                } else if (resultCluster != currentCluster) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                        << "Different source clusters in " << extend.Ref().Content() << ": " << resultCluster
                        << " and " << currentCluster));
                    return {};
                }
            }
            allAreTables = allAreTables && isTable;
            hasTables = hasTables || isTable;
            allAreTableContents = allAreTableContents && isContent;
            hasContents = hasContents || isContent;
        }

        if (!hasTables && !hasContents) {
            return node;
        }

        auto dataSink = TYtDSink(ctx.RenameNode(dataSource.Ref(), "DataSink"));
        if (allAreTables || allAreTableContents) {
            TVector<TExprBase> worlds;
            TVector<TYtPath> paths;
            TExprNode::TListType newExtendParts;
            bool updateChildren = false;
            bool unordered = false;
            bool nonUniq = false;
            for (auto child: extend) {
                newExtendParts.push_back(child.Ptr());

                auto read = child.Maybe<TCoRight>().Input().Maybe<TYtReadTable>();
                if (!read) {
                    read = child.Maybe<TYtTableContent>().Input().Maybe<TYtReadTable>();
                }
                if (read) {
                    YQL_ENSURE(read.Cast().Input().Size() == 1);
                    auto section = read.Cast().Input().Item(0);
                    unordered = unordered || NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Unordered);
                    nonUniq = nonUniq || NYql::HasSetting(section.Settings().Ref(), EYtSettingType::NonUnique);
                    TExprNode::TPtr settings = NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::DirectRead | EYtSettingType::Unordered | EYtSettingType::NonUnique, ctx);
                    if (settings->ChildrenSize() != 0) {
                        if (State_->Types->EvaluationInProgress || allAreTableContents) {
                            return node;
                        }
                        auto scheme = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                        auto path = CopyOrTrivialMap(section.Pos(),
                            read.Cast().World(), dataSink,
                            *scheme,
                            TYtSection(ctx.ChangeChild(section.Ref(), TYtSection::idx_Settings, std::move(settings))),
                            {}, ctx, State_,
                            TCopyOrTrivialMapOpts().SetTryKeepSortness(keepSort).SetRangesResetSort(!keepSort).SetSectionUniq(section.Ref().GetConstraint<TDistinctConstraintNode>()).SetConstraints(extend.Ref().GetConstraintSet()));
                        updateChildren = true;
                        newExtendParts.back() = allAreTableContents
                            ? ctx.ChangeChild(child.Ref(), TYtTableContent::idx_Input, path.Table().Ptr())
                            : path.Table().Ptr();
                    } else {
                        paths.insert(paths.end(), section.Paths().begin(), section.Paths().end());
                        if (allAreTables) {
                            worlds.push_back(GetWorld(child, {}, ctx));
                        }
                    }
                } else {
                    YQL_ENSURE(child.Maybe<TYtOutput>(), "Unknown extend element: " << child.Ref().Content());
                    paths.push_back(
                        Build<TYtPath>(ctx, child.Pos())
                            .Table(child) // child is TYtOutput
                            .Columns<TCoVoid>().Build()
                            .Ranges<TCoVoid>().Build()
                            .Stat<TCoVoid>().Build()
                            .Done()
                        );
                }
            }

            if (updateChildren) {
                return TExprBase(ctx.ChangeChildren(extend.Ref(), std::move(newExtendParts)));
            }

            newExtendParts.clear();

            auto world = worlds.empty()
                ? TExprBase(ctx.NewWorld(extend.Pos()))
                : worlds.size() == 1
                    ? worlds.front()
                    : Build<TCoSync>(ctx, extend.Pos()).Add(worlds).Done();

            auto scheme = extend.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();

            if (keepSort && extend.Maybe<TCoMerge>() && paths.size() > 1) {
                if (State_->Types->EvaluationInProgress) {
                    return node;
                }
                auto path = CopyOrTrivialMap(extend.Pos(),
                    world, dataSink,
                    *scheme,
                    Build<TYtSection>(ctx, extend.Pos())
                        .Paths()
                            .Add(paths)
                        .Build()
                        .Settings()
                        .Build()
                        .Done(),
                    {}, ctx, State_,
                    TCopyOrTrivialMapOpts().SetTryKeepSortness(keepSort).SetRangesResetSort(!keepSort).SetSectionUniq(extend.Ref().GetConstraint<TDistinctConstraintNode>()).SetConstraints(extend.Ref().GetConstraintSet()));
                world = TExprBase(ctx.NewWorld(extend.Pos()));
                paths.assign(1, path);
            }

            if (paths.size() == 1 && paths.front().Columns().Maybe<TCoVoid>() && paths.front().Ranges().Maybe<TCoVoid>()) {
                return allAreTables
                    ? paths.front().Table()
                    : Build<TYtTableContent>(ctx, extend.Pos())
                        .Input(paths.front().Table())
                        .Settings().Build()
                        .Done().Cast<TExprBase>();
            }

            auto newSettings = ctx.NewList(extend.Pos(), {});
            if (nonUniq) {
                newSettings = NYql::AddSetting(*newSettings, EYtSettingType::NonUnique, {}, ctx);
            }
            auto newSection = Build<TYtSection>(ctx, extend.Pos())
                .Paths()
                    .Add(paths)
                .Build()
                .Settings(newSettings)
                .Done();
            if (unordered) {
                newSection = MakeUnorderedSection<true>(newSection, ctx);
            }

            auto resRead = Build<TYtReadTable>(ctx, extend.Pos())
                .World(world)
                .DataSource(dataSource.Cast())
                .Input()
                    .Add(newSection)
                .Build()
                .Done();

            return allAreTables
                ? Build<TCoRight>(ctx, extend.Pos())
                    .Input(resRead)
                    .Done().Cast<TExprBase>()
                : Build<TYtTableContent>(ctx, extend.Pos())
                    .Input(resRead)
                    .Settings().Build()
                    .Done().Cast<TExprBase>();
        }

        if (!hasTables) {
            return node;
        }

        const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
        TExprNode::TListType newExtendParts;
        for (auto child: extend) {
            if (!IsYtProviderInput(child)) {
                if (State_->Types->EvaluationInProgress) {
                    return node;
                }
                TSyncMap syncList;
                if (!IsYtCompleteIsolatedLambda(child.Ref(), syncList, resultCluster, true, false)) {
                    return node;
                }

                const TStructExprType* outItemType = nullptr;
                if (auto type = GetSequenceItemType(child, false, ctx)) {
                    if (!EnsurePersistableType(child.Pos(), *type, ctx)) {
                        return {};
                    }
                    outItemType = type->Cast<TStructExprType>();
                } else {
                    return {};
                }

                TYtOutTableInfo outTable(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
                auto content = child;
                auto sorted = child.Ref().GetConstraint<TSortedConstraintNode>();
                if (keepSort && sorted) {
                    TKeySelectorBuilder builder(child.Pos(), ctx, useNativeDescSort, outItemType);
                    builder.ProcessConstraint(*sorted);
                    builder.FillRowSpecSort(*outTable.RowSpec);

                    if (builder.NeedMap()) {
                        content = Build<TExprApplier>(ctx, child.Pos())
                            .Apply(TCoLambda(builder.MakeRemapLambda(true)))
                            .With(0, content)
                            .Done();
                        outItemType = builder.MakeRemapType();
                    }

                } else if (auto unordered = content.Maybe<TCoUnorderedBase>()) {
                    content = unordered.Cast().Input();
                }
                outTable.RowSpec->SetConstraints(child.Ref().GetConstraintSet());
                outTable.SetUnique(child.Ref().GetConstraint<TDistinctConstraintNode>(), child.Pos(), ctx);

                auto cleanup = CleanupWorld(content, ctx);
                if (!cleanup) {
                    return {};
                }

                newExtendParts.push_back(
                    Build<TYtOutput>(ctx, child.Pos())
                        .Operation<TYtFill>()
                            .World(ApplySyncListToWorld(ctx.NewWorld(child.Pos()), syncList, ctx))
                            .DataSink(dataSink)
                            .Content(MakeJobLambdaNoArg(cleanup.Cast(), ctx))
                            .Output()
                                .Add(outTable.ToExprNode(ctx, child.Pos()).Cast<TYtOutTable>())
                            .Build()
                            .Settings(GetFlowSettings(child.Pos(), *State_, ctx))
                        .Build()
                        .OutIndex().Value(0U).Build()
                    .Done().Ptr()
                );
            }
            else {
                newExtendParts.push_back(child.Ptr());
            }
        }

        return TExprBase(ctx.ChangeChildren(extend.Ref(), std::move(newExtendParts)));
    }

    TMaybeNode<TExprBase> Length(TExprBase node, TExprContext& ctx) const {
        TExprBase list = node.Maybe<TCoLength>()
            ? node.Cast<TCoLength>().List()
            : node.Cast<TCoHasItems>().List();

        TExprBase ytLengthInput = list;
        if (auto content = list.Maybe<TYtTableContent>()) {
            ytLengthInput = content.Cast().Input();
        } else if (!IsYtProviderInput(list)) {
            return node;
        }

        if (auto right = ytLengthInput.Maybe<TCoRight>()) {
            ytLengthInput = right.Cast().Input();
        }
        // Now ytLengthInput is either YtReadTable or YtOutput

        TVector<TCoNameValueTuple> takeSkip;
        if (auto maybeRead = ytLengthInput.Maybe<TYtReadTable>()) {
            auto read = maybeRead.Cast();
            YQL_ENSURE(read.Input().Size() == 1);
            TYtSection section = read.Input().Item(0);
            bool needMaterialize = NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Sample)
                || AnyOf(section.Paths(), [](const TYtPath& path) { return !path.Ranges().Maybe<TCoVoid>() || TYtTableBaseInfo::GetMeta(path.Table())->IsDynamic; });
            for (auto s: section.Settings()) {
                switch (FromString<EYtSettingType>(s.Name().Value())) {
                case EYtSettingType::Take:
                case EYtSettingType::Skip:
                    takeSkip.push_back(s);
                    break;
                default:
                    // Skip other settings
                    break;
                }
            }

            if (needMaterialize) {
                if (State_->Types->EvaluationInProgress) {
                    return node;
                }

                auto scheme = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                ytLengthInput = CopyOrTrivialMap(section.Pos(),
                    TExprBase(ctx.NewWorld(section.Pos())),
                    TYtDSink(ctx.RenameNode(read.DataSource().Ref(), "DataSink")),
                    *scheme,
                    Build<TYtSection>(ctx, section.Pos())
                        .Paths(section.Paths())
                        .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip
                            | EYtSettingType::DirectRead | EYtSettingType::Unordered | EYtSettingType::NonUnique, ctx))
                        .Done(),
                    {}, ctx, State_, TCopyOrTrivialMapOpts()).Table();
            }
            else {
                auto settings = section.Settings().Ptr();
                if (!takeSkip.empty()) {
                    settings = NYql::RemoveSettings(*settings, EYtSettingType::Take | EYtSettingType::Skip
                        | EYtSettingType::DirectRead | EYtSettingType::Unordered | EYtSettingType::NonUnique, ctx);
                }

                if (read.World().Ref().Type() == TExprNode::World && settings->ChildrenSize() == 0 && section.Paths().Size() == 1 && section.Paths().Item(0).Table().Maybe<TYtOutput>()) {
                    // Simplify
                    ytLengthInput = Build<TYtOutput>(ctx, section.Paths().Item(0).Table().Pos())
                        .InitFrom(section.Paths().Item(0).Table().Cast<TYtOutput>())
                        .Mode()
                            .Value(ToString(EYtSettingType::Unordered))
                        .Build()
                        .Done();

                } else {
                    ytLengthInput = Build<TYtReadTable>(ctx, read.Pos())
                        .InitFrom(read)
                        .Input()
                            .Add()
                                .InitFrom(MakeUnorderedSection(section, ctx))
                                .Settings(settings)
                            .Build()
                        .Build()
                        .Done();
                }
            }
        }
        else {
            ytLengthInput = Build<TYtOutput>(ctx, ytLengthInput.Pos())
                .InitFrom(ytLengthInput.Cast<TYtOutput>())
                .Mode()
                    .Value(ToString(EYtSettingType::Unordered))
                .Build()
                .Done();
        }

        TExprBase res = Build<TYtLength>(ctx, node.Pos())
            .Input(ytLengthInput)
            .Done();

        for (TCoNameValueTuple s: takeSkip) {
            switch (FromString<EYtSettingType>(s.Name().Value())) {
            case EYtSettingType::Take:
                res = Build<TCoMin>(ctx, node.Pos())
                    .Add(res)
                    .Add(s.Value().Cast())
                    .Done();
                break;
            case EYtSettingType::Skip:
                res = Build<TCoSub>(ctx, node.Pos())
                    .Left<TCoMax>()
                        .Add(res)
                        .Add(s.Value().Cast())
                    .Build()
                    .Right(s.Value().Cast())
                    .Done();
                break;
            default:
                break;
            }
        }

        if (node.Maybe<TCoHasItems>()) {
            res = Build<TCoAggrNotEqual>(ctx, node.Pos())
                .Left(res)
                .Right<TCoUint64>()
                    .Literal()
                        .Value(0U)
                    .Build()
                .Build()
                .Done();
        }

        return res;
    }

    TMaybeNode<TExprBase> ResPull(TExprBase node, TExprContext& ctx) const {
        auto resPull = node.Cast<TResPull>();

        auto maybeRead = resPull.Data().Maybe<TCoRight>().Input().Maybe<TYtReadTable>();
        if (!maybeRead) {
            // Nothing to optimize in case of ResPull! over YtOutput!
            return node;
        }

        auto read = maybeRead.Cast();
        if (read.Input().Size() != 1) {
            return node;
        }
        auto section = read.Input().Item(0);
        auto scheme = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        bool directRead = NYql::HasSetting(section.Settings().Ref(), EYtSettingType::DirectRead);
        const bool hasSettings = NYql::HasAnySetting(section.Settings().Ref(),
            EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::Sample | EYtSettingType::SysColumns);

        const ui64 nativeTypeFlags = State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES)
             ? GetNativeYtTypeFlags(*scheme->Cast<TStructExprType>())
             : 0ul;

        bool requiresMapOrMerge = false;
        bool hasRanges = false;
        bool hasNonTemp = false;
        bool hasDynamic = false;
        bool first = true;
        TMaybe<NYT::TNode> firstNativeType;
        for (auto path: section.Paths()) {
            TYtPathInfo pathInfo(path);
            if (first) {
                first = false;
                firstNativeType = pathInfo.GetNativeYtType();
            }
            requiresMapOrMerge = requiresMapOrMerge || pathInfo.Table->RequiresRemap()
                || !IsSameAnnotation(*scheme, *pathInfo.Table->RowSpec->GetType())
                || nativeTypeFlags != pathInfo.GetNativeYtTypeFlags()
                || firstNativeType != pathInfo.GetNativeYtType();
            hasRanges = hasRanges || pathInfo.Ranges;
            hasNonTemp = hasNonTemp || !pathInfo.Table->IsTemp;
            hasDynamic = hasDynamic || pathInfo.Table->Meta->IsDynamic;
        }

        if (!requiresMapOrMerge && !hasRanges && !hasSettings)
            return node;

        // Ignore DirectRead pragma for temporary tables and dynamic tables with sampling or ranges
        if (!hasNonTemp || (hasDynamic && (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Sample) || hasRanges))) {
            directRead = false;
        }

        if (directRead) {
            return node;
        }

        bool keepSorted = ctx.IsConstraintEnabled<TSortedConstraintNode>()
            ? (!NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Unordered) && !hasNonTemp && section.Paths().Size() == 1) // single sorted input from operation
            : (!hasDynamic || !NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip)); // compatibility - all except dynamic with limit
        auto path = CopyOrTrivialMap(read.Pos(),
            read.World(),
            TYtDSink(ctx.RenameNode(read.DataSource().Ref(), "DataSink")),
            *scheme,
            Build<TYtSection>(ctx, section.Pos())
                .Paths(section.Paths())
                .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::DirectRead | EYtSettingType::Unordered | EYtSettingType::NonUnique, ctx))
                .Done(),
            {}, ctx, State_,
            TCopyOrTrivialMapOpts().SetTryKeepSortness(keepSorted).SetSectionUniq(section.Ref().GetConstraint<TDistinctConstraintNode>()).SetConstraints(read.Ref().GetConstraintSet()));

        auto newData = path.Columns().Maybe<TCoVoid>() && path.Ranges().Maybe<TCoVoid>()
            ? path.Table()
            : Build<TCoRight>(ctx, resPull.Pos())
                .Input<TYtReadTable>()
                    .World(ctx.NewWorld(resPull.Pos()))
                    .DataSource(read.DataSource())
                    .Input()
                        .Add()
                            .Paths()
                                .Add(path)
                            .Build()
                            .Settings()
                            .Build()
                        .Build()
                    .Build()
                .Build()
                .Done();

        return ctx.ChangeChild(resPull.Ref(), TResPull::idx_Data, newData.Ptr());
    }

    struct TRangeBuildResult {
        TVector<TString> Keys;
        TSet<size_t> TableIndexes;
        IPredicateRangeExtractor::TBuildResult BuildResult;
    };

    static TMaybeNode<TCoLambda> GetLambdaWithPredicate(TCoLambda lambda) {
        if (auto innerFlatMap = lambda.Body().Maybe<TCoFlatMapBase>()) {
            if (auto arg = innerFlatMap.Input().Maybe<TCoFilterNullMembersBase>().Input().Maybe<TCoJust>().Input()) {
                if (arg.Cast().Raw() == lambda.Args().Arg(0).Raw()) {
                    lambda = innerFlatMap.Lambda().Cast();
                }
            }
        }
        if (!lambda.Body().Maybe<TCoConditionalValueBase>()) {
            return {};
        }
        return lambda;
    }

    TMaybe<TVector<TRangeBuildResult>> ExtractKeyRangeFromLambda(TCoLambda lambda, TYtSection section, TExprContext& ctx) const {
        YQL_ENSURE(lambda.Body().Maybe<TCoConditionalValueBase>().IsValid());

        TMap<TVector<TString>, TSet<size_t>> tableIndexesBySortKey;
        TMap<size_t, TString> tableNamesByIndex;
        for (size_t tableIndex = 0; tableIndex < section.Paths().Size(); ++tableIndex) {
            TYtPathInfo pathInfo(section.Paths().Item(tableIndex));
            if (pathInfo.Ranges) {
                return TVector<TRangeBuildResult>{};
            }
            TYtTableBaseInfo::TPtr tableInfo = pathInfo.Table;
            if (tableInfo->RowSpec) {
                auto rowSpec = tableInfo->RowSpec;
                if (rowSpec->IsSorted()) {
                    YQL_ENSURE(rowSpec->SortMembers.size() <= rowSpec->SortDirections.size());
                    TVector<TString> keyPrefix;
                    for (size_t i = 0; i < rowSpec->SortMembers.size(); ++i) {
                        if (!rowSpec->SortDirections[i]) {
                            // TODO: allow native descending YT sort if UseYtKeyBounds is enabled
                            break;
                        }
                        keyPrefix.push_back(rowSpec->SortMembers[i]);
                    }
                    if (!keyPrefix.empty()) {
                        tableIndexesBySortKey[keyPrefix].insert(tableIndex);
                    }
                    tableNamesByIndex[tableIndex] = tableInfo->Name;
                }
            }
        }
        if (tableIndexesBySortKey.empty()) {
            return TVector<TRangeBuildResult>{};
        }

        TPredicateExtractorSettings rangeSettings;
        rangeSettings.MergeAdjacentPointRanges = State_->Configuration->MergeAdjacentPointRanges.Get().GetOrElse(DEFAULT_MERGE_ADJACENT_POINT_RANGES);
        rangeSettings.HaveNextValueCallable = State_->Configuration->KeyFilterForStartsWith.Get().GetOrElse(DEFAULT_KEY_FILTER_FOR_STARTS_WITH);
        rangeSettings.MaxRanges = State_->Configuration->MaxKeyRangeCount.Get().GetOrElse(DEFAULT_MAX_KEY_RANGE_COUNT);

        THashSet<TString> possibleIndexKeys;
        auto rowType = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        auto extractor = MakePredicateRangeExtractor(rangeSettings);
        if (!extractor->Prepare(lambda.Ptr(), *rowType, possibleIndexKeys, ctx, *State_->Types)) {
            return Nothing();
        }

        TVector<TRangeBuildResult> results;
        for (auto& [keys, tableIndexes] : tableIndexesBySortKey) {
            if (AllOf(keys, [&possibleIndexKeys](const TString& key) { return !possibleIndexKeys.contains(key); })) {
                continue;
            }

            TRangeBuildResult result;
            result.Keys = keys;
            result.TableIndexes = tableIndexes;
            result.BuildResult = extractor->BuildComputeNode(keys, ctx, *State_->Types);
            auto& compute = result.BuildResult.ComputeNode;
            if (compute) {
                compute = ctx.NewCallable(compute->Pos(), "EvaluateExprIfPure", { compute });
                results.push_back(result);

                TVector<TString> tableNames;
                for (const auto& idx : tableIndexes) {
                    YQL_ENSURE(tableNamesByIndex.contains(idx));
                    tableNames.push_back(tableNamesByIndex[idx]);
                }

                YQL_CLOG(INFO, ProviderYt) << __FUNCTION__
                    << ": Will use key filter for tables [" << JoinSeq(",", tableNames) << "] with key columns ["
                    << JoinSeq(",", keys) << "]";
            }
        }

        return results;
    }

    TExprNode::TPtr UpdateSectionWithKeyRanges(TPositionHandle pos, TYtSection section, const TVector<TRangeBuildResult>& results, TExprContext& ctx) const {
        const bool sameSort = results.size() == 1 && results.front().TableIndexes.size() == section.Paths().Size();

        auto newSettingsChildren = section.Settings().Ref().ChildrenList();

        TExprNode::TListType updatedPaths = section.Paths().Ref().ChildrenList();
        bool hasPathUpdates = false;
        for (auto& result : results) {
            TExprNodeList items = { result.BuildResult.ComputeNode };

            TExprNodeList usedKeys;
            YQL_ENSURE(result.BuildResult.UsedPrefixLen <= result.Keys.size());
            for (size_t i = 0; i < result.BuildResult.UsedPrefixLen; ++i) {
                usedKeys.push_back(ctx.NewAtom(pos, result.Keys[i]));
            }
            auto usedKeysNode = ctx.NewList(pos, std::move(usedKeys));
            auto usedKeysSetting = ctx.NewList(pos, { ctx.NewAtom(pos, "usedKeys"), usedKeysNode });
            items.push_back(ctx.NewList(pos, { usedKeysSetting }));

            for (const auto& idx : result.TableIndexes) {
                if (auto out = TYtPath(updatedPaths[idx]).Table().Maybe<TYtOutput>(); out && IsUnorderedOutput(out.Cast())) {
                    updatedPaths[idx] = Build<TYtPath>(ctx, updatedPaths[idx]->Pos())
                        .InitFrom(TYtPath(updatedPaths[idx]))
                        .Table<TYtOutput>()
                            .InitFrom(out.Cast())
                            .Mode(TMaybeNode<TCoAtom>())
                        .Build()
                        .Done().Ptr();
                    hasPathUpdates = true;
                }
            }

            if (!sameSort) {
                TExprNodeList idxs;
                for (const auto& idx : result.TableIndexes) {
                    idxs.push_back(ctx.NewAtom(pos, idx));
                }
                items.push_back(ctx.NewList(pos, std::move(idxs)));
            }
            newSettingsChildren.push_back(
                Build<TCoNameValueTuple>(ctx, pos)
                    .Name()
                        .Value(ToString(EYtSettingType::KeyFilter2))
                    .Build()
                    .Value(ctx.NewList(pos, std::move(items)))
                    .Done().Ptr());
        }

        auto newSection = ctx.ChangeChild(section.Ref(), TYtSection::idx_Settings,
            ctx.NewList(section.Settings().Pos(), std::move(newSettingsChildren)));
        if (hasPathUpdates) {
            newSection = ctx.ChangeChild(*newSection, TYtSection::idx_Paths,
                ctx.NewList(section.Paths().Pos(), std::move(updatedPaths)));
        }
        return newSection;
    }

    TMaybeNode<TExprBase> ExtractKeyRangeDqReadWrap(TExprBase node, TExprContext& ctx) const {
        auto flatMap = node.Cast<TCoFlatMapBase>();
        auto maybeYtRead = flatMap.Input().Maybe<TDqReadWrapBase>().Input().Maybe<TYtReadTable>();
        if (!maybeYtRead) {
            return node;
        }
        auto ytRead = maybeYtRead.Cast();
        if (ytRead.Input().Size() > 1) {
            return node;
        }

        TYtDSource dataSource = GetDataSource(ytRead, ctx);
        if (!State_->Configuration->_EnableYtPartitioning.Get(dataSource.Cluster().StringValue()).GetOrElse(false)) {
            return node;
        }

        auto section = ytRead.Input().Item(0);
        if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Take | EYtSettingType::Skip)) {
            return node;
        }

        auto maybeLambda = GetLambdaWithPredicate(flatMap.Lambda());
        if (!maybeLambda) {
            return node;
        }
        auto lambda = maybeLambda.Cast();
        auto maybeResult = ExtractKeyRangeFromLambda(lambda, section, ctx);
        if (!maybeResult) {
            return {};
        }
        auto results = *maybeResult;
        if (results.empty()) {
            return node;
        }
        auto predPos = lambda.Body().Cast<TCoConditionalValueBase>().Predicate().Pos();
        auto newSection = UpdateSectionWithKeyRanges(predPos, section, results, ctx);

        auto newYtRead = Build<TYtReadTable>(ctx, ytRead.Pos())
            .InitFrom(ytRead)
            .Input()
                .Add(newSection)
            .Build()
            .Done().Ptr();

        auto newFlatMap = ctx.ChangeChild(flatMap.Ref(), TCoFlatMapBase::idx_Input,
            ctx.ChangeChild(flatMap.Input().Ref(), TDqReadWrapBase::idx_Input, std::move(newYtRead))
        );

        const bool sameSort = results.size() == 1 && results.front().TableIndexes.size() == section.Paths().Size();
        const bool pruneLambda = State_->Configuration->DqPruneKeyFilterLambda.Get().GetOrElse(DEFAULT_DQ_PRUNE_KEY_FILTER_LAMBDA);
        if (sameSort && pruneLambda) {
            YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": Will prune key filter lambda";
            newFlatMap = ctx.ReplaceNodes(std::move(newFlatMap), {{ lambda.Raw(), results.front().BuildResult.PrunedLambda }});
        }

        return newFlatMap;
    }

    TMaybeNode<TExprBase> ExtractKeyRange(TExprBase node, TExprContext& ctx) const {
        auto op = node.Cast<TYtTransientOpBase>();
        if (op.Input().Size() > 1) {
            // Extract key ranges before horizontal joins
            return node;
        }
        if (op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoVoid>()) {
            return node;
        }

        auto section = op.Input().Item(0);
        if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Take | EYtSettingType::Skip)) {
            return node;
        }

        TCoLambda mapper = op.Maybe<TYtMap>() ? op.Cast<TYtMap>().Mapper() : op.Cast<TYtMapReduce>().Mapper().Cast<TCoLambda>();
        auto maybeLambda = GetFlatMapOverInputStream(mapper).Lambda();
        if (!maybeLambda) {
            return node;
        }
        maybeLambda = GetLambdaWithPredicate(maybeLambda.Cast());
        if (!maybeLambda) {
            return node;
        }
        auto lambda = maybeLambda.Cast();
        auto maybeResult = ExtractKeyRangeFromLambda(lambda, section, ctx);
        if (!maybeResult) {
            return {};
        }
        auto results = *maybeResult;
        if (results.empty()) {
            return node;
        }

        auto predPos = lambda.Body().Cast<TCoConditionalValueBase>().Predicate().Pos();
        auto newSection = UpdateSectionWithKeyRanges(predPos, section, results, ctx);

        auto newOp = ctx.ChangeChild(op.Ref(),
            TYtTransientOpBase::idx_Input,
            Build<TYtSectionList>(ctx, op.Input().Pos())
                .Add(newSection)
            .Done().Ptr()
        );

        const bool sameSort = results.size() == 1 && results.front().TableIndexes.size() == section.Paths().Size();
        const bool pruneLambda = State_->Configuration->PruneKeyFilterLambda.Get().GetOrElse(DEFAULT_PRUNE_KEY_FILTER_LAMBDA);
        if (sameSort && pruneLambda) {
            YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": Will prune key filter lambda";
            newOp = ctx.ReplaceNodes(std::move(newOp), {{ lambda.Raw(), results.front().BuildResult.PrunedLambda }});
        }

        return newOp;
    }

    // All keyFilter settings are combined by OR.
    // keyFilter value := '(<memberItem>+) <optional tableIndex>
    // <memberItem> := '(<memberName> '(<cmpItem>+))
    // <cmpItem> := '(<cmpOp> <value>)
    TMaybeNode<TExprBase> ExtractKeyRangeLegacy(TExprBase node, TExprContext& ctx) const {
        auto op = node.Cast<TYtTransientOpBase>();
        if (op.Input().Size() > 1) {
            // Extract key ranges before horizontal joins
            return node;
        }
        if (op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoVoid>()) {
            return node;
        }

        auto section = op.Input().Item(0);
        if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Take | EYtSettingType::Skip)) {
            return node;
        }

        TYtSortMembersCollection sortMembers;
        for (size_t tableIndex = 0; tableIndex < section.Paths().Size(); ++tableIndex) {
            TYtPathInfo pathInfo(section.Paths().Item(tableIndex));
            if (pathInfo.Ranges) {
                return node;
            }
            TYtTableBaseInfo::TPtr tableInfo = pathInfo.Table;
            if (tableInfo->RowSpec && tableInfo->RowSpec->IsSorted() && !tableInfo->RowSpec->SortMembers.empty()) {
                sortMembers.AddTableInfo(tableIndex, tableInfo->Name,
                    tableInfo->RowSpec->SortMembers,
                    tableInfo->RowSpec->SortedByTypes,
                    tableInfo->RowSpec->SortDirections);
            }
        }
        if (sortMembers.Empty()) {
            return node;
        }

        TCoLambda mapper = op.Maybe<TYtMap>() ? op.Cast<TYtMap>().Mapper() : op.Cast<TYtMapReduce>().Mapper().Cast<TCoLambda>();
        auto maybeLambda = GetFlatMapOverInputStream(mapper).Lambda();
        if (!maybeLambda) {
            return node;
        }
        TCoLambda lambda = maybeLambda.Cast();
        if (auto innerFlatMap = lambda.Body().Maybe<TCoFlatMapBase>()) {
            if (auto arg = innerFlatMap.Input().Maybe<TCoFilterNullMembersBase>().Input().Maybe<TCoJust>().Input()) {
                if (arg.Cast().Raw() == lambda.Args().Arg(0).Raw()) {
                    lambda = innerFlatMap.Lambda().Cast();
                }
            }
        }
        if (!lambda.Body().Maybe<TCoConditionalValueBase>()) {
            return node;
        }
        auto predicate = lambda.Body().Cast<TCoConditionalValueBase>().Predicate();
        if (predicate.Ref().Type() != TExprNode::Callable) {
            return node;
        }

        const size_t maxTables = State_->Configuration->MaxInputTables.Get().GetOrElse(DEFAULT_MAX_INPUT_TABLES);
        TVector<TKeyFilterPredicates> ranges;
        if (!CollectKeyPredicates(lambda.Args().Arg(0), predicate, ranges, maxTables)) {
            return node;
        }

        if (ranges.size() > maxTables) {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": too many tables - " << ranges.size();
            return node;
        }

        if (!sortMembers.ApplyRanges(ranges, ctx)) {
            return {};
        }

        if (sortMembers.Empty()) {
            return node;
        }

        auto newSettingsChildren = section.Settings().Ref().ChildrenList();
        sortMembers.BuildKeyFilters(predicate.Pos(), section.Paths().Size(), ranges.size(), newSettingsChildren, ctx);

        auto newSection = ctx.ChangeChild(section.Ref(), TYtSection::idx_Settings,
            ctx.NewList(section.Settings().Pos(), std::move(newSettingsChildren)));
        return ctx.ChangeChild(op.Ref(), TYtTransientOpBase::idx_Input,
            Build<TYtSectionList>(ctx, op.Input().Pos())
                .Add(newSection)
                .Done().Ptr()
            );
    }

    template <typename TLMapType>
    TMaybeNode<TExprBase> LMap(TExprBase node, TExprContext& ctx) const {
        if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
            return node;
        }

        auto lmap = node.Cast<TLMapType>();

        if (!IsYtProviderInput(lmap.Input(), true)) {
            return node;
        }

        const auto inItemType = GetSequenceItemType(lmap.Input(), true, ctx);
        if (!inItemType) {
            return {};
        }
        const auto outItemType = SilentGetSequenceItemType(lmap.Lambda().Body().Ref(), true);
        if (!outItemType || !outItemType->IsPersistable()) {
            return node;
        }

        auto cluster = TString{GetClusterName(lmap.Input())};
        TSyncMap syncList;
        if (!IsYtCompleteIsolatedLambda(lmap.Lambda().Ref(), syncList, cluster, true, false)) {
            return node;
        }

        auto cleanup = CleanupWorld(lmap.Lambda(), ctx);
        if (!cleanup) {
            return {};
        }

        auto mapper = cleanup.Cast().Ptr();
        bool sortedOutput = false;
        TVector<TYtOutTable> outTables = ConvertOutTablesWithSortAware(mapper, sortedOutput, lmap.Pos(),
            outItemType, ctx, State_, lmap.Ref().GetConstraintSet());

        const bool useFlow = State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW);

        auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, lmap.Pos());
        if (std::is_same<TLMapType, TCoOrderedLMap>::value) {
            settingsBuilder
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::Ordered))
                    .Build()
                .Build();
        }

        if (useFlow) {
            settingsBuilder
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::Flow))
                    .Build()
                .Build();
        }

        auto map = Build<TYtMap>(ctx, lmap.Pos())
            .World(ApplySyncListToWorld(GetWorld(lmap.Input(), {}, ctx).Ptr(), syncList, ctx))
            .DataSink(GetDataSink(lmap.Input(), ctx))
            .Input(ConvertInputTable(lmap.Input(), ctx))
            .Output()
                .Add(outTables)
            .Build()
            .Settings(settingsBuilder.Done())
            .Mapper(MakeJobLambda<false>(TCoLambda(mapper), useFlow, ctx))
            .Done();

        return WrapOp(map, ctx);
    }

    TMaybeNode<TExprBase> FuseReduce(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto outerReduce = node.Cast<TYtReduce>();

        if (outerReduce.Input().Size() != 1 || outerReduce.Input().Item(0).Paths().Size() != 1) {
            return node;
        }
        if (outerReduce.Input().Item(0).Settings().Size() != 0) {
            return node;
        }
        TYtPath path = outerReduce.Input().Item(0).Paths().Item(0);
        if (!path.Ranges().Maybe<TCoVoid>()) {
            return node;
        }
        auto maybeInnerReduce = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtReduce>();
        if (!maybeInnerReduce) {
            return node;
        }
        TYtReduce innerReduce = maybeInnerReduce.Cast();

        if (innerReduce.Ref().StartsExecution() || innerReduce.Ref().HasResult()) {
            return node;
        }
        if (innerReduce.Output().Size() > 1) {
            return node;
        }

        if (outerReduce.DataSink().Cluster().Value() != innerReduce.DataSink().Cluster().Value()) {
            return node;
        }

        const TParentsMap* parentsReduce = getParents();
        if (IsOutputUsedMultipleTimes(innerReduce.Ref(), *parentsReduce)) {
            // Inner reduce output is used more than once
            return node;
        }
        // Check world dependencies
        auto parentsIt = parentsReduce->find(innerReduce.Raw());
        YQL_ENSURE(parentsIt != parentsReduce->cend());
        for (auto dep: parentsIt->second) {
            if (!TYtOutput::Match(dep)) {
                return node;
            }
        }

        if (!NYql::HasSetting(innerReduce.Settings().Ref(), EYtSettingType::KeySwitch) ||
            !NYql::HasSetting(innerReduce.Settings().Ref(), EYtSettingType::Flow) ||
            !NYql::HasSetting(innerReduce.Settings().Ref(), EYtSettingType::ReduceBy)) {
            return node;
        }

        if (NYql::HasSetting(outerReduce.Settings().Ref(), EYtSettingType::SortBy)) {
            return node;
        }

        if (NYql::HasSettingsExcept(innerReduce.Settings().Ref(), EYtSettingType::ReduceBy |
                                                                 EYtSettingType::KeySwitch |
                                                                 EYtSettingType::Flow |
                                                                 EYtSettingType::FirstAsPrimary |
                                                                 EYtSettingType::SortBy |
                                                                 EYtSettingType::NoDq)) {
            return node;
        }

        if (!EqualSettingsExcept(innerReduce.Settings().Ref(), outerReduce.Settings().Ref(),
                                                                EYtSettingType::ReduceBy |
                                                                EYtSettingType::FirstAsPrimary |
                                                                EYtSettingType::NoDq |
                                                                EYtSettingType::SortBy)) {
            return node;
        }

        auto innerLambda = innerReduce.Reducer();
        auto outerLambda = outerReduce.Reducer();
        auto fuseRes = CanFuseLambdas(innerLambda, outerLambda, ctx);
        if (!fuseRes) {
            // Some error
            return {};
        }
        if (!*fuseRes) {
            // Cannot fuse
            return node;
        }

        auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(outerLambda.Ptr(), ctx, State_->Types);
        if (!placeHolder) {
            return {};
        }


        if (lambdaWithPlaceholder != outerLambda.Ptr()) {
            outerLambda = TCoLambda(lambdaWithPlaceholder);
        }

        innerLambda = FallbackLambdaOutput(innerLambda, ctx);
        outerLambda = FallbackLambdaInput(outerLambda, ctx);


        const auto outerReduceBy = NYql::GetSettingAsColumnList(outerReduce.Settings().Ref(), EYtSettingType::ReduceBy);
        auto reduceByList = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            size_t index = 0;
            for (const auto& reduceByName: outerReduceBy) {
                parent.Callable(index++, "Member")
                    .Arg(0, "item")
                    .Atom(1, reduceByName)
                .Seal();
            }
            return parent;
        };

        // adds _yql_sys_tablekeyswitch column which is required for outer lambda
        // _yql_sys_tableswitch equals "true" when reduce key is changed
        TExprNode::TPtr keySwitchLambda = ctx.Builder(node.Pos())
            .Lambda()
                .Param("stream")
                .Callable(0, "Fold1Map")
                    .Arg(0, "stream")
                    .Lambda(1)
                        .Param("item")
                        .List(0)
                            .Callable(0, "AddMember")
                                .Arg(0, "item")
                                .Atom(1, "_yql_sys_tablekeyswitch")
                                .Callable(2, "Bool").Atom(0, "true").Seal()
                            .Seal()
                            .List(1).Do(reduceByList).Seal()
                        .Seal()
                    .Seal()
                    .Lambda(2)
                        .Param("item")
                        .Param("state")
                        .List(0)
                            .Callable(0, "AddMember")
                                .Arg(0, "item")
                                .Atom(1, "_yql_sys_tablekeyswitch")
                                .Callable(2, "If")
                                    .Callable(0, "AggrEquals")
                                        .List(0).Do(reduceByList).Seal()
                                        .Arg(1, "state")
                                    .Seal()
                                    .Callable(1, "Bool").Atom(0, "false").Seal()
                                    .Callable(2, "Bool").Atom(0, "true").Seal()
                                .Seal()
                            .Seal()
                            .List(1).Do(reduceByList).Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Build();

        auto newSettings = innerReduce.Settings().Ptr();
        if (NYql::HasSetting(outerReduce.Settings().Ref(), EYtSettingType::NoDq) &&
           !NYql::HasSetting(innerReduce.Settings().Ref(), EYtSettingType::NoDq)) {
            newSettings = NYql::AddSetting(*newSettings, EYtSettingType::NoDq, {}, ctx);
        }

        return Build<TYtReduce>(ctx, node.Pos())
            .InitFrom(outerReduce)
            .World<TCoSync>()
                .Add(innerReduce.World())
                .Add(outerReduce.World())
            .Build()
            .Input(innerReduce.Input())
            .Reducer()
                .Args({"stream"})
                .Body<TExprApplier>()
                    .Apply(outerLambda)
                    .With<TExprApplier>(0)
                        .Apply(TCoLambda(keySwitchLambda))
                        .With<TExprApplier>(0)
                            .Apply(innerLambda)
                            .With(0, "stream")
                        .Build()
                    .Build()
                    .With(TExprBase(placeHolder), "stream")
                .Build()
            .Build()
            .Settings(newSettings)
            .Done();
    }

    TMaybeNode<TExprBase> FuseInnerMap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto outerMap = node.Cast<TYtMap>();
        if (outerMap.Input().Size() != 1 || outerMap.Input().Item(0).Paths().Size() != 1) {
            return node;
        }

        TYtPath path = outerMap.Input().Item(0).Paths().Item(0);
        auto maybeInnerMap = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtMap>();
        if (!maybeInnerMap) {
            return node;
        }
        TYtMap innerMap = maybeInnerMap.Cast();

        if (innerMap.Ref().StartsExecution() || innerMap.Ref().HasResult()) {
            return node;
        }
        if (innerMap.Output().Size() > 1) {
            return node;
        }
        if (outerMap.DataSink().Cluster().Value() != innerMap.DataSink().Cluster().Value()) {
            return node;
        }
        if (NYql::HasAnySetting(innerMap.Settings().Ref(), EYtSettingType::Limit | EYtSettingType::SortLimitBy | EYtSettingType::JobCount)) {
            return node;
        }
        if (NYql::HasAnySetting(outerMap.Input().Item(0).Settings().Ref(),
            EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::DirectRead | EYtSettingType::Sample | EYtSettingType::SysColumns))
        {
            return node;
        }
        if (NYql::HasSetting(innerMap.Settings().Ref(), EYtSettingType::Flow) != NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Flow)) {
            return node;
        }
        if (NYql::HasAnySetting(outerMap.Settings().Ref(), EYtSettingType::JobCount)) {
            return node;
        }
        if (!path.Ranges().Maybe<TCoVoid>()) {
            return node;
        }

        if (NYql::HasNonEmptyKeyFilter(outerMap.Input().Item(0))) {
            return node;
        }

        const TParentsMap* parentsMap = getParents();
        if (IsOutputUsedMultipleTimes(innerMap.Ref(), *parentsMap)) {
            // Inner map output is used more than once
            return node;
        }
        // Check world dependencies
        auto parentsIt = parentsMap->find(innerMap.Raw());
        YQL_ENSURE(parentsIt != parentsMap->cend());
        for (auto dep: parentsIt->second) {
            if (!TYtOutput::Match(dep)) {
                return node;
            }
        }

        auto innerLambda = innerMap.Mapper();
        auto outerLambda = outerMap.Mapper();
        if (HasYtRowNumber(outerLambda.Body().Ref())) {
            return node;
        }

        auto fuseRes = CanFuseLambdas(innerLambda, outerLambda, ctx);
        if (!fuseRes) {
            // Some error
            return {};
        }
        if (!*fuseRes) {
            // Cannot fuse
            return node;
        }

        const bool unorderedOut = IsUnorderedOutput(path.Table().Cast<TYtOutput>());

        auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(outerLambda.Ptr(), ctx, State_->Types);
        if (!placeHolder) {
            return {};
        }

        if (lambdaWithPlaceholder != outerLambda.Ptr()) {
            outerLambda = TCoLambda(lambdaWithPlaceholder);
        }

        innerLambda = FallbackLambdaOutput(innerLambda, ctx);
        if (unorderedOut) {
            innerLambda = Build<TCoLambda>(ctx, innerLambda.Pos())
                .Args({"stream"})
                .Body<TCoUnordered>()
                    .Input<TExprApplier>()
                        .Apply(innerLambda)
                        .With(0, "stream")
                    .Build()
                .Build()
                .Done();
        }
        outerLambda = FallbackLambdaInput(outerLambda, ctx);

        if (!path.Columns().Maybe<TCoVoid>()) {
            const bool ordered = !unorderedOut && NYql::HasSetting(innerMap.Settings().Ref(), EYtSettingType::Ordered)
                && NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Ordered);
            outerLambda = MapEmbedInputFieldsFilter(outerLambda, ordered, path.Columns().Cast<TCoAtomList>(), ctx);
        } else if (TYqlRowSpecInfo(innerMap.Output().Item(0).RowSpec()).HasAuxColumns()) {
            auto itemType = GetSequenceItemType(path, false, ctx);
            if (!itemType) {
                return {};
            }
            TSet<TStringBuf> fields;
            for (auto item: itemType->Cast<TStructExprType>()->GetItems()) {
                fields.insert(item->GetName());
            }
            const bool ordered = !unorderedOut && NYql::HasSetting(innerMap.Settings().Ref(), EYtSettingType::Ordered)
                && NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Ordered);
            outerLambda = MapEmbedInputFieldsFilter(outerLambda, ordered, TCoAtomList(ToAtomList(fields, node.Pos(), ctx)), ctx);
        }

        const auto mergedSettings = MergeSettings(
            *NYql::RemoveSettings(outerMap.Settings().Ref(), EYtSettingType::Flow, ctx),
            *NYql::RemoveSettings(innerMap.Settings().Ref(), EYtSettingType::Ordered | EYtSettingType::KeepSorted, ctx), ctx);

        return Build<TYtMap>(ctx, node.Pos())
            .InitFrom(outerMap)
            .World<TCoSync>()
                .Add(innerMap.World())
                .Add(outerMap.World())
            .Build()
            .Input(innerMap.Input())
            .Mapper()
                .Args({"stream"})
                .Body<TExprApplier>()
                    .Apply(outerLambda)
                    .With<TExprApplier>(0)
                        .Apply(innerLambda)
                        .With(0, "stream")
                    .Build()
                    .With(TExprBase(placeHolder), "stream")
                .Build()
            .Build()
            .Settings(mergedSettings)
            .Done();
    }

    TMaybeNode<TExprBase> FuseOuterMap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto outerMap = node.Cast<TYtMap>();
        if (outerMap.Input().Size() != 1 || outerMap.Input().Item(0).Paths().Size() != 1) {
            return node;
        }

        TYtPath path = outerMap.Input().Item(0).Paths().Item(0);
        auto maybeInner = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtWithUserJobsOpBase>();
        if (!maybeInner) {
            return node;
        }
        if (!maybeInner.Maybe<TYtReduce>() && !maybeInner.Maybe<TYtMapReduce>()) {
            return node;
        }
        auto inner = maybeInner.Cast();

        if (inner.Ref().StartsExecution() || inner.Ref().HasResult()) {
            return node;
        }
        if (inner.Output().Size() > 1) {
            return node;
        }
        if (outerMap.DataSink().Cluster().Value() != inner.DataSink().Cluster().Value()) {
            return node;
        }
        if (NYql::HasAnySetting(inner.Settings().Ref(), EYtSettingType::Limit | EYtSettingType::SortLimitBy | EYtSettingType::JobCount)) {
            return node;
        }
        if (NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::JobCount)) {
            return node;
        }
        if (outerMap.Input().Item(0).Settings().Size() != 0) {
            return node;
        }
        if (NYql::HasSetting(inner.Settings().Ref(), EYtSettingType::Flow) != NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Flow)) {
            return node;
        }
        if (!path.Ranges().Maybe<TCoVoid>()) {
            return node;
        }
        if (inner.Maybe<TYtMapReduce>()) {
            for (auto out: outerMap.Output()) {
                if (TYqlRowSpecInfo(out.RowSpec()).IsSorted()) {
                    return node;
                }
            }
        }

        const TParentsMap* parentsMap = getParents();
        if (IsOutputUsedMultipleTimes(inner.Ref(), *parentsMap)) {
            // Inner output is used more than once
            return node;
        }
        // Check world dependencies
        auto parentsIt = parentsMap->find(inner.Raw());
        YQL_ENSURE(parentsIt != parentsMap->cend());
        for (auto dep: parentsIt->second) {
            if (!TYtOutput::Match(dep)) {
                return node;
            }
        }

        auto outerLambda = outerMap.Mapper();
        if (HasYtRowNumber(outerLambda.Body().Ref())) {
            return node;
        }

        auto lambda = inner.Maybe<TYtMapReduce>() ? inner.Cast<TYtMapReduce>().Reducer() : inner.Cast<TYtReduce>().Reducer();

        auto fuseRes = CanFuseLambdas(lambda, outerLambda, ctx);
        if (!fuseRes) {
            // Some error
            return {};
        }
        if (!*fuseRes) {
            // Cannot fuse
            return node;
        }

        auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(outerLambda.Ptr(), ctx, State_->Types);
        if (!placeHolder) {
            return {};
        }

        if (lambdaWithPlaceholder != outerLambda.Ptr()) {
            outerLambda = TCoLambda(lambdaWithPlaceholder);
        }

        lambda = FallbackLambdaOutput(lambda, ctx);
        outerLambda = FallbackLambdaInput(outerLambda, ctx);

        if (!path.Columns().Maybe<TCoVoid>()) {
            const bool ordered = inner.Maybe<TYtReduce>() && TYqlRowSpecInfo(inner.Output().Item(0).RowSpec()).IsSorted()
                && NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Ordered);

            outerLambda = MapEmbedInputFieldsFilter(outerLambda, ordered, path.Columns().Cast<TCoAtomList>(), ctx);
        } else if (inner.Maybe<TYtReduce>() && TYqlRowSpecInfo(inner.Output().Item(0).RowSpec()).HasAuxColumns()) {
            auto itemType = GetSequenceItemType(path, false, ctx);
            if (!itemType) {
                return {};
            }
            TSet<TStringBuf> fields;
            for (auto item: itemType->Cast<TStructExprType>()->GetItems()) {
                fields.insert(item->GetName());
            }
            const bool ordered = NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Ordered);
            outerLambda = MapEmbedInputFieldsFilter(outerLambda, ordered, TCoAtomList(ToAtomList(fields, node.Pos(), ctx)), ctx);
        }

        lambda = Build<TCoLambda>(ctx, lambda.Pos())
            .Args({"stream"})
            .Body<TExprApplier>()
                .Apply(outerLambda)
                .With<TExprApplier>(0)
                    .Apply(lambda)
                    .With(0, "stream")
                .Build()
                .With(TExprBase(placeHolder), "stream")
            .Build()
            .Done();

        auto res = ctx.ChangeChild(inner.Ref(),
            inner.Maybe<TYtMapReduce>() ? TYtMapReduce::idx_Reducer : TYtReduce::idx_Reducer,
            lambda.Ptr());
        res = ctx.ChangeChild(*res, TYtWithUserJobsOpBase::idx_Output, outerMap.Output().Ptr());

        auto mergedSettings = NYql::RemoveSettings(outerMap.Settings().Ref(), EYtSettingType::Ordered | EYtSettingType::Sharded | EYtSettingType::Flow, ctx);
        mergedSettings = MergeSettings(inner.Settings().Ref(), *mergedSettings, ctx);
        res = ctx.ChangeChild(*res, TYtWithUserJobsOpBase::idx_Settings, std::move(mergedSettings));
        res = ctx.ChangeChild(*res, TYtWithUserJobsOpBase::idx_World,
            Build<TCoSync>(ctx, inner.Pos())
                .Add(inner.World())
                .Add(outerMap.World())
            .Done().Ptr());

        return TExprBase(res);
    }

    TMaybeNode<TExprBase> AssumeSorted(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
            return node;
        }

        auto assume = node.Cast<TCoAssumeSorted>();
        auto input = assume.Input();
        if (!IsYtProviderInput(input)) {
            return node;
        }

        auto sorted = node.Ref().GetConstraint<TSortedConstraintNode>();
        if (!sorted) {
            // Drop AssumeSorted with unsupported sort modes
            return input;
        }

        auto maybeOp = input.Maybe<TYtOutput>().Operation();
        bool needSeparateOp = !maybeOp
            || maybeOp.Raw()->StartsExecution()
            || (maybeOp.Raw()->HasResult() && maybeOp.Raw()->GetResult().Type() == TExprNode::World)
            || IsOutputUsedMultipleTimes(maybeOp.Ref(), *getParents())
            || maybeOp.Maybe<TYtMapReduce>()
            || maybeOp.Maybe<TYtEquiJoin>();

        bool canMerge = false;
        bool equalSort = false;
        if (auto inputSort = input.Ref().GetConstraint<TSortedConstraintNode>()) {
            if (sorted->IsPrefixOf(*inputSort)) {
                canMerge = true;
                equalSort = sorted->Equals(*inputSort);
            }
        }
        if (equalSort && maybeOp.Maybe<TYtSort>()) {
            return input;
        }

        const TStructExprType* outItemType = nullptr;
        if (auto type = GetSequenceItemType(node, false, ctx)) {
            outItemType = type->Cast<TStructExprType>();
        } else {
            return {};
        }

        const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);

        TKeySelectorBuilder builder(assume.Pos(), ctx, useNativeDescSort, outItemType);
        builder.ProcessConstraint(*sorted);
        needSeparateOp = needSeparateOp || (builder.NeedMap() && !equalSort);

        if (needSeparateOp) {
            TYtOutTableInfo outTable(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
            outTable.RowSpec->SetConstraints(assume.Ref().GetConstraintSet());

            if (auto maybeReadSettings = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>().Input().Item(0).Settings()) {
                if (NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::SysColumns)) {
                    canMerge = false;
                }
            }
            auto inputPaths = GetInputPaths(input);
            TMaybe<NYT::TNode> firstNativeType;
            if (!inputPaths.empty()) {
                firstNativeType = inputPaths.front()->GetNativeYtType();
            }

            canMerge = canMerge && AllOf(inputPaths, [&outTable, firstNativeType] (const TYtPathInfo::TPtr& path) {
                return !path->RequiresRemap()
                    && path->GetNativeYtTypeFlags() == outTable.RowSpec->GetNativeYtTypeFlags()
                    && firstNativeType == path->GetNativeYtType();
            });
            if (canMerge) {
                outTable.RowSpec->CopySortness(*inputPaths.front()->Table->RowSpec, TYqlRowSpecInfo::ECopySort::WithDesc);
                outTable.RowSpec->ClearSortness(sorted->GetContent().size());
                outTable.SetUnique(assume.Ref().GetConstraint<TDistinctConstraintNode>(), assume.Pos(), ctx);
                if (firstNativeType) {
                    outTable.RowSpec->CopyTypeOrders(*firstNativeType);
                }

                YQL_ENSURE(sorted->GetContent().size() == outTable.RowSpec->SortMembers.size());
                const bool useExplicitColumns = AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) {
                    return !path->Table->IsTemp || (path->Table->RowSpec && path->Table->RowSpec->HasAuxColumns());
                });

                TConvertInputOpts opts;
                if (useExplicitColumns) {
                    opts.ExplicitFields(*outTable.RowSpec, assume.Pos(), ctx);
                }

                return Build<TYtOutput>(ctx, assume.Pos())
                    .Operation<TYtMerge>()
                        .World(GetWorld(input, {}, ctx))
                        .DataSink(GetDataSink(input, ctx))
                        .Input(ConvertInputTable(input, ctx, opts))
                        .Output()
                            .Add(outTable.ToExprNode(ctx, assume.Pos()).Cast<TYtOutTable>())
                        .Build()
                        .Settings()
                            .Add()
                                .Name()
                                    .Value(ToString(EYtSettingType::KeepSorted))
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .OutIndex().Value(0U).Build()
                    .Done();
            }
            else {
                builder.FillRowSpecSort(*outTable.RowSpec);
                outTable.SetUnique(assume.Ref().GetConstraint<TDistinctConstraintNode>(), assume.Pos(), ctx);

                TCoLambda mapper = builder.NeedMap()
                    ? Build<TCoLambda>(ctx, assume.Pos())
                        .Args({"stream"})
                        .Body<TExprApplier>()
                            .Apply(TCoLambda(builder.MakeRemapLambda(true)))
                            .With(0, "stream")
                        .Build()
                    .Done()
                    : Build<TCoLambda>(ctx, assume.Pos())
                        .Args({"stream"})
                        .Body("stream")
                    .Done();

                auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, assume.Pos());
                settingsBuilder
                    .Add()
                        .Name()
                            .Value(ToString(EYtSettingType::KeepSorted))
                        .Build()
                    .Build()
                    .Add()
                        .Name()
                            .Value(ToString(EYtSettingType::Ordered))
                        .Build()
                    .Build();
                if (State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
                    settingsBuilder
                        .Add()
                            .Name()
                                .Value(ToString(EYtSettingType::Flow))
                            .Build()
                        .Build();
                }

                return Build<TYtOutput>(ctx, assume.Pos())
                    .Operation<TYtMap>()
                        .World(GetWorld(input, {}, ctx))
                        .DataSink(GetDataSink(input, ctx))
                        .Input(ConvertInputTable(input, ctx))
                        .Output()
                            .Add(outTable.ToExprNode(ctx, assume.Pos()).Cast<TYtOutTable>())
                        .Build()
                        .Settings(settingsBuilder.Done())
                        .Mapper(mapper)
                    .Build()
                    .OutIndex().Value(0U).Build()
                    .Done();
            }
        }

        auto op = GetOutputOp(input.Cast<TYtOutput>());
        TExprNode::TPtr newOp = op.Ptr();
        if (!op.Maybe<TYtSort>()) {
            if (auto settings = op.Maybe<TYtTransientOpBase>().Settings()) {
                if (!NYql::HasSetting(settings.Ref(), EYtSettingType::KeepSorted)) {
                    newOp = ctx.ChangeChild(op.Ref(), TYtTransientOpBase::idx_Settings, NYql::AddSetting(settings.Ref(), EYtSettingType::KeepSorted, {}, ctx));
                }
            } else if (auto settings = op.Maybe<TYtFill>().Settings()) {
                if (!NYql::HasSetting(settings.Ref(), EYtSettingType::KeepSorted)) {
                    newOp = ctx.ChangeChild(op.Ref(), TYtFill::idx_Settings, NYql::AddSetting(settings.Ref(), EYtSettingType::KeepSorted, {}, ctx));
                }
            }
        }
        if (!equalSort) {
            const size_t index = FromString(input.Cast<TYtOutput>().OutIndex().Value());
            TYtOutTableInfo outTable(op.Output().Item(index));
            builder.FillRowSpecSort(*outTable.RowSpec);
            outTable.RowSpec->SetConstraints(assume.Ref().GetConstraintSet());
            outTable.SetUnique(assume.Ref().GetConstraint<TDistinctConstraintNode>(), assume.Pos(), ctx);

            TVector<TYtOutTable> outputs;
            for (size_t i = 0; i < op.Output().Size(); ++i) {
                if (index == i) {
                    outputs.push_back(outTable.ToExprNode(ctx, op.Pos()).Cast<TYtOutTable>());
                } else {
                    outputs.push_back(op.Output().Item(i));
                }
            }

            newOp = ctx.ChangeChild(*newOp, TYtOutputOpBase::idx_Output, Build<TYtOutSection>(ctx, op.Pos()).Add(outputs).Done().Ptr());
        }

        return Build<TYtOutput>(ctx, assume.Pos())
            .Operation(newOp)
            .OutIndex(input.Cast<TYtOutput>().OutIndex())
            .Done();
    }

    TMaybeNode<TExprBase> LambdaFieldsSubset(TYtWithUserJobsOpBase op, size_t lambdaIdx, TExprContext& ctx, const TGetParents& getParents) const {
        auto lambda = TCoLambda(op.Ref().ChildPtr(lambdaIdx));

        bool hasUpdates = false;
        TYtSection section = op.Input().Item(0);

        const TParentsMap* parentsMap = getParents();
        auto parents = parentsMap->find(lambda.Args().Arg(0).Raw());
        if (parents == parentsMap->cend()) {
            // Argument is not used in lambda body
            return op;
        }
        if (parents->second.size() == 1 && TCoExtractMembers::Match(*parents->second.begin())) {
            auto members = TCoExtractMembers(*parents->second.begin()).Members();
            TSet<TStringBuf> memberSet;
            std::for_each(members.begin(), members.end(), [&memberSet](const auto& m) { memberSet.insert(m.Value()); });
            auto reduceBy = NYql::GetSettingAsColumnList(op.Settings().Ref(), EYtSettingType::ReduceBy);
            memberSet.insert(reduceBy.cbegin(), reduceBy.cend());
            auto sortBy = NYql::GetSettingAsColumnList(op.Settings().Ref(), EYtSettingType::SortBy);
            memberSet.insert(sortBy.cbegin(), sortBy.cend());

            auto itemType = GetSeqItemType(lambda.Args().Arg(0).Ref().GetTypeAnn())->Cast<TStructExprType>();
            if (memberSet.size() < itemType->GetSize()) {
                section = UpdateInputFields(section, std::move(memberSet), ctx, NYql::HasSetting(op.Settings().Ref(), EYtSettingType::WeakFields));
                hasUpdates = true;
            }
        }

        if (!hasUpdates) {
            return op;
        }

        auto res = ctx.ChangeChild(op.Ref(), TYtTransientOpBase::idx_Input,
            Build<TYtSectionList>(ctx, op.Input().Pos())
                .Add(section)
                .Done().Ptr());

        res = ctx.ChangeChild(*res, lambdaIdx, ctx.DeepCopyLambda(lambda.Ref()));

        return TExprBase(res);

    }

    TMaybeNode<TExprBase> MapFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto op = node.Cast<TYtWithUserJobsOpBase>();
        if (op.Input().Size() != 1) {
            return node;
        }
        if (auto map = op.Maybe<TYtMap>()) {
            return LambdaFieldsSubset(op, TYtMap::idx_Mapper, ctx, getParents);
        } else if (op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>()) {
            return LambdaFieldsSubset(op, TYtMapReduce::idx_Mapper, ctx, getParents);
        }

        return node;
    }

    TMaybeNode<TExprBase> ReduceFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto op = node.Cast<TYtWithUserJobsOpBase>();
        if (op.Input().Size() != 1) {
            return node;
        }
        if (auto reduce = op.Maybe<TYtReduce>()) {
            return LambdaFieldsSubset(op, TYtReduce::idx_Reducer, ctx, getParents);
        } else if (!op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>()) {
            return LambdaFieldsSubset(op, TYtMapReduce::idx_Reducer, ctx, getParents);
        }

        return node;
    }

    TMaybeNode<TExprBase> LambdaVisitFieldsSubset(TYtWithUserJobsOpBase op, size_t lambdaIdx, TExprContext& ctx, const TGetParents& getParents) const {
        auto opLambda = TCoLambda(op.Ref().ChildPtr(lambdaIdx));

        const TParentsMap* parentsMap = getParents();
        auto maybeLambda = GetFlatMapOverInputStream(opLambda, *parentsMap).Lambda();
        if (!maybeLambda) {
            return op;
        }

        TCoLambda lambda = maybeLambda.Cast();
        auto arg = lambda.Args().Arg(0);

        // Check arg is used only in Visit
        auto it = parentsMap->find(arg.Raw());
        if (it == parentsMap->cend() || it->second.size() != 1 || !TCoVisit::Match(*it->second.begin())) {
            return op;
        }

        const TExprNode* visit = *it->second.begin();
        TVector<std::pair<size_t, TSet<TStringBuf>>> sectionFields;
        for (ui32 index = 1; index < visit->ChildrenSize(); ++index) {
            if (visit->Child(index)->IsAtom()) {
                size_t inputNum = FromString<size_t>(visit->Child(index)->Content());
                YQL_ENSURE(inputNum < op.Input().Size());

                ++index;
                auto visitLambda = visit->ChildPtr(index);

                TSet<TStringBuf> memberSet;
                if (HaveFieldsSubset(visitLambda->TailPtr(), visitLambda->Head().Head(), memberSet, *parentsMap)) {
                    auto itemType = visitLambda->Head().Head().GetTypeAnn()->Cast<TStructExprType>();
                    auto reduceBy = NYql::GetSettingAsColumnList(op.Settings().Ref(), EYtSettingType::ReduceBy);
                    for (auto& col: reduceBy) {
                        if (auto type = itemType->FindItemType(col)) {
                            memberSet.insert(type->Cast<TItemExprType>()->GetName());
                        }
                    }
                    auto sortBy = NYql::GetSettingAsColumnList(op.Settings().Ref(), EYtSettingType::SortBy);
                    for (auto& col: sortBy) {
                        if (auto type = itemType->FindItemType(col)) {
                            memberSet.insert(type->Cast<TItemExprType>()->GetName());
                        }
                    }

                    if (memberSet.size() < itemType->GetSize()) {
                        sectionFields.emplace_back(inputNum, std::move(memberSet));
                    }
                }
            }
        }

        if (sectionFields.empty()) {
            return op;
        }

        auto res = ctx.ChangeChild(op.Ref(), lambdaIdx, ctx.DeepCopyLambda(opLambda.Ref()));

        TVector<TYtSection> updatedSections(op.Input().begin(), op.Input().end());
        const bool hasWeak = NYql::HasSetting(op.Settings().Ref(), EYtSettingType::WeakFields);
        for (auto& pair: sectionFields) {
            auto& section = updatedSections[pair.first];
            auto& memberSet = pair.second;
            section = UpdateInputFields(section, std::move(memberSet), ctx, hasWeak);
        }

        res = ctx.ChangeChild(*res, TYtTransientOpBase::idx_Input,
            Build<TYtSectionList>(ctx, op.Input().Pos())
                .Add(updatedSections)
                .Done().Ptr());

        return TExprBase(res);
    }

    TMaybeNode<TExprBase> MultiMapFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto op = node.Cast<TYtWithUserJobsOpBase>();
        if (op.Input().Size() < 2) {
            return node;
        }
        if (auto map = op.Maybe<TYtMap>()) {
            return LambdaVisitFieldsSubset(op, TYtMap::idx_Mapper, ctx, getParents);
        } else if (op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>()) {
            return LambdaVisitFieldsSubset(op, TYtMapReduce::idx_Mapper, ctx, getParents);
        }

        return node;
    }

    TMaybeNode<TExprBase> MultiReduceFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto op = node.Cast<TYtWithUserJobsOpBase>();
        if (op.Input().Size() < 2) {
            return node;
        }
        if (auto reduce = op.Maybe<TYtReduce>()) {
            return LambdaVisitFieldsSubset(op, TYtReduce::idx_Reducer, ctx, getParents);
        } else if (!op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>()) {
            return LambdaVisitFieldsSubset(op, TYtMapReduce::idx_Reducer, ctx, getParents);
        }

        return node;
    }

    TMaybeNode<TExprBase> WeakFields(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto op = node.Cast<TYtWithUserJobsOpBase>();

        if (op.Input().Size() > 1) {
            return node;
        }

        if (NYql::HasSetting(op.Settings().Ref(), EYtSettingType::WeakFields)) {
            return node;
        }

        auto section = op.Input().Item(0);
        auto inputType = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        if (!inputType->FindItem(YqlOthersColumnName)) {
            return node;
        }

        for (auto path: section.Paths()) {
            TYtTableBaseInfo::TPtr info = TYtTableBaseInfo::Parse(path.Table());
            if (!info->RowSpec || info->RowSpec->StrictSchema || info->Meta->Attrs.contains(QB2Premapper)) {
                return node;
            }
        }

        TMaybeNode<TCoLambda> maybeMapper;
        if (auto map = op.Maybe<TYtMap>()) {
            maybeMapper = map.Mapper();
        } else {
            maybeMapper = op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>();
        }
        if (!maybeMapper) {
            return node;
        }
        auto mapper = maybeMapper.Cast();
        auto mapperIdx = op.Maybe<TYtMap>() ? TYtMap::idx_Mapper : TYtMapReduce::idx_Mapper;

        auto lambdaBody = mapper.Body();
        if (!lambdaBody.Maybe<TCoFlatMapBase>() && !lambdaBody.Maybe<TCoCombineCore>()) {
            return node;
        }

        TVector<TExprBase> stack{lambdaBody};
        auto input = lambdaBody.Cast<TCoInputBase>().Input();
        while (input.Raw() != mapper.Args().Arg(0).Raw()) {
            TMaybe<THashSet<TStringBuf>> passthroughFields;
            if (!input.Maybe<TCoFlatMapBase>()
                || !IsPassthroughFlatMap(input.Cast<TCoFlatMapBase>(), &passthroughFields)
                || (passthroughFields && !passthroughFields->contains(YqlOthersColumnName)))
            {
                return node;
            }
            stack.push_back(input);
            input = input.Cast<TCoFlatMapBase>().Input();
        }

        auto getMemberColumn = [] (const TExprNode* node, const TExprNode* arg) {
            if (auto maybeMember = TMaybeNode<TCoMember>(node)) {
                if (maybeMember.Cast().Struct().Raw() == arg) {
                    return maybeMember.Cast().Name().Value();
                }
            }
            return TStringBuf();
        };

        THashMap<const TExprNode*, TExprNode::TPtr> weaks; // map TryWeakMemberFromDict -> row argument
        THashSet<const TExprNode*> otherMembers;
        TSet<TStringBuf> finalWeakColumns;
        const TParentsMap* parentsMap = getParents();
        for (size_t pass: xrange(stack.size())) {
            auto consumer = stack[pass];

            TExprNode::TListType rowArgs;
            if (auto maybeCombineCore = consumer.Maybe<TCoCombineCore>()) {
                auto combineCore = maybeCombineCore.Cast();
                rowArgs.push_back(combineCore.KeyExtractor().Args().Arg(0).Ptr());
                rowArgs.push_back(combineCore.InitHandler().Args().Arg(1).Ptr());
                rowArgs.push_back(combineCore.UpdateHandler().Args().Arg(1).Ptr());
            }
            else {
                rowArgs.push_back(consumer.Cast<TCoFlatMapBase>().Lambda().Args().Arg(0).Ptr());
            }

            for (const auto& rowArg : rowArgs) {
                auto rowArgParentsIt = parentsMap->find(rowArg.Get());
                YQL_ENSURE(rowArgParentsIt != parentsMap->end());
                for (const auto& memberNode: rowArgParentsIt->second) {
                    if (auto column = getMemberColumn(memberNode, rowArg.Get())) {
                        if (column != YqlOthersColumnName) {
                            continue;
                        }
                    } else {
                        return node;
                    }

                    if (pass > 0) {
                        otherMembers.insert(memberNode);
                    }

                    auto justArgIt = parentsMap->find(memberNode);
                    YQL_ENSURE(justArgIt != parentsMap->end());
                    for (const auto& justNode: justArgIt->second) {
                        if (!justNode->IsCallable("Just") || justNode->Child(0) != memberNode) {
                            if (pass > 0) {
                                continue;
                            }
                            return node;
                        }

                        auto weakIt = parentsMap->find(justNode);
                        YQL_ENSURE(weakIt != parentsMap->end());
                        for (const auto& weakNode : weakIt->second) {
                            if (!weakNode->IsCallable("TryWeakMemberFromDict") || weakNode->Child(0) != justNode) {
                                if (pass > 0) {
                                    continue;
                                }
                                return node;
                            }

                            weaks.insert(std::make_pair(weakNode, rowArg));
                            if (pass == 0) {
                                finalWeakColumns.insert(weakNode->Child(3)->Content());
                            }
                        }
                    }
                }
            }
        }

        TSet<TStringBuf> filteredFields;
        for (auto item: inputType->GetItems()) {
            if (item->GetName() != YqlOthersColumnName) {
                filteredFields.insert(item->GetName());
            }
        }

        TSet<TStringBuf> weakFields;
        TExprNode::TPtr newLambda;
        TOptimizeExprSettings settings(State_->Types);
        settings.VisitChanges = true;
        auto status = OptimizeExpr(mapper.Ptr(), newLambda, [&](const TExprNode::TPtr& input, TExprContext& ctx) {
            if (auto maybeTryWeak = TMaybeNode<TCoTryWeakMemberFromDict>(input)) {
                auto it = weaks.find(input.Get());
                if (it == weaks.end()) {
                    return input;
                }
                auto tryWeak = maybeTryWeak.Cast();
                auto weakName = tryWeak.Name().Value();
                if (!filteredFields.contains(weakName)) {
                    weakFields.insert(weakName);
                }

                TExprBase member = Build<TCoMember>(ctx, input->Pos())
                    .Struct(it->second)
                    .Name(tryWeak.Name())
                    .Done();

                const TStructExprType* structType = it->second->GetTypeAnn()->Cast<TStructExprType>();
                auto structMemberPos = structType->FindItem(weakName);
                bool notYsonMember = false;
                if (structMemberPos) {
                    auto structMemberType = structType->GetItems()[*structMemberPos]->GetItemType();
                    if (structMemberType->GetKind() == ETypeAnnotationKind::Optional) {
                        structMemberType = structMemberType->Cast<TOptionalExprType>()->GetItemType();
                    }
                    if (structMemberType->GetKind() != ETypeAnnotationKind::Data) {
                        notYsonMember = true;
                    } else {
                        auto structMemberSlot = structMemberType->Cast<TDataExprType>()->GetSlot();
                        if (structMemberSlot != EDataSlot::Yson && structMemberSlot != EDataSlot::String) {
                            notYsonMember = true;
                        }
                    }
                }

                TExprBase fromYson = (notYsonMember || tryWeak.Type().Value() == "Yson")
                    ? member
                    : Build<TCoFromYsonSimpleType>(ctx, input->Pos())
                        .Value(member)
                        .Type(tryWeak.Type())
                        .Done();

                if (tryWeak.RestDict().Maybe<TCoNothing>()) {
                    return fromYson.Ptr();
                }

                return Build<TCoCoalesce>(ctx, input->Pos())
                    .Predicate(fromYson)
                    .Value<TCoTryWeakMemberFromDict>()
                        .InitFrom(tryWeak)
                        .OtherDict<TCoNull>()
                        .Build()
                    .Build()
                    .Done().Ptr();
            }

            if (stack.size() > 1) {
                if (auto maybeStruct = TMaybeNode<TCoAsStruct>(input)) {
                    auto asStruct = maybeStruct.Cast();
                    for (size_t i: xrange(asStruct.ArgCount())) {
                        auto list = asStruct.Arg(i);
                        if (list.Item(0).Cast<TCoAtom>().Value() == YqlOthersColumnName && otherMembers.contains(list.Item(1).Raw())) {
                            // rebuild AsStruct without other fields
                            auto row = list.Item(1).Cast<TCoMember>().Struct();
                            auto newChildren = input->ChildrenList();
                            newChildren.erase(newChildren.begin() + i);
                            // and with weak fields for combiner core
                            for (ui32 j = 0; j < newChildren.size(); ++j) {
                                finalWeakColumns.erase(newChildren[j]->Child(0)->Content());
                            }

                            for (auto column : finalWeakColumns) {
                                newChildren.push_back(Build<TExprList>(ctx, input->Pos())
                                    .Add<TCoAtom>()
                                        .Value(column)
                                    .Build()
                                    .Add<TCoMember>()
                                        .Struct(row)
                                        .Name()
                                            .Value(column)
                                        .Build()
                                    .Build()
                                    .Done().Ptr());
                            }

                            return ctx.ChangeChildren(*input, std::move(newChildren));
                        }
                    }

                    return input;
                }
            }

            return input;
        }, ctx, settings);

        if (status.Level == IGraphTransformer::TStatus::Error) {
            return nullptr;
        }

        // refresh CombineCore lambdas
        auto child = newLambda->Child(TCoLambda::idx_Body);
        if (TCoCombineCore::Match(child)) {
            child->ChildRef(TCoCombineCore::idx_KeyExtractor) = ctx.DeepCopyLambda(*child->Child(TCoCombineCore::idx_KeyExtractor));
            child->ChildRef(TCoCombineCore::idx_InitHandler) = ctx.DeepCopyLambda(*child->Child(TCoCombineCore::idx_InitHandler));
            child->ChildRef(TCoCombineCore::idx_UpdateHandler) = ctx.DeepCopyLambda(*child->Child(TCoCombineCore::idx_UpdateHandler));
        }

        // refresh flatmaps too
        if (stack.size() > 1) {
            child = child->Child(TCoInputBase::idx_Input);
            for (size_t i = 1; i < stack.size(); ++i) {
                child->ChildRef(TCoFlatMapBase::idx_Lambda) = ctx.DeepCopyLambda(*child->Child(TCoFlatMapBase::idx_Lambda));
                child = child->Child(TCoInputBase::idx_Input);
            }
        }

        auto columnsBuilder = Build<TExprList>(ctx, op.Pos());
        for (auto& field: filteredFields) {
            columnsBuilder
                .Add<TCoAtom>()
                    .Value(field)
                .Build();
        }
        for (auto& field: weakFields) {
            columnsBuilder
                .Add<TCoAtomList>()
                    .Add()
                        .Value(field)
                    .Build()
                    .Add()
                        .Value("weak")
                    .Build()
                .Build();
        }

        auto res = ctx.ChangeChild(op.Ref(), TYtWithUserJobsOpBase::idx_Input,
            Build<TYtSectionList>(ctx, op.Input().Pos())
                .Add(UpdateInputFields(section, columnsBuilder.Done(), ctx))
                .Done().Ptr());

        res = ctx.ChangeChild(*res, TYtWithUserJobsOpBase::idx_Settings, NYql::AddSetting(op.Settings().Ref(), EYtSettingType::WeakFields, {}, ctx));
        res = ctx.ChangeChild(*res, mapperIdx, std::move(newLambda));

        return TExprBase(res);
    }

    TMaybeNode<TExprBase> EquiJoin(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
            return node;
        }

        auto equiJoin = node.Cast<TCoEquiJoin>();

        TMaybeNode<TYtDSink> dataSink;
        TString usedCluster;
        for (size_t i = 0; i + 2 < equiJoin.ArgCount(); ++i) {
            auto list = equiJoin.Arg(i).Cast<TCoEquiJoinInput>().List();
            if (auto maybeExtractMembers = list.Maybe<TCoExtractMembers>()) {
                list = maybeExtractMembers.Cast().Input();
            }
            if (auto maybeFlatMap = list.Maybe<TCoFlatMapBase>()) {
                TSyncMap syncList;
                if (!IsYtCompleteIsolatedLambda(maybeFlatMap.Cast().Lambda().Ref(), syncList, usedCluster, true, false)) {
                    return node;
                }
                list = maybeFlatMap.Cast().Input();
            }
            if (!IsYtProviderInput(list)) {
                TSyncMap syncList;
                if (!IsYtCompleteIsolatedLambda(list.Ref(), syncList, usedCluster, true, false)) {
                    return node;
                }
                continue;
            }

            if (!dataSink) {
                dataSink = GetDataSink(list, ctx);
            }
            auto cluster = ToString(GetClusterName(list));
            if (!UpdateUsedCluster(usedCluster, cluster)) {
                return node;
            }
        }

        if (!dataSink) {
            return node;
        }

        THashMap<TStringBuf, std::pair<TVector<TStringBuf>, ui32>> tableSortKeysUsage =
            CollectTableSortKeysUsage(State_, equiJoin);

        // label -> join keys
        THashMap<TStringBuf, THashSet<TStringBuf>> tableKeysMap =
            CollectEquiJoinKeyColumnsByLabel(equiJoin.Arg(equiJoin.ArgCount() - 2).Ref());

        TNodeOnNodeOwnedMap updatedInputs;
        TExprNode::TListType sections;
        TExprNode::TListType premaps;
        TSyncMap worldList;
        for (size_t i = 0; i + 2 < equiJoin.ArgCount(); ++i) {
            auto joinInput = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
            auto list = joinInput.List();

            TMaybeNode<TCoLambda> premapLambda;
            TExprNode::TPtr extractedMembers;

            auto listStepForward = list;
            if (auto maybeExtractMembers = listStepForward.Maybe<TCoExtractMembers>()) {
                extractedMembers = maybeExtractMembers.Cast().Members().Ptr();
                listStepForward = maybeExtractMembers.Cast().Input();
            }

            if (auto maybeFlatMap = listStepForward.Maybe<TCoFlatMapBase>()) {
                auto flatMap = maybeFlatMap.Cast();
                if (IsLambdaSuitableForPullingIntoEquiJoin(flatMap, joinInput.Scope().Ref(), tableKeysMap, extractedMembers.Get())) {
                    if (!IsYtCompleteIsolatedLambda(flatMap.Lambda().Ref(), worldList, usedCluster, true, false)) {
                        return node;
                    }

                    auto maybeLambda = CleanupWorld(flatMap.Lambda(), ctx);
                    if (!maybeLambda) {
                        return {};
                    }

                    auto lambda = ctx.DeepCopyLambda(maybeLambda.Cast().Ref());
                    if (!extractedMembers) {
                        premapLambda = lambda;
                        YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": Collected input #" << i << " as premap";
                    } else {
                        premapLambda = ctx.Builder(lambda->Pos())
                            .Lambda()
                                .Param("item")
                                .Callable("ExtractMembers")
                                    .Apply(0, lambda)
                                        .With(0, "item")
                                    .Seal()
                                    .Add(1, extractedMembers)
                                .Seal()
                            .Seal()
                            .Build();
                        YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": Collected input #" << i << " as premap with extract members";
                    }

                    list = flatMap.Input();
                }
            }

            TExprNode::TPtr section;
            if (!IsYtProviderInput(list)) {
                auto& newSection = updatedInputs[list.Raw()];
                if (newSection) {
                    section = Build<TYtSection>(ctx, list.Pos())
                        .InitFrom(TYtSection(newSection))
                        .Settings()
                            .Add()
                                .Name()
                                    .Value(ToString(EYtSettingType::JoinLabel))
                                .Build()
                                .Value(joinInput.Scope())
                            .Build()
                        .Build()
                        .Done().Ptr();
                }
                else {
                    TSyncMap syncList;
                    if (!IsYtCompleteIsolatedLambda(list.Ref(), syncList, usedCluster, true, false)) {
                        return node;
                    }

                    const TStructExprType* outItemType = nullptr;
                    if (auto type = GetSequenceItemType(list, false, ctx)) {
                        if (!EnsurePersistableType(list.Pos(), *type, ctx)) {
                            return {};
                        }
                        outItemType = type->Cast<TStructExprType>();
                    } else {
                        return {};
                    }

                    TYtOutTableInfo outTable(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
                    outTable.RowSpec->SetConstraints(list.Ref().GetConstraintSet());
                    outTable.SetUnique(list.Ref().GetConstraint<TDistinctConstraintNode>(), list.Pos(), ctx);

                    auto cleanup = CleanupWorld(list, ctx);
                    if (!cleanup) {
                        return {};
                    }

                    section = newSection = Build<TYtSection>(ctx, list.Pos())
                        .Paths()
                            .Add()
                                .Table<TYtOutput>()
                                    .Operation<TYtFill>()
                                        .World(ApplySyncListToWorld(ctx.NewWorld(list.Pos()), syncList, ctx))
                                        .DataSink(dataSink.Cast())
                                        .Content(MakeJobLambdaNoArg(cleanup.Cast(), ctx))
                                        .Output()
                                            .Add(outTable.ToExprNode(ctx, list.Pos()).Cast<TYtOutTable>())
                                        .Build()
                                        .Settings(GetFlowSettings(list.Pos(), *State_, ctx))
                                    .Build()
                                    .OutIndex().Value(0U).Build()
                                .Build()
                                .Columns<TCoVoid>().Build()
                                .Ranges<TCoVoid>().Build()
                                .Stat<TCoVoid>().Build()
                            .Build()
                        .Build()
                        .Settings()
                            .Add()
                                .Name()
                                    .Value(ToString(EYtSettingType::JoinLabel))
                                .Build()
                                .Value(joinInput.Scope())
                            .Build()
                        .Build()
                        .Done().Ptr();
                }
            }
            else {
                auto settings = Build<TCoNameValueTupleList>(ctx, list.Pos())
                    .Add()
                        .Name()
                            .Value(ToString(EYtSettingType::JoinLabel))
                        .Build()
                        .Value(joinInput.Scope())
                    .Build()
                    .Done();

                TExprNode::TPtr world;
                if (auto maybeRead = list.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
                    auto read = maybeRead.Cast();
                    if (read.World().Ref().Type() != TExprNode::World) {
                        world = read.World().Ptr();
                    }
                }

                bool makeUnordered = false;
                if (ctx.IsConstraintEnabled<TSortedConstraintNode>() && joinInput.Scope().Ref().IsAtom()) {
                    makeUnordered = (0 == tableSortKeysUsage[joinInput.Scope().Ref().Content()].second);
                }

                auto sectionList = ConvertInputTable(list, ctx, TConvertInputOpts().Settings(settings).MakeUnordered(makeUnordered));
                YQL_ENSURE(sectionList.Size() == 1, "EquiJoin input should contain exactly one section, but has: " << sectionList.Size());
                bool clearWorld = false;
                auto sectionNode = sectionList.Item(0);

                if (NYql::HasSetting(sectionNode.Settings().Ref(), EYtSettingType::Sample)) {
                    auto scheme = list.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();

                    auto path = CopyOrTrivialMap(sectionNode.Pos(),
                        TExprBase(world ? world : ctx.NewWorld(sectionNode.Pos())),
                        dataSink.Cast(),
                        *scheme,
                        Build<TYtSection>(ctx, sectionNode.Pos())
                            .InitFrom(sectionNode)
                            .Settings(NYql::RemoveSettings(sectionNode.Settings().Ref(), EYtSettingType::StatColumns
                                    | EYtSettingType::JoinLabel | EYtSettingType::Unordered, ctx))
                            .Done(),
                        {}, ctx, State_,
                        TCopyOrTrivialMapOpts().SetTryKeepSortness(!makeUnordered).SetSectionUniq(list.Ref().GetConstraint<TDistinctConstraintNode>()));

                    clearWorld = true;

                    sectionNode = Build<TYtSection>(ctx, sectionNode.Pos())
                        .Paths()
                            .Add(path)
                        .Build()
                        .Settings(NYql::RemoveSettings(sectionNode.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip |
                            EYtSettingType::Sample, ctx))
                        .Done();
                }

                if (!clearWorld && world) {
                    worldList.emplace(world, worldList.size());
                }

                section = sectionNode.Ptr();
            }

            YQL_ENSURE(section);
            sections.push_back(section);
            auto premap = BuildYtEquiJoinPremap(list, premapLambda, ctx);
            if (!premap) {
                return {};
            }
            premaps.push_back(premap);
        }

        const TStructExprType* outItemType = nullptr;
        if (auto type = GetSequenceItemType(node, false, ctx)) {
            if (!EnsurePersistableType(node.Pos(), *type, ctx)) {
                return {};
            }
            outItemType = type->Cast<TStructExprType>();
        } else {
            return {};
        }

        auto parentsMap = getParents();
        YQL_ENSURE(parentsMap);
        auto joinOptions = CollectPreferredSortsForEquiJoinOutput(node, equiJoin.Arg(equiJoin.ArgCount() - 1).Ptr(), ctx, *parentsMap);

        const auto join = Build<TYtEquiJoin>(ctx, node.Pos())
            .World(ApplySyncListToWorld(ctx.NewWorld(node.Pos()), worldList, ctx))
            .DataSink(dataSink.Cast())
            .Input()
                .Add(sections)
            .Build()
            .Output()
                .Add(TYtOutTableInfo(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE)
                    .ToExprNode(ctx, node.Pos()).Cast<TYtOutTable>())
            .Build()
            .Settings()
            .Build()
            .Joins(equiJoin.Arg(equiJoin.ArgCount() - 2))
            .JoinOptions(joinOptions)
            .Done();

        auto children = join.Ref().ChildrenList();
        children.reserve(children.size() + premaps.size());
        std::move(premaps.begin(), premaps.end(), std::back_inserter(children));
        return Build<TYtOutput>(ctx, node.Pos())
            .Operation(ctx.ChangeChildren(join.Ref(), std::move(children)))
            .OutIndex().Value(0U).Build()
            .Done();
    }

    TMaybeNode<TExprBase> EarlyMergeJoin(TExprBase node, TExprContext& ctx) const {
        if (State_->Configuration->JoinMergeTablesLimit.Get()) {
            auto equiJoin = node.Cast<TYtEquiJoin>();
            const auto tree = ImportYtEquiJoin(equiJoin, ctx);
            if (State_->Configuration->JoinMergeForce.Get() || tree->LinkSettings.ForceSortedMerge) {
                const auto rewriteStatus = RewriteYtEquiJoinLeaves(equiJoin, *tree, State_, ctx);
                switch (rewriteStatus.Level) {
                    case TStatus::Repeat:
                        return node;
                    case TStatus::Error:
                        return {};
                    case TStatus::Ok:
                        break;
                    default:
                        YQL_ENSURE(false, "Unexpected rewrite status");
                }
                return ExportYtEquiJoin(equiJoin, *tree, ctx, State_);
            }
        }
        return node;
    }

    TMaybeNode<TExprBase> RuntimeEquiJoin(TExprBase node, TExprContext& ctx) const {
        auto equiJoin = node.Cast<TYtEquiJoin>();

        const bool tryReorder = State_->Types->CostBasedOptimizer != ECostBasedOptimizerType::Disable
            && equiJoin.Input().Size() > 2
            && HasOnlyOneJoinType(*equiJoin.Joins().Ptr(), "Inner")
            && !HasSetting(equiJoin.JoinOptions().Ref(), "cbo_passed");

        const bool waitAllInputs = State_->Configuration->JoinWaitAllInputs.Get().GetOrElse(false) || tryReorder;

        if (waitAllInputs) {
            for (auto section: equiJoin.Input()) {
                for (auto path: section.Paths()) {
                    TYtPathInfo pathInfo(path);
                    if (!pathInfo.Table->Stat) {
                        return node;
                    }
                }
            }
        }

        const auto tree = ImportYtEquiJoin(equiJoin, ctx);
        if (tryReorder) {
            const auto optimizedTree = OrderJoins(tree, State_, ctx);
            if (optimizedTree != tree) {
                return ExportYtEquiJoin(equiJoin, *optimizedTree, ctx, State_);
            }
        }
        const auto rewriteStatus = RewriteYtEquiJoin(equiJoin, *tree, State_, ctx);

        switch (rewriteStatus.Level) {
            case TStatus::Repeat:
                YQL_ENSURE(!waitAllInputs);
                return node;
            case TStatus::Error:
                return {};
            case TStatus::Ok:
                break;
            default:
                YQL_ENSURE(false, "Unexpected rewrite status");
        }

        return ExportYtEquiJoin(equiJoin, *tree, ctx, State_);
    }

    TMaybeNode<TExprBase> TableContentWithSettings(TExprBase node, TExprContext& ctx) const {
        auto op = node.Cast<TYtOutputOpBase>();

        TExprNode::TPtr res = op.Ptr();

        TNodeSet nodesToOptimize;
        TProcessedNodesSet processedNodes;
        processedNodes.insert(res->Head().UniqueId());
        VisitExpr(res, [&nodesToOptimize, &processedNodes](const TExprNode::TPtr& input) {
            if (processedNodes.contains(input->UniqueId())) {
                return false;
            }

            if (auto read = TMaybeNode<TYtLength>(input).Input().Maybe<TYtReadTable>()) {
                nodesToOptimize.insert(read.Cast().Raw());
                return false;
            }

            if (auto read = TMaybeNode<TYtTableContent>(input).Input().Maybe<TYtReadTable>()) {
                nodesToOptimize.insert(read.Cast().Raw());
                return false;
            }
            if (TYtOutput::Match(input.Get())) {
                processedNodes.insert(input->UniqueId());
                return false;
            }
            return true;
        });

        if (nodesToOptimize.empty()) {
            return node;
        }

        TSyncMap syncList;
        TOptimizeExprSettings settings(State_->Types);
        settings.ProcessedNodes = &processedNodes; // Prevent optimizer to go deeper than current operation
        auto status = OptimizeExpr(res, res, [&syncList, &nodesToOptimize, state = State_](const TExprNode::TPtr& input, TExprContext& ctx) -> TExprNode::TPtr {
            if (nodesToOptimize.find(input.Get()) != nodesToOptimize.end()) {
                return OptimizeReadWithSettings(input, false, true, syncList, state, ctx);
            }
            return input;
        }, ctx, settings);

        if (status.Level == IGraphTransformer::TStatus::Error) {
            return {};
        }

        if (status.Level == IGraphTransformer::TStatus::Ok) {
            return node;
        }

        if (!syncList.empty()) {
            using TPair = std::pair<TExprNode::TPtr, ui64>;
            TVector<TPair> sortedList(syncList.cbegin(), syncList.cend());
            TExprNode::TListType syncChildren;
            syncChildren.push_back(res->ChildPtr(TYtOutputOpBase::idx_World));
            ::Sort(sortedList, [](const TPair& x, const TPair& y) { return x.second < y.second; });
            for (auto& x: sortedList) {
                auto world = ctx.NewCallable(node.Pos(), TCoLeft::CallableName(), { x.first });
                syncChildren.push_back(world);
            }

            res = ctx.ChangeChild(*res, TYtOutputOpBase::idx_World,
                ctx.NewCallable(node.Pos(), TCoSync::CallableName(), std::move(syncChildren)));
        }

        return TExprBase(res);
    }

    TMaybeNode<TExprBase> NonOptimalTableContent(TExprBase node, TExprContext& ctx) const {
        auto op = node.Cast<TYtOutputOpBase>();

        TExprNode::TPtr res = op.Ptr();

        TNodeSet nodesToOptimize;
        TProcessedNodesSet processedNodes;
        processedNodes.insert(res->Head().UniqueId());
        VisitExpr(res, [&nodesToOptimize, &processedNodes](const TExprNode::TPtr& input) {
            if (processedNodes.contains(input->UniqueId())) {
                return false;
            }

            if (TYtTableContent::Match(input.Get())) {
                nodesToOptimize.insert(input.Get());
                return false;
            }
            if (TYtOutput::Match(input.Get())) {
                processedNodes.insert(input->UniqueId());
                return false;
            }
            return true;
        });

        if (nodesToOptimize.empty()) {
            return node;
        }

        TSyncMap syncList;
        const auto maxTables = State_->Configuration->TableContentMaxInputTables.Get().GetOrElse(1000);
        const auto minChunkSize = State_->Configuration->TableContentMinAvgChunkSize.Get().GetOrElse(1_GB);
        const auto maxChunks = State_->Configuration->TableContentMaxChunksForNativeDelivery.Get().GetOrElse(1000ul);
        auto state = State_;
        auto world = res->ChildPtr(TYtOutputOpBase::idx_World);
        TOptimizeExprSettings settings(State_->Types);
        settings.ProcessedNodes = &processedNodes; // Prevent optimizer to go deeper than current operation
        auto status = OptimizeExpr(res, res, [&syncList, &nodesToOptimize, maxTables, minChunkSize, maxChunks, state, world](const TExprNode::TPtr& input, TExprContext& ctx) -> TExprNode::TPtr {
            if (nodesToOptimize.find(input.Get()) != nodesToOptimize.end()) {
                if (auto read = TYtTableContent(input).Input().Maybe<TYtReadTable>()) {
                    bool materialize = false;
                    const bool singleSection = 1 == read.Cast().Input().Size();
                    TVector<TYtSection> newSections;
                    for (auto section: read.Cast().Input()) {
                        if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Sample | EYtSettingType::SysColumns)) {
                            materialize = true;
                        }
                        else if (section.Paths().Size() > maxTables) {
                            materialize = true;
                        }
                        else {
                            TMaybeNode<TYtMerge> oldOp;
                            if (section.Paths().Size() == 1) {
                                oldOp = section.Paths().Item(0).Table().Maybe<TYtOutput>().Operation().Maybe<TYtMerge>();
                            }
                            if (!oldOp.IsValid() || !NYql::HasSetting(oldOp.Cast().Settings().Ref(), EYtSettingType::CombineChunks)) {
                                for (auto path: section.Paths()) {
                                    TYtTableBaseInfo::TPtr tableInfo = TYtTableBaseInfo::Parse(path.Table());
                                    if (auto tableStat = tableInfo->Stat) {
                                        if (tableStat->ChunkCount > maxChunks || (tableStat->ChunkCount > 1 && tableStat->DataSize / tableStat->ChunkCount < minChunkSize)) {
                                            materialize = true;
                                            break;
                                        }
                                    }
                                    if (!tableInfo->IsTemp && tableInfo->Meta) {
                                        auto p = tableInfo->Meta->Attrs.FindPtr("erasure_codec");
                                        if (p && *p != "none") {
                                            materialize = true;
                                            break;
                                        }
                                        else if (tableInfo->Meta->IsDynamic) {
                                            materialize = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        if (materialize) {
                            auto path = CopyOrTrivialMap(section.Pos(),
                                TExprBase(world),
                                TYtDSink(ctx.RenameNode(read.DataSource().Ref(), "DataSink")),
                                *section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType(),
                                Build<TYtSection>(ctx, section.Pos())
                                    .Paths(section.Paths())
                                    .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::Unordered | EYtSettingType::NonUnique, ctx))
                                    .Done(),
                                {}, ctx, state,
                                TCopyOrTrivialMapOpts()
                                    .SetTryKeepSortness(!NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Unordered))
                                    .SetSectionUniq(section.Ref().GetConstraint<TDistinctConstraintNode>())
                                    .SetConstraints(section.Ref().GetConstraintSet())
                                    .SetCombineChunks(true)
                                );

                            syncList[path.Table().Cast<TYtOutput>().Operation().Ptr()] = syncList.size();

                            if (singleSection) {
                                return ctx.ChangeChild(*input, TYtTableContent::idx_Input, path.Table().Ptr());
                            } else {
                                newSections.push_back(Build<TYtSection>(ctx, section.Pos())
                                    .Paths()
                                        .Add(path)
                                    .Build()
                                    .Settings().Build()
                                    .Done());
                            }

                        } else {
                            newSections.push_back(section);
                        }

                    }
                    if (materialize) {
                        auto newRead = Build<TYtReadTable>(ctx, read.Cast().Pos())
                            .InitFrom(read.Cast())
                            .Input()
                                .Add(newSections)
                            .Build()
                            .Done();

                        return ctx.ChangeChild(*input, TYtTableContent::idx_Input, newRead.Ptr());
                    }
                }
                else if (auto out = TYtTableContent(input).Input().Maybe<TYtOutput>()) {
                    auto oldOp = GetOutputOp(out.Cast());
                    if (!oldOp.Maybe<TYtMerge>() || !NYql::HasSetting(oldOp.Cast<TYtMerge>().Settings().Ref(), EYtSettingType::CombineChunks)) {
                        auto outTable = GetOutTable(out.Cast());
                        TYtOutTableInfo tableInfo(outTable);
                        if (auto tableStat = tableInfo.Stat) {
                            if (tableStat->ChunkCount > maxChunks || (tableStat->ChunkCount > 1 && tableStat->DataSize / tableStat->ChunkCount < minChunkSize)) {
                                auto newOp = Build<TYtMerge>(ctx, input->Pos())
                                    .World(world)
                                    .DataSink(oldOp.DataSink())
                                    .Output()
                                        .Add()
                                            .InitFrom(outTable.Cast<TYtOutTable>())
                                            .Name().Value("").Build()
                                            .Stat<TCoVoid>().Build()
                                        .Build()
                                    .Build()
                                    .Input()
                                        .Add()
                                            .Paths()
                                                .Add()
                                                    .Table(out.Cast())
                                                    .Columns<TCoVoid>().Build()
                                                    .Ranges<TCoVoid>().Build()
                                                    .Stat<TCoVoid>().Build()
                                                .Build()
                                            .Build()
                                            .Settings<TCoNameValueTupleList>()
                                            .Build()
                                        .Build()
                                    .Build()
                                    .Settings()
                                        .Add()
                                            .Name().Value(ToString(EYtSettingType::CombineChunks)).Build()
                                        .Build()
                                        .Add()
                                            .Name().Value(ToString(EYtSettingType::ForceTransform)).Build()
                                        .Build()
                                    .Build()
                                    .Done();

                                syncList[newOp.Ptr()] = syncList.size();

                                auto newOutput = Build<TYtOutput>(ctx, input->Pos())
                                    .Operation(newOp)
                                    .OutIndex().Value(0U).Build()
                                    .Done().Ptr();

                                return ctx.ChangeChild(*input, TYtTableContent::idx_Input, std::move(newOutput));
                            }
                        }
                    }
                }
            }
            return input;
        }, ctx, settings);

        if (status.Level == IGraphTransformer::TStatus::Error) {
            return {};
        }

        if (status.Level == IGraphTransformer::TStatus::Ok) {
            return node;
        }

        if (!syncList.empty()) {
            using TPair = std::pair<TExprNode::TPtr, ui64>;
            TVector<TPair> sortedList(syncList.cbegin(), syncList.cend());
            TExprNode::TListType syncChildren;
            syncChildren.push_back(res->ChildPtr(TYtOutputOpBase::idx_World));
            ::Sort(sortedList, [](const TPair& x, const TPair& y) { return x.second < y.second; });
            for (auto& x: sortedList) {
                auto world = ctx.NewCallable(node.Pos(), TCoLeft::CallableName(), { x.first });
                syncChildren.push_back(world);
            }

            res = ctx.ChangeChild(*res, TYtOutputOpBase::idx_World,
                ctx.NewCallable(node.Pos(), TCoSync::CallableName(), std::move(syncChildren)));
        }

        return TExprBase(res);
    }

    TMaybeNode<TExprBase> ReadWithSettings(TExprBase node, TExprContext& ctx) const {
        if (State_->PassiveExecution) {
            return node;
        }

        auto maybeRead = node.Cast<TCoRight>().Input().Maybe<TYtReadTable>();
        if (!maybeRead) {
            return node;
        }

        auto read = maybeRead.Cast().Ptr();
        TSyncMap syncList;
        auto ret = OptimizeReadWithSettings(read, true, true, syncList, State_, ctx);
        if (ret != read) {
            if (ret) {
                if (!syncList.empty()) {
                    ret = ctx.ChangeChild(*ret, TYtReadTable::idx_World,
                        ApplySyncListToWorld(ret->ChildPtr(TYtReadTable::idx_World), syncList, ctx));
                }
                return Build<TCoRight>(ctx, node.Pos())
                    .Input(ret)
                    .Done();
            }
            return {};
        }
        return node;
    }

    TMaybeNode<TExprBase> TransientOpWithSettings(TExprBase node, TExprContext& ctx) const {
        auto op = node.Cast<TYtTransientOpBase>();

        if (node.Ref().HasResult() && node.Ref().GetResult().Type() == TExprNode::World) {
            return node;
        }

        TYqlRowSpecInfo::TPtr outRowSpec;
        bool keepSortness = false;
        if (op.Maybe<TYtReduce>()) {
            keepSortness = true;
        } else if (op.Maybe<TYtCopy>() || op.Maybe<TYtMerge>()) {
            outRowSpec = MakeIntrusive<TYqlRowSpecInfo>(op.Output().Item(0).RowSpec());
            keepSortness = outRowSpec->IsSorted();
        } else if (op.Maybe<TYtMap>()) {
            keepSortness = AnyOf(op.Output(), [] (const TYtOutTable& out) {
                return TYqlRowSpecInfo(out.RowSpec()).IsSorted();
            });
        }

        bool hasUpdates = false;
        TVector<TExprBase> updatedSections;
        TSyncMap syncList;
        for (auto section: op.Input()) {
            updatedSections.push_back(section);

            if (auto updatedSection = UpdateSectionWithSettings(op.World(), section, op.DataSink(), outRowSpec, keepSortness, true, true, syncList, State_, ctx)) {
                updatedSections.back() = updatedSection.Cast();
                hasUpdates = true;
            }
        }
        if (!hasUpdates) {
            return node;
        }

        auto res = ctx.ChangeChild(op.Ref(), TYtTransientOpBase::idx_Input,
            Build<TYtSectionList>(ctx, op.Input().Pos())
                .Add(updatedSections)
                .Done().Ptr());
        if (!syncList.empty()) {
            res = ctx.ChangeChild(*res, TYtTransientOpBase::idx_World,
                ApplySyncListToWorld(res->ChildPtr(TYtTransientOpBase::idx_World), syncList, ctx));
        }
        // Transform YtCopy to YtMerge in case of ranges
        if (op.Maybe<TYtCopy>()) {
            if (AnyOf(updatedSections.front().Cast<TYtSection>().Paths(), [](TYtPath path) { return !path.Ranges().Maybe<TCoVoid>(); })) {
                res = ctx.RenameNode(*res, TYtMerge::CallableName());
            }
        }
        return TExprBase(res);
    }

    TMaybeNode<TExprBase> ReplaceStatWriteTable(TExprBase node, TExprContext& ctx) const {
        if (State_->PassiveExecution) {
            return node;
        }

        auto write = node.Cast<TStatWriteTable>();
        auto input = write.Input();

        TMaybeNode<TYtOutput> newInput;

        if (!IsYtProviderInput(input, false)) {
            if (!EnsurePersistable(input.Ref(), ctx)) {
                return {};
            }

            TString cluster;
            TSyncMap syncList;
            if (!IsYtCompleteIsolatedLambda(input.Ref(), syncList, cluster, true, false)) {
                return node;
            }

            const TTypeAnnotationNode* outItemType;
            if (!GetSequenceItemType(input.Ref(), outItemType, ctx)) {
                return {};
            }
            if (!EnsurePersistableType(input.Pos(), *outItemType, ctx)) {
                return {};
            }

            auto cleanup = CleanupWorld(input, ctx);
            if (!cleanup) {
                return {};
            }

            TYtOutTableInfo outTable {outItemType->Cast<TStructExprType>(),
                State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE};
            outTable.RowSpec->SetConstraints(input.Ref().GetConstraintSet());
            outTable.SetUnique(input.Ref().GetConstraint<TDistinctConstraintNode>(), input.Pos(), ctx);

            if (!cluster) {
                cluster = State_->Configuration->DefaultCluster
                    .Get().GetOrElse(State_->Gateway->GetDefaultClusterName());
            }

            input = Build<TYtOutput>(ctx, write.Pos())
                .Operation<TYtFill>()
                    .World(ApplySyncListToWorld(ctx.NewWorld(write.Pos()), syncList, ctx))
                    .DataSink<TYtDSink>()
                        .Category()
                            .Value(YtProviderName)
                        .Build()
                        .Cluster()
                            .Value(cluster)
                        .Build()
                    .Build()
                    .Output()
                        .Add(outTable.ToExprNode(ctx, write.Pos()).Cast<TYtOutTable>())
                    .Build()
                    .Content(MakeJobLambdaNoArg(cleanup.Cast(), ctx))
                    .Settings(GetFlowSettings(write.Pos(), *State_, ctx))
                .Build()
                .OutIndex()
                    .Value("0")
                .Build()
                .Done();
        }

        if (auto maybeRead = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
            auto read = maybeRead.Cast();
            if (read.Input().Size() != 1) {
                ctx.AddError(TIssue(ctx.GetPosition(read.Input().Pos()), TStringBuilder() <<
                    "Unexpected read with several sections on " << node.Ptr()->Content()
                ));
                return {};
            }

            auto section = read.Input().Item(0);
            auto scheme = section.Ptr()->GetTypeAnn()->Cast<TListExprType>()->GetItemType();

            auto path = CopyOrTrivialMap(section.Pos(),
                GetWorld(input, {}, ctx),
                GetDataSink(input, ctx),
                *scheme,
                Build<TYtSection>(ctx, section.Pos())
                    .InitFrom(section)
                    .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::Unordered | EYtSettingType::Unordered, ctx))
                .Done(),
                {}, ctx, State_,
                TCopyOrTrivialMapOpts().SetTryKeepSortness(true).SetSectionUniq(section.Ref().GetConstraint<TDistinctConstraintNode>()));

            YQL_ENSURE(
                path.Ranges().Maybe<TCoVoid>(),
                "Unexpected slices: " << path.Ranges().Ref().Content()
            );

            YQL_ENSURE(
                path.Table().Maybe<TYtOutput>().Operation(),
                "Unexpected node: " << path.Table().Ref().Content()
            );

            newInput = path.Table().Cast<TYtOutput>();
        } else if (auto op = input.Maybe<TYtOutput>().Operation()) {
            newInput = input.Cast<TYtOutput>();
        } else {
            YQL_ENSURE(false, "Unexpected operation input: " << input.Ptr()->Content());
        }

        auto table = ctx.RenameNode(write.Table().Ref(), TYtStatOutTable::CallableName());

        return Build<TYtStatOut>(ctx, write.Pos())
            .World(GetWorld(input, {}, ctx))
            .DataSink(GetDataSink(input, ctx))
            .Input(newInput.Cast())
            .Table(table)
            .ReplaceMask(write.ReplaceMask())
            .Settings(write.Settings())
            .Done();
    }

    TMaybeNode<TExprBase> TopSort(TExprBase node, TExprContext& ctx) const {
        auto sort = node.Cast<TYtSort>();

        auto settings = sort.Settings();
        auto limitSetting = NYql::GetSetting(settings.Ref(), EYtSettingType::Limit);
        if (!limitSetting) {
            return node;
        }
        if (HasNodesToCalculate(node.Ptr())) {
            return node;
        }

        if (NYql::HasAnySetting(sort.Input().Item(0).Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip)) {
            return node;
        }

        if (NYql::HasNonEmptyKeyFilter(sort.Input().Item(0))) {
            return node;
        }

        const ui64 maxLimit = State_->Configuration->TopSortMaxLimit.Get().GetOrElse(DEFAULT_TOP_SORT_LIMIT);
        TMaybe<ui64> limit = GetLimit(settings.Ref());
        if (!limit || *limit == 0 || *limit > maxLimit) {
            YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << " !!! TopSort - zero limit";
            // Keep empty "limit" setting to prevent repeated Limits optimization
            if (limitSetting->ChildPtr(1)->ChildrenSize() != 0) {
                auto updatedSettings = NYql::RemoveSetting(settings.Ref(), EYtSettingType::Limit, ctx);
                updatedSettings = NYql::AddSetting(*updatedSettings, EYtSettingType::Limit, ctx.NewList(node.Pos(), {}), ctx);
                return TExprBase(ctx.ChangeChild(node.Ref(), TYtSort::idx_Settings, std::move(updatedSettings)));
            }
            return node;
        }

        ui64 size = 0;
        ui64 rows = 0;
        for (auto path: sort.Input().Item(0).Paths()) {
            auto tableInfo = TYtTableBaseInfo::Parse(path.Table());
            if (!tableInfo->Stat) {
                return node;
            }
            ui64 tableSize = tableInfo->Stat->DataSize;
            ui64 tableRows = tableInfo->Stat->RecordsCount;

            if (!path.Ranges().Maybe<TCoVoid>()) {
                if (TMaybe<ui64> usedRows = TYtRangesInfo(path.Ranges()).GetUsedRows(tableRows)) {
                    // Make it proportional to used rows
                    tableSize = tableSize * usedRows.GetRef() / tableRows;
                    tableRows = usedRows.GetRef();
                } else {
                    // non-row ranges are present
                    return node;
                }
            }
            size += tableSize;
            rows += tableRows;
        }

        if (rows <= *limit) {
            // Just do YtSort
            // Keep empty "limit" setting to prevent repeated Limits optimization
            auto updatedSettings = NYql::RemoveSetting(settings.Ref(), EYtSettingType::Limit, ctx);
            updatedSettings = NYql::AddSetting(*updatedSettings, EYtSettingType::Limit, ctx.NewList(node.Pos(), {}), ctx);
            return TExprBase(ctx.ChangeChild(node.Ref(), TYtSort::idx_Settings, std::move(updatedSettings)));
        }

        const ui64 sizePerJob = State_->Configuration->TopSortSizePerJob.Get().GetOrElse(128_MB);
        const ui64 rowMultiplierPerJob = State_->Configuration->TopSortRowMultiplierPerJob.Get().GetOrElse(10u);
        ui64 partsBySize = size / sizePerJob;
        ui64 partsByRecords = rows / (rowMultiplierPerJob * limit.GetRef());
        ui64 jobCount = Max<ui64>(Min<ui64>(partsBySize, partsByRecords), 1);
        if (partsBySize <= 1 || partsByRecords <= 10) {
            jobCount = 1;
        }

        auto sortedBy = TYqlRowSpecInfo(sort.Output().Item(0).RowSpec()).GetForeignSort();
        auto updatedSettings = NYql::AddSettingAsColumnPairList(sort.Settings().Ref(), EYtSettingType::SortLimitBy, sortedBy, ctx);
        if (jobCount <= 5000) {
            updatedSettings = NYql::AddSetting(*updatedSettings, EYtSettingType::JobCount, ctx.NewAtom(sort.Pos(), ToString(jobCount)), ctx);
        }

        auto inputItemType = GetSequenceItemType(sort.Input().Item(0), false, ctx);
        if (!inputItemType) {
            return {};
        }

        if (jobCount == 1) {
            updatedSettings = NYql::AddSetting(*updatedSettings, EYtSettingType::KeepSorted, {}, ctx);

            auto mapper = Build<TCoLambda>(ctx, sort.Pos())
                .Args({"stream"})
                .Body<TCoTopSort>()
                    .Input("stream")
                    .Count<TCoUint64>()
                        .Literal()
                            .Value(ToString(*limit))
                        .Build()
                    .Build()
                    .SortDirections([&sortedBy] (TExprNodeBuilder& builder) {
                        auto listBuilder = builder.List();
                        for (size_t i: xrange(sortedBy.size())) {
                            listBuilder.Callable(i, TCoBool::CallableName())
                                .Atom(0, sortedBy[i].second ? "True" : "False")
                                .Seal();
                        }
                        listBuilder.Seal();
                    })
                    .KeySelectorLambda()
                        .Args({"item"})
                        .Body([&sortedBy] (TExprNodeBuilder& builder) {
                            auto listBuilder = builder.List();
                            for (size_t i: xrange(sortedBy.size())) {
                                listBuilder.Callable(i, TCoMember::CallableName())
                                    .Arg(0, "item")
                                    .Atom(1, sortedBy[i].first)
                                    .Seal();
                            }
                            listBuilder.Seal();
                        })
                    .Build()
                .Build().Done();

            // Final map
            return Build<TYtMap>(ctx, sort.Pos())
                .World(sort.World())
                .DataSink(sort.DataSink())
                .Input(sort.Input())
                .Output(sort.Output())
                .Settings(GetFlowSettings(sort.Pos(), *State_, ctx, updatedSettings))
                .Mapper(mapper)
                .Done();
        }

        auto mapper = Build<TCoLambda>(ctx, sort.Pos())
            .Args({"stream"})
            .Body<TCoTop>()
                .Input("stream")
                .Count<TCoUint64>()
                    .Literal()
                        .Value(ToString(*limit))
                    .Build()
                .Build()
                .SortDirections([&sortedBy] (TExprNodeBuilder& builder) {
                    auto listBuilder = builder.List();
                    for (size_t i: xrange(sortedBy.size())) {
                        listBuilder.Callable(i, TCoBool::CallableName())
                            .Atom(0, sortedBy[i].second ? "True" : "False")
                            .Seal();
                    }
                    listBuilder.Seal();
                })
                .KeySelectorLambda()
                    .Args({"item"})
                    .Body([&sortedBy] (TExprNodeBuilder& builder) {
                        auto listBuilder = builder.List();
                        for (size_t i: xrange(sortedBy.size())) {
                            listBuilder.Callable(i, TCoMember::CallableName())
                                .Arg(0, "item")
                                .Atom(1, sortedBy[i].first)
                                .Seal();
                        }
                        listBuilder.Seal();
                    })
                .Build()
            .Build().Done();

        TYtOutTableInfo outTable(inputItemType->Cast<TStructExprType>(), State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
        outTable.RowSpec->SetConstraints(sort.Ref().GetConstraintSet());

        return Build<TYtSort>(ctx, sort.Pos())
            .InitFrom(sort)
            .World<TCoWorld>().Build()
            .Input()
                .Add()
                    .Paths()
                        .Add()
                            .Table<TYtOutput>()
                                .Operation<TYtMap>()
                                    .World(sort.World())
                                    .DataSink(sort.DataSink())
                                    .Input(sort.Input())
                                    .Output()
                                        .Add(outTable.SetUnique(sort.Ref().GetConstraint<TDistinctConstraintNode>(), sort.Pos(), ctx).ToExprNode(ctx, sort.Pos()).Cast<TYtOutTable>())
                                    .Build()
                                    .Settings(GetFlowSettings(sort.Pos(), *State_, ctx, updatedSettings))
                                    .Mapper(mapper)
                                .Build()
                                .OutIndex()
                                    .Value(0U)
                                .Build()
                            .Build()
                            .Columns<TCoVoid>().Build()
                            .Ranges<TCoVoid>().Build()
                            .Stat<TCoVoid>().Build()
                        .Build()
                    .Build()
                    .Settings()
                    .Build()
                .Build()
            .Build()
            .Done();
    }

    TMaybeNode<TExprBase> EmbedLimit(TExprBase node, TExprContext& ctx) const {
        auto op = node.Cast<TYtWithUserJobsOpBase>();
        if (op.Output().Size() != 1) {
            return node;
        }
        auto settings = op.Settings();
        auto limitSetting = NYql::GetSetting(settings.Ref(), EYtSettingType::Limit);
        if (!limitSetting) {
            return node;
        }
        if (HasNodesToCalculate(node.Ptr())) {
            return node;
        }

        TMaybe<ui64> limit = GetLimit(settings.Ref());
        if (!limit) {
            return node;
        }

        auto sortLimitBy = NYql::GetSettingAsColumnPairList(settings.Ref(), EYtSettingType::SortLimitBy);
        if (!sortLimitBy.empty() && *limit > State_->Configuration->TopSortMaxLimit.Get().GetOrElse(DEFAULT_TOP_SORT_LIMIT)) {
            return node;
        }

        size_t lambdaIdx = op.Maybe<TYtMapReduce>()
            ? TYtMapReduce::idx_Reducer
            : op.Maybe<TYtReduce>()
                ? TYtReduce::idx_Reducer
                : TYtMap::idx_Mapper;

        auto lambda = TCoLambda(op.Ref().ChildPtr(lambdaIdx));
        if (IsEmptyContainer(lambda.Body().Ref()) || IsEmpty(lambda.Body().Ref(), *State_->Types)) {
            return node;
        }

        if (sortLimitBy.empty()) {
            if (lambda.Body().Maybe<TCoTake>()) {
                return node;
            }

            lambda = Build<TCoLambda>(ctx, lambda.Pos())
                .Args({"stream"})
                .Body<TCoTake>()
                    .Input<TExprApplier>()
                        .Apply(lambda)
                        .With(0, "stream")
                    .Build()
                    .Count<TCoUint64>()
                        .Literal()
                            .Value(ToString(*limit))
                        .Build()
                    .Build()
                .Build()
                .Done();
        } else {
            if (lambda.Body().Maybe<TCoTopBase>()) {
                return node;
            }

            if (const auto& body = lambda.Body().Ref(); body.IsCallable("ExpandMap") && body.Head().IsCallable({"Top", "TopSort"})) {
                return node;
            }

            lambda = Build<TCoLambda>(ctx, lambda.Pos())
                .Args({"stream"})
                .Body<TCoTop>()
                    .Input<TExprApplier>()
                        .Apply(lambda)
                        .With(0, "stream")
                    .Build()
                    .Count<TCoUint64>()
                        .Literal()
                            .Value(ToString(*limit))
                        .Build()
                    .Build()
                    .SortDirections([&sortLimitBy] (TExprNodeBuilder& builder) {
                        auto listBuilder = builder.List();
                        for (size_t i: xrange(sortLimitBy.size())) {
                            listBuilder.Callable(i, TCoBool::CallableName())
                                .Atom(0, sortLimitBy[i].second ? "True" : "False")
                                .Seal();
                        }
                        listBuilder.Seal();
                    })
                    .KeySelectorLambda()
                        .Args({"item"})
                        .Body([&sortLimitBy] (TExprNodeBuilder& builder) {
                            auto listBuilder = builder.List();
                            for (size_t i: xrange(sortLimitBy.size())) {
                                listBuilder.Callable(i, TCoMember::CallableName())
                                    .Arg(0, "item")
                                    .Atom(1, sortLimitBy[i].first)
                                    .Seal();
                            }
                            listBuilder.Seal();
                        })
                    .Build()
                .Build().Done();

            if (auto& l = lambda.Ref(); l.Tail().Head().IsCallable("ExpandMap")) {
                lambda = TCoLambda(ctx.ChangeChild(l, 1U, ctx.SwapWithHead(l.Tail())));
            }
         }

        return TExprBase(ctx.ChangeChild(op.Ref(), lambdaIdx, lambda.Ptr()));
    }

    TMaybeNode<TExprBase> PushMergeLimitToInput(TExprBase node, TExprContext& ctx) const {
        if (node.Ref().HasResult() && node.Ref().GetResult().Type() != TExprNode::World) {
            return node;
        }

        auto op = node.Cast<TYtMerge>();

        auto settings = op.Settings();
        auto limitSetting = NYql::GetSetting(settings.Ref(), EYtSettingType::Limit);
        if (!limitSetting) {
            return node;
        }

        auto section = op.Input().Item(0);
        if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Skip | EYtSettingType::Sample)) {
            return node;
        }
        if (NYql::HasNonEmptyKeyFilter(section)) {
            return node;
        }

        if (AnyOf(section.Paths(), [](const TYtPath& path) { return !path.Ranges().Maybe<TCoVoid>().IsValid(); })) {
            return node;
        }

        for (auto path: section.Paths()) {
            TYtPathInfo pathInfo(path);
            // Dynamic tables don't support range selectors
            if (pathInfo.Table->Meta->IsDynamic) {
                return node;
            }
        }

        TExprNode::TPtr effectiveLimit = GetLimitExpr(limitSetting, ctx);
        if (!effectiveLimit) {
            return node;
        }

        auto sectionSettings = section.Settings().Ptr();
        auto sectionLimitSetting = NYql::GetSetting(*sectionSettings, EYtSettingType::Take);
        if (sectionLimitSetting) {
            effectiveLimit = ctx.NewCallable(node.Pos(), "Min", { effectiveLimit, sectionLimitSetting->ChildPtr(1) });
            sectionSettings = NYql::RemoveSetting(*sectionSettings, EYtSettingType::Take, ctx);
        }

        sectionSettings = NYql::AddSetting(*sectionSettings, EYtSettingType::Take, effectiveLimit, ctx);

        // Keep empty "limit" setting to prevent repeated Limits optimization
        auto updatedSettings = NYql::RemoveSetting(settings.Ref(), EYtSettingType::Limit, ctx);
        updatedSettings = NYql::AddSetting(*updatedSettings, EYtSettingType::Limit, ctx.NewList(node.Pos(), {}), ctx);

        return Build<TYtMerge>(ctx, op.Pos())
            .InitFrom(op)
            .Input()
                .Add()
                    .InitFrom(section)
                    .Settings(sectionSettings)
                .Build()
            .Build()
            .Settings(updatedSettings)
            .Done();
    }

    TMaybeNode<TExprBase> PushDownKeyExtract(TExprBase node, TExprContext& ctx) const {
        if (node.Ref().HasResult() && node.Ref().GetResult().Type() != TExprNode::World) {
            return node;
        }

        auto op = node.Cast<TYtTransientOpBase>();

        auto getInnerOpForUpdate = [] (const TYtPath& path, const TVector<TStringBuf>& usedKeyFilterColumns) -> TMaybeNode<TYtTransientOpBase> {
            auto maybeOp = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtTransientOpBase>();
            if (!maybeOp) {
                return {};
            }
            auto innerOp = maybeOp.Cast();
            if (innerOp.Ref().StartsExecution() || innerOp.Ref().HasResult()) {
                return {};
            }

            if (!innerOp.Maybe<TYtMerge>() && !innerOp.Maybe<TYtMap>()) {
                return {};
            }

            if (innerOp.Input().Size() != 1 || innerOp.Output().Size() != 1) {
                return {};
            }

            if (NYql::HasSetting(innerOp.Settings().Ref(), EYtSettingType::Limit)) {
                return {};
            }
            const auto outSorted = innerOp.Output().Item(0).Ref().GetConstraint<TSortedConstraintNode>();
            if (!outSorted) {
                return {};
            }
            for (auto path: innerOp.Input().Item(0).Paths()) {
                const auto inputSorted = path.Ref().GetConstraint<TSortedConstraintNode>();
                if (!inputSorted || !inputSorted->Includes(*outSorted)) {
                    return {};
                }
            }

            auto innerSection = innerOp.Input().Item(0);
            if (NYql::HasSettingsExcept(innerSection.Settings().Ref(), EYtSettingType::SysColumns)) {
                return {};
            }

            if (auto maybeMap = innerOp.Maybe<TYtMap>()) {
                // lambda must be passthrough for columns used in key filter
                // TODO: use passthrough constraints here
                TCoLambda lambda = maybeMap.Cast().Mapper();
                TMaybe<THashSet<TStringBuf>> passthroughColumns;
                bool analyzeJustMember = true;
                if (&lambda.Args().Arg(0).Ref() != &lambda.Body().Ref()) {
                    auto maybeInnerFlatMap = GetFlatMapOverInputStream(lambda);
                    if (!maybeInnerFlatMap) {
                        return {};
                    }

                    if (!IsPassthroughFlatMap(maybeInnerFlatMap.Cast(), &passthroughColumns, analyzeJustMember)) {
                        return {};
                    }
                }

                if (passthroughColumns &&
                    !AllOf(usedKeyFilterColumns, [&](const TStringBuf& col) { return passthroughColumns->contains(col); }))
                {
                    return {};
                }
            }

            return maybeOp;
        };

        bool hasUpdates = false;
        TVector<TExprBase> updatedSections;
        for (auto section: op.Input()) {
            bool hasPathUpdates = false;
            TVector<TYtPath> updatedPaths;
            auto settings = section.Settings().Ptr();
            const EYtSettingType kfType = NYql::HasSetting(*settings, EYtSettingType::KeyFilter2) ?
                EYtSettingType::KeyFilter2 : EYtSettingType::KeyFilter;
            const auto keyFilters = NYql::GetAllSettingValues(*settings, kfType);
            // Non empty filters and without table index
            const bool haveNonEmptyKeyFiltersWithoutIndex =
                AnyOf(keyFilters, [](const TExprNode::TPtr& f) { return f->ChildrenSize() > 0; }) &&
                AllOf(keyFilters, [&](const TExprNode::TPtr& f) { return f->ChildrenSize() < GetMinChildrenForIndexedKeyFilter(kfType); });

            bool allPathUpdated = true;
            if (haveNonEmptyKeyFiltersWithoutIndex) {

                TSyncMap syncList;
                for (auto filter: keyFilters) {
                    if (!IsYtCompleteIsolatedLambda(*filter, syncList, true, false)) {
                        return node;
                    }
                }

                // TODO: should actually be true for both kf1/kf2 - enforce in ValidateSettings()
                YQL_ENSURE(kfType == EYtSettingType::KeyFilter || keyFilters.size() == 1);
                const auto kfColumns = GetKeyFilterColumns(section, kfType);
                YQL_ENSURE(!kfColumns.empty());
                for (auto path: section.Paths()) {
                    if (auto maybeOp = getInnerOpForUpdate(path, kfColumns)) {
                        auto innerOp = maybeOp.Cast();
                        if (kfType == EYtSettingType::KeyFilter2) {
                            // check input/output keyFilter columns are of same type
                            const TStructExprType* inputType =
                                innerOp.Input().Item(0).Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                            const TStructExprType* outputType =
                                innerOp.Output().Item(0).Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                            bool sameTypes = true;
                            for (auto& keyColumn : kfColumns) {
                                auto inPos = inputType->FindItem(keyColumn);
                                auto outPos = outputType->FindItem(keyColumn);
                                YQL_ENSURE(inPos);
                                YQL_ENSURE(outPos);
                                const TTypeAnnotationNode* inColumnType = inputType->GetItems()[*inPos]->GetItemType();
                                const TTypeAnnotationNode* outColumnType = outputType->GetItems()[*outPos]->GetItemType();
                                if (!IsSameAnnotation(*inColumnType, *outColumnType)) {
                                    sameTypes = false;
                                    break;
                                }
                            }

                            if (!sameTypes) {
                                // TODO: improve
                                updatedPaths.push_back(path);
                                allPathUpdated = false;
                                continue;
                            }
                        }

                        auto innerOpSection = innerOp.Input().Item(0);
                        auto updatedSection = Build<TYtSection>(ctx, innerOpSection.Pos())
                            .InitFrom(innerOpSection)
                            .Settings(NYql::MergeSettings(innerOpSection.Settings().Ref(), *NYql::KeepOnlySettings(section.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2, ctx), ctx))
                            .Done();

                        auto updatedSectionList = Build<TYtSectionList>(ctx, innerOp.Input().Pos()).Add(updatedSection).Done();
                        auto updatedInnerOp = ctx.ChangeChild(innerOp.Ref(), TYtTransientOpBase::idx_Input, updatedSectionList.Ptr());
                        if (!syncList.empty()) {
                            updatedInnerOp = ctx.ChangeChild(*updatedInnerOp, TYtTransientOpBase::idx_World, ApplySyncListToWorld(innerOp.World().Ptr(), syncList, ctx));
                        }

                        updatedPaths.push_back(
                            Build<TYtPath>(ctx, path.Pos())
                                .InitFrom(path)
                                .Table<TYtOutput>()
                                    .InitFrom(path.Table().Cast<TYtOutput>())
                                    .Operation(updatedInnerOp)
                                .Build()
                                .Done());

                        hasPathUpdates = true;
                    } else {
                        updatedPaths.push_back(path);
                        allPathUpdated = false;
                    }
                }
            }
            if (hasPathUpdates) {
                hasUpdates = true;
                if (allPathUpdated) {
                    settings = NYql::RemoveSettings(*settings, EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2, ctx);
                    settings = NYql::AddSetting(*settings, kfType, ctx.NewList(section.Pos(), {}), ctx);
                }
                updatedSections.push_back(
                    Build<TYtSection>(ctx, section.Pos())
                        .InitFrom(section)
                        .Paths()
                            .Add(updatedPaths)
                        .Build()
                        .Settings(settings)
                        .Done());
            } else {
                updatedSections.push_back(section);
            }
        }

        if (!hasUpdates) {
            return node;
        }

        auto sectionList = Build<TYtSectionList>(ctx, op.Input().Pos())
            .Add(updatedSections)
            .Done();

        return TExprBase(ctx.ChangeChild(node.Ref(), TYtTransientOpBase::idx_Input, sectionList.Ptr()));
    }

    TMaybeNode<TExprBase> BypassMerge(TExprBase node, TExprContext& ctx) const {
        if (node.Ref().HasResult()) {
            return node;
        }

        auto op = node.Cast<TYtTransientOpBase>();
        if (op.Maybe<TYtCopy>()) {
            return node;
        }

        if (op.Maybe<TYtWithUserJobsOpBase>()) {
            size_t lambdaIdx = op.Maybe<TYtMapReduce>()
                ? TYtMapReduce::idx_Mapper
                : op.Maybe<TYtReduce>()
                    ? TYtReduce::idx_Reducer
                    : TYtMap::idx_Mapper;

            bool usesTableIndex = false;
            VisitExpr(op.Ref().ChildPtr(lambdaIdx), [&usesTableIndex](const TExprNode::TPtr& n) {
                if (TYtTableIndex::Match(n.Get())) {
                    usesTableIndex = true;
                } else if (TYtOutput::Match(n.Get())) {
                    return false;
                }
                return !usesTableIndex;
            });
            if (usesTableIndex) {
                return node;
            }
        }

        auto maxTables = State_->Configuration->MaxInputTables.Get();
        auto maxSortedTables = State_->Configuration->MaxInputTablesForSortedMerge.Get();
        const bool opOrdered = NYql::HasSetting(op.Settings().Ref(), EYtSettingType::Ordered);
        bool hasUpdates = false;
        TVector<TExprBase> updatedSections;
        TSyncMap syncList;
        for (auto section: op.Input()) {
            const EYtSettingType kfType = NYql::HasSetting(section.Settings().Ref(), EYtSettingType::KeyFilter2) ?
                EYtSettingType::KeyFilter2 : EYtSettingType::KeyFilter;
            const auto keyFiltersValues = NYql::GetAllSettingValues(section.Settings().Ref(), kfType);
            const bool hasTableKeyFilters = AnyOf(keyFiltersValues,
                [kfType](const TExprNode::TPtr& keyFilter) {
                    return keyFilter->ChildrenSize() >= GetMinChildrenForIndexedKeyFilter(kfType);
                });
            const bool hasTakeSkip = NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip);

            bool hasPathUpdates = false;
            TVector<TYtPath> updatedPaths;
            size_t inputCount = section.Paths().Size();
            if (!hasTableKeyFilters) {
                for (auto path: section.Paths()) {
                    updatedPaths.push_back(path);

                    if (!path.Ranges().Maybe<TCoVoid>()) {
                        bool pathLimits = false;
                        for (auto range: path.Ranges().Cast<TExprList>()) {
                            if (range.Maybe<TYtRow>() || range.Maybe<TYtRowRange>()) {
                                pathLimits = true;
                                break;
                            }
                        }
                        if (pathLimits) {
                            continue;
                        }
                    }
                    auto maybeInnerMerge = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtMerge>();
                    if (!maybeInnerMerge) {
                        continue;
                    }
                    auto innerMerge = maybeInnerMerge.Cast();

                    if (innerMerge.Ref().StartsExecution() || innerMerge.Ref().HasResult()) {
                        continue;
                    }

                    if (NYql::HasSettingsExcept(innerMerge.Settings().Ref(), EYtSettingType::KeepSorted | EYtSettingType::Limit)) {
                        continue;
                    }

                    if (auto limitSetting = NYql::GetSetting(innerMerge.Settings().Ref(), EYtSettingType::Limit)) {
                        if (limitSetting->ChildPtr(1)->ChildrenSize()) {
                            continue;
                        }
                    }

                    auto innerMergeSection = innerMerge.Input().Item(0);

                    bool hasIncompatibleSettings = false;
                    TExprNode::TListType innerMergeKeyFiltersValues;
                    for (auto& setting : innerMergeSection.Settings().Ref().Children()) {
                        const auto type = FromString<EYtSettingType>(setting->Child(0)->Content());
                        if (setting->ChildrenSize() == 2 && (type == EYtSettingType::KeyFilter || type == EYtSettingType::KeyFilter2)) {
                            innerMergeKeyFiltersValues.push_back(setting->ChildPtr(1));
                        } else {
                            hasIncompatibleSettings = true;
                            break;
                        }
                    }

                    if (hasIncompatibleSettings) {
                        continue;
                    }
                    if (AnyOf(innerMergeKeyFiltersValues, [](const TExprNode::TPtr& keyFilter) { return keyFilter->ChildrenSize() > 0; })) {
                        continue;
                    }

                    auto mergeOutRowSpec = TYqlRowSpecInfo(innerMerge.Output().Item(0).RowSpec());
                    const bool sortedMerge = mergeOutRowSpec.IsSorted();
                    if (hasTakeSkip && sortedMerge && NYql::HasSetting(innerMerge.Settings().Ref(), EYtSettingType::KeepSorted)) {
                        continue;
                    }
                    if (hasTakeSkip && AnyOf(innerMergeSection.Paths(), [](const auto& path) { return !path.Ranges().template Maybe<TCoVoid>(); })) {
                        continue;
                    }

                    const bool unordered = IsUnorderedOutput(path.Table().Cast<TYtOutput>());
                    if (innerMergeSection.Paths().Size() > 1) {
                        if (hasTakeSkip && sortedMerge) {
                            continue;
                        }
                        // Only YtMap can change semantic if substitute single sorted input by multiple sorted ones.
                        // Other operations (YtMerge, YtReduce, YtMapReduce, YtEquiJoin, YtSort) can be safely optimized.
                        // YtCopy cannot, but it is ignored early
                        if (op.Maybe<TYtMap>() && opOrdered && !unordered && sortedMerge) {
                            continue;
                        }
                        auto limit = maxTables;
                        if (maxSortedTables && (op.Maybe<TYtReduce>() || (op.Maybe<TYtMerge>() && TYqlRowSpecInfo(op.Output().Item(0).RowSpec()).IsSorted()))) {
                            limit = maxSortedTables;
                        }
                        if (limit && (inputCount - 1 + innerMergeSection.Paths().Size()) > *limit) {
                            continue;
                        }

                        if (mergeOutRowSpec.GetAllConstraints(ctx).GetConstraint<TDistinctConstraintNode>() || mergeOutRowSpec.GetAllConstraints(ctx).GetConstraint<TUniqueConstraintNode>()) {
                            continue;
                        }
                    }

                    hasPathUpdates = true;
                    updatedPaths.pop_back();
                    TMaybeNode<TExprBase> columns;
                    if (!path.Columns().Maybe<TCoVoid>()) {
                        columns = path.Columns();
                    } else if ((op.Maybe<TYtWithUserJobsOpBase>() || op.Maybe<TYtEquiJoin>()) && mergeOutRowSpec.HasAuxColumns()) {
                        TVector<TStringBuf> items;
                        for (auto item: mergeOutRowSpec.GetType()->GetItems()) {
                            items.push_back(item->GetName());
                        }
                        columns = ToAtomList(items, op.Pos(), ctx);
                    }

                    if (!columns.IsValid() && path.Ranges().Maybe<TCoVoid>() && !unordered) {
                        for (auto mergePath: innerMergeSection.Paths()) {
                            updatedPaths.push_back(mergePath);
                        }
                    } else {
                        for (auto mergePath: innerMergeSection.Paths()) {
                            auto builder = Build<TYtPath>(ctx, mergePath.Pos()).InitFrom(mergePath);

                            if (columns) {
                                builder.Columns(columns.Cast());
                            }
                            if (!path.Ranges().Maybe<TCoVoid>()) {
                                builder.Ranges(path.Ranges());
                            }

                            auto updatedPath = builder.Done();
                            if (unordered) {
                                updatedPath = MakeUnorderedPath(updatedPath, false, ctx);
                            }

                            updatedPaths.push_back(updatedPath);
                        }
                    }

                    if (innerMerge.World().Ref().Type() != TExprNode::World) {
                        syncList.emplace(innerMerge.World().Ptr(), syncList.size());
                    }
                    inputCount += innerMergeSection.Paths().Size() - 1;
                }
            }
            if (hasPathUpdates) {
                hasUpdates = true;
                updatedSections.push_back(
                    Build<TYtSection>(ctx, section.Pos())
                        .InitFrom(section)
                        .Paths()
                            .Add(updatedPaths)
                        .Build()
                        .Done());
            } else {
                updatedSections.push_back(section);
            }
        }
        if (!hasUpdates) {
            return node;
        }

        auto sectionList = Build<TYtSectionList>(ctx, op.Input().Pos())
            .Add(updatedSections)
            .Done();

        auto res = ctx.ChangeChild(node.Ref(), TYtTransientOpBase::idx_Input, sectionList.Ptr());
        if (!syncList.empty()) {
            res = ctx.ChangeChild(*res, TYtTransientOpBase::idx_World, ApplySyncListToWorld(op.World().Ptr(), syncList, ctx));
        }
        return TExprBase(res);
    }

    TMaybeNode<TExprBase> BypassMergeBeforePublish(TExprBase node, TExprContext& ctx) const {
        if (node.Ref().HasResult()) {
            return node;
        }

        auto publish = node.Cast<TYtPublish>();

        auto cluster = publish.DataSink().Cluster().StringValue();
        auto path = publish.Publish().Name().StringValue();
        auto commitEpoch = TEpochInfo::Parse(publish.Publish().CommitEpoch().Ref()).GetOrElse(0);

        auto dstRowSpec = State_->TablesData->GetTable(cluster, path, commitEpoch).RowSpec;

        auto maxTables = dstRowSpec->IsSorted() ? State_->Configuration->MaxInputTablesForSortedMerge.Get() : State_->Configuration->MaxInputTables.Get();
        bool hasUpdates = false;
        TVector<TYtOutput> updateInputs;
        size_t inputCount = publish.Input().Size();
        for (auto out: publish.Input()) {
            updateInputs.push_back(out);
            if (auto maybeMerge = out.Operation().Maybe<TYtMerge>()) {
                auto merge = maybeMerge.Cast();

                if (!merge.World().Ref().IsWorld()) {
                    continue;
                }

                if (merge.Ref().StartsExecution() || merge.Ref().HasResult()) {
                    continue;
                }

                if (merge.Settings().Size() != 0) {
                    continue;
                }

                auto mergeSection = merge.Input().Item(0);
                if (NYql::HasSettingsExcept(mergeSection.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2)) {
                    continue;
                }
                if (HasNonEmptyKeyFilter(mergeSection)) {
                    continue;
                }

                if (maxTables && inputCount + mergeSection.Paths().Size() - 1 > *maxTables) {
                    continue;
                }

                if (mergeSection.Paths().Size() < 2) {
                    continue;
                }

                if (!AllOf(mergeSection.Paths(), [](TYtPath path) {
                    return path.Table().Maybe<TYtOutput>()
                        && path.Columns().Maybe<TCoVoid>()
                        && path.Ranges().Maybe<TCoVoid>()
                        && !TYtTableBaseInfo::GetMeta(path.Table())->IsDynamic;
                })) {
                    continue;
                }

                if (dstRowSpec->GetColumnOrder().Defined() && AnyOf(mergeSection.Paths(), [colOrder = *dstRowSpec->GetColumnOrder()](auto path) {
                    auto rowSpec = TYtTableBaseInfo::GetRowSpec(path.Table());
                    return rowSpec->GetColumnOrder().Defined() && rowSpec->GetColumnOrder() != colOrder;
                })) {
                    continue;
                }

                hasUpdates = true;
                inputCount += mergeSection.Paths().Size() - 1;
                updateInputs.pop_back();
                if (IsUnorderedOutput(out)) {
                    std::transform(mergeSection.Paths().begin(), mergeSection.Paths().end(), std::back_inserter(updateInputs),
                        [mode = out.Mode(), &ctx](TYtPath path) {
                            auto origOut = path.Table().Cast<TYtOutput>();
                            return Build<TYtOutput>(ctx, origOut.Pos())
                                .InitFrom(origOut)
                                .Mode(mode)
                                .Done();
                        }
                    );
                } else {
                    std::transform(mergeSection.Paths().begin(), mergeSection.Paths().end(), std::back_inserter(updateInputs),
                        [](TYtPath path) {
                            return path.Table().Cast<TYtOutput>();
                        }
                    );
                }
            }
        }
        if (hasUpdates) {
            return Build<TYtPublish>(ctx, publish.Pos())
                .InitFrom(publish)
                .Input()
                    .Add(updateInputs)
                .Build()
                .Done().Ptr();
        }
        return node;
    }

    TMaybeNode<TExprBase> MapToMerge(TExprBase node, TExprContext& ctx) const {
        auto map = node.Cast<TYtMap>();

        auto mapper = map.Mapper();
        if (mapper.Body().Raw() != mapper.Args().Arg(0).Raw()) {
            // Only trivial lambda
            return node;
        }

        if (map.Ref().HasResult()) {
            return node;
        }

        if (map.Input().Size() > 1 || map.Output().Size() > 1) {
            return node;
        }

        if (NYql::HasAnySetting(map.Settings().Ref(), EYtSettingType::JobCount | EYtSettingType::WeakFields | EYtSettingType::Sharded | EYtSettingType::SortLimitBy)) {
            return node;
        }

        auto section = map.Input().Item(0);
        if (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::SysColumns)) {
            return node;
        }
        bool useExplicitColumns = false;
        const auto outRowSpec = TYqlRowSpecInfo(map.Output().Item(0).RowSpec());
        const auto nativeType = outRowSpec.GetNativeYtType();
        const auto nativeTypeFlags = outRowSpec.GetNativeYtTypeFlags();

        for (auto path: section.Paths()) {
            TYtPathInfo pathInfo(path);
            if (pathInfo.RequiresRemap()) {
                return node;
            }
            if (nativeType != pathInfo.GetNativeYtType()
                || nativeTypeFlags != pathInfo.GetNativeYtTypeFlags()) {
                return node;
            }
            if (!pathInfo.HasColumns() && (!pathInfo.Table->IsTemp || (pathInfo.Table->RowSpec && pathInfo.Table->RowSpec->HasAuxColumns()))) {
                useExplicitColumns = true;
            }
        }

        if (auto outSorted = map.Output().Item(0).Ref().GetConstraint<TSortedConstraintNode>()) {
            auto inputSorted = map.Input().Item(0).Ref().GetConstraint<TSortedConstraintNode>();
            if (!inputSorted || !outSorted->IsPrefixOf(*inputSorted)) {
                // Don't convert YtMap, which produces sorted output from unsorted input
                return node;
            }
            if (auto maxTablesForSortedMerge = State_->Configuration->MaxInputTablesForSortedMerge.Get()) {
                if (map.Input().Item(0).Paths().Size() > *maxTablesForSortedMerge) {
                    return node;
                }
            }
        }

        if (useExplicitColumns) {
            auto inputStructType = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            TSet<TStringBuf> columns;
            for (auto item: inputStructType->GetItems()) {
                columns.insert(item->GetName());
            }

            section = UpdateInputFields(section, std::move(columns), ctx, false);
        }

        return Build<TYtMerge>(ctx, node.Pos())
            .World(map.World())
            .DataSink(map.DataSink())
            .Output(map.Output())
            .Input()
                .Add(section)
            .Build()
            .Settings(NYql::KeepOnlySettings(map.Settings().Ref(), EYtSettingType::Limit | EYtSettingType::KeepSorted, ctx))
            .Done();
    }

    TMaybeNode<TExprBase> UnorderedPublishTarget(TExprBase node, TExprContext& ctx) const {
        auto publish = node.Cast<TYtPublish>();

        auto cluster = TString{publish.DataSink().Cluster().Value()};
        auto pubTableInfo = TYtTableInfo(publish.Publish());
        if (auto commitEpoch = pubTableInfo.CommitEpoch.GetOrElse(0)) {
            const TYtTableDescription& nextDescription = State_->TablesData->GetTable(cluster, pubTableInfo.Name, commitEpoch);
            YQL_ENSURE(nextDescription.RowSpec);
            if (!nextDescription.RowSpec->IsSorted()) {
                bool modified = false;
                TVector<TYtOutput> outs;
                for (auto out: publish.Input()) {
                    if (!IsUnorderedOutput(out) && TYqlRowSpecInfo(GetOutTable(out).Cast<TYtOutTable>().RowSpec()).IsSorted()) {
                        outs.push_back(Build<TYtOutput>(ctx, out.Pos())
                            .InitFrom(out)
                            .Mode()
                                .Value(ToString(EYtSettingType::Unordered))
                            .Build()
                            .Done());
                        modified = true;
                    } else {
                        outs.push_back(out);
                    }
                }
                if (modified) {
                    return Build<TYtPublish>(ctx, publish.Pos())
                        .InitFrom(publish)
                        .Input()
                            .Add(outs)
                        .Build()
                        .Done();
                }
            }
        }
        return node;
    }

    TMaybeNode<TExprBase> AddTrivialMapperForNativeYtTypes(TExprBase node, TExprContext& ctx) const {
        if (State_->Configuration->UseIntermediateSchema.Get().GetOrElse(DEFAULT_USE_INTERMEDIATE_SCHEMA)) {
            return node;
        }

        auto op = node.Cast<TYtMapReduce>();
        if (!op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoVoid>()) {
            return node;
        }

        bool needMapper = AnyOf(op.Input(), [](const TYtSection& section) {
            return AnyOf(section.Paths(), [](const TYtPath& path) {
                auto rowSpec = TYtTableBaseInfo::GetRowSpec(path.Table());
                return rowSpec && 0 != rowSpec->GetNativeYtTypeFlags();
            });
        });

        if (!needMapper) {
            return node;
        }

        auto mapper = Build<TCoLambda>(ctx, node.Pos()).Args({"stream"}).Body("stream").Done();

        return ctx.ChangeChild(node.Ref(), TYtMapReduce::idx_Mapper, mapper.Ptr());
    }

    TMaybeNode<TExprBase> YtDqWrite(TExprBase node, TExprContext& ctx) const {
        const auto write = node.Cast<TYtDqWrite>();

        if (ETypeAnnotationKind::Stream == write.Ref().GetTypeAnn()->GetKind()) {
            return Build<TCoFromFlow>(ctx, write.Pos())
                .Input<TYtDqWrite>()
                    .Settings(write.Settings())
                    .Input<TCoToFlow>()
                        .Input(write.Input())
                    .Build()
                .Build().Done();
        }

        const auto& items = GetSeqItemType(write.Input().Ref().GetTypeAnn())->Cast<TStructExprType>()->GetItems();
        auto expand = ctx.Builder(write.Pos())
            .Callable("ExpandMap")
                .Add(0, write.Input().Ptr())
                .Lambda(1)
                    .Param("item")
                    .Do([&](TExprNodeBuilder& lambda) -> TExprNodeBuilder& {
                        ui32 i = 0U;
                        for (const auto& item : items) {
                            lambda.Callable(i++, "Member")
                                .Arg(0, "item")
                                .Atom(1, item->GetName())
                            .Seal();
                        }
                        return lambda;
                    })
                .Seal()
            .Seal().Build();

        return Build<TCoDiscard>(ctx, write.Pos())
            .Input<TYtDqWideWrite>()
                .Input(std::move(expand))
                .Settings(write.Settings())
            .Build().Done();
    }

    TMaybeNode<TExprBase> PushDownYtMapOverSortedMerge(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto map = node.Cast<TYtMap>();

        if (map.Ref().HasResult()) {
            return node;
        }

        if (map.Input().Size() > 1 || map.Output().Size() > 1) {
            return node;
        }

        if (NYql::HasAnySetting(map.Settings().Ref(), EYtSettingType::Sharded | EYtSettingType::JobCount)) {
            return node;
        }

        if (!NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Ordered)) {
            return node;
        }

        auto section = map.Input().Item(0);
        if (section.Paths().Size() > 1) {
            return node;
        }
        if (NYql::HasSettingsExcept(section.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2)) {
            return node;
        }
        if (NYql::HasNonEmptyKeyFilter(section)) {
            return node;
        }
        auto path = section.Paths().Item(0);
        if (!path.Columns().Maybe<TCoVoid>() || !path.Ranges().Maybe<TCoVoid>()) {
            return node;
        }
        auto maybeMerge = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtMerge>();
        if (!maybeMerge) {
            return node;
        }
        auto merge = maybeMerge.Cast();
        if (merge.Ref().StartsExecution() || merge.Ref().HasResult()) {
            return node;
        }
        const auto rowSpec = TYqlRowSpecInfo(merge.Output().Item(0).RowSpec());
        if (!rowSpec.IsSorted()) {
            return node;
        }
        TMaybeNode<TExprBase> columns;
        if (rowSpec.HasAuxColumns()) {
            TSet<TStringBuf> members;
            for (auto item: rowSpec.GetType()->GetItems()) {
                members.insert(item->GetName());
            }
            columns = TExprBase(ToAtomList(members, merge.Pos(), ctx));
        }

        auto mergeSection = merge.Input().Item(0);
        if (NYql::HasSettingsExcept(mergeSection.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2)) {
            return node;
        }
        if (NYql::HasNonEmptyKeyFilter(mergeSection)) {
            return node;
        }
        if (merge.Settings().Size() > 0) {
            return node;
        }

        const TParentsMap* parentsMap = getParents();
        if (IsOutputUsedMultipleTimes(merge.Ref(), *parentsMap)) {
            // Merge output is used more than once
            return node;
        }

        auto world = map.World().Ptr();
        if (!merge.World().Ref().IsWorld()) {
            world = Build<TCoSync>(ctx, map.Pos()).Add(world).Add(merge.World()).Done().Ptr();
        }
        TVector<TYtPath> paths;
        for (auto path: mergeSection.Paths()) {
            auto newPath = Build<TYtPath>(ctx, map.Pos())
                .Table<TYtOutput>()
                    .Operation<TYtMap>()
                        .InitFrom(map)
                        .World(world)
                        .Input()
                            .Add()
                                .Paths()
                                    .Add<TYtPath>()
                                        .InitFrom(path)
                                        .Columns(columns.IsValid() ? columns.Cast() : path.Columns())
                                    .Build()
                                .Build()
                                .Settings(section.Settings())
                            .Build()
                        .Build()
                    .Build()
                    .OutIndex().Value("0").Build()
                .Build()
                .Columns<TCoVoid>().Build()
                .Ranges<TCoVoid>().Build()
                .Stat<TCoVoid>().Build()
                .Done();
            paths.push_back(std::move(newPath));
        }

        return Build<TYtMerge>(ctx, node.Pos())
            .World<TCoWorld>().Build()
            .DataSink(merge.DataSink())
            .Output(map.Output()) // Rewrite output type from YtMap
            .Input()
                .Add()
                    .Paths()
                        .Add(paths)
                    .Build()
                    .Settings(mergeSection.Settings())
                .Build()
            .Build()
            .Settings()
            .Build()
            .Done();
    }

    TMaybeNode<TExprBase> MergeToCopy(TExprBase node, TExprContext& ctx) const {
        auto merge = node.Cast<TYtMerge>();

        if (merge.Ref().HasResult()) {
            return node;
        }

        if (merge.Input().Item(0).Paths().Size() > 1) {
            return node;
        }

        if (NYql::HasAnySetting(merge.Settings().Ref(), EYtSettingType::ForceTransform | EYtSettingType::CombineChunks)) {
            return node;
        }

        auto limitNode = NYql::GetSetting(merge.Settings().Ref(), EYtSettingType::Limit);
        if (limitNode && limitNode->ChildrenSize() > 0) {
            return node;
        }

        TYtSection section = merge.Input().Item(0);
        TYtPath path = section.Paths().Item(0);
        if (!path.Ranges().Maybe<TCoVoid>() || !path.Ref().GetTypeAnn()->Equals(*path.Table().Ref().GetTypeAnn())) {
            return node;
        }
        if (path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtEquiJoin>()) {
            // YtEquiJoin may change output sort after rewrite
            return node;
        }
        auto tableInfo = TYtTableBaseInfo::Parse(path.Table());
        if (path.Table().Maybe<TYtTable>() || tableInfo->Meta->IsDynamic || !tableInfo->RowSpec || !tableInfo->RowSpec->StrictSchema) {
            return node;
        }
        if (tableInfo->IsUnordered && tableInfo->RowSpec->IsSorted()) {
            return node;
        }
        if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::Sample)) {
            return node;
        }
        if (NYql::HasNonEmptyKeyFilter(section)) {
            return node;
        }
        if (NYql::HasSetting(merge.Settings().Ref(), EYtSettingType::KeepSorted)) {
            auto op = path.Table().Maybe<TYtOutput>().Operation().Cast();
            if (!(op.Ref().HasResult() && op.Ref().GetResult().Type() == TExprNode::World || op.Maybe<TYtTouch>())) {
                return node;
            }
        }
        TYtOutTableInfo outTableInfo(merge.Output().Item(0));
        if (!tableInfo->RowSpec->CompareSortness(*outTableInfo.RowSpec)) {
            return node;
        }

        return Build<TYtCopy>(ctx, node.Pos())
            .World(merge.World())
            .DataSink(merge.DataSink())
            .Output(merge.Output())
            .Input()
                .Add()
                    .Paths()
                        .Add()
                            .InitFrom(path)
                            .Columns<TCoVoid>().Build()
                        .Build()
                    .Build()
                    .Settings()
                    .Build()
                .Build()
            .Build()
            .Settings()
            .Build()
            .Done();
    }
private:
    const TYtState::TPtr State_;
};

class TAsyncSyncCompositeTransformer : public TGraphTransformerBase {
public:
    TAsyncSyncCompositeTransformer(THolder<IGraphTransformer>&& async, THolder<IGraphTransformer>&& sync)
        : Async(std::move(async))
        , Sync(std::move(sync))
    {
    }
private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        auto status = Async->Transform(input, output, ctx);
        if (status.Level != TStatus::Ok) {
            return status;
        }
        return InstantTransform(*Sync, output, ctx, true);
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) override {
        return Async->GetAsyncFuture(input);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        return Async->ApplyAsyncChanges(input, output, ctx);
    }

    void Rewind() final {
        Async->Rewind();
        Sync->Rewind();
    }

    const THolder<IGraphTransformer> Async;
    const THolder<IGraphTransformer> Sync;

};

} // namespce

THolder<IGraphTransformer> CreateYtPhysicalOptProposalTransformer(TYtState::TPtr state) {
    return THolder(new TAsyncSyncCompositeTransformer(CreateYtLoadColumnarStatsTransformer(state),
                                              THolder<IGraphTransformer>(new TYtPhysicalOptProposalTransformer(state))));
}

} // namespace NYql
