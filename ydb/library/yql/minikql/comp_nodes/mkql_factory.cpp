#include "mkql_factories.h"

#include "mkql_addmember.h"
#include "mkql_aggrcount.h"
#include "mkql_append.h"
#include "mkql_apply.h"
#include "mkql_block_add.h"
#include "mkql_blocks.h"
#include "mkql_callable.h"
#include "mkql_chain_map.h"
#include "mkql_chain1_map.h"
#include "mkql_chopper.h"
#include "mkql_coalesce.h"
#include "mkql_collect.h"
#include "mkql_combine.h"
#include "mkql_contains.h"
#include "mkql_decimal_div.h"
#include "mkql_decimal_mod.h"
#include "mkql_decimal_mul.h"
#include "mkql_dictitems.h"
#include "mkql_discard.h"
#include "mkql_element.h"
#include "mkql_ensure.h"
#include "mkql_enumerate.h"
#include "mkql_exists.h"
#include "mkql_extend.h"
#include "mkql_filter.h"
#include "mkql_flatmap.h"
#include "mkql_flow.h"
#include "mkql_fold.h"
#include "mkql_fold1.h"
#include "mkql_frombytes.h"
#include "mkql_fromstring.h"
#include "mkql_fromyson.h" 
#include "mkql_guess.h"
#include "mkql_group.h"
#include "mkql_heap.h"
#include "mkql_hasitems.h"
#include "mkql_hopping.h"
#include "mkql_if.h"
#include "mkql_ifpresent.h"
#include "mkql_invoke.h"
#include "mkql_iterable.h"
#include "mkql_iterator.h"
#include "mkql_join.h"
#include "mkql_join_dict.h"
#include "mkql_lazy_list.h"
#include "mkql_length.h"
#include "mkql_listfromrange.h"
#include "mkql_logical.h"
#include "mkql_lookup.h"
#include "mkql_map.h"
#include "mkql_map_join.h"
#include "mkql_multihopping.h"
#include "mkql_multimap.h"
#include "mkql_next_value.h"
#include "mkql_now.h"
#include "mkql_null.h"
#include "mkql_pickle.h"
#include "mkql_prepend.h"
#include "mkql_queue.h" 
#include "mkql_random.h"
#include "mkql_range.h"
#include "mkql_reduce.h"
#include "mkql_removemember.h"
#include "mkql_replicate.h"
#include "mkql_reverse.h"
#include "mkql_round.h"
#include "mkql_seq.h"
#include "mkql_size.h"
#include "mkql_skip.h"
#include "mkql_sort.h"
#include "mkql_condense.h"
#include "mkql_condense1.h"
#include "mkql_source.h"
#include "mkql_squeeze_to_list.h"
#include "mkql_switch.h"
#include "mkql_take.h"
#include "mkql_timezone.h"
#include "mkql_tobytes.h"
#include "mkql_todict.h"
#include "mkql_toindexdict.h"
#include "mkql_tooptional.h"
#include "mkql_tostring.h"
#include "mkql_udf.h"
#include "mkql_unwrap.h"
#include "mkql_varitem.h"
#include "mkql_visitall.h"
#include "mkql_way.h"
#include "mkql_weakmember.h" 
#include "mkql_while.h"
#include "mkql_wide_chain_map.h"
#include "mkql_wide_chopper.h"
#include "mkql_wide_combine.h"
#include "mkql_wide_condense.h"
#include "mkql_wide_filter.h"
#include "mkql_wide_map.h"
#include "mkql_zip.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>

#include <string_view>
#include <unordered_map>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapArg(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 0, "Expected 0 args");
    MKQL_ENSURE(callable.GetType()->IsMergeDisabled(), "Merge mode is not disabled");
    return new TExternalCodegeneratorNode(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()));
}

IComputationNode* WrapWideFlowArg(TCallable& callable, const TComputationNodeFactoryContext&) {
    MKQL_ENSURE(callable.GetInputsCount() == 0, "Expected 0 args");
    MKQL_ENSURE(callable.GetType()->IsMergeDisabled(), "Merge mode is not disabled");
    return new TWideFlowProxyCodegeneratorNode;
}

using TCallableComputationNodeBuilderPtr = IComputationNode* (*const)(TCallable& callable, const TComputationNodeFactoryContext& ctx);
using TCallableComputationNodeBuilderMap = std::unordered_map<std::string_view, TCallableComputationNodeBuilderPtr>;

namespace {

struct TCallableComputationNodeBuilderFuncMapFiller {
    const TCallableComputationNodeBuilderMap Map;

    TCallableComputationNodeBuilderFuncMapFiller()
        : Map(InitList)
    {}

    static constexpr std::initializer_list<TCallableComputationNodeBuilderMap::value_type> InitList = {
        {"Append", &WrapAppend},
        {"Prepend", &WrapPrepend},
        {"Extend", &WrapExtend},
        {"Arg", &WrapArg},
        {"Null", &WrapNull},
        {"Fold", &WrapFold},
        {"Condense", &WrapCondense},
        {"Condense1", &WrapCondense1},
        {"Squeeze", &WrapSqueeze},
        {"Squeeze1", &WrapSqueeze1},
        {"Discard", &WrapDiscard},
        {"Fold1", &WrapFold1},
        {"Map", &WrapMap},
        {"OrderedMap", &WrapMap},
        {"MultiMap", &WrapMultiMap},
        {"FlatMap", &WrapFlatMap},
        {"OrderedFlatMap", &WrapFlatMap},
        {"ChainMap", &WrapChainMap},
        {"Chain1Map", &WrapChain1Map},
        {"Filter", &WrapFilter},
        {"OrderedFilter", &WrapFilter},
        {"TakeWhile", &WrapTakeWhile},
        {"SkipWhile", &WrapSkipWhile},
        {"TakeWhileInclusive", &WrapTakeWhileInclusive},
        {"SkipWhileInclusive", &WrapSkipWhileInclusive},
        {"AddMember", &WrapAddMember},
        {"Member", &WrapMember},
        {"RemoveMember", &WrapRemoveMember},
        {"Exists", &WrapExists},
        {"Contains", &WrapContains},
        {"Lookup", &WrapLookup},
        {"ToSortedDict", &WrapToSortedDict},
        {"ToHashedDict", &WrapToHashedDict},
        {"SqueezeToList", &WrapSqueezeToList},
        {"SqueezeToSortedDict", &WrapSqueezeToSortedDict},
        {"SqueezeToHashedDict", &WrapSqueezeToHashedDict},
        {"NarrowSqueezeToSortedDict", &WrapSqueezeToSortedDict},
        {"NarrowSqueezeToHashedDict", &WrapSqueezeToHashedDict},
        {"Coalesce", &WrapCoalesce},
        {"ToOptional", &WrapHead},
        {"Head", &WrapHead},
        {"Last", &WrapLast},
        {"Unwrap", &WrapUnwrap},
        {"Ensure", &WrapEnsure},
        {"If", &WrapIf},
        {"IfPresent", &WrapIfPresent},
        {"And", &WrapAnd},
        {"Or", &WrapOr},
        {"Xor", &WrapXor},
        {"Not", &WrapNot},
        {"Zip", &WrapZip<false>},
        {"ZipAll", &WrapZip<true>},
        {"Reduce", &WrapReduce},
        {"Length", &WrapLength},
        {"Iterable", &WrapIterable},
        {"Iterator", &WrapIterator},
        {"EmptyIterator", &WrapEmptyIterator},
        {"ForwardList", &WrapForwardList},
        {"Switch", &WrapSwitch},
        {"Collect", &WrapCollect},
        {"ListFromRange", &WrapListFromRange},
        {"HasItems", &WrapHasItems},
        {"Reverse", &WrapReverse},
        {"Skip", &WrapSkip},
        {"Take", &WrapTake},
        {"Replicate", &WrapReplicate},
        {"Invoke", &WrapInvoke},
        {"Udf", &WrapUdf},
        {"ScriptUdf", &WrapScriptUdf},
        {"Apply", &WrapApply},
        {"Apply2", &WrapApply},
        {"Callable", &WrapCallable},
        {"Size", &WrapSize},
        {"ToString", &WrapToString},
        {"FromString", &WrapFromString},
        {"StrictFromString", &WrapStrictFromString},
        {"Enumerate", &WrapEnumerate},
        {"Sort", &WrapSort},
        {"UnstableSort", &WrapUnstableSort},
        {"DictItems", &WrapDictItems},
        {"DictKeys", &WrapDictKeys},
        {"DictPayloads", &WrapDictPayloads},
        {"Nth", &WrapNth},
        {"ToIndexDict", &WrapToIndexDict},
        {"JoinDict", &WrapJoinDict},
        {"MapJoinCore", &WrapMapJoinCore},
        {"CommonJoinCore", &WrapCommonJoinCore},
        {"CombineCore", &WrapCombineCore},
        {"GroupingCore", &WrapGroupingCore},
        {"HoppingCore", &WrapHoppingCore},
        {"MultiHoppingCore", &WrapMultiHoppingCore},
        {"ToBytes", &WrapToBytes},
        {"FromBytes", &WrapFromBytes},
        {"NewMTRand", &WrapNewMTRand},
        {"NextMTRand", &WrapNextMTRand},
        {"Random", &WrapRandom<ERandom::Double>},
        {"RandomNumber", &WrapRandom<ERandom::Number>},
        {"RandomUuid", &WrapRandom<ERandom::Uuid>},
        {"Now", &WrapNow},
        {"Pickle", &WrapPickle},
        {"StablePickle", &WrapStablePickle},
        {"Unpickle", &WrapUnpickle},
        {"Ascending", &WrapAscending},
        {"Descending", &WrapDescending},
        {"Guess", &WrapGuess},
        {"VariantItem", &WrapVariantItem},
        {"Way", &WrapWay},
        {"VisitAll", &WrapVisitAll},
        {"AggrCountInit", &WrapAggrCountInit},
        {"AggrCountUpdate", &WrapAggrCountUpdate},
        {"QueueCreate", &WrapQueueCreate},
        {"QueuePush", &WrapQueuePush},
        {"QueuePop", &WrapQueuePop},
        {"QueuePeek", &WrapQueuePeek},
        {"QueueRange", &WrapQueueRange},
        {"Seq", &WrapSeq},
        {"PreserveStream", &WrapPreserveStream},
        {"FromYsonSimpleType", &WrapFromYsonSimpleType},
        {"TryWeakMemberFromDict", &WrapTryWeakMemberFromDict},
        {"TimezoneId", &WrapTimezoneId},
        {"TimezoneName", &WrapTimezoneName},
        {"AddTimezone", &WrapAddTimezone},
        {"DecimalDiv", &WrapDecimalDiv},
        {"DecimalMod", &WrapDecimalMod},
        {"DecimalMul", &WrapDecimalMul},
        {"ToFlow", &WrapToFlow},
        {"FromFlow", &WrapFromFlow},
        {"ToBlocks", &WrapToBlocks},
        {"WideToBlocks", &WrapWideToBlocks},
        {"BlockAdd", &WrapBlockAdd},
        {"FromBlocks", &WrapFromBlocks},
        {"AsSingle", &WrapAsSingle},
        {"MakeHeap", &WrapMakeHeap},
        {"PushHeap", &WrapPushHeap},
        {"PopHeap", &WrapPopHeap},
        {"SortHeap", &WrapSortHeap},
        {"StableSort", &WrapStableSort},
        {"NthElement", &WrapNthElement},
        {"PartialSort", &WrapPartialSort},
        {"KeepTop", &WrapKeepTop},
        {"Top", &WrapTop},
        {"TopSort", &WrapTopSort},
        {"SourceOf", &WrapSourceOf},
        {"LazyList", &WrapLazyList},
        {"Chopper", &WrapChopper},
        {"ExpandMap", &WrapExpandMap},
        {"WideMap", &WrapWideMap},
        {"WideChain1Map", &WrapWideChain1Map},
        {"NarrowMap", &WrapNarrowMap},
        {"NarrowFlatMap", &WrapNarrowFlatMap},
        {"NarrowMultiMap", &WrapNarrowMultiMap},
        {"WideFilter", &WrapWideFilter},
        {"WideTakeWhile", &WrapWideTakeWhile},
        {"WideSkipWhile", &WrapWideSkipWhile},
        {"WideTakeWhileInclusive", &WrapWideTakeWhileInclusive},
        {"WideSkipWhileInclusive", &WrapWideSkipWhileInclusive},
        {"WideCombiner", &WrapWideCombiner},
        {"WideLastCombiner", &WrapWideLastCombiner},
        {"WideCondense1", &WrapWideCondense1},
        {"WideChopper", &WrapWideChopper},
        {"WideFlowArg", &WrapWideFlowArg},
        {"Source", &WrapSource},
        {"RangeCreate", &WrapRangeCreate},
        {"RangeUnion", &WrapRangeUnion},
        {"RangeIntersect", &WrapRangeIntersect},
        {"RangeMultiply", &WrapRangeMultiply},
        {"RangeFinalize", &WrapRangeFinalize},
        {"RoundUp", &WrapRound},
        {"RoundDown", &WrapRound},
        {"NextValue", &WrapNextValue},
    };
};

}

TComputationNodeFactory GetBuiltinFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        const auto& map = Singleton<TCallableComputationNodeBuilderFuncMapFiller>()->Map;
        const auto it = map.find(callable.GetType()->GetName());
        if (it == map.end())
            return nullptr;

        return it->second(callable, ctx);
    };
}

TComputationNodeFactory GetCompositeWithBuiltinFactory(TVector<TComputationNodeFactory> factories) {
    return [factories = std::move(factories), builtins = GetBuiltinFactory()](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        for (auto& f: factories) {
            if (auto res = f(callable, ctx)) {
                return res;
            }
        }

        return builtins(callable, ctx);
    };
}

}
}
