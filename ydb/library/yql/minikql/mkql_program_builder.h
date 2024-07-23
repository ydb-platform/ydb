#pragma once

#include "defs.h"
#include "mkql_node.h"
#include "mkql_node_builder.h"
#include "mkql_type_builder.h"
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/core/sql_types/match_recognize.h>

#include <functional>

namespace NKikimr {
namespace NMiniKQL {

class IFunctionRegistry;
class TBuiltinFunctionRegistry;

constexpr std::string_view RandomMTResource = "MTRand";
constexpr std::string_view ResourceQueuePrefix = "TResourceQueue:";

enum class EJoinKind {
    Min = 1,
    LeftOnly = 1,
    Inner = 2,
    RightOnly = 4,
    Left = 1 | 2,
    Right = 4 | 2,
    Exclusion = 1 | 4,
    Full = 1 | 2 | 4,
    Max = 7,
    LeftSemi = 2 | 8 | 0,
    RightSemi = 2 | 8 | 16,
    SemiMask = 8,
    SemiSide = 16,
    Cross = 32
};

enum class EDictItems {
    Both = 0,
    Keys = 1,
    Payloads = 2
};

enum class EAnyJoinSettings {
    None  = 0,
    Left  = 1,
    Right = 2,
    Both  = 1 | 2,
};

inline EJoinKind GetJoinKind(ui32 kind) {
    MKQL_ENSURE(kind >= (ui32)EJoinKind::Min && kind <= (ui32)EJoinKind::Max ||
        kind == (ui32)EJoinKind::LeftSemi || kind == (ui32)EJoinKind::RightSemi ||
        kind == (ui32)EJoinKind::Cross, "Bad join kind: " << kind);
    return (EJoinKind)kind;
}

constexpr bool IsLeftOptional(EJoinKind kind) {
    return ((ui32)EJoinKind::RightOnly & (ui32)kind) != 0 && (kind != EJoinKind::RightOnly);
}

constexpr bool IsRightOptional(EJoinKind kind) {
    return ((ui32)EJoinKind::LeftOnly & (ui32)kind) != 0 && (kind != EJoinKind::LeftOnly);
}

constexpr bool IsSemiJoin(EJoinKind kind) {
    return ((ui32)EJoinKind::SemiMask & (ui32)kind) != 0;
}

inline EAnyJoinSettings GetAnyJoinSettings(ui32 settings) {
    MKQL_ENSURE(settings <= (ui32)EAnyJoinSettings::Both, "Bad AnyJoin settings: " << settings);
    return (EAnyJoinSettings)settings;
}

inline bool IsAnyJoinLeft(EAnyJoinSettings settings) {
    return ((ui32)settings & (ui32)EAnyJoinSettings::Left) != 0;
}

inline bool IsAnyJoinRight(EAnyJoinSettings settings) {
    return ((ui32)settings & (ui32)EAnyJoinSettings::Right) != 0;
}

inline void AddAnyJoinSide(EAnyJoinSettings& combined, EAnyJoinSettings value) {
    ui32 combinedVal = (ui32)combined;
    combinedVal |= (ui32)value;
    combined = (EAnyJoinSettings)combinedVal;
}

inline bool HasSpillingFlag(const TCallable& callable) {
    return TStringBuf(callable.GetType()->GetName()).EndsWith("WithSpilling"_sb);
}

#define MKQL_SCRIPT_TYPES(xx) \
    xx(Unknown, 0, unknown, false) \
    xx(Python, 1, python, false) \
    xx(Lua, 2, lua, false) \
    xx(ArcPython, 3, arcpython, false) \
    xx(CustomPython, 4, custompython, true) \
    xx(Javascript, 5, javascript, false) \
    xx(Python2, 6, python2, false) \
    xx(ArcPython2, 7, arcpython2, false) \
    xx(CustomPython2, 8, custompython2, true) \
    xx(Python3, 9, python3, false) \
    xx(ArcPython3, 10, arcpython3, false) \
    xx(CustomPython3, 11, custompython3, true) \
    xx(SystemPython2, 12, systempython2, false) \
    xx(SystemPython3, 13, systempython3, false) \
    xx(SystemPython3_8, 14, systempython3_8, false) \
    xx(SystemPython3_9, 15, systempython3_9, false) \
    xx(SystemPython3_10, 16, systempython3_10, false) \
    xx(SystemPython3_11, 17, systempython3_11, false) \
    xx(SystemPython3_12, 18, systempython3_12, false) \
    xx(SystemPython3_13, 19, systempython3_13, false) \

enum class EScriptType {
    MKQL_SCRIPT_TYPES(ENUM_VALUE_GEN)
};

std::string_view ScriptTypeAsStr(EScriptType type);
EScriptType ScriptTypeFromStr(std::string_view str);
bool IsCustomPython(EScriptType type);
bool IsSystemPython(EScriptType type);
EScriptType CanonizeScriptType(EScriptType type);

struct TSwitchInput {
    std::vector<ui32> Indicies;
    TType* InputType = nullptr;
    std::optional<ui32> ResultVariantOffset;
};

struct TAggInfo {
    TString Name;
    std::vector<ui32> ArgsColumns;
};

class TProgramBuilder : public TTypeBuilder {
public:
    TProgramBuilder(const TTypeEnvironment& env, const IFunctionRegistry& functionRegistry, bool voidWithEffects = false);

    const TTypeEnvironment& GetTypeEnvironment() const;
    const IFunctionRegistry& GetFunctionRegistry() const;

    TRuntimeNode Arg(TType* type) const;
    TRuntimeNode WideFlowArg(TType* type) const;

    //-- literal functions
    TRuntimeNode NewVoid();
    TRuntimeNode NewNull();



    template <typename T, typename = std::enable_if_t<NUdf::TKnownDataType<T>::Result>>
    TRuntimeNode NewDataLiteral(T data) const {
        return TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod(data), NUdf::TDataType<T>::Id, Env), true);
    }


    template <typename T, typename = std::enable_if_t<NUdf::TTzDataType<T>::Result>>
    TRuntimeNode NewTzDataLiteral(typename NUdf::TDataType<T>::TLayout value, ui16 tzId) const {
        auto data = NUdf::TUnboxedValuePod(value);
        data.SetTimezoneId(tzId);
        return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<T>::Id, Env), true);
    }

    template <NUdf::EDataSlot Type>
    TRuntimeNode NewDataLiteral(const NUdf::TStringRef& data) const;

    TRuntimeNode NewDecimalLiteral(NYql::NDecimal::TInt128 data, ui8 precision, ui8 scale) const;

    TRuntimeNode NewEmptyOptional(TType* optionalOrPgType);
    TRuntimeNode NewEmptyOptionalDataLiteral(NUdf::TDataTypeId schemeType);
    TRuntimeNode NewOptional(TRuntimeNode data);
    TRuntimeNode NewOptional(TType* optionalType, TRuntimeNode data);

    TRuntimeNode NewEmptyStruct();
    TRuntimeNode NewStruct(const TArrayRef<const std::pair<std::string_view, TRuntimeNode>>& members);
    TRuntimeNode NewStruct(TType* structType, const TArrayRef<const std::pair<std::string_view, TRuntimeNode>>& members);

    TRuntimeNode NewEmptyList();
    TRuntimeNode NewEmptyList(TType* itemType);
    TRuntimeNode NewEmptyListOfVoid();
    TRuntimeNode NewList(TType* itemType, const TArrayRef<const TRuntimeNode>& items);

    TRuntimeNode NewEmptyDict();
    TRuntimeNode NewDict(TType* dictType, const TArrayRef<const std::pair<TRuntimeNode, TRuntimeNode>>& items);

    TRuntimeNode NewEmptyTuple();
    TRuntimeNode NewTuple(TType* tupleType, const TArrayRef<const TRuntimeNode>& elements);
    TRuntimeNode NewTuple(const TArrayRef<const TRuntimeNode>& elements);

    TRuntimeNode NewVariant(TRuntimeNode item, ui32 tupleIndex, TType* variantType);
    TRuntimeNode NewVariant(TRuntimeNode item, const std::string_view& member, TType* variantType);

    // generic data transformation, some args could be optional
    TRuntimeNode Convert(TRuntimeNode data, TType* type);
    TRuntimeNode ToIntegral(TRuntimeNode data, TType* type);
    TRuntimeNode ToDecimal(TRuntimeNode data, ui8 precision, ui8 scale);
    TRuntimeNode Concat(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode AggrConcat(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode Substring(TRuntimeNode data, TRuntimeNode start, TRuntimeNode count);
    TRuntimeNode Find(TRuntimeNode haystack, TRuntimeNode needle, TRuntimeNode pos);
    TRuntimeNode RFind(TRuntimeNode haystack, TRuntimeNode needle, TRuntimeNode pos);
    TRuntimeNode StartsWith(TRuntimeNode string, TRuntimeNode prefix);
    TRuntimeNode EndsWith(TRuntimeNode string, TRuntimeNode suffix);
    TRuntimeNode StringContains(TRuntimeNode string, TRuntimeNode pattern);
    TRuntimeNode ByteAt(TRuntimeNode data, TRuntimeNode index);
    TRuntimeNode Size(TRuntimeNode data);
    template <bool Utf8 = false>
    TRuntimeNode ToString(TRuntimeNode data);
    TRuntimeNode FromString(TRuntimeNode data, TType* type);
    TRuntimeNode StrictFromString(TRuntimeNode data, TType* type);
    TRuntimeNode ToBytes(TRuntimeNode data);
    TRuntimeNode FromBytes(TRuntimeNode data, NUdf::TDataTypeId schemeType);
    TRuntimeNode InversePresortString(TRuntimeNode data);
    TRuntimeNode InverseString(TRuntimeNode data);
    TRuntimeNode Random(const TArrayRef<const TRuntimeNode>& dependentNodes);
    TRuntimeNode RandomNumber(const TArrayRef<const TRuntimeNode>& dependentNodes);
    TRuntimeNode RandomUuid(const TArrayRef<const TRuntimeNode>& dependentNodes);

    TRuntimeNode Now(const TArrayRef<const TRuntimeNode>& dependentNodes);
    TRuntimeNode CurrentUtcDate(const TArrayRef<const TRuntimeNode>& dependentNodes);
    TRuntimeNode CurrentUtcDatetime(const TArrayRef<const TRuntimeNode>& dependentNodes);
    TRuntimeNode CurrentUtcTimestamp(const TArrayRef<const TRuntimeNode>& dependentNodes);

    TRuntimeNode Pickle(TRuntimeNode data);
    TRuntimeNode StablePickle(TRuntimeNode data);
    TRuntimeNode Unpickle(TType* type, TRuntimeNode serialized);
    TRuntimeNode Ascending(TRuntimeNode data);
    TRuntimeNode Descending(TRuntimeNode data);

    TRuntimeNode ToFlow(TRuntimeNode stream);
    TRuntimeNode FromFlow(TRuntimeNode flow);
    TRuntimeNode Steal(TRuntimeNode input);

    TRuntimeNode ToBlocks(TRuntimeNode flow);
    TRuntimeNode WideToBlocks(TRuntimeNode flow);
    TRuntimeNode FromBlocks(TRuntimeNode flow);
    TRuntimeNode WideFromBlocks(TRuntimeNode flow);
    TRuntimeNode WideSkipBlocks(TRuntimeNode flow, TRuntimeNode count);
    TRuntimeNode WideTakeBlocks(TRuntimeNode flow, TRuntimeNode count);
    TRuntimeNode WideTopBlocks(TRuntimeNode flow, TRuntimeNode count, const std::vector<std::pair<ui32, TRuntimeNode>>& keys);
    TRuntimeNode WideTopSortBlocks(TRuntimeNode flow, TRuntimeNode count, const std::vector<std::pair<ui32, TRuntimeNode>>& keys);
    TRuntimeNode WideSortBlocks(TRuntimeNode flow, const std::vector<std::pair<ui32, TRuntimeNode>>& keys);
    TRuntimeNode AsScalar(TRuntimeNode value);
    TRuntimeNode ReplicateScalar(TRuntimeNode value, TRuntimeNode count);
    TRuntimeNode BlockCompress(TRuntimeNode flow, ui32 bitmapIndex);
    TRuntimeNode BlockExpandChunked(TRuntimeNode flow);
    TRuntimeNode BlockCoalesce(TRuntimeNode first, TRuntimeNode second);
    TRuntimeNode BlockExists(TRuntimeNode data);
    TRuntimeNode BlockMember(TRuntimeNode structure, const std::string_view& memberName);
    TRuntimeNode BlockNth(TRuntimeNode tuple, ui32 index);
    TRuntimeNode BlockAsStruct(const TArrayRef<std::pair<std::string_view, TRuntimeNode>>& args);
    TRuntimeNode BlockAsTuple(const TArrayRef<const TRuntimeNode>& args);
    TRuntimeNode BlockToPg(TRuntimeNode input, TType* returnType);
    TRuntimeNode BlockFromPg(TRuntimeNode input, TType* returnType);
    TRuntimeNode BlockPgResolvedCall(const std::string_view& name, ui32 id,
        const TArrayRef<const TRuntimeNode>& args, TType* returnType);
    TRuntimeNode BlockMapJoinCore(TRuntimeNode flow, TRuntimeNode dict,
        EJoinKind joinKind, const TArrayRef<const ui32>& leftKeyColumns);

    //-- logical functions
    TRuntimeNode BlockNot(TRuntimeNode data);
    TRuntimeNode BlockAnd(TRuntimeNode first, TRuntimeNode second);
    TRuntimeNode BlockOr(TRuntimeNode first, TRuntimeNode second);
    TRuntimeNode BlockXor(TRuntimeNode first, TRuntimeNode second);

    TRuntimeNode BlockIf(TRuntimeNode condition, TRuntimeNode thenBranch, TRuntimeNode elseBranch);
    TRuntimeNode BlockJust(TRuntimeNode data);

    TRuntimeNode BlockFunc(const std::string_view& funcName, TType* returnType, const TArrayRef<const TRuntimeNode>& args);
    TRuntimeNode BlockBitCast(TRuntimeNode value, TType* targetType);
    TRuntimeNode BlockCombineAll(TRuntimeNode flow, std::optional<ui32> filterColumn,
        const TArrayRef<const TAggInfo>& aggs, TType* returnType);
    TRuntimeNode BlockCombineHashed(TRuntimeNode flow, std::optional<ui32> filterColumn, const TArrayRef<ui32>& keys,
        const TArrayRef<const TAggInfo>& aggs, TType* returnType);
    TRuntimeNode BlockMergeFinalizeHashed(TRuntimeNode flow, const TArrayRef<ui32>& keys,
        const TArrayRef<const TAggInfo>& aggs, TType* returnType);
    TRuntimeNode BlockMergeManyFinalizeHashed(TRuntimeNode flow, const TArrayRef<ui32>& keys,
        const TArrayRef<const TAggInfo>& aggs, ui32 streamIndex, const TVector<TVector<ui32>>& streams, TType* returnType);

    // udfs
    TRuntimeNode Udf(
            const std::string_view& funcName,
            TRuntimeNode runConfig = TRuntimeNode(),
            TType* userType = nullptr,
            const std::string_view& typeConfig = std::string_view(""));

    TRuntimeNode TypedUdf(
        const std::string_view& funcName,
        TType* funcType,
        TRuntimeNode runConfig = TRuntimeNode(),
        TType* userType = nullptr,
        const std::string_view& typeConfig = std::string_view(""),
        const std::string_view& file = std::string_view(""), ui32 row = 0, ui32 column = 0);

    TRuntimeNode ScriptUdf(
        const std::string_view& moduleName,
        const std::string_view& funcName,
        TType* funcType,
        TRuntimeNode script,
        const std::string_view& file = std::string_view(""), ui32 row = 0, ui32 column = 0);

    typedef std::function<TRuntimeNode ()> TZeroLambda;
    typedef std::function<TRuntimeNode (TRuntimeNode)> TUnaryLambda;
    typedef std::function<TRuntimeNode (TRuntimeNode, TRuntimeNode)> TBinaryLambda;
    typedef std::function<TRuntimeNode (TRuntimeNode, TRuntimeNode, TRuntimeNode)> TTernaryLambda;
    typedef std::function<TRuntimeNode(const TArrayRef<const TRuntimeNode>& args)> TArrayLambda;

    typedef std::function<TRuntimeNodePair (TRuntimeNode)> TUnarySplitLambda;
    typedef std::function<TRuntimeNodePair (TRuntimeNode, TRuntimeNode)> TBinarySplitLambda;

    typedef std::function<TRuntimeNode::TList (TRuntimeNode)> TExpandLambda;
    typedef std::function<TRuntimeNode::TList (TRuntimeNode::TList)> TWideLambda;
    typedef std::function<TRuntimeNode::TList (TRuntimeNode::TList, TRuntimeNode::TList)> TBinaryWideLambda;
    typedef std::function<TRuntimeNode::TList (TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList)> TTernaryWideLambda;
    typedef std::function<TRuntimeNode (TRuntimeNode::TList)> TNarrowLambda;

    typedef std::function<TRuntimeNode (TRuntimeNode::TList, TRuntimeNode::TList)> TWideSwitchLambda;

    TRuntimeNode Apply(TRuntimeNode callableNode, const TArrayRef<const TRuntimeNode>& args, ui32 dependentCount = 0);
    TRuntimeNode Apply(TRuntimeNode callableNode, const TArrayRef<const TRuntimeNode>& args,
         const std::string_view& file, ui32 row, ui32 column, ui32 dependentCount = 0);
    TRuntimeNode Callable(TType* callableType, const TArrayLambda& handler);

    //-- struct functions
    TRuntimeNode Member(TRuntimeNode structObj, const std::string_view& memberName);
    TRuntimeNode Element(TRuntimeNode tuple, const std::string_view& memberName);
    TRuntimeNode AddMember(TRuntimeNode structObj, const std::string_view& memberName, TRuntimeNode memberValue);
    TRuntimeNode RemoveMember(TRuntimeNode structObj, const std::string_view& memberName, bool forced);
    TRuntimeNode RemoveMembers(TRuntimeNode structObj, const TArrayRef<const std::string_view>& members, bool forced);

    //-- list functions
    TRuntimeNode Append(TRuntimeNode list, TRuntimeNode item);
    TRuntimeNode Prepend(TRuntimeNode item, TRuntimeNode list);
    TRuntimeNode Extend(const TArrayRef<const TRuntimeNode>& lists);
    TRuntimeNode OrderedExtend(const TArrayRef<const TRuntimeNode>& lists);
    // returns list of tuples with items, stops at the shortest list
    TRuntimeNode Zip(const TArrayRef<const TRuntimeNode>& lists);
    // returns list of tuples with optional of items, has length of the longest list
    TRuntimeNode ZipAll(const TArrayRef<const TRuntimeNode>& lists);
    TRuntimeNode Enumerate(TRuntimeNode list);
    TRuntimeNode Enumerate(TRuntimeNode list, TRuntimeNode start, TRuntimeNode step);
    TRuntimeNode Fold(TRuntimeNode list, TRuntimeNode state, const TBinaryLambda& handler);
    TRuntimeNode Fold1(TRuntimeNode list, const TUnaryLambda& init, const TBinaryLambda& handler);
    TRuntimeNode Reduce(TRuntimeNode list, TRuntimeNode state1,
        const TBinaryLambda& handler1,
        const TUnaryLambda& handler2,
        TRuntimeNode state3,
        const TBinaryLambda& handler3);
    TRuntimeNode Condense(TRuntimeNode stream, TRuntimeNode state,
        const TBinaryLambda& switcher,
        const TBinaryLambda& handler, bool useCtx = false);
    TRuntimeNode Condense1(TRuntimeNode stream, const TUnaryLambda& init,
        const TBinaryLambda& switcher,
        const TBinaryLambda& handler, bool useCtx = false);
    TRuntimeNode Squeeze(TRuntimeNode stream, TRuntimeNode state,
        const TBinaryLambda& handler,
        const TUnaryLambda& save = {},
        const TUnaryLambda& load = {});
    TRuntimeNode Squeeze1(TRuntimeNode stream, const TUnaryLambda& init,
        const TBinaryLambda& handler,
        const TUnaryLambda& save = {},
        const TUnaryLambda& load = {});
    TRuntimeNode Discard(TRuntimeNode stream);
    TRuntimeNode Map(TRuntimeNode list, const TUnaryLambda& handler);
    TRuntimeNode OrderedMap(TRuntimeNode list, const TUnaryLambda& handler);
    TRuntimeNode MapNext(TRuntimeNode list, const TBinaryLambda& handler);
    TRuntimeNode Extract(TRuntimeNode list, const std::string_view& name);
    TRuntimeNode OrderedExtract(TRuntimeNode list, const std::string_view& name);
    TRuntimeNode ChainMap(TRuntimeNode list, TRuntimeNode state, const TBinaryLambda& handler);
    TRuntimeNode ChainMap(TRuntimeNode list, TRuntimeNode state, const TBinarySplitLambda& handler);
    TRuntimeNode Chain1Map(TRuntimeNode list, const TUnaryLambda& init, const TBinaryLambda& handler);
    TRuntimeNode Chain1Map(TRuntimeNode list, const TUnarySplitLambda& init, const TBinarySplitLambda& handler);
    TRuntimeNode FlatMap(TRuntimeNode list, const TUnaryLambda& handler);
    TRuntimeNode OrderedFlatMap(TRuntimeNode list, const TUnaryLambda& handler);
    TRuntimeNode MultiMap(TRuntimeNode list, const TExpandLambda& handler);
    TRuntimeNode Filter(TRuntimeNode list, const TUnaryLambda& handler);
    TRuntimeNode Filter(TRuntimeNode list, TRuntimeNode limit, const TUnaryLambda& handler);
    TRuntimeNode OrderedFilter(TRuntimeNode list, const TUnaryLambda& handler);
    TRuntimeNode OrderedFilter(TRuntimeNode list, TRuntimeNode limit, const TUnaryLambda& handler);
    TRuntimeNode TakeWhile(TRuntimeNode list, const TUnaryLambda& handler);
    TRuntimeNode SkipWhile(TRuntimeNode list, const TUnaryLambda& handler);
    TRuntimeNode TakeWhileInclusive(TRuntimeNode list, const TUnaryLambda& handler);
    TRuntimeNode SkipWhileInclusive(TRuntimeNode list, const TUnaryLambda& handler);

    TRuntimeNode FilterNullMembers(TRuntimeNode list);
    TRuntimeNode SkipNullMembers(TRuntimeNode list);
    TRuntimeNode FilterNullMembers(TRuntimeNode list, const TArrayRef<const std::string_view>& members);
    TRuntimeNode SkipNullMembers(TRuntimeNode list, const TArrayRef<const std::string_view>& members);

    TRuntimeNode FilterNullElements(TRuntimeNode list);
    TRuntimeNode SkipNullElements(TRuntimeNode list);
    TRuntimeNode FilterNullElements(TRuntimeNode list, const TArrayRef<const ui32>& elements);
    TRuntimeNode SkipNullElements(TRuntimeNode list, const TArrayRef<const ui32>& elements);

    TRuntimeNode ExpandMap(TRuntimeNode flow, const TExpandLambda& handler);
    TRuntimeNode WideMap(TRuntimeNode flow, const TWideLambda& handler);
    TRuntimeNode NarrowMap(TRuntimeNode flow, const TNarrowLambda& handler);
    TRuntimeNode NarrowFlatMap(TRuntimeNode flow, const TNarrowLambda& handler);
    TRuntimeNode NarrowMultiMap(TRuntimeNode flow, const TWideLambda& handler);

    TRuntimeNode WideChain1Map(TRuntimeNode flow, const TWideLambda& init, const TBinaryWideLambda& update);

    TRuntimeNode WideFilter(TRuntimeNode flow, const TNarrowLambda& handler);
    TRuntimeNode WideFilter(TRuntimeNode flow, TRuntimeNode limit, const TNarrowLambda& handler);
    TRuntimeNode WideTakeWhile(TRuntimeNode flow, const TNarrowLambda& handler);
    TRuntimeNode WideSkipWhile(TRuntimeNode flow, const TNarrowLambda& handler);
    TRuntimeNode WideTakeWhileInclusive(TRuntimeNode flow, const TNarrowLambda& handler);
    TRuntimeNode WideSkipWhileInclusive(TRuntimeNode flow, const TNarrowLambda& handler);

    TRuntimeNode WideCombiner(TRuntimeNode flow, i64 memLimit, const TWideLambda& keyExtractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish);
    TRuntimeNode WideLastCombinerCommon(const TStringBuf& funcName, TRuntimeNode flow, const TWideLambda& keyExtractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish);
    TRuntimeNode WideLastCombinerCommonWithSpilling(const TStringBuf& funcName, TRuntimeNode flow, const TWideLambda& keyExtractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish, const TWideLambda& load);
    TRuntimeNode WideLastCombiner(TRuntimeNode flow, const TWideLambda& keyExtractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish);
    TRuntimeNode WideLastCombinerWithSpilling(TRuntimeNode flow, const TWideLambda& keyExtractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish, const TWideLambda& load);
    TRuntimeNode WideCondense1(TRuntimeNode stream, const TWideLambda& init, const TWideSwitchLambda& switcher, const TBinaryWideLambda& handler, bool useCtx = false);

    TRuntimeNode WideTop(TRuntimeNode flow, TRuntimeNode count, const std::vector<std::pair<ui32, TRuntimeNode>>& keys);
    TRuntimeNode WideTopSort(TRuntimeNode flow, TRuntimeNode count, const std::vector<std::pair<ui32, TRuntimeNode>>& keys);
    TRuntimeNode WideSort(TRuntimeNode flow, const std::vector<std::pair<ui32, TRuntimeNode>>& keys);

    TRuntimeNode Length(TRuntimeNode listOrDict);
    TRuntimeNode Iterator(TRuntimeNode list, const TArrayRef<const TRuntimeNode>& dependentNodes);
    TRuntimeNode EmptyIterator(TType* streamType);
    TRuntimeNode Collect(TRuntimeNode listOrStream);
    TRuntimeNode LazyList(TRuntimeNode list);
    TRuntimeNode ListFromRange(TRuntimeNode start, TRuntimeNode end, TRuntimeNode step);
    TRuntimeNode ForwardList(TRuntimeNode stream);
    TRuntimeNode Switch(TRuntimeNode stream,
        const TArrayRef<const TSwitchInput>& handlerInputs,
        std::function<TRuntimeNode(ui32 index, TRuntimeNode item)> handler,
        ui64 memoryLimitBytes, TType* returnType);
    TRuntimeNode HasItems(TRuntimeNode listOrDict);
    TRuntimeNode Reverse(TRuntimeNode list);
    TRuntimeNode Skip(TRuntimeNode list, TRuntimeNode count);
    TRuntimeNode Take(TRuntimeNode list, TRuntimeNode count);
    TRuntimeNode Replicate(TRuntimeNode item, TRuntimeNode count, const std::string_view& file, ui32 row, ui32 column);
    TRuntimeNode Sort(TRuntimeNode list, TRuntimeNode ascending, const TUnaryLambda& keyExtractor);
    TRuntimeNode Top(TRuntimeNode list, TRuntimeNode count, TRuntimeNode ascending, const TUnaryLambda& keyExtractor);
    TRuntimeNode TopSort(TRuntimeNode list, TRuntimeNode count, TRuntimeNode ascending, const TUnaryLambda& keyExtractor);
    TRuntimeNode KeepTop(TRuntimeNode count, TRuntimeNode list, TRuntimeNode item, TRuntimeNode ascending, const TUnaryLambda& keyExtractor);

    TRuntimeNode ListIf(TRuntimeNode predicate, TRuntimeNode item);

    TRuntimeNode AsList(TRuntimeNode item);
    TRuntimeNode AsList(const TArrayRef<const TRuntimeNode>& items);
    TRuntimeNode MapJoinCore(TRuntimeNode flow, TRuntimeNode dict, EJoinKind joinKind,
        const TArrayRef<const ui32>& leftKeyColumns, const TArrayRef<const ui32>& leftRenames,
        const TArrayRef<const ui32>& rightRenames, TType* returnType);
    TRuntimeNode CommonJoinCore(TRuntimeNode list, EJoinKind joinKind,
        const TArrayRef<const ui32>& leftColumns, const TArrayRef<const ui32>& rightColumns,
        const TArrayRef<const ui32>& requiredColumns, const TArrayRef<const ui32>& keyColumns,
        ui64 memLimit, std::optional<ui32> sortedTableOrder,
        EAnyJoinSettings anyJoinSettings, const ui32 tableIndexField,
        TType* returnType);
    TRuntimeNode GraceJoinCommon(const TStringBuf& funcName, TRuntimeNode flowLeft, TRuntimeNode flowRight, EJoinKind joinKind,
        const TArrayRef<const ui32>& leftKeyColumns, const TArrayRef<const ui32>& rightKeyColumns,
        const TArrayRef<const ui32>& leftRenames, const TArrayRef<const ui32>& rightRenames, TType* returnType, EAnyJoinSettings anyJoinSettings = EAnyJoinSettings::None);
    TRuntimeNode GraceJoin(TRuntimeNode flowLeft, TRuntimeNode flowRight, EJoinKind joinKind,
        const TArrayRef<const ui32>& leftKeyColumns, const TArrayRef<const ui32>& rightKeyColumns,
        const TArrayRef<const ui32>& leftRenames, const TArrayRef<const ui32>& rightRenames, TType* returnType, EAnyJoinSettings anyJoinSettings = EAnyJoinSettings::None);
    TRuntimeNode GraceSelfJoin(TRuntimeNode flowLeft,  EJoinKind joinKind, const TArrayRef<const ui32>& leftKeyColumns, const TArrayRef<const ui32>& rightKeyColumns,
        const TArrayRef<const ui32>& leftRenames, const TArrayRef<const ui32>& rightRenames, TType* returnType, EAnyJoinSettings anyJoinSettings = EAnyJoinSettings::None);
    TRuntimeNode GraceJoinWithSpilling(TRuntimeNode flowLeft, TRuntimeNode flowRight, EJoinKind joinKind,
        const TArrayRef<const ui32>& leftKeyColumns, const TArrayRef<const ui32>& rightKeyColumns,
        const TArrayRef<const ui32>& leftRenames, const TArrayRef<const ui32>& rightRenames, TType* returnType, EAnyJoinSettings anyJoinSettings = EAnyJoinSettings::None);
    TRuntimeNode GraceSelfJoinWithSpilling(TRuntimeNode flowLeft,  EJoinKind joinKind, const TArrayRef<const ui32>& leftKeyColumns, const TArrayRef<const ui32>& rightKeyColumns,
        const TArrayRef<const ui32>& leftRenames, const TArrayRef<const ui32>& rightRenames, TType* returnType, EAnyJoinSettings anyJoinSettings = EAnyJoinSettings::None);
    TRuntimeNode CombineCore(TRuntimeNode stream,
        const TUnaryLambda& keyExtractor,
        const TBinaryLambda& init,
        const TTernaryLambda& update,
        const TBinaryLambda& finish,
        ui64 memLimit);
    TRuntimeNode CombineCoreWithSpilling(TRuntimeNode stream,
        const TUnaryLambda& keyExtractor,
        const TBinaryLambda& init,
        const TTernaryLambda& update,
        const TBinaryLambda& finish,
        const TUnaryLambda& load,
        ui64 memLimit);
    TRuntimeNode GroupingCore(TRuntimeNode stream,
        const TBinaryLambda& groupSwitch,
        const TUnaryLambda& keyExtractor,
        const TUnaryLambda& handler = {});
    TRuntimeNode HoppingCore(TRuntimeNode list,
        const TUnaryLambda& timeExtractor,
        const TUnaryLambda& init,
        const TBinaryLambda& update,
        const TUnaryLambda& save,
        const TUnaryLambda& load,
        const TBinaryLambda& merge,
        const TBinaryLambda& finish,
        TRuntimeNode hop, TRuntimeNode interval, TRuntimeNode delay);
    TRuntimeNode MultiHoppingCore(TRuntimeNode list,
        const TUnaryLambda& keyExtractor,
        const TUnaryLambda& timeExtractor,
        const TUnaryLambda& init,
        const TBinaryLambda& update,
        const TUnaryLambda& save,
        const TUnaryLambda& load,
        const TBinaryLambda& merge,
        const TTernaryLambda& finish,
        TRuntimeNode hop, TRuntimeNode interval, TRuntimeNode delay,
        TRuntimeNode dataWatermarks, TRuntimeNode watermarksMode);

    TRuntimeNode Chopper(TRuntimeNode flow, const TUnaryLambda& keyExtractor, const TBinaryLambda& groupSwitch, const TBinaryLambda& groupHandler);

    TRuntimeNode WideChopper(TRuntimeNode flow, const TWideLambda& keyExtractor, const TWideSwitchLambda& groupSwitch,
        const std::function<TRuntimeNode (TRuntimeNode::TList, TRuntimeNode)>& groupHandler
    );

    //-- dict functions
    TRuntimeNode Contains(TRuntimeNode dict, TRuntimeNode key);
    TRuntimeNode Lookup(TRuntimeNode dict, TRuntimeNode key);
    // all - keep all payloads in list or keep first payload only
    TRuntimeNode ToSortedDict(TRuntimeNode list, bool all, const TUnaryLambda& keySelector,
        const TUnaryLambda& payloadSelector, bool isCompact = false, ui64 itemsCountHint = 0);
    TRuntimeNode ToHashedDict(TRuntimeNode list, bool all, const TUnaryLambda& keySelector,
        const TUnaryLambda& payloadSelector, bool isCompact = false, ui64 itemsCountHint = 0);
    TRuntimeNode SqueezeToSortedDict(TRuntimeNode stream, bool all, const TUnaryLambda& keySelector,
        const TUnaryLambda& payloadSelector, bool isCompact = false, ui64 itemsCountHint = 0);
    TRuntimeNode SqueezeToHashedDict(TRuntimeNode stream, bool all, const TUnaryLambda& keySelector,
        const TUnaryLambda& payloadSelector, bool isCompact = false, ui64 itemsCountHint = 0);
    TRuntimeNode NarrowSqueezeToSortedDict(TRuntimeNode stream, bool all, const TNarrowLambda& keySelector,
        const TNarrowLambda& payloadSelector, bool isCompact = false, ui64 itemsCountHint = 0);
    TRuntimeNode NarrowSqueezeToHashedDict(TRuntimeNode stream, bool all, const TNarrowLambda& keySelector,
        const TNarrowLambda& payloadSelector, bool isCompact = false, ui64 itemsCountHint = 0);
    TRuntimeNode SqueezeToList(TRuntimeNode flow, TRuntimeNode limit);

    // return list of 2-item tuples with key and payload
    TRuntimeNode DictItems(TRuntimeNode dict);
    TRuntimeNode DictKeys(TRuntimeNode dict);
    TRuntimeNode DictPayloads(TRuntimeNode dict);

    TRuntimeNode ToIndexDict(TRuntimeNode list); // make a dict ui64->item
    // build a list from tuple of payloads or just a list if semijoin kind is used
    TRuntimeNode JoinDict(TRuntimeNode dict1, bool isMulti1, TRuntimeNode dict2, bool isMulti2, EJoinKind joinKind);

    //-- branching functions
    TRuntimeNode Coalesce(TRuntimeNode data, TRuntimeNode defaultData);
    TRuntimeNode Unwrap(TRuntimeNode optional, TRuntimeNode message, const std::string_view& file, ui32 row, ui32 column);
    TRuntimeNode Exists(TRuntimeNode data);
    TRuntimeNode If(const TArrayRef<const TRuntimeNode>& args);
    TRuntimeNode If(TRuntimeNode condition, TRuntimeNode thenBranch, TRuntimeNode elseBranch);
    TRuntimeNode IfPresent(TRuntimeNode::TList optional, const TNarrowLambda& thenBranch, TRuntimeNode elseBranch);
    TRuntimeNode ToList(TRuntimeNode optional);
    TRuntimeNode Iterable(TZeroLambda lambda);
    TRuntimeNode ToOptional(TRuntimeNode list);
    TRuntimeNode Head(TRuntimeNode list);
    TRuntimeNode Last(TRuntimeNode list);
    TRuntimeNode Nanvl(TRuntimeNode data, TRuntimeNode dataIfNaN);
    TRuntimeNode Ensure(TRuntimeNode value, TRuntimeNode predicate, TRuntimeNode message, const std::string_view& file, ui32 row, ui32 column);

    TRuntimeNode SourceOf(TType* returnType);
    TRuntimeNode Source();

    TRuntimeNode MakeHeap(TRuntimeNode list, const TBinaryLambda& comparator);
    TRuntimeNode PushHeap(TRuntimeNode list, const TBinaryLambda& comparator);
    TRuntimeNode PopHeap(TRuntimeNode list, const TBinaryLambda& comparator);
    TRuntimeNode SortHeap(TRuntimeNode list, const TBinaryLambda& comparator);
    TRuntimeNode StableSort(TRuntimeNode list, const TBinaryLambda& comparator);

    TRuntimeNode NthElement(TRuntimeNode list, TRuntimeNode n, const TBinaryLambda& comparator);
    TRuntimeNode PartialSort(TRuntimeNode list, TRuntimeNode n, const TBinaryLambda& comparator);

    //-- arithmetic functions
    TRuntimeNode Increment(TRuntimeNode data);
    TRuntimeNode Decrement(TRuntimeNode data);
    TRuntimeNode Abs(TRuntimeNode data);
    TRuntimeNode Plus(TRuntimeNode data);
    TRuntimeNode Minus(TRuntimeNode data);

    TRuntimeNode Add(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode Sub(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode Mul(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode Div(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode Mod(TRuntimeNode data1, TRuntimeNode data2);

    TRuntimeNode Min(const TArrayRef<const TRuntimeNode>& args);
    TRuntimeNode Max(const TArrayRef<const TRuntimeNode>& args);

    TRuntimeNode Min(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode Max(TRuntimeNode data1, TRuntimeNode data2);

    TRuntimeNode DecimalDiv(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode DecimalMod(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode DecimalMul(TRuntimeNode data1, TRuntimeNode data2);

    //-- bit logical functions
    TRuntimeNode BitNot(TRuntimeNode data);
    TRuntimeNode CountBits(TRuntimeNode data);
    TRuntimeNode BitAnd(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode BitOr(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode BitXor(TRuntimeNode data1, TRuntimeNode data2);

    //-- bit shifts
    TRuntimeNode ShiftLeft(TRuntimeNode arg, TRuntimeNode bits);
    TRuntimeNode RotLeft(TRuntimeNode arg, TRuntimeNode bits);
    TRuntimeNode ShiftRight(TRuntimeNode arg, TRuntimeNode bits);
    TRuntimeNode RotRight(TRuntimeNode arg, TRuntimeNode bits);

    // -- sql comparison functions - empty optional looks like an unknown value
    TRuntimeNode Equals(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode NotEquals(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode Less(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode LessOrEqual(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode Greater(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode GreaterOrEqual(TRuntimeNode data1, TRuntimeNode data2);

    // -- aggr comparison functions
    TRuntimeNode AggrEquals(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode AggrNotEquals(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode AggrLess(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode AggrLessOrEqual(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode AggrGreater(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode AggrGreaterOrEqual(TRuntimeNode data1, TRuntimeNode data2);

    //-- logical functions
    TRuntimeNode Not(TRuntimeNode data);

    TRuntimeNode And(const TArrayRef<const TRuntimeNode>& args);
    TRuntimeNode Or(const TArrayRef<const TRuntimeNode>& args);
    TRuntimeNode Xor(const TArrayRef<const TRuntimeNode>& args);

    //-- tuple functions
    TRuntimeNode Nth(TRuntimeNode tuple, ui32 index);
    TRuntimeNode Element(TRuntimeNode tuple, ui32 index);

    //-- variant functions
    TRuntimeNode Guess(TRuntimeNode variant, ui32 tupleIndex);
    TRuntimeNode Guess(TRuntimeNode variant, const std::string_view& memberName);
    TRuntimeNode VisitAll(TRuntimeNode variant, std::function<TRuntimeNode(ui32, TRuntimeNode)> handler);
    TRuntimeNode Way(TRuntimeNode variant);
    TRuntimeNode VariantItem(TRuntimeNode variant);

    //-- random functions
    // expects ui64 seed, returns resource
    TRuntimeNode NewMTRand(TRuntimeNode seed);
    // returns tuple of (ui64 random value, resource)
    TRuntimeNode NextMTRand(TRuntimeNode rand);

    //-- aggregation functions
    TRuntimeNode AggrCountInit(TRuntimeNode value);
    TRuntimeNode AggrCountUpdate(TRuntimeNode value, TRuntimeNode state);
    TRuntimeNode AggrMin(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode AggrMax(TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode AggrAdd(TRuntimeNode data1, TRuntimeNode data2);

    //-- queue functions
    TRuntimeNode QueueCreate(TRuntimeNode initCapacity, TRuntimeNode initCreate, const TArrayRef<const TRuntimeNode>& dependentNodes, TType* returnType);
    TRuntimeNode QueuePush(TRuntimeNode resource, TRuntimeNode value);
    TRuntimeNode QueuePop(TRuntimeNode resource);
    TRuntimeNode QueuePeek(TRuntimeNode resource, TRuntimeNode index, const TArrayRef<const TRuntimeNode>& dependentNodes, TType* returnType);
    TRuntimeNode QueueRange(TRuntimeNode resource, TRuntimeNode begin, TRuntimeNode end, const TArrayRef<const TRuntimeNode>& dependentNodes, TType* returnType);

    TRuntimeNode PreserveStream(TRuntimeNode stream, TRuntimeNode preserve, TRuntimeNode outpace);

    TRuntimeNode Seq(const TArrayRef<const TRuntimeNode>& items, TType* returnType);

    TRuntimeNode FromYsonSimpleType(TRuntimeNode input, NUdf::TDataTypeId schemeType);
    TRuntimeNode TryWeakMemberFromDict(TRuntimeNode other, TRuntimeNode rest, NUdf::TDataTypeId schemeType, const std::string_view& memberName);

    TRuntimeNode TimezoneId(TRuntimeNode name);
    TRuntimeNode TimezoneName(TRuntimeNode id);
    TRuntimeNode AddTimezone(TRuntimeNode utc, TRuntimeNode id);
    TRuntimeNode RemoveTimezone(TRuntimeNode local);

    TRuntimeNode AllOf(TRuntimeNode list, const TUnaryLambda& predicate);
    TRuntimeNode NotAllOf(TRuntimeNode list, const TUnaryLambda& predicate);

    TRuntimeNode Cast(TRuntimeNode data, TType* type);
    TRuntimeNode Default(TType* type);

    TRuntimeNode RangeCreate(TRuntimeNode list);
    TRuntimeNode RangeUnion(const TArrayRef<const TRuntimeNode>& lists);
    TRuntimeNode RangeIntersect(const TArrayRef<const TRuntimeNode>& lists);
    TRuntimeNode RangeMultiply(const TArrayRef<const TRuntimeNode>& args);
    TRuntimeNode RangeFinalize(TRuntimeNode list);

    TRuntimeNode Round(const std::string_view& callableName, TRuntimeNode source, TType* targetType);

    TRuntimeNode NextValue(TRuntimeNode value);

    TRuntimeNode Nop(TRuntimeNode value, TType* returnType);

    typedef TRuntimeNode (TProgramBuilder::*UnaryFunctionMethod)(TRuntimeNode);
    typedef TRuntimeNode (TProgramBuilder::*BinaryFunctionMethod)(TRuntimeNode, TRuntimeNode);
    typedef TRuntimeNode (TProgramBuilder::*TernaryFunctionMethod)(TRuntimeNode, TRuntimeNode, TRuntimeNode);
    typedef TRuntimeNode (TProgramBuilder::*ArrayFunctionMethod)(const TArrayRef<const TRuntimeNode>&);
    typedef TRuntimeNode (TProgramBuilder::*ProcessFunctionMethod)(TRuntimeNode, const TUnaryLambda&);
    typedef TRuntimeNode (TProgramBuilder::*NarrowFunctionMethod)(TRuntimeNode, const TNarrowLambda&);

    TRuntimeNode PgConst(TPgType* pgType, const std::string_view& value, TRuntimeNode typeMod = {});
    TRuntimeNode PgResolvedCall(bool useContext, const std::string_view& name, ui32 id,
        const TArrayRef<const TRuntimeNode>& args, TType* returnType, bool rangeFunction);
    TRuntimeNode PgCast(TRuntimeNode input, TType* returnType, TRuntimeNode typeMod = {});
    TRuntimeNode FromPg(TRuntimeNode input, TType* returnType);
    TRuntimeNode ToPg(TRuntimeNode input, TType* returnType);
    TRuntimeNode PgClone(TRuntimeNode input, const TArrayRef<const TRuntimeNode>& dependentNodes);
    TRuntimeNode WithContext(TRuntimeNode input, const std::string_view& contextType);
    TRuntimeNode PgInternal0(TType* returnType);
    TRuntimeNode PgArray(const TArrayRef<const TRuntimeNode>& args, TType* returnType);
    TRuntimeNode PgTableContent(
        const std::string_view& cluster,
        const std::string_view& table,
        TType* returnType);
    TRuntimeNode PgToRecord(TRuntimeNode input, const TArrayRef<std::pair<std::string_view, std::string_view>>& members);

    TRuntimeNode ScalarApply(const TArrayRef<const TRuntimeNode>& args, const TArrayLambda& handler);

    TRuntimeNode MatchRecognizeCore(
        TRuntimeNode inputStream,
        const TUnaryLambda& getPartitionKeySelectorNode,
        const TArrayRef<TStringBuf>& partitionColumns,
        const TArrayRef<std::pair<TStringBuf, TBinaryLambda>>& getMeasures,
        const NYql::NMatchRecognize::TRowPattern& pattern,
        const TArrayRef<std::pair<TStringBuf, TTernaryLambda>>& getDefines,
        bool streamingMode
    );

    TRuntimeNode TimeOrderRecover(
        TRuntimeNode inputStream,
        const TUnaryLambda& getTimeExtractor,
        TRuntimeNode delay,
        TRuntimeNode ahead,
        TRuntimeNode rowLimit
    );

protected:
    TRuntimeNode Invoke(const std::string_view& funcName, TType* resultType, const TArrayRef<const TRuntimeNode>& args);
    TRuntimeNode IfPresent(TRuntimeNode optional, const TUnaryLambda& thenBranch, TRuntimeNode elseBranch);

    void ThrowIfListOfVoid(TType* type);

    template <typename ResultType>
    TRuntimeNode BuildContainerProperty(const std::string_view& callableName, TRuntimeNode listOrDict);
    TRuntimeNode Filter(TRuntimeNode list, const TUnaryLambda& handler, TType* resultType);
    template <bool Ordered>
    TRuntimeNode BuildExtract(TRuntimeNode list, const std::string_view& name);

    TRuntimeNode BuildMap(const std::string_view& callableName, TRuntimeNode list, const TUnaryLambda& handler);
    TRuntimeNode BuildFlatMap(const std::string_view& callableName, TRuntimeNode list, const TUnaryLambda& handler);
    TRuntimeNode BuildFilter(const std::string_view& callableName, TRuntimeNode list, const TUnaryLambda& handler, TType* resultType = nullptr);
    TRuntimeNode BuildFilter(const std::string_view& callableName, TRuntimeNode list, TRuntimeNode limit, const TUnaryLambda& handler, TType* resultType = nullptr);
    TRuntimeNode BuildSort(const std::string_view& callableName, TRuntimeNode list, TRuntimeNode ascending, const TUnaryLambda& keyExtractor);
    TRuntimeNode BuildListSort(const std::string_view& callableName, TRuntimeNode list, TRuntimeNode ascending, const TUnaryLambda& keyExtractor);
    TRuntimeNode BuildNth(const std::string_view& callableName, TRuntimeNode list, TRuntimeNode n, TRuntimeNode ascending, const TUnaryLambda& keyExtractor);
    TRuntimeNode BuildListNth(const std::string_view& callableName, TRuntimeNode list, TRuntimeNode n, TRuntimeNode ascending, const TUnaryLambda& keyExtractor);
    TRuntimeNode BuildTake(const std::string_view& callableName, TRuntimeNode list, TRuntimeNode count);
    TRuntimeNode BuildHeap(const std::string_view& callableName, TRuntimeNode list, const TBinaryLambda& comparator);
    TRuntimeNode BuildNth(const std::string_view& callableName, TRuntimeNode list, TRuntimeNode n, const TBinaryLambda& comparator);
    TRuntimeNode BuildLogical(const std::string_view& callableName, const TArrayRef<const TRuntimeNode>& args);
    TRuntimeNode BuildBinaryLogical(const std::string_view& callableName, TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode BuildMinMax(const std::string_view& callableName, const TRuntimeNode* data, size_t size);
    TRuntimeNode BuildWideSkipTakeBlocks(const std::string_view& callableName, TRuntimeNode flow, TRuntimeNode count);
    TRuntimeNode BuildBlockLogical(const std::string_view& callableName, TRuntimeNode first, TRuntimeNode second);
    TRuntimeNode BuildExtend(const std::string_view& callableName, const TArrayRef<const TRuntimeNode>& lists);
private:
    TRuntimeNode BuildWideFilter(const std::string_view& callableName, TRuntimeNode flow, const TNarrowLambda& handler);

    TRuntimeNode DictItems(TRuntimeNode dict, EDictItems mode);
    TRuntimeNode If(TRuntimeNode condition, TRuntimeNode thenBranch, TRuntimeNode elseBranch, TType* resultType);

    TRuntimeNode ToDict(TRuntimeNode list, bool multi, const TUnaryLambda& keySelector,
        const TUnaryLambda& payloadSelector, std::string_view callableName, bool isCompact, ui64 itemsCountHint);
    TRuntimeNode SqueezeToDict(TRuntimeNode stream, bool multi, const TUnaryLambda& keySelector,
        const TUnaryLambda& payloadSelector, std::string_view callableName, bool isCompact, ui64 itemsCountHint);
    TRuntimeNode NarrowSqueezeToDict(TRuntimeNode stream, bool multi, const TNarrowLambda& keySelector,
        const TNarrowLambda& payloadSelector, std::string_view callableName, bool isCompact, ui64 itemsCountHint);

    TRuntimeNode UnaryDataFunction(TRuntimeNode data, const std::string_view& callableName, ui32 flags);

    template<bool IsFilter, bool OnStruct>
    TRuntimeNode BuildFilterNulls(TRuntimeNode list);
    template<bool IsFilter, bool OnStruct>
    TRuntimeNode BuildFilterNulls(TRuntimeNode list, const TArrayRef<std::conditional_t<OnStruct, const std::string_view, const ui32>>& members);
    template<bool OnStruct>
    TRuntimeNode BuildFilterNulls(TRuntimeNode list, const TArrayRef<std::conditional_t<OnStruct, const std::string_view, const ui32>>& members,
        const std::conditional_t<OnStruct, std::vector<std::pair<std::string_view, TType*>>, std::vector<TType*>>& filteredItems);

    TRuntimeNode BuildWideTopOrSort(const std::string_view& callableName, TRuntimeNode flow, TMaybe<TRuntimeNode> count, const std::vector<std::pair<ui32, TRuntimeNode>>& keys);

    TRuntimeNode InvokeBinary(const std::string_view& callableName, TType* type, TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode AggrCompare(const std::string_view& callableName, TRuntimeNode data1, TRuntimeNode data2);
    TRuntimeNode DataCompare(const std::string_view& callableName, TRuntimeNode data1, TRuntimeNode data2);

    TRuntimeNode BuildRangeLogical(const std::string_view& callableName, const TArrayRef<const TRuntimeNode>& lists);

    template<bool Default>
    TRuntimeNode DefaultResult(TRuntimeNode data);

    template<BinaryFunctionMethod Compare>
    TRuntimeNode BuildOptionalCompare(TRuntimeNode data1, TRuntimeNode data2);

    template<BinaryFunctionMethod Compare, bool OnEqual>
    TRuntimeNode BuildOptionalCompare(TRuntimeNode data1, TRuntimeNode data2);

    template<BinaryFunctionMethod Compare, bool OnEqual, bool Asc>
    TRuntimeNode BuildOptionalCompare(TRuntimeNode data1, TRuntimeNode data2);

    template<BinaryFunctionMethod Equal, BinaryFunctionMethod Compare, BinaryFunctionMethod FinalCompare>
    TRuntimeNode BuildByNthCompare(ui32 count, TRuntimeNode data1, TRuntimeNode data2, ui32 index = 0U);

    template<BinaryFunctionMethod Compare, bool Fallback>
    TRuntimeNode BuildVariantCompare(TRuntimeNode data1, TRuntimeNode data2);

    template<BinaryFunctionMethod Compare>
    TRuntimeNode BuildVariantCompare(TRuntimeNode data1, TRuntimeNode data2);

    template<BinaryFunctionMethod Equal, BinaryFunctionMethod Compare, BinaryFunctionMethod FinalCompare>
    TRuntimeNode BuildMembersCompare(const TStructType* type, TRuntimeNode data1, TRuntimeNode data2, ui32 index = 0U);

    template<TProgramBuilder::BinaryFunctionMethod Compare, TProgramBuilder::ArrayFunctionMethod Join, bool OnEqual>
    TRuntimeNode BuildStructCompare(TRuntimeNode data1, TRuntimeNode data2);

    template<BinaryFunctionMethod Equal, BinaryFunctionMethod Compare, BinaryFunctionMethod FinalCompare, bool OnEqual>
    TRuntimeNode BuildStructCompare(TRuntimeNode data1, TRuntimeNode data2);

    template<TProgramBuilder::BinaryFunctionMethod Compare, TProgramBuilder::ArrayFunctionMethod Join, bool OnEqual>
    TRuntimeNode BuildTupleCompare(TRuntimeNode data1, TRuntimeNode data2);

    template<BinaryFunctionMethod Equal, BinaryFunctionMethod Compare, BinaryFunctionMethod FinalCompare, bool OnEqual>
    TRuntimeNode BuildTupleCompare(TRuntimeNode data1, TRuntimeNode data2);

    template<bool Equal>
    TRuntimeNode BuildAggrCompare(const std::string_view& callableName, TRuntimeNode data1, TRuntimeNode data2);

    template <bool Equal>
    TRuntimeNode BuildSqlCompare(const std::string_view& callableName, TRuntimeNode data1, TRuntimeNode data2);

    template <bool Asc, bool Equal>
    TRuntimeNode BuildAggrCompare(const std::string_view& callableName, TRuntimeNode data1, TRuntimeNode data2);

    template <bool Asc, bool Equal>
    TRuntimeNode BuildSqlCompare(const std::string_view& callableName, TRuntimeNode data1, TRuntimeNode data2);

    TType* ChooseCommonType(TType* type1, TType* type2);
    TType* BuildArithmeticCommonType(TType* type1, TType* type2);

    bool IsNull(TRuntimeNode arg);
protected:
    const IFunctionRegistry& FunctionRegistry;
    const bool VoidWithEffects;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
};

bool CanExportType(TType* type, const TTypeEnvironment& env);
void EnsureDataOrOptionalOfData(TRuntimeNode node);

}
}
