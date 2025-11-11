#include "yql_yt_io_discovery_partitions.h"
#include "yql_yt_key.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/sql_types/simple_types.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/yql/providers/yt/common/yql_names.h>

#include <util/string/ascii.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYql {

using namespace NNodes;

namespace {
enum class ERegexKind {
    UNINIT,
    LITERAL,
    NORMAL,
};

struct TRegex {
    TString Re;
    TString Name;
    ERegexKind Kind = ERegexKind::UNINIT;
    NUdf::EDataSlot Slot = NUdf::EDataSlot::String;
};

struct TPatternPathComponent {
    TVector<TRegex> Regexps;
};

bool IsValidColumnName(const TString& name) {
    if (name.empty() || !AllOf(name.begin(), name.end(), [](auto ch) { return IsAsciiAlnum(ch) || ch == '_'; })) {
        return false;
    }
    return IsAsciiAlpha(name[0]);
}

TString TrimLeadingSlashes(const TString& path) {
    size_t startIdx = 0;
    while (startIdx < path.size() && path[startIdx] == '/') {
        ++startIdx;
    }
    return path.substr(startIdx);
}

bool SplitPatternBySlashAndDedupStars(const TString& pattern, TVector<TString>& result) {
    result.clear();
    result.emplace_back();
    bool insideBraces = false;
    for (auto it = pattern.begin(); it != pattern.end(); ++it) {
        if (!insideBraces) {
            if (*it == '/') {
                if (!result.back().empty()) {
                    result.emplace_back();
                }
                continue;
            }

            // dedup consequent *
            while (*it == '*' && it + 1 != pattern.end() && *(it + 1) == '*') {
                ++it;
            }

            if (*it == '$' && it + 1 != pattern.end() && *(it + 1) == '{') {
                result.back().push_back(*it++);
                insideBraces = true;
            }
        } else if (*it == '}') {
            insideBraces = false;
        }
        result.back().push_back(*it);
    }
    return !insideBraces;
}

TString MakeRegexForType(NUdf::EDataSlot slot) {
    auto maybeRe = NKikimr::NMiniKQL::RegexMatchingValidStringValues(slot, NKikimr::NMiniKQL::ERegexFlavor::RE2);
    return maybeRe.GetOrElse(".*?");
}

bool ParseBraces(const TString& content, TPositionHandle pos, size_t idx, TRegex& result,
    TMap<TString, NUdf::EDataSlot>& extraColumns, TExprContext& ctx)
{
    TString name = content;
    NUdf::EDataSlot type = NUdf::EDataSlot::String;

    TString errStr = TStringBuilder() << "Failed to parse pattern component #" << idx << ": ";

    if (auto colonIdx = content.find_last_of(':'); colonIdx != TString::npos) {
        name = content.substr(0, colonIdx);
        TString typeStr = content.substr(colonIdx + 1);
        if (typeStr.empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << errStr << "got empty type string"));
            return false;
        }
        auto yqlTypeName = LookupSimpleTypeBySqlAlias(typeStr, true);
        if (!yqlTypeName) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << errStr << "invalid or unsupported type '" << typeStr << "'"));
            return false;
        }

        auto maybeSlot = NUdf::FindDataSlot(*yqlTypeName);
        if (!maybeSlot) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << errStr << "unsupported type '" << typeStr << "'"));
            return false;
        }

        type = *maybeSlot;
    }

    if (name.empty()) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << errStr << "empty column name is not allowed"));
        return false;
    }

    if (!IsValidColumnName(name)) {
        ctx.AddError(TIssue(ctx.GetPosition(pos),
            TStringBuilder() << errStr << "invalid column name '" << name << "' - name should only contain alphanumeric symbols and start with letter"));
        return false;
    }

    if (name == MrPartitionListTableMember) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << errStr << name << " is a reserved name for table path and can not be used"));
        return false;
    }

    if (!extraColumns.insert({name, type}).second) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << errStr << "duplicate column name '" << name << "'"));
        return false;
    }

    result.Kind = ERegexKind::NORMAL;
    result.Name = name;
    result.Slot = type;
    result.Re = TStringBuilder() << "(?P<" << name << ">" << MakeRegexForType(type) << ")";
    return true;
}

bool MakePattern(const TString& patternComponent, TPositionHandle pos, size_t idx,
    TPatternPathComponent& result, TMap<TString, NUdf::EDataSlot>& extraColumns,
    TExprContext& ctx)
{
    YQL_ENSURE(!patternComponent.empty());
    bool insideBraces = false;
    TString currentBraces;
    for (auto it = patternComponent.begin(); it != patternComponent.end(); ++it) {
        TMaybe<char> next;
        if (it + 1 != patternComponent.end()) {
            next = *(it + 1);
        }
        if (!insideBraces) {
            if (*it == '*') {
                if (result.Regexps.empty() || result.Regexps.back().Kind != ERegexKind::UNINIT) {
                    result.Regexps.emplace_back();
                }
                auto& newRegex = result.Regexps.back();
                newRegex.Kind = ERegexKind::NORMAL;
                newRegex.Re = ".*?";
                continue;
            }
            if (*it == '$' && next && *next == '{') {
                insideBraces = true;
                YQL_ENSURE(currentBraces.empty());
                currentBraces.clear();
                ++it;
                continue;
            }
            if (result.Regexps.empty() || result.Regexps.back().Kind != ERegexKind::LITERAL) {
                result.Regexps.emplace_back();
                result.Regexps.back().Kind = ERegexKind::LITERAL;
            }
            auto& currRegex = result.Regexps.back();
            currRegex.Re.push_back(*it);
        } else if (*it == '}') {
            if (result.Regexps.empty() || result.Regexps.back().Kind != ERegexKind::UNINIT) {
                result.Regexps.emplace_back();
            }
            auto& currRegex = result.Regexps.back();
            if (!ParseBraces(currentBraces, pos, idx, currRegex, extraColumns, ctx)) {
                return false;
            }
            insideBraces = false;
            currentBraces.clear();
        } else {
            currentBraces.push_back(*it);
        }
    }

    if (insideBraces) {
        ctx.AddError(TIssue(ctx.GetPosition(pos),
            TStringBuilder() << "Failed to parse pattern component #" << idx << ": brace is not closed"));
        return false;
    }

    return true;
}

bool ParsePattern(const TString& pattern, TPositionHandle pos, TVector<TPatternPathComponent>& components,
    TMap<TString, NUdf::EDataSlot>& extraColumns, TExprContext& ctx)
{
    TString effectivePattern = TrimLeadingSlashes(pattern);
    if (effectivePattern.empty()) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Empty pattern is invalid"));
        return false;
    }

    if (effectivePattern.EndsWith('/')) {
        effectivePattern.push_back('*');
    }

    TVector<TString> pathComponents;
    if (!SplitPatternBySlashAndDedupStars(effectivePattern, pathComponents)) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Invalid pattern '" << pattern << "'"));
        return false;
    }
    YQL_ENSURE(!pathComponents.empty() && !pathComponents.back().empty());

    for (size_t i = 0; i < pathComponents.size(); ++i) {
        components.emplace_back();
        if (!MakePattern(pathComponents[i], pos, i, components.back(), extraColumns, ctx)) {
            return false;
        }
    }
    return true;
}

TString FuseRegexps(const TVector<TRegex>& regexps) {
    TStringBuilder result;
    for (auto& r : regexps) {
        YQL_ENSURE(r.Kind != ERegexKind::UNINIT);
        YQL_ENSURE(!r.Re.empty());
        switch (r.Kind) {
        case ERegexKind::LITERAL:
            result << re2::RE2::QuoteMeta(r.Re);
            break;
        case ERegexKind::NORMAL:
            result << r.Re;
            break;
        case ERegexKind::UNINIT:
            YQL_ENSURE(false, "Unexpected state");
        }
    }
    return result;
}

}

TExprNode::TPtr ExpandMrPartitions(TYtRead read, TExprContext& ctx, TTypeAnnotationContext& types) {
    const auto& partitionsNode = read.Arg(2).Ref();
    YQL_ENSURE(partitionsNode.IsCallable({MrPartitionsName, MrPartitionsStrictName}));
    if (partitionsNode.ChildrenSize() != 2) {
        ctx.AddError(TIssue(ctx.GetPosition(partitionsNode.Pos()),
            TStringBuilder() << partitionsNode.Content() << " should have exactly 2 args, but got " << partitionsNode.ChildrenSize()));
        return {};
    }

    if (!partitionsNode.Head().IsCallable("Key")) {
        ctx.AddError(TIssue(ctx.GetPosition(partitionsNode.Head().Pos()),
            TStringBuilder() << "Expecting Key"));
        return {};
    }

    TYtKey key;
    if (!key.Parse(partitionsNode.Head(), ctx)) {
        return {};
    }

    if (key.GetType() != TYtKey::EType::Table) {
        ctx.AddError(TIssue(ctx.GetPosition(partitionsNode.Head().Pos()),
            TStringBuilder() << "Expecting table in Key"));
        return {};
    }

    if (key.IsAnonymous()) {
        ctx.AddError(TIssue(ctx.GetPosition(partitionsNode.Head().Pos()),
            TStringBuilder() << "Unexpected anonymous table"));
        return {};
    }

    const TExprNode* patternNode = partitionsNode.Child(1);
    if (!patternNode->IsAtom()) {
        ctx.AddError(TIssue(ctx.GetPosition(partitionsNode.Pos()),
            TStringBuilder() << partitionsNode.Content() << " should have an atom as second argument"));
        return {};
    }

    const TPositionHandle pos = patternNode->Pos();

    TVector<TPatternPathComponent> components;
    TMap<TString, NUdf::EDataSlot> extraColumns;
    if (!ParsePattern(ToString(patternNode->Content()), pos, components, extraColumns, ctx)) {
        return {};
    }

    YQL_ENSURE(!components.empty());
    TExprNode::TPtr matchRegexpsList;
    if (components.size() > 1) {
        TExprNodeList matchRegexps;
        for (size_t i = 0; i + 1 < components.size(); ++i) {
            TString matchRe = TStringBuilder() << "^" << FuseRegexps(components[i].Regexps) << "$";
            matchRegexps.emplace_back(ctx.NewCallable(pos, "String", { ctx.NewAtom(pos, matchRe) }));
        }
        matchRegexpsList = ctx.NewCallable(pos, "AsList", std::move(matchRegexps));
    } else {
        matchRegexpsList = ctx.Builder(pos)
            .Callable("List")
                .Add(0, ExpandType(pos, *ctx.MakeType<TListExprType>(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String)), ctx))
            .Seal()
            .Build();
    }

    auto diveHandler = ctx.Builder(pos)
        .Lambda()
            .Param("nodes")
            .Param("state")
            .Param("rootAttrs")
            .Param("level")
            .List()
                .Callable(0, "Filter")
                    .Callable(0, "Map")
                        .Arg(0, "nodes")
                        .Lambda(1)
                            .Param("node")
                            .List()
                                .Callable(0, "Member")
                                    .Arg(0, "node")
                                    .Atom(1, "Path")
                                .Seal()
                                .Arg(1, "rootAttrs")
                            .Seal()
                        .Seal()
                    .Seal()
                    .Lambda(1)
                        .Param("pathTuple")
                        .Callable("IfPresent")
                            .Callable(0, "Lookup")
                                .Callable(0, "ToIndexDict")
                                    .Add(0, matchRegexpsList)
                                .Seal()
                                .Arg(1, "level")
                            .Seal()
                            .Lambda(1)
                                .Param("re")
                                .Callable("Apply")
                                    .Callable(0, "Udf")
                                        .Atom(0, "Re2.Match")
                                        .List(1)
                                            .Arg(0, "re")
                                            .Callable(1, "Null")
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                    .Callable(1, "TableName")
                                        .Callable(0, "Nth")
                                            .Arg(0, "pathTuple")
                                            .Atom(1, 0)
                                        .Seal()
                                        .Atom(1, YtProviderName)
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Callable(2, "Bool")
                                .Atom(0, "false")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Arg(1, "state")
            .Seal()
        .Seal()
        .Build();

    const TStructExprType* stateItemType = nullptr;
    {
        TVector<const TItemExprType*> items;
        for (auto& [name, slot] : extraColumns) {
            items.emplace_back(ctx.MakeType<TItemExprType>(name, ctx.MakeType<TDataExprType>(slot)));
        }
        items.emplace_back(ctx.MakeType<TItemExprType>(MrPartitionListTableMember, ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String)));
        stateItemType = ctx.MakeType<TStructExprType>(items);
    }

    const TString startDir = key.GetPath();
    TString startPrefix = startDir.StartsWith("//") ? startDir.substr(2) : startDir;
    if (startPrefix.EndsWith("/")) {
        startPrefix.pop_back();
    }

    TStringBuilder fullRegex;
    fullRegex << "^" << re2::RE2::QuoteMeta(startPrefix);
    for (const auto& component : components) {
        fullRegex << "/" << FuseRegexps(component.Regexps);
    }
    fullRegex << "$";

    auto postHandler = ctx.Builder(pos)
        .Lambda()
            .Param("nodes")
            .Param("state")
            .Param("unused_level")
            .Callable("Extend")
                .Arg(0, "state")
                .Callable(1, "FilterNullMembers")
                    .Callable(0, "Map")
                        .Callable(0, "Map")
                            .Callable(0, "Filter")
                                .Arg(0, "nodes")
                                .Lambda(1)
                                    .Param("node")
                                    .Callable("!=")
                                        .Callable(0, "Member")
                                            .Arg(0, "node")
                                            .Atom(1, "Type")
                                        .Seal()
                                        .Callable(1, "String")
                                            .Atom(0, "map_node")
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Lambda(1)
                                .Param("node")
                                .Callable("Apply")
                                    .Callable(0, "Udf")
                                        .Atom(0, "Re2.Capture")
                                        .List(1)
                                            .Callable(0, "String")
                                                .Atom(0, fullRegex)
                                            .Seal()
                                            .Callable(1, "Null")
                                            .Seal()
                                        .Seal()
                                        .Callable(2, "Void")
                                        .Seal()
                                        .Atom(3, fullRegex)
                                    .Seal()
                                    .Callable(1, "Member")
                                        .Arg(0, "node")
                                        .Atom(1, "Path")
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                        .Lambda(1)
                            .Param("stateItem")
                            .Callable("AsStruct")
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    size_t idx = 0;
                                    for (const auto& item : stateItemType->GetItems()) {
                                        auto name = item->GetName();
                                        parent
                                            .List(idx++)
                                                .Atom(0, name)
                                                .Callable(1, "SafeCast")
                                                    .Callable(0, "Member")
                                                        .Arg(0, "stateItem")
                                                        .Atom(1, name == MrPartitionListTableMember ? "_0" : name)
                                                    .Seal()
                                                    .Add(1, ExpandType(pos, *ctx.MakeType<TOptionalExprType>(item->GetItemType()), ctx))
                                                .Seal()
                                            .Seal();
                                    }
                                    return parent;
                                })
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto stateType = ExpandType(pos, *ctx.MakeType<TListExprType>(stateItemType), ctx);
    auto initState = ctx.NewCallable(pos, "List", { stateType });

    TExprNode::TPtr anyNodeDiveHandler;
    TExprNode::TPtr makeResolveDiveHandlersType;
    {
        TNodeOnNodeOwnedMap deepClones;
        auto exportsPtr = types.Modules->GetModule("/lib/yql/walk_folders.yqls");
        YQL_ENSURE(exportsPtr);
        const auto& exports = exportsPtr->Symbols();

        auto ex = exports.find("AnyNodeDiveHandler");
        YQL_ENSURE(exports.cend() != ex);
        anyNodeDiveHandler = ctx.DeepCopy(*ex->second, exportsPtr->ExprCtx(), deepClones, true, false);

        ex = exports.find("MakeResolveDiveHandlersType");
        YQL_ENSURE(exports.cend() != ex);
        makeResolveDiveHandlersType = ctx.DeepCopy(*ex->second, exportsPtr->ExprCtx(), deepClones, true, false);
    }

    const auto resolveDiveHandlerType = ctx.Builder(pos)
        .Callable("EvaluateType")
            .Callable(0, "TypeHandle")
                .Callable(0, "Apply")
                    .Add(0, makeResolveDiveHandlersType)
                    .Callable(1, "TypeOf")
                        .Add(0, initState)
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto partitionListNode = ctx.Builder(pos)
        .Callable(partitionsNode.IsCallable(MrPartitionListStrictName) ? MrPartitionListStrictName : MrPartitionListName)
            .Callable(0, "EvaluateExpr")
                .Callable(0, "FlatMap")
                    .Callable(0, "Right!")
                        .Callable(0, "Read!")
                            .Add(0, read.World().Ptr())
                            .Add(1, read.DataSource().Ptr())
                            .Callable(2, "MrWalkFolders")
                                .Atom(0, startDir)       // startDir
                                .Atom(1, "")             // attrs
                                .Callable(2, "Pickle")   // pickled initial state
                                    .Add(0, initState)
                                .Seal()
                                .Add(3, stateType)       // type of state
                                .Callable(4, "Void")     // pre handler
                                .Seal()
                                .Callable(5, "Callable") // resolve handler
                                    .Add(0, resolveDiveHandlerType)
                                    .Add(1, anyNodeDiveHandler)
                                .Seal()
                                .Callable(6, "Callable") // dive handler
                                    .Add(0, resolveDiveHandlerType)
                                    .Add(1, diveHandler)
                                .Seal()
                                .Add(7, postHandler)      // post handler
                            .Seal()
                            .Callable(3, "Void")
                            .Seal()
                            .List(4)
                            .Seal()
                        .Seal()
                    .Seal()
                    .Lambda(1)
                        .Param("walkStruct")
                        .Callable("Member")
                            .Arg(0, "walkStruct")
                            .Atom(1, "State")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    YQL_CLOG(INFO, ProviderYt) << "Expanding " << partitionsNode.Content() << "(`" << startDir << "``, '" << patternNode->Content() << "')";
    YQL_CLOG(INFO, ProviderYt) << "Full regex is @@" << fullRegex << "@@";
    return ctx.ChangeChild(read.Ref(), 2, std::move(partitionListNode));
}

} // namespace NYql
