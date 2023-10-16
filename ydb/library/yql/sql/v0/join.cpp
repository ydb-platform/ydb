#include "node.h"
#include "context.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/charset/ci_string.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/join.h>

using namespace NYql;

namespace NSQLTranslationV0 {

namespace {

}

TString NormalizeJoinOp(const TString& joinOp) {
    TVector<TString> joinOpsParts;
    Split(joinOp, " ", joinOpsParts);
    for (auto&x : joinOpsParts) {
        x.to_title();
    }

    return JoinSeq("", joinOpsParts);
}

struct TJoinDescr {
    TString Op;

    struct TFullColumn {
        ui32 Source;
        TNodePtr Column;
    };

    TVector<std::pair<TFullColumn, TFullColumn>> Keys;

    TJoinDescr(const TString& op)
        : Op(op)
    {}
};

class TJoinBase: public IJoin {
public:
    TJoinBase(TPosition pos, TVector<TSourcePtr>&& sources)
        : IJoin(pos)
        , Sources(std::move(sources))
    {
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        ISource* srcByName = nullptr;
        if (column.IsArtificial()) {
            return true;
        }
        if (const auto sourceName = *column.GetSourceName()) {
            for (auto& source: Sources) {
                if (sourceName == source->GetLabel()) {
                    srcByName = source.Get();
                    break;
                }
            }
            if (!srcByName) {
                if (column.IsAsterisk()) {
                    ctx.Error(column.GetPos()) << "Unknown correlation name for asterisk: " << sourceName;
                    return {};
                }
                // \todo add warning, either mistake in correlation name, either it's a column
                column.ResetColumn("", sourceName);
                column.SetUseSourceAsColumn();
                column.SetAsNotReliable();
            }
        }

        if (column.IsAsterisk()) {
            if (!column.GetCountHint()) {
                if (srcByName) {
                    srcByName->AllColumns();
                } else {
                    for (auto& source: Sources) {
                        source->AllColumns();
                    }
                }
            }
            return true;
        }
        if (srcByName) {
            column.ResetAsReliable();
            if (!srcByName->AddColumn(ctx, column)) {
                return {};
            }
            if (!KeysInitializing && !column.IsAsterisk()) {
                column.SetUseSource();
            }
            return true;
        } else {
            unsigned acceptedColumns = 0;
            TIntrusivePtr<TColumnNode> tryColumn = static_cast<TColumnNode*>(column.Clone().Get());
            tryColumn->SetAsNotReliable();
            TString lastAcceptedColumnSource;
            for (auto& source: Sources) {
                if (source->AddColumn(ctx, *tryColumn)) {
                    ++acceptedColumns;
                    lastAcceptedColumnSource = source->GetLabel();
                }
            }
            if (!acceptedColumns) {
                TStringBuilder sb;
                const auto& fullColumnName = FullColumnName(column);
                sb << "Column " << fullColumnName << " is not fit to any source";
                for (auto& source: Sources) {
                    if (const auto mistype = source->FindColumnMistype(fullColumnName)) {
                        sb << ". Did you mean " << mistype.GetRef() << "?";
                        break;
                    }
                }
                ctx.Error(column.GetPos()) << sb;
                return {};
            } else {
                column.SetAsNotReliable();
            }
            return false;
        }
    }

    const TColumns* GetColumns() const override {
        YQL_ENSURE(IsColumnDone, "Unable to GetColumns while it's not finished");
        return &JoinedColumns;
    }

    void GetInputTables(TTableList& tableList) const override {
        for (auto& src: Sources) {
            src->GetInputTables(tableList);
        }
        ISource::GetInputTables(tableList);
    }

    TNodePtr BuildJoinKeys(TContext& ctx, const TVector<TDeferredAtom>& names) override {
        const size_t n = JoinOps.size();
        TString what(Sources[n]->GetLabel());
        static const TSet<TString> noRightSourceJoinOps = {"LeftOnly", "LeftSemi"};
        for (size_t nn = n; nn > 0 && noRightSourceJoinOps.contains(JoinOps[nn-1]); --nn) {
            what = Sources[nn-1]->GetLabel();
        }
        const TString with(Sources[n + 1]->GetLabel());

        for (auto index = n; index <= n + 1; ++index) {
            const auto& label = Sources[index]->GetLabel();
            if (label.Contains('.')) {
                ctx.Error(Sources[index]->GetPos()) << "Invalid label: " << label << ", unable to use name with dot symbol, you should use AS <simple alias name>";
                return nullptr;
            }
        }
        if (what.empty() && with.empty()) {
            ctx.Error() << "At least one correlation name is required in join";
            return nullptr;
        }
        if (what == with) {
            ctx.Error() << "Self joins are not supporting ON syntax";
            return nullptr;
        }
        TPosition pos(ctx.Pos());
        TNodePtr expr;
        for (auto& name: names) {
            auto lhs = BuildColumn(Pos, name, what);
            auto rhs = BuildColumn(Pos, name, with);
            if (!lhs || !rhs) {
                return nullptr;
            }
            TNodePtr eq(BuildBinaryOp(pos, "==", lhs, rhs));
            if (expr) {
                expr = BuildBinaryOp(pos, "And", expr, eq);
            } else {
                expr = eq;
            }
        }
        if (expr && Sources.size() > 2) {
            ctx.Warning(ctx.Pos(), TIssuesIds::YQL_MULTIWAY_JOIN_WITH_USING) << "Multi-way JOINs should be connected with ON clause instead of USING clause";
        }
        return expr;
    }

    bool DoInit(TContext& ctx, ISource* src) override;

    void SetupJoin(const TString& opName, TNodePtr expr) override {
        JoinOps.push_back(opName);
        JoinExprs.push_back(expr);
    }

protected:
    static TString FullColumnName(const TColumnNode& column) {
        auto sourceName = *column.GetSourceName();
        auto columnName = *column.GetColumnName();
        return sourceName ? DotJoin(sourceName, columnName) : columnName;
    }

    bool InitKeysOrFilters(TContext& ctx, ui32 joinIdx, TNodePtr expr) {
        const TString joinOp(JoinOps[joinIdx]);
        const TCallNode* op = nullptr;
        if (expr) {
            const TString opName(expr->GetOpName());
            if (opName != "==") {
                ctx.Error(expr->GetPos()) << "JOIN ON expression must be a conjunction of equality predicates";
                return false;
            }

            op = dynamic_cast<const TCallNode*>(expr.Get());
            YQL_ENSURE(op, "Invalid JOIN equal operation node");
            YQL_ENSURE(op->GetArgs().size() == 2, "Invalid JOIN equal operation arguments");
        }

        ui32 idx = 0;
        THashMap<TString, ui32> sources;
        for (auto& source: Sources) {
            sources.insert({ source->GetLabel(), idx });
            ++idx;
        }
        if (sources.size() != Sources.size()) {
            ctx.Error(expr ? expr->GetPos() : Pos) << "JOIN: all correlation names must be different";
            return false;
        }

        ui32 pos = 0;
        ui32 leftArg = 0;
        ui32 rightArg = 0;
        ui32 leftSourceIdx = 0;
        ui32 rightSourceIdx = 0;
        const TString* leftSource = nullptr;
        const TString* rightSource = nullptr;
        const TString* sameColumnNamePtr = nullptr;
        TSet<TString> joinedSources;
        if (op) {
            const TString* columnNamePtr = nullptr;
            for (auto& arg : op->GetArgs()) {
                const auto sourceNamePtr = arg->GetSourceName();
                if (!sourceNamePtr) {
                    ctx.Error(expr->GetPos()) << "JOIN: equality predicate arguments must not be constant";
                    return false;
                }
                const auto sourceName = *sourceNamePtr;
                if (sourceName.empty()) {
                    ctx.Error(expr->GetPos()) << "JOIN: column requires correlation name";
                    return false;
                }
                auto it = sources.find(sourceName);
                if (it != sources.end()) {
                    joinedSources.insert(sourceName);
                    if (it->second == joinIdx + 1) {
                        rightArg = pos;
                        rightSource = sourceNamePtr;
                        rightSourceIdx = it->second;
                    }
                    else if (it->second > joinIdx + 1) {
                        ctx.Error(expr->GetPos()) << "JOIN: can not use source: " << sourceName << " in equality predicate, it is out of current join scope";
                        return false;
                    }
                    else {
                        leftArg = pos;
                        leftSource = sourceNamePtr;
                        leftSourceIdx = it->second;
                    }
                }
                else {
                    ctx.Error(expr->GetPos()) << "JOIN: unknown corellation name: " << sourceName;
                    return false;
                }
                if (!columnNamePtr) {
                    columnNamePtr = arg->GetColumnName();
                } else {
                    auto curColumnNamePtr = arg->GetColumnName();
                    if (curColumnNamePtr && *curColumnNamePtr == *columnNamePtr) {
                        sameColumnNamePtr = columnNamePtr;
                    }
                }
                ++pos;
            }
        } else {
            for (auto& x : sources) {
                if (x.second == joinIdx) {
                    leftArg = pos;
                    leftSourceIdx = x.second;
                    joinedSources.insert(x.first);
                }
                else if (x.second = joinIdx + 1) {
                    rightArg = pos;
                    rightSourceIdx = x.second;
                    joinedSources.insert(x.first);
                }
            }
        }

        if (joinedSources.size() == 1) {
            ctx.Error(expr ? expr->GetPos() : Pos) << "JOIN: different correlation names are required for joined tables";
            return false;
        }

        if (op) {
            if (joinedSources.size() != 2) {
                ctx.Error(expr->GetPos()) << "JOIN ON expression must be a conjunction of equality predicates over at most two sources";
                return false;
            }
            if (!rightSource) {
                ctx.Error(expr->GetPos()) << "JOIN ON equality predicate must have one of its arguments from the rightmost source";
                return false;
            }
        }

        KeysInitializing = true;
        if (op) {
            ctx.PushBlockShortcuts();
            for (auto& arg : op->GetArgs()) {
                if (!arg->Init(ctx, this)) {
                    return false;
                }
            }
            KeysGround = ctx.GroundBlockShortcuts(GetPos(), KeysGround);

            Y_DEBUG_ABORT_UNLESS(leftSource);
            if (sameColumnNamePtr) {
                SameKeyMap[*sameColumnNamePtr].insert(*leftSource);
                SameKeyMap[*sameColumnNamePtr].insert(*rightSource);
            }
        }

        if (joinIdx == JoinDescrs.size()) {
            JoinDescrs.push_back(TJoinDescr(joinOp));
        }

        JoinDescrs.back().Keys.push_back({ { leftSourceIdx, op ? op->GetArgs()[leftArg] : nullptr},
            { rightSourceIdx, op ? op->GetArgs()[rightArg] : nullptr } });
        KeysInitializing = false;
        return true;
    }

    bool IsJoinKeysInitializing() const override {
        return KeysInitializing;
    }

protected:
    TVector<TString> JoinOps;
    TVector<TNodePtr> JoinExprs;
    TVector<TJoinDescr> JoinDescrs;
    TNodePtr KeysGround;
    THashMap<TString, THashSet<TString>> SameKeyMap;
    TVector<TSourcePtr> Sources;
    TColumns JoinedColumns;
    bool KeysInitializing = false;
    bool IsColumnDone = false;

    void FinishColumns() override {
        if (IsColumnDone) {
            return;
        }
        YQL_ENSURE(JoinOps.size()+1 == Sources.size());
        bool excludeNextSource = false;
        decltype(JoinOps)::const_iterator opIter = JoinOps.begin();
        for (auto& src: Sources) {
            if (excludeNextSource) {
                excludeNextSource = false;
                if (opIter != JoinOps.end()) {
                    ++opIter;
                }
                continue;
            }
            if (opIter != JoinOps.end()) {
                auto joinOper = *opIter;
                ++opIter;
                if (joinOper == "LeftSemi" || joinOper == "LeftOnly") {
                    excludeNextSource = true;
                }
                if (joinOper == "RightSemi" || joinOper == "RightOnly") {
                    continue;
                }
            }
            auto columnsPtr = src->GetColumns();
            if (!columnsPtr) {
                continue;
            }
            TColumns upColumns;
            upColumns.Merge(*columnsPtr);
            upColumns.SetPrefix(src->GetLabel());
            JoinedColumns.Merge(upColumns);
        }
        IsColumnDone = true;
    }
};

bool TJoinBase::DoInit(TContext& ctx, ISource* src) {
    for (auto& source: Sources) {
        if (!source->Init(ctx, src)) {
            return false;
        }
    }

    YQL_ENSURE(JoinOps.size() == JoinExprs.size(), "Invalid join exprs number");

    const TSet<TString> allowedJoinOps = {"Inner", "Left", "Right", "Full", "LeftOnly", "RightOnly", "Exclusion", "LeftSemi", "RightSemi", "Cross"};
    for (auto& opName: JoinOps) {
        if (!allowedJoinOps.contains(opName)) {
            ctx.Error(Pos) << "Invalid join op: " << opName;
            return false;
        }
    }

    ui32 idx = 0;
    for (auto expr: JoinExprs) {
        if (expr) {
            TDeque<TNodePtr> conjQueue;
            conjQueue.push_back(expr);
            while (!conjQueue.empty()) {
                TNodePtr cur = conjQueue.front();
                conjQueue.pop_front();
                if (cur->GetOpName() == "And") {
                    auto conj = dynamic_cast<const TCallNode*>(cur.Get());
                    YQL_ENSURE(conj, "Invalid And operation node");
                    conjQueue.insert(conjQueue.begin(), conj->GetArgs().begin(), conj->GetArgs().end());
                } else if (!InitKeysOrFilters(ctx, idx, cur)) {
                    return false;
                }
            }
        } else {
            if (!InitKeysOrFilters(ctx, idx, nullptr)) {
                return false;
            }
        }
        ++idx;
    }

    TSet<ui32> joinedSources;
    for (auto& descr: JoinDescrs) {
        for (auto& key : descr.Keys) {
            joinedSources.insert(key.first.Source);
            joinedSources.insert(key.second.Source);
        }
    }
    for (idx = 0; idx < Sources.size(); ++idx) {
        if (!joinedSources.contains(idx)) {
            ctx.Error(Sources[idx]->GetPos()) << "Source: " << Sources[idx]->GetLabel() << " was not used in join expressions";
            return false;
        }
    }

    return ISource::DoInit(ctx, src);
}

class TEquiJoin: public TJoinBase {
public:
    TEquiJoin(TPosition pos, TVector<TSourcePtr>&& sources)
        : TJoinBase(pos, std::move(sources))
    {
    }

    TNodePtr Build(TContext& ctx) override {
        TMap<std::pair<TString, TString>, TNodePtr> extraColumns;
        TNodePtr joinTree;
        for (auto& descr: JoinDescrs) {
            auto leftBranch = joinTree;
            if (!leftBranch) {
                leftBranch = BuildQuotedAtom(Pos, Sources[descr.Keys[0].first.Source]->GetLabel());
            }
            auto leftKeys = GetColumnNames(ctx, extraColumns, descr.Keys, true);
            auto rightKeys = GetColumnNames(ctx, extraColumns, descr.Keys, false);
            if (!leftKeys || !rightKeys) {
                return nullptr;
            }

            joinTree = Q(Y(
                Q(descr.Op),
                leftBranch,
                BuildQuotedAtom(Pos, Sources[descr.Keys[0].second.Source]->GetLabel()),
                leftKeys,
                rightKeys,
                Q(Y())
            ));
        }

        TNodePtr equiJoin(Y("EquiJoin"));
        bool ordered = false;
        for (auto& source: Sources) {
            auto sourceNode = source->Build(ctx);
            if (!sourceNode) {
                return nullptr;
            }
            const bool useOrderedForSource = ctx.UseUnordered(*source);
            ordered = ordered || useOrderedForSource;
            if (source->IsFlattenByColumns() || source->IsFlattenColumns()) {
                auto flatten = source->IsFlattenByColumns() ?
                    source->BuildFlattenByColumns("row") :
                    source->BuildFlattenColumns("row");

                if (!flatten) {
                    return nullptr;
                }
                auto block = Y(Y("let", "flatten", sourceNode));
                block = L(block, Y("let", "flatten", Y(useOrderedForSource ? "OrderedFlatMap" : "FlatMap", "flatten", BuildLambda(Pos, Y("row"), flatten, "res"))));
                sourceNode = Y("block", Q(L(block, Y("return", "flatten"))));
            }
            TNodePtr extraMembers;
            for (auto it = extraColumns.lower_bound({ source->GetLabel(), "" }); it != extraColumns.end(); ++it) {
                if (it->first.first != source->GetLabel()) {
                    break;
                }
                if (!extraMembers) {
                    extraMembers = KeysGround ? KeysGround : Y();
                }
                extraMembers = L(
                    extraMembers,
                    Y("let", "row", Y("AddMember", "row", BuildQuotedAtom(it->second->GetPos(), it->first.second), it->second))
                );
            }
            if (extraMembers) {
                sourceNode = Y(useOrderedForSource ? "OrderedMap" : "Map", sourceNode, BuildLambda(Pos, Y("row"), extraMembers, "row"));
            }
            if (ctx.EnableSystemColumns && source->IsTableSource()) {
                sourceNode = Y("RemoveSystemMembers", sourceNode);
            }
            equiJoin = L(equiJoin, Q(Y(sourceNode, BuildQuotedAtom(source->GetPos(), source->GetLabel()))));
        }
        TNodePtr removeMembers;
        for(auto it: extraColumns) {
            if (!removeMembers) {
                removeMembers = Y();
            }
            removeMembers = L(
                removeMembers,
                Y("let", "row", Y("ForceRemoveMember", "row", BuildQuotedAtom(Pos, DotJoin(it.first.first, it.first.second))))
            );
        }
        auto options = Y();
        equiJoin = L(equiJoin, joinTree, Q(options));
        if (removeMembers) {
            equiJoin = Y(ordered ? "OrderedMap" : "Map", equiJoin, BuildLambda(Pos, Y("row"), removeMembers, "row"));
        }
        return equiJoin;
    }

    const THashMap<TString, THashSet<TString>>& GetSameKeysMap() const override {
        return SameKeyMap;
    }

    const TSet<TString> GetJoinLabels() const override {
        TSet<TString> labels;
        for (auto& source: Sources) {
            const auto label = source->GetLabel();
            YQL_ENSURE(label);
            labels.emplace(label);
        }
        return labels;
    }

    TPtr DoClone() const final {
        TVector<TSourcePtr> clonedSources;
        for (auto& cur: Sources) {
            clonedSources.push_back(cur->CloneSource());
        }
        auto newSource = MakeIntrusive<TEquiJoin>(Pos, std::move(clonedSources));
        newSource->JoinOps = JoinOps;
        newSource->JoinExprs = CloneContainer(JoinExprs);
        return newSource;
    }

private:
    TNodePtr GetColumnNames(
        TContext& ctx,
        TMap<std::pair<TString, TString>, TNodePtr>& extraColumns,
        const TVector<std::pair<TJoinDescr::TFullColumn, TJoinDescr::TFullColumn>>& keys,
        bool left
    ) {
        Y_UNUSED(ctx);
        auto res = Y();
        for (auto& it: keys) {
            auto tableName = Sources[left ? it.first.Source : it.second.Source]->GetLabel();
            TString columnName;
            auto column = left ? it.first.Column : it.second.Column;
            if (!column) {
                continue;
            }

            if (column->GetColumnName()) {
                columnName = *column->GetColumnName();
            } else {
                TStringStream str;
                str << "_equijoin_column_" << extraColumns.size();
                columnName = str.Str();
                extraColumns.insert({ std::make_pair(tableName, columnName), column });
            }

            res = L(res, BuildQuotedAtom(Pos, tableName));
            res = L(res, BuildQuotedAtom(Pos, columnName));
        }

        return Q(res);
    }
};

TSourcePtr BuildEquiJoin(TPosition pos, TVector<TSourcePtr>&& sources) {
    return new TEquiJoin(pos, std::move(sources));
}

} // namespace NSQLTranslationV0
