// Included inside the snapshot exporter's anonymous namespace.
// Snapshot column identities are the decimal plan-local IDs, never registry
// display names. The views keep output IDs attached to their defining values.
struct TSnapshotIUHash {
    size_t operator()(const TInfoUnit& value) const {
        return THash<TString>{}(value.GetFullName());
    }
};
using TInfoUnitSet = THashSet<TInfoUnit, TSnapshotIUHash>;

TInfoUnit SnapshotIU(TInfoUnitId id) {
    return TInfoUnit(ToString(id));
}

template <class TIds>
TVector<TInfoUnit> SnapshotIUs(const TIds& ids) {
    TVector<TInfoUnit> result;
    if constexpr (requires { ids.Items(); }) {
        return SnapshotIUs(ids.Items());
    } else {
        for (const auto id : ids) {
            result.push_back(SnapshotIU(id));
        }
    }
    return result;
}

TVector<TInfoUnit> SnapshotOutputIUs(IOperator& op) {
    return SnapshotIUs(op.GetOutputIUs());
}

TVector<TInfoUnit> SnapshotKeyColumns(const TOpAggregate& op) {
    return SnapshotIUs(op.GetKeyColumns());
}

struct TSnapshotAggregationTrait {
    TInfoUnit OriginalColName;
    TInfoUnit ResultColName;
    TString AggFunction;
    bool Distinct;
    bool Unwrap;
};

struct TSnapshotMapElement {
    TInfoUnit Output;
    const TMapElement* Element;

    bool IsRename() const { return false; } // Main's logical Maps only append.
    bool IsColumnAccess() const { return Element->IsColumnAccess(); }
    TInfoUnit GetColumnAccess() const { return SnapshotIU(Element->GetColumnAccess()); }
    TInfoUnit GetRename() const { return GetColumnAccess(); }
    const TInfoUnit& GetElementName() const { return Output; }
    const TExpression& GetExpression() const { return Element->GetExpression(); }
};

struct TSnapshotOperatorViews {
    THashMap<const TOpMap*, TVector<TSnapshotMapElement>> Maps;
    THashMap<const TOpAggregate*, TVector<TSnapshotAggregationTrait>> Aggregates;
    THashMap<const TOpAddDependencies*, TVector<TInfoUnit>> Dependencies;
    THashMap<const TOpAddDependencies*, TVector<const TTypeAnnotationNode*>> DependencyTypes;
    THashMap<std::pair<const TOpRead*, const TInfoUnitRegistry*>, TVector<TString>> ReadColumns;
    THashMap<const TOpRoot*, TVector<TString>> RootOrder;
};
thread_local TSnapshotOperatorViews* CurrentSnapshotViews = nullptr;

class TSnapshotOperatorViewScope {
public:
    TSnapshotOperatorViewScope() : Previous(CurrentSnapshotViews) {
        CurrentSnapshotViews = &Views;
    }
    TSnapshotOperatorViewScope(const TSnapshotOperatorViewScope&) = delete;
    TSnapshotOperatorViewScope& operator=(const TSnapshotOperatorViewScope&) = delete;
    ~TSnapshotOperatorViewScope() { CurrentSnapshotViews = Previous; }
private:
    TSnapshotOperatorViews Views;
    TSnapshotOperatorViews* Previous;
};

const TVector<TSnapshotMapElement>& SnapshotMapElements(const TOpMap& op) {
    Y_ENSURE(CurrentSnapshotViews, "Snapshot map views require a capture scope");
    auto [it, inserted] = CurrentSnapshotViews->Maps.try_emplace(&op);
    if (inserted) {
        for (const auto& [output, element] : op.GetMapElements().Items()) {
            it->second.push_back({SnapshotIU(output), &element});
        }
    }
    return it->second;
}

const TVector<TSnapshotAggregationTrait>& SnapshotAggregationTraits(const TOpAggregate& op) {
    Y_ENSURE(CurrentSnapshotViews);
    auto [it, inserted] = CurrentSnapshotViews->Aggregates.try_emplace(&op);
    if (!inserted) {
        return it->second;
    }
    auto& result = it->second;
    for (const auto& [output, trait] : op.GetAggregationTraits().Items()) {
        result.push_back({SnapshotIU(trait.Input), SnapshotIU(output), trait.AggFunction, trait.Distinct, trait.Unwrap});
    }
    return result;
}

const TSnapshotMapElement* SnapshotFindOutputElement(const TOpMap& op, const TInfoUnit& output) {
    for (const auto& element : SnapshotMapElements(op)) {
        if (element.Output == output) {
            return &element;
        }
    }
    return nullptr;
}

struct TSnapshotSortElement {
    TInfoUnit SortColumn;
    bool Ascending;
    bool NullsFirst;
};

template <class TOp>
TVector<TSnapshotSortElement> SnapshotSortElements(const TOp& op) {
    TVector<TSnapshotSortElement> result;
    for (const auto& [column, order] : op.GetSortElements().Items()) {
        // Native Sort/WideSort lower only the direction. Their complete key
        // order puts NULL first in ASC and last in DESC; the source flag is
        // not passed to either physical primitive.
        result.push_back({SnapshotIU(column), order.Ascending, order.Ascending});
    }
    return result;
}

TVector<TInfoUnit> SnapshotInputIUs(const TExpression& expression, bool subplans = false, bool dependencies = false) {
    return SnapshotIUs(expression.GetInputIUs(subplans, dependencies));
}

TVector<TInfoUnit> SnapshotSubplanIUs(const IOperator& op, const TPlanProps& props) {
    TUnorderedIUs calls;
    for (const auto expression : op.GetExpressions()) {
        auto inputs = expression.get().GetInputIUs(true, false);
        inputs.IntersectWith(props.Subplans.Bindings());
        calls.UnionWith(inputs);
    }
    return SnapshotIUs(calls);
}

TVector<TExpression> SnapshotBooleanTerms(const TExpression& expression, TStringBuf operation) {
    // Main's optimizer splitters rebind row arguments and invalidate annotations.
    // An audit must retain the original typed bodies instead of rewriting them.
    const auto original = expression.GetLambda();
    TVector<TExprNode::TPtr> pending{expression.GetExpressionBody()};
    TVector<TExpression> result;
    while (!pending.empty()) {
        auto body = std::move(pending.back());
        pending.pop_back();
        if (body->IsCallable(operation)) {
            for (size_t index = body->ChildrenSize(); index > 0; --index) {
                pending.push_back(body->ChildPtr(index - 1));
            }
        } else if (body.Get() == expression.GetExpressionBody().Get()) {
            result.push_back(expression);
        } else {
            auto lambda = expression.Ctx->NewLambda(body->Pos(), original->ChildPtr(0), std::move(body));
            result.emplace_back(std::move(lambda), expression.Ctx, expression.PlanProps);
        }
    }
    return result;
}

// Old expression-carried window metadata is absent on main. Such matchers
// cannot admit a plan; dedicated Window operators require their own export.
struct TSnapshotWindowMetadata {
    TExprNode::TPtr Definition;
    TVector<THashMap<TInfoUnit, TInfoUnit, TSnapshotIUHash>> RenameHistory;
};
std::optional<TSnapshotWindowMetadata> SnapshotWindowMetadata(const TExpression&) { return std::nullopt; }
TVector<TInfoUnit> SnapshotWindowPartitionBy(const TExpression&) { return {}; }
TVector<TInfoUnit> SnapshotWindowOrderBy(const TExpression&) { return {}; }

struct TSnapshotSubplanEntry {
    TIntrusivePtr<IOperator> Plan;
    TVector<TInfoUnit> Tuple;
    TVector<TInfoUnit> DependentIUs;
    std::optional<TInfoUnit> ResultIU;
    ESubplanType Type;
};

TSnapshotSubplanEntry SnapshotSubplanEntry(const TSubplanEntry& entry) {
    return {entry.Plan, SnapshotIUs(entry.Tuple), SnapshotIUs(entry.DependentIUs),
        entry.ResultIU ? std::optional<TInfoUnit>(SnapshotIU(*entry.ResultIU)) : std::nullopt, entry.Type};
}

TVector<TInfoUnit> SnapshotSubplanBindings(const TSubplans& subplans) {
    TVector<TInfoUnit> result;
    for (const auto& [binding, entry] : subplans) {
        Y_UNUSED(entry);
        result.push_back(SnapshotIU(binding));
    }
    return result;
}

const TVector<TInfoUnit>& SnapshotDependencies(const TOpAddDependencies& op) {
    Y_ENSURE(CurrentSnapshotViews);
    auto [it, inserted] = CurrentSnapshotViews->Dependencies.try_emplace(&op);
    if (!inserted) {
        return it->second;
    }
    auto& result = it->second;
    for (const auto& [local, capture] : op.GetDependencies().Items()) {
        Y_UNUSED(capture);
        result.push_back(SnapshotIU(local));
    }
    return result;
}

const TVector<const TTypeAnnotationNode*>& SnapshotDependencyTypes(const TOpAddDependencies& op) {
    Y_ENSURE(CurrentSnapshotViews);
    auto [it, inserted] = CurrentSnapshotViews->DependencyTypes.try_emplace(&op);
    if (!inserted) {
        return it->second;
    }
    auto& result = it->second;
    for (const auto& [local, capture] : op.GetDependencies().Items()) {
        Y_UNUSED(local);
        result.push_back(capture.Type);
    }
    return result;
}

const TVector<TString>& SnapshotReadColumns(const TOpRead& op, const TInfoUnitRegistry& registry) {
    Y_ENSURE(CurrentSnapshotViews);
    auto [it, inserted] = CurrentSnapshotViews->ReadColumns.try_emplace(std::make_pair(&op, &registry));
    if (!inserted) {
        return it->second;
    }
    auto& result = it->second;
    for (const auto id : op.GetColumns()) {
        result.push_back(registry.Get(id).GetColumnName());
    }
    return result;
}

struct TSnapshotJoinKey {
    TInfoUnit Left;
    TInfoUnit Right;
    bool EqualNulls;
};

TVector<TSnapshotJoinKey> SnapshotJoinKeys(const TOpJoin& op) {
    TVector<TSnapshotJoinKey> result;
    for (const auto& key : op.JoinKeys.Items()) {
        result.push_back({SnapshotIU(key.first), SnapshotIU(key.second), key.EqualNulls});
    }
    return result;
}

const TVector<TString>& SnapshotRootColumnOrder(const TOpRoot& root) {
    Y_ENSURE(CurrentSnapshotViews);
    auto [it, inserted] = CurrentSnapshotViews->RootOrder.try_emplace(&root);
    if (inserted) {
        for (const auto& [id, label] : root.GetColumns().Items()) {
            Y_UNUSED(label);
            it->second.push_back(ToString(id));
        }
    }
    return it->second;
}

TVector<TSnapshotSortElement> SnapshotMergeOrder(const TMergeConnection& connection) {
    TVector<TSnapshotSortElement> result;
    for (const auto& [id, order] : connection.Order.Items()) {
        // DqCnMerge likewise receives the direction, not the source NULL flag.
        result.push_back({SnapshotIU(id), order.Ascending, order.Ascending});
    }
    return result;
}
