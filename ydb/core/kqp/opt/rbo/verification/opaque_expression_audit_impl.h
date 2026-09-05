// Included exactly once inside semantic_snapshot.cpp's anonymous namespace.
// Requires its closed scalar-callable, type, metadata and JSON helpers.

class TRestrictedConcatAuditor;

class TRestrictedConcatAuditToken {
    friend class TRestrictedConcatAuditor;

private:
    TRestrictedConcatAuditToken() = default;
};

class TRestrictedFloatingPredicateAuditor;

class TRestrictedFloatingPredicateAuditToken {
    friend class TRestrictedFloatingPredicateAuditor;

private:
    TRestrictedFloatingPredicateAuditToken() = default;
};

class TPassiveDoubleCarrierAuditor;

class TPassiveDoubleCarrierAuditToken {
    friend class TPassiveDoubleCarrierAuditor;

private:
    TPassiveDoubleCarrierAuditToken() = default;
};

enum class ERestrictedFloatingConstant : ui8 {
    PointNine,
    OnePointTwo,
    TwoThirds,
    ThreeHalves,
};

struct TRestrictedFloatingConstant {
    ERestrictedFloatingConstant Kind;
    TStringBuf Fingerprint;
};

std::optional<TRestrictedFloatingConstant> RecognizeRestrictedFloatingConstant(
    const TExprNode& node)
{
    // Tags name the exact IEEE-754 binary64 payload produced by YQL parsing
    // and constant folding.  The two division spellings are admitted only as
    // aliases of the directly observed folded literals.
    const auto literal = [](const TExprNode& value)
        -> std::optional<TStringBuf>
    {
        if (!value.IsCallable("Double") ||
            value.ChildrenSize() != 1 ||
            !value.Child(0)->IsAtom())
        {
            return std::nullopt;
        }
        return value.Child(0)->Content();
    };

    if (const auto value = literal(node)) {
        if (*value == "0.9") {
            return TRestrictedFloatingConstant{
                ERestrictedFloatingConstant::PointNine,
                "yql-double-bits-3feccccccccccccd-v1"};
        }
        if (*value == "1.2") {
            return TRestrictedFloatingConstant{
                ERestrictedFloatingConstant::OnePointTwo,
                "yql-double-bits-3ff3333333333333-v1"};
        }
        if (*value == "0.6666666666666666") {
            return TRestrictedFloatingConstant{
                ERestrictedFloatingConstant::TwoThirds,
                "yql-double-bits-3fe5555555555555-v1"};
        }
        if (*value == "1.5") {
            return TRestrictedFloatingConstant{
                ERestrictedFloatingConstant::ThreeHalves,
                "yql-double-bits-3ff8000000000000-v1"};
        }
        return std::nullopt;
    }

    if (!node.IsCallable("/") || node.ChildrenSize() != 2) {
        return std::nullopt;
    }
    const auto left = literal(*node.Child(0));
    const auto right = literal(*node.Child(1));
    if (left && right && *left == "2.0" && *right == "3.0") {
        return TRestrictedFloatingConstant{
            ERestrictedFloatingConstant::TwoThirds,
            "yql-double-bits-3fe5555555555555-v1"};
    }
    if (left && right && *left == "3.0" && *right == "2.0") {
        return TRestrictedFloatingConstant{
            ERestrictedFloatingConstant::ThreeHalves,
            "yql-double-bits-3ff8000000000000-v1"};
    }
    return std::nullopt;
}

class TOpaqueSourceAuditor;

// Completed source admission: serialization cannot inspect or reinterpret AST
// nodes. The verifier still independently checks the opaque wire contract.
class TAuditedOpaqueExpression {
    friend class TOpaqueSourceAuditor;

    struct TExternalArgument {
        bool IsBound;
        TString Column;
        size_t Depth;
    };

public:
    NJson::TJsonValue Export(
        TExactScalarBudget& budget,
        size_t argumentDepth) const
    {
        budget.Charge(argumentDepth, ExternalArguments.size());
        auto args = JsonArray();
        for (const auto& argument : ExternalArguments) {
            args.AppendValue(argument.IsBound
                ? BoundExpr(argument.Depth)
                : ColumnExpr(argument.Column));
        }

        auto result = JsonMap();
        result["kind"] = PassiveDouble ? "opaque_double" : "opaque";
        result["fingerprint"] = Fingerprint;
        result["type"] = ResultType;
        result["nullable"] = Nullable;
        result["args"] = std::move(args);
        return result;
    }

private:
    TAuditedOpaqueExpression(
        TString fingerprint,
        TString resultType,
        bool nullable,
        bool passiveDouble,
        TVector<TExternalArgument> externalArguments)
        : Fingerprint(std::move(fingerprint))
        , ResultType(std::move(resultType))
        , Nullable(nullable)
        , PassiveDouble(passiveDouble)
        , ExternalArguments(std::move(externalArguments))
    {
    }

    TString Fingerprint;
    TString ResultType;
    bool Nullable;
    bool PassiveDouble;
    TVector<TExternalArgument> ExternalArguments;
};

class TOpaqueSourceAuditor {
public:
    TOpaqueSourceAuditor(
        const TExprNode* rowArgument,
        const THashSet<TString>& visibleColumns,
        TVector<const TExprNode*> boundArguments = {})
        : RowArgument(rowArgument)
        , VisibleColumns(visibleColumns)
        , BoundArguments(std::move(boundArguments))
    {
    }

    TOpaqueSourceAuditor(
        TRestrictedConcatAuditToken,
        const TExprNode* rowArgument,
        const THashSet<TString>& visibleColumns)
        : RowArgument(rowArgument)
        , VisibleColumns(visibleColumns)
        , AllowRestrictedConcat(true)
    {
    }

    TOpaqueSourceAuditor(
        TRestrictedFloatingPredicateAuditToken,
        const TExprNode* rowArgument,
        const THashSet<TString>& visibleColumns,
        const TExprNode* restrictedFloatingComparison,
        const TExprNode* restrictedFloatingConstant)
        : RowArgument(rowArgument)
        , VisibleColumns(visibleColumns)
        , RestrictedFloatingComparison(restrictedFloatingComparison)
        , RestrictedFloatingConstantNode(restrictedFloatingConstant)
    {
    }

    TOpaqueSourceAuditor(
        TPassiveDoubleCarrierAuditToken,
        const TExprNode* rowArgument,
        const THashSet<TString>& visibleColumns,
        const TExprNode* passiveDoubleRoot,
        const TExprNode* passiveDoubleDivision,
        const TExprNode* passiveDoubleConstant)
        : RowArgument(rowArgument)
        , VisibleColumns(visibleColumns)
        , PassiveDoubleRoot(passiveDoubleRoot)
        , PassiveDoubleDivision(passiveDoubleDivision)
        , PassiveDoubleConstantNode(passiveDoubleConstant)
    {
    }

    // Source admission only: the lowering still owns its semantic shape checks.
    // Count the same identity bytes without constructing a discarded fingerprint.
    void RequireExactLoweringSource(const TExprNode& node) {
        AllowNestedIfPresent = true;
        TIdentitySink identity;
        AuditRoot(node, identity);
    }

    TAuditedOpaqueExpression AuditOpaque(const TExprNode& node) {
        bool nullable = false;
        const TString resultType = [&] {
            if (&node != PassiveDoubleRoot) {
                return ScalarTypeName(node, &nullable);
            }
            if (!IsExactDataAnnotation(
                    node.GetTypeAnn(),
                    NUdf::EDataSlot::Double,
                    true))
            {
                Unsupported(
                    "Audited passive Double root lost Optional<Double> type");
            }
            nullable = true;
            return TString("Double");
        }();

        TStringBuilder fingerprint;
        TIdentitySink identity(&fingerprint);
        AuditRoot(node, identity);
        return TAuditedOpaqueExpression(
            TString(fingerprint),
            resultType,
            nullable,
            &node == PassiveDoubleRoot,
            ExternalArguments);
    }

private:
    static constexpr size_t MaxNodes = 256;
    static constexpr size_t MaxDepth = 64;
    static constexpr size_t MaxFingerprintBytes = 64 * 1024;

    using TExternalArgument = TAuditedOpaqueExpression::TExternalArgument;

    // One field formatter serves counting and materializing admission. Once the
    // cap is exceeded, stop retaining bytes but finish the source walk so later
    // safety failures keep their original precedence over the byte-limit error.
    class TIdentitySink {
    public:
        explicit TIdentitySink(TStringBuilder* output = nullptr)
            : Output(output)
        {
        }

        void Field(TStringBuf name, TStringBuf value) {
            Append(name);
            Append(":");
            Append(ToString(value.size()));
            Append(":");
            Append(value);
            Append(";");
        }

        bool ExceedsLimit() const {
            return Bytes > MaxFingerprintBytes;
        }

    private:
        void Append(TStringBuf part) {
            if (ExceedsLimit()) {
                return;
            }
            if (part.size() > MaxFingerprintBytes - Bytes) {
                Bytes = MaxFingerprintBytes + 1;
                return;
            }
            Bytes += part.size();
            if (Output) {
                *Output << part;
            }
        }

        TStringBuilder* Output;
        size_t Bytes = 0;
    };

    void AuditRoot(const TExprNode& node, TIdentitySink& fingerprint) {
        if (!node.IsCallable()) {
            Unsupported("Opaque scalar root is not a callable");
        }
        fingerprint.Field(
            "format",
            &node == PassiveDoubleRoot
                ? TStringBuf("yql-passive-double-v1")
                : TStringBuf("yql-opaque-v1"));
        AuditNode(node, fingerprint, 0);
        if (fingerprint.ExceedsLimit()) {
            Unsupported("Opaque scalar fingerprint exceeds the audit limit");
        }
    }

    TString TypeFingerprint(const TExprNode& node) const {
        return node.GetTypeAnn() ? FormatType(node.GetTypeAnn()) : TString("<none>");
    }

    void CheckSafeNode(const TExprNode& node) {
        if (++NodeCount > MaxNodes) {
            Unsupported("Opaque scalar exceeds the node audit limit");
        }
        CheckScalarSafetyMetadata(node);
    }

    void AuditMember(const TExprNode& node, TIdentitySink& out) {
        if (node.ChildrenSize() != 2 || !node.Child(1)->IsAtom()) {
            Unsupported("Malformed Member expression");
        }
        const TString column(node.Child(1)->Content());
        if (node.Child(0) != RowArgument || !VisibleColumns.contains(column)) {
            Unsupported(TStringBuilder() << "Member does not reference the input row column " << column);
        }
        ScalarTypeName(node);
        CheckSafeNode(*node.Child(1));

        const size_t index = ExternalIndex(
            TStringBuilder() << "column:" << column.size() << ":" << column,
            {false, column, 0});

        out.Field("node", "member");
        out.Field("type", TypeFingerprint(node));
        out.Field("argument", ToString(index));
    }

    size_t BoundDepth(const TExprNode& node) const {
        const auto it = std::find(BoundArguments.begin(), BoundArguments.end(), &node);
        if (it == BoundArguments.end()) {
            Unsupported("Opaque scalar contains a free Argument");
        }
        return static_cast<size_t>(it - BoundArguments.begin());
    }

    size_t ExternalIndex(TString key, TExternalArgument argument) {
        const auto [it, inserted] = ExternalIndices.emplace(
            std::move(key),
            ExternalArguments.size());
        if (inserted) {
            ExternalArguments.push_back(std::move(argument));
        }
        return it->second;
    }

    void AuditBound(const TExprNode& node, TIdentitySink& out) {
        bool nullable = false;
        ScalarTypeName(node, &nullable);
        if (nullable) {
            Unsupported("IfPresent bound argument must be non-nullable");
        }
        const size_t depth = BoundDepth(node);
        const size_t index = ExternalIndex(
            TStringBuilder() << "bound:" << depth,
            {true, {}, depth});
        out.Field("node", "bound");
        out.Field("type", TypeFingerprint(node));
        out.Field("argument", ToString(index));
    }

    void AuditIfPresent(
        const TExprNode& node,
        TIdentitySink& out,
        size_t depth)
    {
        if (!AllowNestedIfPresent) {
            Unsupported("Opaque scalar cannot hide an IfPresent binder");
        }
        const auto signature = CheckIfPresentCallable(node);
        const auto& handler = *node.Child(1);
        const auto& arguments = *handler.Child(0);
        CheckSafeNode(handler);
        CheckSafeNode(arguments);
        CheckSafeNode(*signature.Argument);

        out.Field("node", "callable");
        out.Field("content", "IfPresent");
        out.Field("type", TypeFingerprint(node));
        out.Field("children", "3");
        AuditNode(*signature.Optional, out, depth + 1);

        out.Field("node", "lambda");
        out.Field("argument_type", TypeFingerprint(*signature.Argument));
        out.Field("result_type", TypeFingerprint(*signature.Present));
        out.Field("children", "1");
        if (BoundArguments.size() >= MaxIfPresentBindingDepth) {
            Unsupported("IfPresent binding depth exceeds the audit limit");
        }
        BoundArguments.insert(BoundArguments.begin(), signature.Argument);
        AuditNode(*signature.Present, out, depth + 1);
        BoundArguments.erase(BoundArguments.begin());

        AuditNode(*signature.Missing, out, depth + 1);
    }

    void AuditNode(
        const TExprNode& node,
        TIdentitySink& out,
        size_t depth,
        bool allowExactUint32LiteralConversion = false)
    {
        if (depth > MaxDepth) {
            Unsupported("Opaque scalar exceeds the nesting audit limit");
        }
        CheckSafeNode(node);

        if (node.IsCallable("Member")) {
            AuditMember(node, out);
            return;
        }
        if (node.IsArgument()) {
            AuditBound(node, out);
            return;
        }
        if (node.IsCallable("IfPresent")) {
            AuditIfPresent(node, out, depth);
            return;
        }
        if (&node == RestrictedFloatingConstantNode && depth == 1) {
            const auto constant =
                RecognizeRestrictedFloatingConstant(node);
            if (!constant) {
                Unsupported(
                    "Audited floating constant has no canonical tag");
            }
            out.Field("node", "restricted-floating-constant");
            out.Field("content", constant->Fingerprint);
            out.Field("type", TypeFingerprint(node));
            out.Field("children", "0");
            return;
        }
        if (&node == PassiveDoubleConstantNode) {
            out.Field("node", "passive-double-constant");
            out.Field("content", "yql-double-bits-4008000000000000-v1");
            out.Field("type", TypeFingerprint(node));
            out.Field("children", "0");
            return;
        }

        switch (node.Type()) {
            case TExprNode::Callable:
                if (node.IsCallable("Concat")) {
                    if (!AllowRestrictedConcat) {
                        Unsupported("Unsupported scalar callable Concat");
                    }
                } else {
                    const bool auditedFloatingPredicateRoot =
                        &node == RestrictedFloatingComparison && depth == 0;
                    const bool auditedPassiveDoubleCallable =
                        &node == PassiveDoubleRoot ||
                        &node == PassiveDoubleDivision;
                    if (!auditedFloatingPredicateRoot &&
                        !auditedPassiveDoubleCallable)
                    {
                        CheckOpaqueCallable(
                            node,
                            allowExactUint32LiteralConversion);
                    }
                }
                out.Field("node", "callable");
                out.Field("content", node.Content());
                break;
            case TExprNode::Atom:
                out.Field("node", "atom");
                out.Field("content", node.Content());
                out.Field("flags", ToString(node.GetFlagsToCompare()));
                break;
            case TExprNode::List:
                Unsupported("Opaque scalar contains an unsupported List node");
            case TExprNode::Lambda:
                Unsupported("Opaque scalar contains a nested Lambda");
            case TExprNode::Argument:
                Unsupported("Opaque scalar contains a free Argument");
            case TExprNode::Arguments:
                Unsupported("Opaque scalar contains an Arguments node");
            case TExprNode::World:
                Unsupported("Opaque scalar contains World");
        }

        out.Field("type", TypeFingerprint(node));
        out.Field("children", ToString(node.ChildrenSize()));
        for (size_t index = 0; index < node.ChildrenSize(); ++index) {
            AuditNode(
                *node.Child(index),
                out,
                depth + 1,
                node.IsCallable("Substring") && index > 0);
        }
    }

private:
    const TExprNode* RowArgument;
    const THashSet<TString>& VisibleColumns;
    TVector<const TExprNode*> BoundArguments;
    // These counts/indices accumulate when one auditor admits multiple roots.
    THashMap<TString, size_t> ExternalIndices;
    TVector<TExternalArgument> ExternalArguments;
    size_t NodeCount = 0;
    bool AllowNestedIfPresent = false;
    bool AllowRestrictedConcat = false;
    const TExprNode* RestrictedFloatingComparison = nullptr;
    const TExprNode* RestrictedFloatingConstantNode = nullptr;
    const TExprNode* PassiveDoubleRoot = nullptr;
    const TExprNode* PassiveDoubleDivision = nullptr;
    const TExprNode* PassiveDoubleConstantNode = nullptr;
};
