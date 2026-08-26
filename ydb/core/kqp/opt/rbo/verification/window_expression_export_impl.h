// Included exactly once inside semantic_snapshot.cpp's anonymous namespace.
// Relies on the private helpers declared above its include site.

enum class EWholePartitionWindowFunction {
    Sum,
    Avg,
};

struct TWholePartitionWindowKey {
    TString Name;
    TString Type;
    ui32 AggregateKeyIndex = 0;
};

struct TWholePartitionWindow {
    NJson::TJsonValue Expression;
    TString Input;
    TVector<TWholePartitionWindowKey> PartitionBy;
    EWholePartitionWindowFunction Function;
};

struct TGlobalRankWindow {
    NJson::TJsonValue Expression;
    TString WindowName;
    TString OrderColumn;
    ui32 SourceOrdinal = 0;
};

struct TGlobalRankProjectionWindow {
    TString Output;
    TString WindowName;
    TString OrderColumn;
    ui32 SourceOrdinal = 0;
    ui32 ExecutionOrder = 0;
};

enum class EQ51WindowFunction {
    Sum,
    Max,
};

struct TQ51Window {
    NJson::TJsonValue Expression;
    TString Input;
    TString PartitionColumn;
    TString OrderColumn;
    TString WindowName;
    EQ51WindowFunction Function;
    ui32 SourceOrdinal = 0;
};

struct TQ51ProjectionWindow {
    TString Output;
    TString Input;
    TString PartitionColumn;
    TString OrderColumn;
    TString WindowName;
    EQ51WindowFunction Function;
    ui32 SourceOrdinal = 0;
    ui32 ExecutionOrder = 0;
};

TStringBuf WindowLabel(EWholePartitionWindowFunction function) {
    return function == EWholePartitionWindowFunction::Sum
        ? TStringBuf("Window sum")
        : TStringBuf("Window avg");
}

TString WindowContext(TStringBuf label, TStringBuf context) {
    return TStringBuilder() << label << context;
}

void CheckExactWindowSafetyTree(const TExprNode& root) {
    TVector<std::pair<const TExprNode*, size_t>> pending{{&root, 1}};
    size_t nodes = 0;
    while (!pending.empty()) {
        const auto [node, depth] = pending.back();
        pending.pop_back();
        if (++nodes > 128 || depth > 16) {
            Unsupported(
                "Whole-partition window source tree exceeds its audit limit");
        }
        CheckScalarSafetyMetadata(*node);
        for (const auto& child : node->Children()) {
            pending.emplace_back(child.Get(), depth + 1);
        }
    }
}

void CheckExactWindowAtom(
    const TExprNode& node,
    TStringBuf expected,
    TStringBuf label)
{
    CheckScalarSafetyMetadata(node);
    if (!node.IsAtom(expected)) {
        Unsupported(TStringBuilder()
            << label << " must be the exact atom " << expected);
    }
}

TVector<TWholePartitionWindowKey> AuditWholePartitionWindowDefinition(
    const TExpression::TWindowMetadata& metadata,
    TStringBuf windowName,
    EWholePartitionWindowFunction function)
{
    const TStringBuf label = WindowLabel(function);
    if (!metadata.Definition) {
        Unsupported(TStringBuilder()
            << label << " metadata has no source definition");
    }
    const auto& definition = *metadata.Definition;
    CheckExactWindowSafetyTree(definition);
    if (!definition.IsCallable("YqlWindow") ||
        definition.ChildrenSize() != 5)
    {
        Unsupported(TStringBuilder()
            << label << " requires an exact five-child YqlWindow");
    }

    const auto& name = *definition.Child(0);
    CheckScalarSafetyMetadata(name);
    if (!name.IsAtom() || name.Content().empty() ||
        name.Content() != windowName)
    {
        Unsupported(TStringBuilder()
            << label << " definition name does not match YqlAggWin");
    }
    CheckExactWindowAtom(
        *definition.Child(1), "",
        WindowContext(label, " inherited window"));

    const auto& partitions = *definition.Child(2);
    CheckScalarSafetyMetadata(partitions);
    const size_t partitionCount = partitions.ChildrenSize();
    if (!partitions.IsList() ||
        (function == EWholePartitionWindowFunction::Sum
            ? partitionCount != 1
            : partitionCount < 1 || partitionCount > 4))
    {
        Unsupported(TStringBuilder()
            << label << " requires "
            << (function == EWholePartitionWindowFunction::Sum
                    ? TStringBuf("exactly one")
                    : TStringBuf("between one and four"))
            << " partition expressions");
    }

    TVector<TWholePartitionWindowKey> result;
    result.reserve(partitionCount);
    THashSet<TString> sourceNames;
    THashSet<ui32> sourceIndices;
    for (const auto& partition : partitions.Children()) {
        const auto& group = *partition;
        CheckScalarSafetyMetadata(group);
        if (!group.IsCallable("YqlGroup") || group.ChildrenSize() != 2) {
            Unsupported(TStringBuilder()
                << label << " partition must be one exact YqlGroup");
        }

        const auto& rowDescriptor = *group.Child(0);
        CheckScalarSafetyMetadata(rowDescriptor);
        if (!rowDescriptor.IsCallable("StructType") ||
            rowDescriptor.ChildrenSize() != 1)
        {
            Unsupported(TStringBuilder()
                << label
                << " partition row descriptor must contain one field");
        }
        const auto& field = *rowDescriptor.Child(0);
        CheckScalarSafetyMetadata(field);
        if (!field.IsList() || field.ChildrenSize() != 2 ||
            !field.Child(0)->IsAtom() || field.Child(0)->Content().empty())
        {
            Unsupported(TStringBuilder()
                << label
                << " partition row field descriptor is not canonical");
        }
        CheckScalarSafetyMetadata(*field.Child(0));
        bool fieldNullable = false;
        const TString fieldType =
            DataTypeDescriptorName(*field.Child(1), &fieldNullable);
        if (!fieldNullable ||
            (fieldType != "String" && fieldType != "Int64") ||
            (function == EWholePartitionWindowFunction::Sum &&
                fieldType != "String"))
        {
            if (function == EWholePartitionWindowFunction::Sum) {
                Unsupported(
                    "Window sum partition row field must be Optional<String>");
            }
            Unsupported(
                "Window avg partition row field must be Optional<String> "
                "or Optional<Int64>");
        }
        const auto slot = fieldType == "String"
            ? NUdf::EDataSlot::String
            : NUdf::EDataSlot::Int64;
        const auto& describedRow = DescribedType(
            rowDescriptor,
            WindowContext(label, " partition row descriptor"));
        if (describedRow.GetKind() != ETypeAnnotationKind::Struct) {
            Unsupported(TStringBuilder()
                << label
                << " partition row descriptor must describe Struct");
        }
        const auto& rowItems =
            describedRow.Cast<TStructExprType>()->GetItems();
        if (rowItems.size() != 1 ||
            rowItems.front()->GetName() != field.Child(0)->Content() ||
            !IsExactDataAnnotation(
                rowItems.front()->GetItemType(), slot, true) ||
            !IsSameAnnotation(
                DescribedType(
                    *field.Child(1),
                    WindowContext(label, " partition row field")),
                *rowItems.front()->GetItemType()))
        {
            Unsupported(TStringBuilder()
                << label
                << " partition row descriptor annotation disagrees");
        }

        const auto& lambda = *group.Child(1);
        CheckScalarSafetyMetadata(lambda);
        if (!lambda.IsLambda() || lambda.ChildrenSize() != 2 ||
            !lambda.Child(0)->IsArguments() ||
            lambda.Child(0)->ChildrenSize() != 1 ||
            !lambda.Child(0)->Child(0)->IsArgument())
        {
            Unsupported(TStringBuilder()
                << label << " partition must be one unary lambda");
        }
        const auto& arguments = *lambda.Child(0);
        const auto& argument = *arguments.Child(0);
        CheckScalarSafetyMetadata(arguments);
        CheckScalarSafetyMetadata(argument);
        if (!argument.GetTypeAnn() ||
            !IsSameAnnotation(*argument.GetTypeAnn(), describedRow))
        {
            Unsupported(TStringBuilder()
                << label
                << " partition lambda argument disagrees with its row "
                   "descriptor");
        }

        const auto& groupRef = *lambda.Child(1);
        CheckScalarSafetyMetadata(groupRef);
        if (!groupRef.IsCallable("YqlGroupRef") ||
            groupRef.ChildrenSize() != 4 ||
            groupRef.Child(0) != &argument ||
            !groupRef.Child(2)->IsAtom() ||
            !groupRef.Child(3)->IsAtom() ||
            groupRef.Child(3)->Content().empty() ||
            groupRef.Child(3)->Content() != field.Child(0)->Content())
        {
            Unsupported(TStringBuilder()
                << label
                << " partition must be one direct named YqlGroupRef");
        }
        CheckScalarSafetyMetadata(*groupRef.Child(2));
        CheckScalarSafetyMetadata(*groupRef.Child(3));
        const ui32 partitionIndex = ParseInteger<ui32>(
            groupRef.Child(2)->Content(), "window partition index");
        if (groupRef.Child(2)->Content() != ToString(partitionIndex) ||
            (function == EWholePartitionWindowFunction::Sum &&
                partitionIndex != 3))
        {
            Unsupported(TStringBuilder()
                << label << " has a noncanonical partition index");
        }
        bool descriptorNullable = false;
        if (DataTypeDescriptorName(
                *groupRef.Child(1), &descriptorNullable) != fieldType ||
            !descriptorNullable)
        {
            Unsupported(TStringBuilder()
                << label << " partition descriptor type disagrees");
        }
        bool partitionNullable = false;
        if (ScalarTypeName(groupRef, &partitionNullable) != fieldType ||
            !partitionNullable ||
            !IsSameAnnotation(
                DescribedType(
                    *groupRef.Child(1),
                    WindowContext(label, " partition descriptor")),
                *groupRef.GetTypeAnn()))
        {
            Unsupported(TStringBuilder()
                << label << " partition reference type disagrees");
        }

        const TString sourceName(groupRef.Child(3)->Content());
        if (!sourceNames.insert(sourceName).second ||
            !sourceIndices.insert(partitionIndex).second)
        {
            Unsupported(TStringBuilder()
                << label
                << " partition names and indices must be unique");
        }
        result.push_back({sourceName, fieldType, partitionIndex});
    }

    const auto& order = *definition.Child(3);
    CheckScalarSafetyMetadata(order);
    if (!order.IsList() || order.ChildrenSize() != 0) {
        Unsupported(TStringBuilder()
            << label << " does not admit window ordering");
    }

    const auto& frame = *definition.Child(4);
    CheckScalarSafetyMetadata(frame);
    if (!frame.IsList() || frame.ChildrenSize() != 3) {
        Unsupported(TStringBuilder()
            << label
            << " requires one exact whole-partition ROWS frame");
    }
    const std::array<std::pair<TStringBuf, TStringBuf>, 3> expectedFrame = {{
        {"type", "rows"},
        {"from", "up"},
        {"to", "uf"},
    }};
    for (size_t index = 0; index < expectedFrame.size(); ++index) {
        const auto& setting = *frame.Child(index);
        CheckScalarSafetyMetadata(setting);
        if (!setting.IsList() || setting.ChildrenSize() != 2) {
            Unsupported(TStringBuilder()
                << label << " has a malformed frame setting");
        }
        CheckExactWindowAtom(
            *setting.Child(0),
            expectedFrame[index].first,
            WindowContext(label, " frame setting name"));
        CheckExactWindowAtom(
            *setting.Child(1),
            expectedFrame[index].second,
            WindowContext(label, " frame setting value"));
    }

    THashSet<TString> resolvedNames;
    for (auto& partition : result) {
        TInfoUnit resolved(partition.Name);
        for (const auto& renameMap : metadata.RenameHistory) {
            if (const auto it = renameMap.find(resolved);
                it != renameMap.end())
            {
                resolved = it->second;
            }
        }
        partition.Name = resolved.GetFullName();
        if (partition.Name.empty() ||
            !resolvedNames.insert(partition.Name).second)
        {
            Unsupported(TStringBuilder()
                << label
                << " resolved partition names must be nonempty and unique");
        }
    }
    return result;
}

TString AuditWindowMember(
    const TExprNode& member,
    const TExprNode* rowArgument,
    const THashSet<TString>& visibleColumns,
    TStringBuf label)
{
    CheckScalarSafetyMetadata(member);
    bool nullable = false;
    if (!member.IsCallable("Member") || member.ChildrenSize() != 2 ||
        member.Child(0) != rowArgument || !member.Child(1)->IsAtom() ||
        ScalarTypeName(member, &nullable) != "Decimal(35,2)" || !nullable)
    {
        Unsupported(TStringBuilder()
            << label
            << " must be one direct Optional<Decimal(35,2)> member");
    }
    CheckScalarSafetyMetadata(*member.Child(1));
    const TString result(member.Child(1)->Content());
    if (!visibleColumns.contains(result)) {
        Unsupported(TStringBuilder()
            << label << " references unavailable column " << result);
    }
    return result;
}

TString AuditWholePartitionWindowCall(
    const TExprNode& window,
    const TExprNode* rowArgument,
    const THashSet<TString>& visibleColumns,
    TStringBuf expectedFactory,
    TStringBuf label)
{
    CheckScalarSafetyMetadata(window);
    bool windowNullable = false;
    if (!window.IsCallable("YqlAggWin") || window.ChildrenSize() != 5 ||
        ScalarTypeName(window, &windowNullable) != "Decimal(35,2)" ||
        !windowNullable)
    {
        Unsupported(TStringBuilder()
            << label
            << " requires an Optional<Decimal(35,2)> YqlAggWin");
    }

    const auto& factory = *window.Child(0);
    CheckScalarSafetyMetadata(factory);
    if (!factory.IsCallable("YqlWinFactory") ||
        factory.ChildrenSize() != 1 ||
        !factory.Child(0)->IsAtom(expectedFactory) ||
        !factory.GetTypeAnn() ||
        factory.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Unit)
    {
        Unsupported(TStringBuilder()
            << label << " requires the exact " << expectedFactory
            << " YqlWinFactory");
    }
    CheckScalarSafetyMetadata(*factory.Child(0));

    const auto& name = *window.Child(1);
    CheckScalarSafetyMetadata(name);
    if (!name.IsAtom() || name.Content().empty()) {
        Unsupported(TStringBuilder()
            << label << " has an invalid window name");
    }

    const auto& options = *window.Child(2);
    CheckScalarSafetyMetadata(options);
    if (!options.IsList() || options.ChildrenSize() != 0) {
        Unsupported(TStringBuilder()
            << label << " does not admit aggregation options");
    }

    bool descriptorNullable = false;
    if (DataTypeDescriptorName(
            *window.Child(3), &descriptorNullable) != "Decimal(35,2)" ||
        !descriptorNullable ||
        !IsSameAnnotation(
            DescribedType(
                *window.Child(3),
                WindowContext(label, " result descriptor")),
            *window.GetTypeAnn()))
    {
        Unsupported(TStringBuilder()
            << label
            << " descriptor must exactly match Optional<Decimal(35,2)>");
    }

    return AuditWindowMember(
        *window.Child(4), rowArgument, visibleColumns,
        WindowContext(label, " input"));
}

const TExprNode* AuditWholePartitionWindowLambda(
    const TExpression& expression,
    TStringBuf label)
{
    if (!expression.Node || !expression.Node->IsLambda() ||
        expression.Node->ChildrenSize() != 2 ||
        !expression.Node->Child(0)->IsArguments() ||
        expression.Node->Child(0)->ChildrenSize() != 1 ||
        !expression.Node->Child(0)->Child(0)->IsArgument())
    {
        Unsupported(TStringBuilder()
            << label << " is not a one-body row lambda");
    }
    CheckScalarSafetyMetadata(*expression.Node);
    CheckScalarSafetyMetadata(*expression.Node->Child(0));
    CheckScalarSafetyMetadata(*expression.Node->Child(0)->Child(0));
    return expression.Node->Child(0)->Child(0);
}

constexpr TStringBuf GlobalRankWindowNamePrefix =
    "_yql_anonymous_window";
constexpr ui32 MaxGlobalRankWindowOrdinal = 5;

ui32 AuditGlobalRankWindowName(TStringBuf windowName) {
    if (!windowName.StartsWith(GlobalRankWindowNamePrefix)) {
        Unsupported(
            "Global rank window name must use the exact q49 anonymous prefix");
    }
    const TStringBuf suffix =
        windowName.SubStr(GlobalRankWindowNamePrefix.size());
    const ui32 ordinal = ParseInteger<ui32>(
        suffix,
        "global rank window ordinal");
    if (suffix != ToString(ordinal) ||
        ordinal > MaxGlobalRankWindowOrdinal)
    {
        Unsupported(
            "Global rank window name has a noncanonical or out-of-range ordinal");
    }
    return ordinal;
}

TString AuditGlobalRankWindowDefinition(
    const TExpression& expression,
    TStringBuf windowName,
    const THashSet<TString>& visibleColumns)
{
    const auto& metadata = expression.GetWindowMetadata();
    if (!metadata || !metadata->Definition) {
        Unsupported("Global rank expression has no source window metadata");
    }
    const auto& definition = *metadata->Definition;
    CheckExactWindowSafetyTree(definition);
    if (!definition.IsCallable("YqlWindow") ||
        definition.ChildrenSize() != 5)
    {
        Unsupported("Global rank requires an exact five-child YqlWindow");
    }

    const auto& definitionName = *definition.Child(0);
    if (!definitionName.IsAtom(windowName)) {
        Unsupported("Global rank definition name does not match YqlWin");
    }
    CheckExactWindowAtom(
        *definition.Child(1),
        "",
        "Global rank inherited window");

    const auto& partitions = *definition.Child(2);
    if (!partitions.IsList() || partitions.ChildrenSize() != 0 ||
        !expression.GetWindowPartitionBy().empty())
    {
        Unsupported("Global rank requires an empty partition list");
    }

    const auto& order = *definition.Child(3);
    if (!order.IsList() || order.ChildrenSize() != 1) {
        Unsupported("Global rank requires exactly one order expression");
    }
    const auto& sort = *order.Child(0);
    if (!sort.IsCallable("YqlSort") || sort.ChildrenSize() != 4) {
        Unsupported("Global rank order must be one exact YqlSort");
    }

    const auto& rowDescriptor = *sort.Child(0);
    if (!rowDescriptor.IsCallable("StructType") ||
        rowDescriptor.ChildrenSize() != 1)
    {
        Unsupported(
            "Global rank order row descriptor must contain exactly one field");
    }
    const auto& field = *rowDescriptor.Child(0);
    if (!field.IsList() || field.ChildrenSize() != 2 ||
        !field.Child(0)->IsAtom() || field.Child(0)->Content().empty())
    {
        Unsupported("Global rank order field descriptor is not canonical");
    }
    bool fieldNullable = false;
    if (DataTypeDescriptorName(*field.Child(1), &fieldNullable) !=
            "Decimal(15,4)" ||
        fieldNullable)
    {
        Unsupported(
            "Global rank order descriptor must be non-null Decimal(15,4)");
    }
    const auto& describedRow = DescribedType(
        rowDescriptor,
        "Global rank order row descriptor");
    if (describedRow.GetKind() != ETypeAnnotationKind::Struct) {
        Unsupported("Global rank order row descriptor must describe Struct");
    }
    const auto& rowItems = describedRow.Cast<TStructExprType>()->GetItems();
    if (rowItems.size() != 1 ||
        rowItems.front()->GetName() != field.Child(0)->Content() ||
        TypeName(rowItems.front()->GetItemType()) != "Decimal(15,4)" ||
        !IsSameAnnotation(
            DescribedType(
                *field.Child(1),
                "Global rank order field descriptor"),
            *rowItems.front()->GetItemType()))
    {
        Unsupported("Global rank order row descriptor annotation disagrees");
    }

    const auto& lambda = *sort.Child(1);
    if (!lambda.IsLambda() || lambda.ChildrenSize() != 2 ||
        !lambda.Child(0)->IsArguments() ||
        lambda.Child(0)->ChildrenSize() != 1 ||
        !lambda.Child(0)->Child(0)->IsArgument())
    {
        Unsupported("Global rank order must be one unary lambda");
    }
    const auto& argument = *lambda.Child(0)->Child(0);
    if (!argument.GetTypeAnn() ||
        !IsSameAnnotation(*argument.GetTypeAnn(), describedRow))
    {
        Unsupported(
            "Global rank order lambda argument disagrees with its row descriptor");
    }
    const auto& member = *lambda.Child(1);
    bool memberNullable = false;
    if (!member.IsCallable("Member") || member.ChildrenSize() != 2 ||
        member.Child(0) != &argument || !member.Child(1)->IsAtom() ||
        member.Child(1)->Content() != field.Child(0)->Content() ||
        ScalarTypeName(member, &memberNullable) != "Decimal(15,4)" ||
        memberNullable)
    {
        Unsupported(
            "Global rank order must be one direct non-null Decimal(15,4) Member");
    }
    CheckExactWindowAtom(*sort.Child(2), "asc", "Global rank direction");
    CheckExactWindowAtom(*sort.Child(3), "first", "Global rank NULL order");

    const auto& frame = *definition.Child(4);
    if (!frame.IsList() || frame.ChildrenSize() != 4) {
        Unsupported(
            "Global rank requires the exact cumulative ROWS frame");
    }
    const std::array<std::pair<TStringBuf, TStringBuf>, 3> settings = {{
        {"type", "rows"},
        {"from", "up"},
        {"to", "f"},
    }};
    for (size_t index = 0; index < settings.size(); ++index) {
        const auto& setting = *frame.Child(index);
        if (!setting.IsList() || setting.ChildrenSize() != 2) {
            Unsupported("Global rank has a malformed frame setting");
        }
        CheckExactWindowAtom(
            *setting.Child(0),
            settings[index].first,
            "Global rank frame setting name");
        CheckExactWindowAtom(
            *setting.Child(1),
            settings[index].second,
            "Global rank frame setting value");
    }
    const auto& currentRow = *frame.Child(3);
    if (!currentRow.IsList() || currentRow.ChildrenSize() != 2) {
        Unsupported("Global rank current-row frame setting is malformed");
    }
    CheckExactWindowAtom(
        *currentRow.Child(0),
        "to_value",
        "Global rank current-row setting name");
    const auto& zero = *currentRow.Child(1);
    if (!zero.IsCallable("Int32") || zero.ChildrenSize() != 1 ||
        !zero.Child(0)->IsAtom("0") ||
        !IsExactDataAnnotation(
            zero.GetTypeAnn(),
            NUdf::EDataSlot::Int32,
            false))
    {
        Unsupported("Global rank frame endpoint must be exact Int32(0)");
    }
    LiteralExpr(zero);

    TInfoUnit resolved(TString(member.Child(1)->Content()));
    for (const auto& renameMap : metadata->RenameHistory) {
        if (const auto it = renameMap.find(resolved);
            it != renameMap.end())
        {
            resolved = it->second;
        }
    }
    const TString resolvedName = resolved.GetFullName();
    const auto resolvedByApi = expression.GetWindowOrderBy();
    if (resolvedName.empty() || resolvedByApi.size() != 1 ||
        resolvedByApi.front().GetFullName() != resolvedName ||
        !visibleColumns.contains(resolvedName))
    {
        Unsupported(
            "Global rank resolved order key is unavailable or disagrees with metadata");
    }
    return resolvedName;
}

TGlobalRankWindow ExportGlobalRankWindow(
    const TExpression& expression,
    const THashSet<TString>& visibleColumns)
{
    const auto* rowArgument =
        AuditWholePartitionWindowLambda(expression, "Global rank");
    Y_UNUSED(rowArgument);
    const auto& window = *expression.GetExpressionBody();
    CheckExactWindowSafetyTree(window);
    bool resultNullable = false;
    if (!window.IsCallable("YqlWin") || window.ChildrenSize() != 4 ||
        ScalarTypeName(window, &resultNullable) != "Uint64" ||
        resultNullable)
    {
        Unsupported("Global rank requires one direct non-null Uint64 YqlWin");
    }
    CheckExactWindowAtom(*window.Child(0), "rank", "Global rank function");
    const auto& name = *window.Child(1);
    if (!name.IsAtom() || name.Content().empty()) {
        Unsupported("Global rank has an invalid window name");
    }
    const TString windowName(name.Content());
    const ui32 sourceOrdinal = AuditGlobalRankWindowName(windowName);
    const auto& options = *window.Child(2);
    if (!options.IsList() || options.ChildrenSize() != 0) {
        Unsupported("Global rank does not admit function options");
    }
    bool descriptorNullable = false;
    if (DataTypeDescriptorName(
            *window.Child(3),
            &descriptorNullable) != "Uint64" ||
        descriptorNullable ||
        !IsSameAnnotation(
            DescribedType(
                *window.Child(3),
                "Global rank result descriptor"),
            *window.GetTypeAnn()))
    {
        Unsupported(
            "Global rank descriptor must exactly match non-null Uint64");
    }

    const TString orderColumn = AuditGlobalRankWindowDefinition(
        expression,
        windowName,
        visibleColumns);
    auto orderBy = JsonArray();
    auto orderItem = JsonMap();
    orderItem["column"] = orderColumn;
    orderItem["ascending"] = true;
    orderItem["nulls_first"] = true;
    orderBy.AppendValue(std::move(orderItem));

    auto result = JsonMap();
    result["kind"] = "window_rank";
    result["window_name"] = windowName;
    result["partition_by"] = JsonArray();
    result["order_by"] = std::move(orderBy);
    result["frame"] = "rows_unbounded_preceding_current_row";
    result["type"] = "Uint64";
    result["nullable"] = false;
    return {
        .Expression = std::move(result),
        .WindowName = windowName,
        .OrderColumn = orderColumn,
        .SourceOrdinal = sourceOrdinal,
    };
}

constexpr ui32 MaxQ51WindowOrdinal = 3;

TStringBuf Q51WindowLabel(EQ51WindowFunction function) {
    return function == EQ51WindowFunction::Sum
        ? TStringBuf("q51 running SUM")
        : TStringBuf("q51 running MAX");
}

ui32 AuditQ51WindowName(TStringBuf windowName) {
    if (!windowName.StartsWith(GlobalRankWindowNamePrefix)) {
        Unsupported(
            "q51 window name must use the exact anonymous-window prefix");
    }
    const TStringBuf suffix =
        windowName.SubStr(GlobalRankWindowNamePrefix.size());
    const ui32 ordinal = ParseInteger<ui32>(suffix, "q51 window ordinal");
    if (suffix != ToString(ordinal) || ordinal > MaxQ51WindowOrdinal) {
        Unsupported(
            "q51 window name has a noncanonical or out-of-range ordinal");
    }
    return ordinal;
}

TString AuditQ51WindowDefinitionKey(
    const TExprNode& rowDescriptor,
    const TExprNode& lambda,
    EQ51WindowFunction function,
    TStringBuf expectedType,
    NUdf::EDataSlot expectedSlot,
    bool expectedNullable,
    ui32 expectedIndex,
    TStringBuf context)
{
    const TStringBuf label = Q51WindowLabel(function);
    const TString rowDescriptorContext = TStringBuilder()
        << label << " " << context << " row descriptor";
    const TString fieldDescriptorContext = TStringBuilder()
        << label << " " << context << " field descriptor";
    const TString indexContext = TStringBuilder()
        << label << " " << context << " index";
    const TString referenceDescriptorContext = TStringBuilder()
        << label << " " << context << " reference descriptor";
    if (!rowDescriptor.IsCallable("StructType") ||
        rowDescriptor.ChildrenSize() != 1)
    {
        Unsupported(TStringBuilder()
            << label << " " << context
            << " row descriptor must contain exactly one field");
    }
    const auto& field = *rowDescriptor.Child(0);
    if (!field.IsList() || field.ChildrenSize() != 2 ||
        !field.Child(0)->IsAtom() || field.Child(0)->Content().empty())
    {
        Unsupported(TStringBuilder()
            << label << " " << context
            << " field descriptor is not canonical");
    }
    bool fieldNullable = false;
    if (DataTypeDescriptorName(*field.Child(1), &fieldNullable) !=
            expectedType ||
        fieldNullable != expectedNullable)
    {
        Unsupported(TStringBuilder()
            << label << " " << context << " field must be exact "
            << (expectedNullable ? TStringBuf("Optional<") : TStringBuf())
            << expectedType
            << (expectedNullable ? TStringBuf(">") : TStringBuf()));
    }

    const auto& describedRow = DescribedType(
        rowDescriptor,
        rowDescriptorContext);
    if (describedRow.GetKind() != ETypeAnnotationKind::Struct) {
        Unsupported(TStringBuilder()
            << label << " " << context
            << " row descriptor must describe Struct");
    }
    const auto& rowItems = describedRow.Cast<TStructExprType>()->GetItems();
    if (rowItems.size() != 1 ||
        rowItems.front()->GetName() != field.Child(0)->Content() ||
        !IsExactDataAnnotation(
            rowItems.front()->GetItemType(),
            expectedSlot,
            expectedNullable) ||
        !IsSameAnnotation(
            DescribedType(
                *field.Child(1),
                fieldDescriptorContext),
            *rowItems.front()->GetItemType()))
    {
        Unsupported(TStringBuilder()
            << label << " " << context
            << " row descriptor annotation disagrees");
    }

    if (!lambda.IsLambda() || lambda.ChildrenSize() != 2 ||
        !lambda.Child(0)->IsArguments() ||
        lambda.Child(0)->ChildrenSize() != 1 ||
        !lambda.Child(0)->Child(0)->IsArgument())
    {
        Unsupported(TStringBuilder()
            << label << " " << context << " must be one unary lambda");
    }
    const auto& argument = *lambda.Child(0)->Child(0);
    if (!argument.GetTypeAnn() ||
        !IsSameAnnotation(*argument.GetTypeAnn(), describedRow))
    {
        Unsupported(TStringBuilder()
            << label << " " << context
            << " lambda argument disagrees with its row descriptor");
    }

    const auto& reference = *lambda.Child(1);
    bool referenceNullable = false;
    if (function == EQ51WindowFunction::Sum) {
        if (!reference.IsCallable("YqlGroupRef") ||
            reference.ChildrenSize() != 4 ||
            reference.Child(0) != &argument ||
            !reference.Child(2)->IsAtom() ||
            !reference.Child(3)->IsAtom() ||
            reference.Child(3)->Content().empty() ||
            reference.Child(3)->Content() != field.Child(0)->Content())
        {
            Unsupported(TStringBuilder()
                << label << " " << context
                << " must be one direct named YqlGroupRef");
        }
        const ui32 index = ParseInteger<ui32>(
            reference.Child(2)->Content(),
            indexContext);
        if (reference.Child(2)->Content() != ToString(index) ||
            index != expectedIndex)
        {
            Unsupported(TStringBuilder()
                << label << " " << context
                << " has a noncanonical source index");
        }
        bool descriptorNullable = false;
        if (DataTypeDescriptorName(
                *reference.Child(1),
                &descriptorNullable) != expectedType ||
            descriptorNullable != expectedNullable ||
            !reference.GetTypeAnn() ||
            ScalarTypeName(reference, &referenceNullable) != expectedType ||
            referenceNullable != expectedNullable ||
            !IsSameAnnotation(
                DescribedType(
                    *reference.Child(1),
                    referenceDescriptorContext),
                *reference.GetTypeAnn()))
        {
            Unsupported(TStringBuilder()
                << label << " " << context
                << " reference type disagrees");
        }
    } else {
        if (!reference.IsCallable("Member") ||
            reference.ChildrenSize() != 2 ||
            reference.Child(0) != &argument ||
            !reference.Child(1)->IsAtom() ||
            reference.Child(1)->Content().empty() ||
            reference.Child(1)->Content() != field.Child(0)->Content() ||
            !reference.GetTypeAnn() ||
            ScalarTypeName(reference, &referenceNullable) != expectedType ||
            referenceNullable != expectedNullable)
        {
            Unsupported(TStringBuilder()
                << label << " " << context
                << " must be one exact direct Member");
        }
    }
    return TString(field.Child(0)->Content());
}

TString ResolveQ51WindowColumn(
    TString source,
    const TExpression::TWindowMetadata& metadata)
{
    TInfoUnit resolved(std::move(source));
    for (const auto& renameMap : metadata.RenameHistory) {
        if (const auto it = renameMap.find(resolved); it != renameMap.end()) {
            resolved = it->second;
        }
    }
    return resolved.GetFullName();
}

std::pair<TString, TString> AuditQ51WindowDefinition(
    const TExpression& expression,
    TStringBuf windowName,
    EQ51WindowFunction function,
    const THashSet<TString>& visibleColumns)
{
    const TStringBuf label = Q51WindowLabel(function);
    const auto& metadata = expression.GetWindowMetadata();
    if (!metadata || !metadata->Definition) {
        Unsupported(TStringBuilder()
            << label << " expression has no source window metadata");
    }
    const auto& definition = *metadata->Definition;
    CheckExactWindowSafetyTree(definition);
    if (!definition.IsCallable("YqlWindow") ||
        definition.ChildrenSize() != 5)
    {
        Unsupported(TStringBuilder()
            << label << " requires an exact five-child YqlWindow");
    }
    if (!definition.Child(0)->IsAtom(windowName)) {
        Unsupported(TStringBuilder()
            << label << " definition name does not match YqlAggWin");
    }
    CheckExactWindowAtom(
        *definition.Child(1),
        "",
        WindowContext(label, " inherited window"));

    const auto& partitions = *definition.Child(2);
    if (!partitions.IsList() || partitions.ChildrenSize() != 1) {
        Unsupported(TStringBuilder()
            << label << " requires exactly one partition expression");
    }
    const auto& group = *partitions.Child(0);
    if (!group.IsCallable("YqlGroup") || group.ChildrenSize() != 2) {
        Unsupported(TStringBuilder()
            << label << " partition must be one exact YqlGroup");
    }
    TString partition = AuditQ51WindowDefinitionKey(
        *group.Child(0),
        *group.Child(1),
        function,
        "Int64",
        NUdf::EDataSlot::Int64,
        function == EQ51WindowFunction::Max,
        0,
        "partition");

    const auto& order = *definition.Child(3);
    if (!order.IsList() || order.ChildrenSize() != 1) {
        Unsupported(TStringBuilder()
            << label << " requires exactly one order expression");
    }
    const auto& sort = *order.Child(0);
    if (!sort.IsCallable("YqlSort") || sort.ChildrenSize() != 4) {
        Unsupported(TStringBuilder()
            << label << " order must be one exact YqlSort");
    }
    TString orderColumn = AuditQ51WindowDefinitionKey(
        *sort.Child(0),
        *sort.Child(1),
        function,
        "Date",
        NUdf::EDataSlot::Date,
        true,
        1,
        "order");
    CheckExactWindowAtom(
        *sort.Child(2),
        "asc",
        WindowContext(label, " direction"));
    CheckExactWindowAtom(
        *sort.Child(3),
        "first",
        WindowContext(label, " NULL order"));

    const auto& frame = *definition.Child(4);
    if (!frame.IsList() || frame.ChildrenSize() != 4) {
        Unsupported(TStringBuilder()
            << label << " requires the exact cumulative ROWS frame");
    }
    const std::array<std::pair<TStringBuf, TStringBuf>, 3> settings = {{
        {"type", "rows"},
        {"from", "up"},
        {"to", "f"},
    }};
    for (size_t index = 0; index < settings.size(); ++index) {
        const auto& setting = *frame.Child(index);
        if (!setting.IsList() || setting.ChildrenSize() != 2) {
            Unsupported(TStringBuilder()
                << label << " has a malformed frame setting");
        }
        CheckExactWindowAtom(
            *setting.Child(0),
            settings[index].first,
            WindowContext(label, " frame setting name"));
        CheckExactWindowAtom(
            *setting.Child(1),
            settings[index].second,
            WindowContext(label, " frame setting value"));
    }
    const auto& currentRow = *frame.Child(3);
    if (!currentRow.IsList() || currentRow.ChildrenSize() != 2) {
        Unsupported(TStringBuilder()
            << label << " current-row frame setting is malformed");
    }
    CheckExactWindowAtom(
        *currentRow.Child(0),
        "to_value",
        WindowContext(label, " current-row setting name"));
    const auto& zero = *currentRow.Child(1);
    if (!zero.IsCallable("Int32") || zero.ChildrenSize() != 1 ||
        !zero.Child(0)->IsAtom("0") ||
        !IsExactDataAnnotation(
            zero.GetTypeAnn(),
            NUdf::EDataSlot::Int32,
            false))
    {
        Unsupported(TStringBuilder()
            << label << " frame endpoint must be exact Int32(0)");
    }
    LiteralExpr(zero);

    partition = ResolveQ51WindowColumn(std::move(partition), *metadata);
    orderColumn = ResolveQ51WindowColumn(std::move(orderColumn), *metadata);
    const auto resolvedPartitions = expression.GetWindowPartitionBy();
    const auto resolvedOrder = expression.GetWindowOrderBy();
    if (partition.empty() || orderColumn.empty() ||
        partition == orderColumn ||
        resolvedPartitions.size() != 1 ||
        resolvedPartitions.front().GetFullName() != partition ||
        resolvedOrder.size() != 1 ||
        resolvedOrder.front().GetFullName() != orderColumn ||
        !visibleColumns.contains(partition) ||
        !visibleColumns.contains(orderColumn))
    {
        Unsupported(TStringBuilder()
            << label
            << " resolved partition/order keys are unavailable or disagree "
               "with metadata");
    }
    return {std::move(partition), std::move(orderColumn)};
}

TQ51Window ExportQ51Window(
    const TExpression& expression,
    const THashSet<TString>& visibleColumns)
{
    const auto* rowArgument =
        AuditWholePartitionWindowLambda(expression, "q51 ROWS window");
    const auto& window = *expression.GetExpressionBody();
    CheckExactWindowSafetyTree(window);
    bool resultNullable = false;
    if (!window.IsCallable("YqlAggWin") || window.ChildrenSize() != 5 ||
        ScalarTypeName(window, &resultNullable) != "Decimal(35,2)" ||
        !resultNullable)
    {
        Unsupported(
            "q51 ROWS window requires one direct Optional<Decimal(35,2)> YqlAggWin");
    }

    const auto& factory = *window.Child(0);
    if (!factory.IsCallable("YqlWinFactory") ||
        factory.ChildrenSize() != 1 || !factory.Child(0)->IsAtom() ||
        !factory.GetTypeAnn() ||
        factory.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Unit)
    {
        Unsupported("q51 ROWS window has a malformed YqlWinFactory");
    }
    const TStringBuf factoryName = factory.Child(0)->Content();
    const EQ51WindowFunction function = factoryName == "sum"
        ? EQ51WindowFunction::Sum
        : EQ51WindowFunction::Max;
    if (factoryName != "sum" && factoryName != "max") {
        Unsupported("q51 ROWS window requires exact sum or max factory");
    }

    const auto& name = *window.Child(1);
    if (!name.IsAtom() || name.Content().empty()) {
        Unsupported("q51 ROWS window has an invalid window name");
    }
    const TString windowName(name.Content());
    const ui32 sourceOrdinal = AuditQ51WindowName(windowName);

    const auto& options = *window.Child(2);
    if (!options.IsList() || options.ChildrenSize() != 0) {
        Unsupported("q51 ROWS window does not admit aggregation options");
    }
    bool descriptorNullable = false;
    if (DataTypeDescriptorName(
            *window.Child(3),
            &descriptorNullable) != "Decimal(35,2)" ||
        !descriptorNullable ||
        !IsSameAnnotation(
            DescribedType(
                *window.Child(3),
                "q51 ROWS window result descriptor"),
            *window.GetTypeAnn()))
    {
        Unsupported(
            "q51 ROWS window descriptor must exactly match Optional<Decimal(35,2)>");
    }
    const TString input = AuditWindowMember(
        *window.Child(4),
        rowArgument,
        visibleColumns,
        "q51 ROWS window input");
    auto [partition, orderColumn] = AuditQ51WindowDefinition(
        expression,
        windowName,
        function,
        visibleColumns);

    auto partitionBy = JsonArray();
    partitionBy.AppendValue(partition);
    auto orderBy = JsonArray();
    auto orderItem = JsonMap();
    orderItem["column"] = orderColumn;
    orderItem["ascending"] = true;
    orderItem["nulls_first"] = true;
    orderBy.AppendValue(std::move(orderItem));

    auto result = JsonMap();
    result["kind"] = function == EQ51WindowFunction::Sum
        ? "window_rows_sum"
        : "window_rows_max";
    result["input"] = input;
    result["partition_by"] = std::move(partitionBy);
    result["order_by"] = std::move(orderBy);
    result["frame"] = "rows_unbounded_preceding_current_row";
    result["window_name"] = windowName;
    result["type"] = "Decimal(35,2)";
    result["nullable"] = true;
    return {
        .Expression = std::move(result),
        .Input = input,
        .PartitionColumn = std::move(partition),
        .OrderColumn = std::move(orderColumn),
        .WindowName = windowName,
        .Function = function,
        .SourceOrdinal = sourceOrdinal,
    };
}

TWholePartitionWindow ExportWholePartitionWindowSum(
    const TExpression& expression,
    const THashSet<TString>& visibleColumns)
{
    const auto& metadata = expression.GetWindowMetadata();
    if (!metadata) {
        Unsupported("Window sum expression has no source metadata");
    }
    const auto* rowArgument =
        AuditWholePartitionWindowLambda(expression, "Window sum");

    const auto& root = *expression.GetExpressionBody();
    CheckExactWindowSafetyTree(root);
    if (!root.IsCallable("DecimalDiv") || root.ChildrenSize() != 2 ||
        !root.Child(1)->IsCallable("YqlAggWin"))
    {
        Unsupported(
            "Window sum ratio must divide directly by one YqlAggWin result");
    }
    const auto& window = *root.Child(1);
    const TString input = AuditWholePartitionWindowCall(
        window, rowArgument, visibleColumns, "sum", "Window sum");
    const auto partitions = AuditWholePartitionWindowDefinition(
        *metadata,
        window.Child(1)->Content(),
        EWholePartitionWindowFunction::Sum);
    Y_ENSURE(partitions.size() == 1);

    const auto rootSignature = CheckDecimalArithmeticCallable(root);
    if (rootSignature.ResultType != "Decimal(35,2)" ||
        !rootSignature.ResultNullable)
    {
        Unsupported(
            "Window sum ratio must return Optional<Decimal(35,2)>");
    }

    const auto& numerator = *root.Child(0);
    CheckScalarSafetyMetadata(numerator);
    if (!numerator.IsCallable("DecimalMul") ||
        numerator.ChildrenSize() != 2)
    {
        Unsupported(
            "Window sum ratio numerator must be one DecimalMul");
    }
    const auto numeratorSignature =
        CheckDecimalArithmeticCallable(numerator);
    if (numeratorSignature.ResultType != "Decimal(35,2)" ||
        !numeratorSignature.ResultNullable ||
        AuditWindowMember(
            *numerator.Child(0),
            rowArgument,
            visibleColumns,
            "Window sum ratio numerator") != input)
    {
        Unsupported(
            "Window sum ratio numerator must reuse its aggregate input");
    }
    if (!numerator.Child(1)->IsCallable("Int32") ||
        numerator.Child(1)->ChildrenSize() != 1 ||
        !numerator.Child(1)->Child(0)->IsAtom("100") ||
        !IsExactDataAnnotation(
            numerator.Child(1)->GetTypeAnn(),
            NUdf::EDataSlot::Int32,
            false))
    {
        Unsupported(
            "Window sum ratio multiplier must be exact non-null Int32(100)");
    }
    LiteralExpr(*numerator.Child(1));

    TExactScalarBudget budget;
    budget.Charge(1);
    auto windowExpr = JsonMap();
    windowExpr["kind"] = "window_sum";
    windowExpr["input"] = input;
    windowExpr["partition_by"] = partitions.front().Name;
    windowExpr["type"] = "Decimal(35,2)";
    windowExpr["nullable"] = true;
    budget.Charge(2);

    auto result = BinaryExpr(
        "div",
        ExportExprNode(
            numerator,
            rowArgument,
            visibleColumns,
            {},
            budget,
            2,
            2),
        std::move(windowExpr));
    result["type"] = rootSignature.ResultType;
    result["nullable"] = rootSignature.ResultNullable;
    AuditExactScalarExpression(result);
    return {
        std::move(result),
        input,
        partitions,
        EWholePartitionWindowFunction::Sum,
    };
}

TWholePartitionWindow ExportWholePartitionWindowAvg(
    const TExpression& expression,
    const THashSet<TString>& visibleColumns)
{
    const auto& metadata = expression.GetWindowMetadata();
    if (!metadata) {
        Unsupported("Window avg expression has no source metadata");
    }
    const auto* rowArgument =
        AuditWholePartitionWindowLambda(expression, "Window avg");
    const auto& window = *expression.GetExpressionBody();
    CheckExactWindowSafetyTree(window);
    const TString input = AuditWholePartitionWindowCall(
        window, rowArgument, visibleColumns, "avg", "Window avg");
    const auto partitions = AuditWholePartitionWindowDefinition(
        *metadata,
        window.Child(1)->Content(),
        EWholePartitionWindowFunction::Avg);

    auto partitionBy = JsonArray();
    for (const auto& partition : partitions) {
        partitionBy.AppendValue(partition.Name);
    }
    auto result = JsonMap();
    result["kind"] = "window_avg";
    result["input"] = input;
    result["partition_by"] = std::move(partitionBy);
    result["type"] = "Decimal(35,2)";
    result["nullable"] = true;
    AuditExactScalarExpression(result);
    return {
        std::move(result),
        input,
        partitions,
        EWholePartitionWindowFunction::Avg,
    };
}

TWholePartitionWindow ExportWholePartitionWindow(
    const TExpression& expression,
    const THashSet<TString>& visibleColumns)
{
    if (expression.GetExpressionBody()->IsCallable("YqlAggWin")) {
        return ExportWholePartitionWindowAvg(expression, visibleColumns);
    }
    return ExportWholePartitionWindowSum(expression, visibleColumns);
}
