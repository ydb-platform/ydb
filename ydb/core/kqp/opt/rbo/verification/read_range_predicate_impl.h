// Included exactly once inside semantic_snapshot.cpp's anonymous namespace.
// This closed matcher intentionally depends on the scalar safety, JSON, and
// read-column helpers declared immediately above its include site.

class TExactReadRangePredicateExporter {
private:
    static constexpr ui64 ExtractorRangeCap = 10'000;
    static constexpr ui64 ExtractorOverflowProbe = ExtractorRangeCap + 1;
    static constexpr size_t MaxFinitePointSet = 64;

public:
    TExactReadRangePredicateExporter(
        const TOpRead& read,
        const TSemanticSnapshotCatalogTableV1& table,
        const TOpRead::TRangeInfo& range,
        const TOlapColumnMap& columns)
        : Read(read)
        , Table(table)
        , Range(range)
        , Columns(columns)
    {
    }

    NJson::TJsonValue Export() {
        if (Range.KeyColumns.size() != 1 || Range.UsedPrefixLen != 1) {
            Unsupported(
                "Read range must constrain exactly one complete key prefix");
        }
        if (Table.UniqueKeys.size() != 1 ||
            Table.UniqueKeys.front().NullsDistinct ||
            Table.UniqueKeys.front().Columns.size() != 1)
        {
            Unsupported(
                "Read range requires one exact single-column primary key");
        }
        const TString& physicalKey =
            Table.UniqueKeys.front().Columns.front();
        const TSemanticSnapshotCatalogColumnV1* catalogKey = nullptr;
        for (const auto& column : Table.Columns) {
            if (column.Name == physicalKey) {
                if (catalogKey) {
                    Unsupported(
                        "Read range primary key occurs twice in the catalog");
                }
                catalogKey = &column;
            }
        }
        if (!catalogKey || catalogKey->Type != "Int64" ||
            catalogKey->Nullable)
        {
            Unsupported(
                "Read range primary key must be a catalog non-null Int64");
        }

        size_t physicalKeyMappings = 0;
        for (size_t index = 0; index < Read.Columns.size(); ++index) {
            if (Read.Columns[index] == physicalKey) {
                ++physicalKeyMappings;
                KeyOutput = Read.OutputIUs[index].GetFullName();
            }
        }
        if (physicalKeyMappings != 1 || KeyOutput.empty()) {
            Unsupported(
                "Read range must emit its physical primary key exactly once");
        }

        const auto* physical =
            ResolveOlapColumn(physicalKey, Columns);
        const auto* described =
            ResolveOlapColumn(Range.KeyColumns.front(), Columns);
        if (physical->Output != KeyOutput ||
            physical->Type != catalogKey->Type ||
            physical->Nullable != catalogKey->Nullable ||
            described->Output != KeyOutput ||
            described->Type != catalogKey->Type ||
            described->Nullable != catalogKey->Nullable)
        {
            Unsupported(
                "Read range KeyColumns does not identify its emitted physical primary key");
        }

        const auto& finalize = Callable(
            Range.ComputeNode,
            "RangeFinalize",
            1,
            1);
        ExactInt64RangeListType(
            finalize,
            true,
            "RangeFinalize result");
        const auto& multiply = Callable(
            finalize.ChildPtr(0),
            "RangeMultiply",
            2,
            2);
        ExactInt64RangeListType(
            multiply,
            false,
            "outer RangeMultiply result");
        Uint64Literal(*multiply.Child(0), ExtractorRangeCap, "outer range cap", 3);
        const auto& rangeUnion = Callable(
            multiply.ChildPtr(1),
            "RangeUnion",
            1,
            3);
        ExactInt64RangeListType(
            rangeUnion,
            false,
            "outer RangeUnion result");

        NJson::TJsonValue result;
        const auto& body = *rangeUnion.Child(0);
        if (body.IsCallable("RangeFor")) {
            if (rangeUnion.ChildrenSize() != 1) {
                Unsupported("Point read range has more than one union argument");
            }
            result = ExportPoint(body);
            CheckExpectedMaxRanges(1);
        } else if (body.IsCallable("IfPresent")) {
            if (rangeUnion.ChildrenSize() != 1) {
                Unsupported(
                    "Finite point-set read range has more than one union argument");
            }
            result = ExportFinitePointSet(body);
        } else {
            Unsupported(
                "Read range is not an audited point or finite point-set shape");
        }

        AuditExactScalarExpression(result);
        return result;
    }

private:
    const TExprNode& Node(
        const TExprNode::TPtr& node,
        size_t depth,
        bool allowUnorderedChildren = false)
    {
        if (!node) {
            Unsupported("Read range contains a null expression node");
        }
        return Node(*node, depth, allowUnorderedChildren);
    }

    const TExprNode& Node(
        const TExprNode& node,
        size_t depth,
        bool allowUnorderedChildren = false)
    {
        SourceBudget.Charge(depth);
        CheckScalarSafetyMetadata(node, allowUnorderedChildren);
        return node;
    }

    const TExprNode& Callable(
        const TExprNode::TPtr& node,
        TStringBuf name,
        size_t minimumArity,
        size_t maximumArity,
        size_t depth = 1)
    {
        const auto& result = Node(node, depth);
        if (!result.IsCallable(name) ||
            result.ChildrenSize() < minimumArity ||
            result.ChildrenSize() > maximumArity)
        {
            Unsupported(TStringBuilder()
                << "Read range expected " << name << " with arity in ["
                << minimumArity << ", " << maximumArity << "]");
        }
        return result;
    }

    const TExprNode& Atom(
        const TExprNode::TPtr& node,
        TStringBuf value,
        TStringBuf field,
        size_t depth)
    {
        const auto& result = Node(node, depth);
        if (!result.IsAtom(value)) {
            Unsupported(TStringBuilder()
                << "Read range " << field << " must be atom " << value);
        }
        return result;
    }

    void ExactScalarType(
        const TExprNode& node,
        TStringBuf expected,
        bool expectedNullable,
        TStringBuf field)
    {
        // Range extractor wrappers are created after its private annotation
        // pipeline.  At this boundary, generated nodes may legitimately be
        // unannotated; the later physical annotation pass derives their type
        // from the exact syntax audited here.  Any annotation already present
        // is additional evidence and must agree.
        if (!node.GetTypeAnn()) {
            return;
        }
        bool nullable = false;
        if (ScalarTypeName(node, &nullable) != expected ||
            nullable != expectedNullable)
        {
            Unsupported(TStringBuilder()
                << "Read range " << field << " must have exact "
                << (expectedNullable ? "Optional<" : "")
                << expected
                << (expectedNullable ? ">" : "")
                << " type");
        }
    }

    void ExactInt64RangeListType(
        const TExprNode& node,
        bool finalized,
        TStringBuf field)
    {
        if (!node.GetTypeAnn()) {
            return;
        }
        if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::List)
        {
            Unsupported(TStringBuilder()
                << "Read range " << field << " must have exact List type");
        }
        const auto* list = node.GetTypeAnn()->Cast<TListExprType>();
        const auto* range = list->GetItemType();
        if (range->GetKind() != ETypeAnnotationKind::Tuple ||
            range->Cast<TTupleExprType>()->GetSize() != 2)
        {
            Unsupported(TStringBuilder()
                << "Read range " << field << " has an invalid range Tuple");
        }
        const auto& boundaries =
            range->Cast<TTupleExprType>()->GetItems();
        if (!IsSameAnnotation(*boundaries[0], *boundaries[1]) ||
            boundaries[0]->GetKind() != ETypeAnnotationKind::Tuple)
        {
            Unsupported(TStringBuilder()
                << "Read range " << field
                << " has mismatched boundary annotations");
        }
        const auto& components =
            boundaries[0]->Cast<TTupleExprType>()->GetItems();
        const size_t expectedComponents = finalized ? 2 : 3;
        if (components.size() != expectedComponents) {
            Unsupported(TStringBuilder()
                << "Read range " << field
                << " has the wrong boundary arity");
        }
        const auto checkComponent = [&](
            size_t index,
            TStringBuf type,
            bool nullable)
        {
            bool actualNullable = false;
            if (TypeName(components[index], &actualNullable) != type ||
                actualNullable != nullable)
            {
                Unsupported(TStringBuilder()
                    << "Read range " << field
                    << " has the wrong boundary component " << index);
            }
        };
        if (finalized) {
            checkComponent(0, "Int64", true);
            checkComponent(1, "Int32", false);
        } else {
            checkComponent(0, "Int32", false);
            checkComponent(1, "Int64", true);
            checkComponent(2, "Int32", false);
        }
    }

    void Uint64Literal(
        const TExprNode& node,
        ui64 expected,
        TStringBuf field,
        size_t depth)
    {
        Node(node, depth);
        if (!node.IsCallable("Uint64") || node.ChildrenSize() != 1) {
            Unsupported(TStringBuilder()
                << "Read range " << field << " must be a Uint64 literal");
        }
        Node(node.ChildPtr(0), depth + 1);
        ExactScalarType(node, "Uint64", false, field);
        const auto literal = LiteralExpr(node);
        if (literal["value"].GetUIntegerSafe() != expected) {
            Unsupported(TStringBuilder()
                << "Read range " << field << " must equal " << expected);
        }
    }

    NJson::TJsonValue Int32Literal(
        const TExprNode& node,
        TStringBuf field,
        size_t depth)
    {
        Node(node, depth);
        if (!node.IsCallable("Int32") || node.ChildrenSize() != 1) {
            Unsupported(TStringBuilder()
                << "Read range " << field << " must be an Int32 literal");
        }
        Node(node.ChildPtr(0), depth + 1);
        ExactScalarType(node, "Int32", false, field);
        return LiteralExpr(node);
    }

    void Int64Descriptor(
        const TExprNode& node,
        bool optional,
        TStringBuf field,
        size_t depth)
    {
        Node(node, depth);
        if (optional) {
            if (!node.IsCallable("OptionalType") ||
                node.ChildrenSize() != 1)
            {
                Unsupported(TStringBuilder()
                    << "Read range " << field
                    << " must be OptionalType(DataType(Int64))");
            }
            Node(node.ChildPtr(0), depth + 1);
            if (!node.Child(0)->IsCallable("DataType") ||
                node.Child(0)->ChildrenSize() != 1)
            {
                Unsupported(TStringBuilder()
                    << "Read range " << field
                    << " must be OptionalType(DataType(Int64))");
            }
            Node(node.Child(0)->ChildPtr(0), depth + 2);
        } else {
            if (!node.IsCallable("DataType") || node.ChildrenSize() != 1) {
                Unsupported(TStringBuilder()
                    << "Read range " << field << " must be DataType(Int64)");
            }
            Node(node.ChildPtr(0), depth + 1);
        }

        bool nullable = false;
        if (DataTypeDescriptorName(node, &nullable) != "Int64" ||
            nullable != optional)
        {
            Unsupported(TStringBuilder()
                << "Read range " << field
                << " has a mismatched Int64 type descriptor");
        }
        if (node.GetTypeAnn() &&
            node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Type)
        {
            Unsupported(TStringBuilder()
                << "Read range " << field
                << " has no exact Type annotation");
        }
        if (node.GetTypeAnn()) {
            const auto* described =
                node.GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            bool describedNullable = false;
            if (!described ||
                TypeName(described, &describedNullable) != "Int64" ||
                describedNullable != optional)
            {
                Unsupported(TStringBuilder()
                    << "Read range " << field
                    << " Type annotation disagrees with its content");
            }
        }
        if (optional && node.Child(0)->GetTypeAnn() &&
            node.Child(0)->GetTypeAnn()->GetKind() !=
                ETypeAnnotationKind::Type)
        {
            Unsupported(TStringBuilder()
                << "Read range " << field
                << " inner DataType has a non-Type annotation");
        }
        if (optional && node.Child(0)->GetTypeAnn()) {
            const auto* dataDescriptor = node.Child(0);
            const auto* dataType = dataDescriptor->GetTypeAnn()
                ->Cast<TTypeExprType>()->GetType();
            bool dataNullable = false;
            if (!dataType ||
                TypeName(dataType, &dataNullable) != "Int64" ||
                dataNullable)
            {
                Unsupported(TStringBuilder()
                    << "Read range " << field
                    << " inner DataType annotation disagrees");
            }
        }
    }

    const TExprNode& UnaryLambda(
        const TExprNode::TPtr& node,
        TStringBuf field,
        size_t depth)
    {
        const auto& lambda = Node(node, depth);
        if (!lambda.IsLambda() || lambda.ChildrenSize() != 2) {
            Unsupported(TStringBuilder()
                << "Read range " << field << " must be a unary lambda");
        }
        const auto& arguments = Node(lambda.ChildPtr(0), depth + 1);
        if (!arguments.IsArguments() || arguments.ChildrenSize() != 1) {
            Unsupported(TStringBuilder()
                << "Read range " << field << " must have one argument");
        }
        const auto& argument = Node(arguments.ChildPtr(0), depth + 2);
        if (!argument.IsArgument()) {
            Unsupported(TStringBuilder()
                << "Read range " << field << " has a non-argument binder");
        }
        return lambda;
    }

    void CheckRangeFor(
        const TExprNode& node,
        const TExprNode* expectedValue,
        size_t depth)
    {
        Node(node, depth);
        if (!node.IsCallable("RangeFor") || node.ChildrenSize() != 3) {
            Unsupported("Read range point must be a three-argument RangeFor");
        }
        ExactInt64RangeListType(node, false, "RangeFor result");
        Atom(node.ChildPtr(0), "===", "point operation", depth + 1);
        if (expectedValue && node.Child(1) != expectedValue) {
            Unsupported(
                "Read range point value must be the exact point-lambda argument");
        }
        Int64Descriptor(
            *node.Child(2),
            false,
            "point key descriptor",
            depth + 1);
    }

    NJson::TJsonValue ExportPoint(const TExprNode& node) {
        CheckRangeFor(node, nullptr, 4);
        if (!node.Child(1)->GetTypeAnn()) {
            Unsupported(
                "Read range direct point literal has no exact type annotation");
        }
        auto literal = Int32Literal(
            *node.Child(1),
            "point value",
            5);
        return BinaryExpr(
            "eq",
            ColumnExpr(KeyOutput),
            std::move(literal));
    }

    const TTypeAnnotationNode* ExactTupleItemType(
        const TExprNode& tuple,
        size_t itemCount)
    {
        if (!tuple.GetTypeAnn() ||
            tuple.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Tuple)
        {
            Unsupported(
                "Read range static point collection must have Tuple type");
        }
        const auto* tupleType = tuple.GetTypeAnn()->Cast<TTupleExprType>();
        if (tupleType->GetSize() != itemCount || itemCount == 0) {
            Unsupported(
                "Read range static point collection Tuple arity disagrees");
        }
        const auto* itemType = tupleType->GetItems().front();
        bool nullable = false;
        if (TypeName(itemType, &nullable) != "Int32" || nullable) {
            Unsupported(
                "Read range static point collection items must be non-null Int32");
        }
        for (const auto* candidate : tupleType->GetItems()) {
            if (!IsSameAnnotation(*candidate, *itemType)) {
                Unsupported(
                    "Read range static point collection has mixed item types");
            }
        }
        return itemType;
    }

    const TListExprType* ExactListType(
        const TExprNode& node,
        const TTypeAnnotationNode& itemType,
        TStringBuf field)
    {
        if (!node.GetTypeAnn()) {
            return nullptr;
        }
        if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::List)
        {
            Unsupported(TStringBuilder()
                << "Read range " << field << " must have List type");
        }
        const auto* listType = node.GetTypeAnn()->Cast<TListExprType>();
        if (!IsSameAnnotation(*listType->GetItemType(), itemType)) {
            Unsupported(TStringBuilder()
                << "Read range " << field << " has the wrong List item type");
        }
        return listType;
    }

    void ExactOptionalItemType(
        const TExprNode& node,
        const TTypeAnnotationNode& itemType,
        TStringBuf field)
    {
        if (!node.GetTypeAnn()) {
            return;
        }
        if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional ||
            !IsSameAnnotation(
                *node.GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType(),
                itemType))
        {
            Unsupported(TStringBuilder()
                << "Read range " << field << " has the wrong Optional type");
        }
    }

    void ExactOptionalListItemType(
        const TExprNode& node,
        const TTypeAnnotationNode& itemType,
        TStringBuf field)
    {
        if (!node.GetTypeAnn()) {
            return;
        }
        if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional) {
            Unsupported(TStringBuilder()
                << "Read range " << field << " must have Optional<List> type");
        }
        const auto* list = node.GetTypeAnn()
            ->Cast<TOptionalExprType>()->GetItemType();
        if (list->GetKind() != ETypeAnnotationKind::List ||
            !IsSameAnnotation(
                *list->Cast<TListExprType>()->GetItemType(),
                itemType))
        {
            Unsupported(TStringBuilder()
                << "Read range " << field
                << " has the wrong Optional<List> item type");
        }
    }

    TVector<NJson::TJsonValue> ExportStaticPointCollection(
        const TExprNode& map,
        size_t depth)
    {
        Node(map, depth);
        if (!map.IsCallable("Map") || map.ChildrenSize() != 2) {
            Unsupported(
                "Read range point collection must be a two-argument Map");
        }
        const auto& just = Callable(
            map.ChildPtr(0),
            "Just",
            1,
            1,
            depth + 1);
        const auto& tuple = Node(just.ChildPtr(0), depth + 2);
        const size_t itemCount = tuple.ChildrenSize();
        if (!tuple.IsList() || itemCount == 0 ||
            itemCount > MaxFinitePointSet)
        {
            Unsupported(TStringBuilder()
                << "Read range static point collection size must be in [1, "
                << MaxFinitePointSet << "]");
        }
        const auto* itemType = ExactTupleItemType(tuple, itemCount);

        TVector<NJson::TJsonValue> result;
        result.reserve(itemCount);
        for (size_t index = 0; index < itemCount; ++index) {
            const auto& item = *tuple.Child(index);
            if (!item.GetTypeAnn() ||
                !IsSameAnnotation(*item.GetTypeAnn(), *itemType))
            {
                Unsupported(
                    "Read range static point literal annotation disagrees");
            }
            result.push_back(Int32Literal(item, "static point", depth + 3));
        }
        ExactOptionalItemType(just, *tuple.GetTypeAnn(), "Just collection");

        const auto& lambda = UnaryLambda(
            map.ChildPtr(1),
            "collection normalization",
            depth + 1);
        const auto& argument = *lambda.Child(0)->Child(0);
        if (argument.GetTypeAnn() &&
            !IsSameAnnotation(*argument.GetTypeAnn(), *tuple.GetTypeAnn()))
        {
            Unsupported(
                "Read range collection lambda argument has the wrong Tuple type");
        }
        const auto& asList = Callable(
            lambda.ChildPtr(1),
            "AsList",
            itemCount,
            itemCount,
            depth + 2);
        ExactListType(
            asList,
            *itemType,
            "normalized collection");
        for (size_t index = 0; index < itemCount; ++index) {
            const auto& nth = Callable(
                asList.ChildPtr(index),
                "Nth",
                2,
                2,
                depth + 3);
            if (nth.Child(0) != &argument) {
                Unsupported(
                    "Read range collection normalization must read its own argument");
            }
            Node(nth.ChildPtr(0), depth + 4);
            Atom(
                nth.ChildPtr(1),
                ToString(index),
                "collection Tuple index",
                depth + 4);
            if (nth.GetTypeAnn() &&
                !IsSameAnnotation(*nth.GetTypeAnn(), *itemType))
            {
                Unsupported(
                    "Read range collection Nth has the wrong item type");
            }
        }
        ExactOptionalListItemType(
            map,
            *itemType,
            "Map collection");
        StaticPointItemType = itemType;
        return result;
    }

    void CheckFullInt64Range(const TExprNode& node, size_t depth) {
        Node(node, depth);
        if (!node.IsCallable("AsRange") || node.ChildrenSize() != 1) {
            Unsupported(
                "Read range overflow branch must be one exact full AsRange");
        }
        const auto& ranges = Node(node.ChildPtr(0), depth + 1);
        if (!ranges.IsList() || ranges.ChildrenSize() != 2 ||
            ranges.Child(0) != ranges.Child(1))
        {
            Unsupported(
                "Read range full AsRange must reuse one boundary twice");
        }
        const auto& boundary = Node(ranges.ChildPtr(0), depth + 2);
        if (!boundary.IsList() || boundary.ChildrenSize() != 2) {
            Unsupported(
                "Read range full AsRange has an invalid Int64 boundary");
        }
        if ((ranges.GetTypeAnn() == nullptr) !=
            (boundary.GetTypeAnn() == nullptr))
        {
            Unsupported(
                "Read range full AsRange has partially annotated input");
        }
        if (ranges.GetTypeAnn() &&
            (ranges.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Tuple ||
             ranges.GetTypeAnn()->Cast<TTupleExprType>()->GetSize() != 2 ||
             !IsSameAnnotation(
                 *ranges.GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[0],
                 *boundary.GetTypeAnn()) ||
             !IsSameAnnotation(
                 *ranges.GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1],
                 *boundary.GetTypeAnn())))
        {
            Unsupported(
                "Read range full AsRange input has the wrong range Tuple type");
        }
        if (boundary.GetTypeAnn() &&
            (boundary.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Tuple ||
             boundary.GetTypeAnn()->Cast<TTupleExprType>()->GetSize() != 2))
        {
            Unsupported(
                "Read range full AsRange boundary has the wrong Tuple type");
        }
        const auto& nothing = Callable(
            boundary.ChildPtr(0),
            "Nothing",
            1,
            1,
            depth + 3);
        ExactScalarType(
            nothing,
            "Int64",
            true,
            "full-range sentinel");
        Int64Descriptor(
            *nothing.Child(0),
            true,
            "full-range sentinel descriptor",
            depth + 4);
        const auto inclusive = Int32Literal(
            *boundary.Child(1),
            "full-range inclusivity sentinel",
            depth + 3);
        if (boundary.GetTypeAnn() &&
            (!nothing.GetTypeAnn() ||
             !boundary.Child(1)->GetTypeAnn() ||
             !IsSameAnnotation(
                 *boundary.GetTypeAnn()
                      ->Cast<TTupleExprType>()->GetItems()[0],
                 *nothing.GetTypeAnn()) ||
             !IsSameAnnotation(
                 *boundary.GetTypeAnn()
                      ->Cast<TTupleExprType>()->GetItems()[1],
                 *boundary.Child(1)->GetTypeAnn())))
        {
            Unsupported(
                "Read range full AsRange boundary annotation disagrees with its values");
        }
        if (inclusive["value"].GetIntegerSafe() != 0) {
            Unsupported(
                "Read range full AsRange inclusivity sentinel must be zero");
        }
        ExactInt64RangeListType(node, false, "full AsRange result");
    }

    void CheckEmptyInt64Range(const TExprNode& node, size_t depth) {
        Node(node, depth);
        if (!node.IsCallable("RangeEmpty") || node.ChildrenSize() != 1) {
            Unsupported(
                "Read range missing collection branch must be RangeEmpty");
        }
        ExactInt64RangeListType(node, false, "RangeEmpty result");
        Int64Descriptor(
            *node.Child(0),
            false,
            "empty-range descriptor",
            depth + 1);
    }

    void CheckPointFlatMap(
        const TExprNode& flatMap,
        const TExprNode& collectionArgument,
        const TTypeAnnotationNode& itemType,
        size_t depth)
    {
        Node(flatMap, depth);
        if (!flatMap.IsCallable("FlatMap") || flatMap.ChildrenSize() != 2 ||
            flatMap.Child(0) != &collectionArgument)
        {
            Unsupported(
                "Read range point expansion must FlatMap its collection argument");
        }
        ExactInt64RangeListType(flatMap, false, "point FlatMap result");
        Node(flatMap.ChildPtr(0), depth + 1);
        const auto& lambda = UnaryLambda(
            flatMap.ChildPtr(1),
            "point expansion",
            depth + 1);
        const auto& itemArgument = *lambda.Child(0)->Child(0);
        if (itemArgument.GetTypeAnn() &&
            !IsSameAnnotation(*itemArgument.GetTypeAnn(), itemType))
        {
            Unsupported(
                "Read range point lambda argument has the wrong item type");
        }
        const auto& multiply = Callable(
            lambda.ChildPtr(1),
            "RangeMultiply",
            2,
            2,
            depth + 2);
        ExactInt64RangeListType(
            multiply,
            false,
            "point RangeMultiply result");
        Uint64Literal(
            *multiply.Child(0),
            ExtractorRangeCap,
            "point range cap",
            depth + 3);
        CheckRangeFor(*multiply.Child(1), &itemArgument, depth + 3);
        Node(multiply.Child(1)->ChildPtr(1), depth + 4);
    }

    NJson::TJsonValue ExportFinitePointSet(const TExprNode& ifPresent) {
        Node(ifPresent, 4);
        if (!ifPresent.IsCallable("IfPresent") ||
            ifPresent.ChildrenSize() != 3)
        {
            Unsupported(
                "Read range finite point set must be a three-argument IfPresent");
        }
        auto items = ExportStaticPointCollection(*ifPresent.Child(0), 5);
        const size_t itemCount = items.size();
        CheckExpectedMaxRanges(itemCount);

        const auto& lambda = UnaryLambda(
            ifPresent.ChildPtr(1),
            "present collection",
            5);
        const auto& collectionArgument = *lambda.Child(0)->Child(0);
        if (!StaticPointItemType)
        {
            Unsupported(
                "Read range static point item type is unavailable");
        }
        if (collectionArgument.GetTypeAnn() &&
            (collectionArgument.GetTypeAnn()->GetKind() !=
                 ETypeAnnotationKind::List ||
             !IsSameAnnotation(
                 *collectionArgument.GetTypeAnn()
                      ->Cast<TListExprType>()->GetItemType(),
                 *StaticPointItemType)))
        {
            Unsupported(
                "Read range present lambda argument has the wrong List type");
        }
        CheckEmptyInt64Range(*ifPresent.Child(2), 5);

        const auto& strict = Callable(
            lambda.ChildPtr(1),
            "IfStrict",
            3,
            3,
            6);
        ExactInt64RangeListType(strict, false, "point IfStrict result");
        const auto& comparison = Callable(
            strict.ChildPtr(0),
            ">",
            2,
            2,
            7);
        ExactScalarType(
            comparison,
            "Bool",
            false,
            "overflow comparison");
        const auto& length = Callable(
            comparison.ChildPtr(0),
            "Length",
            1,
            1,
            8);
        ExactScalarType(length, "Uint64", false, "point collection length");
        Uint64Literal(
            *comparison.Child(1),
            ExtractorRangeCap,
            "overflow comparison cap",
            8);

        const auto& collect = Callable(
            length.ChildPtr(0),
            "Collect",
            1,
            1,
            9);
        ExactInt64RangeListType(collect, false, "point Collect result");
        const auto& take = Callable(
            collect.ChildPtr(0),
            "Take",
            2,
            2,
            10);
        ExactInt64RangeListType(take, false, "point Take result");
        CheckPointFlatMap(
            *take.Child(0),
            collectionArgument,
            *StaticPointItemType,
            11);
        Uint64Literal(
            *take.Child(1),
            ExtractorOverflowProbe,
            "overflow probe",
            11);

        CheckFullInt64Range(*strict.Child(1), 7);
        const auto& finalUnion = Callable(
            strict.ChildPtr(2),
            "RangeUnion",
            1,
            1,
            7);
        ExactInt64RangeListType(
            finalUnion,
            false,
            "finite point RangeUnion result");
        if (finalUnion.Child(0) != &collect) {
            Unsupported(
                "Read range overflow check and final union must share one Collect");
        }
        ExactInt64RangeListType(
            ifPresent,
            false,
            "finite point IfPresent result");

        auto result = JsonMap();
        result["kind"] = "in";
        result["lookup"] = ColumnExpr(KeyOutput);
        auto jsonItems = JsonArray();
        for (auto& item : items) {
            jsonItems.AppendValue(std::move(item));
        }
        result["items"] = std::move(jsonItems);
        return result;
    }

    void CheckExpectedMaxRanges(size_t expected) const {
        if (!Range.ExpectedMaxRanges ||
            *Range.ExpectedMaxRanges != expected)
        {
            Unsupported(TStringBuilder()
                << "Read range ExpectedMaxRanges must equal " << expected);
        }
    }

private:
    const TOpRead& Read;
    const TSemanticSnapshotCatalogTableV1& Table;
    const TOpRead::TRangeInfo& Range;
    const TOlapColumnMap& Columns;
    TExactScalarBudget SourceBudget;
    TString KeyOutput;
    const TTypeAnnotationNode* StaticPointItemType = nullptr;
};
