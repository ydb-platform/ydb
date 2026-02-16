#include "yql_lineage.h"
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_join.h>
#include <yql/essentials/utils/limiting_allocator.h>
#include <yql/essentials/utils/std_allocator.h>

#include <library/cpp/yson/node/node_io.h>
#include <util/system/env.h>

namespace NYql {

namespace {

template <typename T>
using TNodeMapLimited = std::unordered_map<const TExprNode*, T, std::hash<const TExprNode*>, std::equal_to<const TExprNode*>, TStdIAllocator<std::pair<const TExprNode* const, T>>>;

using TNodeSetLimited = std::unordered_set<const TExprNode*, std::hash<const TExprNode*>, std::equal_to<const TExprNode*>, TStdIAllocator<const TExprNode*>>;

template <class TKey,
          class TValue>
using THashMapLimited = THashMap<TKey, TValue, THash<TKey>, TEqualTo<TKey>, TStdIAllocator<std::pair<const TKey, TValue>>>;

template <class TValue>
using TVectorLimited = TVector<TValue, TStdIAllocator<const TValue>>;

using NodeDataProviderPair = std::pair<const TExprNode*, IDataProvider*>;

class TLimitedStringStream: public TStringStream {
public:
    explicit TLimitedStringStream(size_t maxSize)
        : MaxSize_(maxSize)
        , WrittenBytes_(0)
    {
    }

protected:
    void DoWrite(const void* buf, size_t len) override {
        if (WrittenBytes_ >= MaxSize_) {
            throw yexception() << "Lineage is too large";
        }
        TStringStream::DoWrite(buf, len);
        WrittenBytes_ += len;
    }

private:
    size_t MaxSize_;
    size_t WrittenBytes_;
};

class TLineageScanner {
public:
    TLineageScanner(const TExprNode& root, const TTypeAnnotationContext& ctx, TExprContext& exprCtx, bool standalone)
        : Root_(root)
        , Ctx_(ctx)
        , ExprCtx_(exprCtx)
        , Allocator_(MakeLimitingAllocator(ctx.LineageMemoryLimit, TDefaultAllocator::Instance()))
        , Reads_(Allocator_.get())
        , Writes_(Allocator_.get())
        , ReadIds_(Allocator_.get())
        , Lineages_(Allocator_.get())
        , HasReads_(Allocator_.get())
        , Standalone_(standalone)
    {
    }

    TString Process() {
        VisitExpr(Root_, [&](const TExprNode& node) {
            for (auto& p : Ctx_.DataSources) {
                if (p->IsRead(node)) {
                    Reads_[&node] = p.Get();
                    HasReads_.emplace(&node);
                }
            }

            for (auto& p : Ctx_.DataSinks) {
                if (p->IsWrite(node)) {
                    Writes_[&node] = p.Get();
                }
            }

            return true; }, [&](const TExprNode& node) {
            for (const auto& child : node.Children()) {
                if (HasReads_.contains(child.Get())) {
                    HasReads_.emplace(&node);
                    break;
                }
            }

            return true; });

        TLimitedStringStream s(Ctx_.LineageOutputLimit);
        NYson::TYsonWriter writer(&s, NYson::EYsonFormat::Binary);
        writer.OnBeginMap();
        writer.OnKeyedItem("Reads");
        writer.OnBeginList();
        for (const auto& r : Reads_) {
            TVector<TPinInfo> inputs;
            auto& formatter = r.second->GetPlanFormatter();
            formatter.GetInputs(*r.first, inputs, /* withLimits */ false);
            TVectorLimited<ui32> readIds(Allocator_.get());
            for (const auto& i : inputs) {
                auto id = ++NextReadId_;
                readIds.push_back(id);
                writer.OnListItem();
                writer.OnBeginMap();
                writer.OnKeyedItem("Id");
                writer.OnInt64Scalar(id);
                writer.OnKeyedItem("Name");
                writer.OnStringScalar(i.DisplayName);
                writer.OnKeyedItem("Schema");
                const auto& itemType = *r.first->GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1]->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                WriteSchema(writer, itemType, nullptr);
                if (formatter.WriteSchemaHeader(writer)) {
                    WriteSchema(writer, itemType, &formatter);
                }

                writer.OnEndMap();
            }
            if (!readIds.empty()) {
                ReadIds_.insert({r.first, readIds});
            }
        }

        writer.OnEndList();
        writer.OnKeyedItem("Writes");
        writer.OnBeginList();
        THashMapLimited<TString, TVectorLimited<NodeDataProviderPair>> writeTables(Allocator_.get());
        for (const auto& w : Writes_) {
            TVector<TPinInfo> outputs;
            w.second->GetPlanFormatter().GetOutputs(*w.first, outputs, /* withLimits */ false);
            YQL_ENSURE(outputs.size() == 1);
            writeTables.try_emplace(outputs.front().DisplayName, TVectorLimited<NodeDataProviderPair>(Allocator_.get())).first->second.push_back(w);
        }
        for (const auto& w : writeTables) {
            writer.OnListItem();
            writer.OnBeginMap();
            writer.OnKeyedItem("Id");
            writer.OnInt64Scalar(++NextWriteId_);
            writer.OnKeyedItem("Name");
            writer.OnStringScalar(w.first);
            writer.OnKeyedItem("Schema");
            auto data = w.second[0].first->Child(3);
            const auto& itemType = *data->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            WriteSchema(writer, itemType, nullptr);
            auto& formatter = w.second[0].second->GetPlanFormatter();
            if (formatter.WriteSchemaHeader(writer)) {
                WriteSchema(writer, itemType, &formatter);
            }

            writer.OnKeyedItem("Lineage");
            if (w.second.size() == 1) {
                WriteLineage(writer, *CollectLineage(*data));
            } else {
                TVectorLimited<TLineage> lineages(Allocator_.get());
                lineages.reserve(w.second.size());
                Transform(w.second.begin(),
                          w.second.end(),
                          std::back_inserter(lineages),
                          [this](const auto& e) {
                              return *CollectLineage(*e.first->Child(3));
                          });
                TLineage lineage;
                MergeLineages(lineage, lineages);
                WriteLineage(writer, lineage);
            }
            writer.OnEndMap();
        }

        writer.OnEndList();
        writer.OnEndMap();
        return s.Str();
    }

private:
    void WriteSchema(NYson::TYsonWriter& writer, const TStructExprType& structType, IPlanFormatter* formatter) {
        writer.OnBeginMap();
        for (const auto& i : structType.GetItems()) {
            if (i->GetName().StartsWith("_yql_sys_")) {
                continue;
            }

            writer.OnKeyedItem(i->GetName());
            if (formatter) {
                formatter->WriteTypeDetails(writer, *i->GetItemType());
            } else {
                writer.OnStringScalar(FormatType(i->GetItemType()));
            }
        }

        writer.OnEndMap();
    }

    enum class ETransformsType {
        Copy,
        None
    };

    struct TFieldLineage {
        ui32 InputIndex;
        TStringBuf Field;
        ETransformsType Transforms;

        struct TFieldHash {
            std::size_t operator()(const TFieldLineage& x) const noexcept {
                return CombineHashes(
                    CombineHashes(std::hash<ui32>()(x.InputIndex), THash<TStringBuf>()(x.Field)),
                    std::hash<ETransformsType>()(x.Transforms));
            }
        };

        bool operator==(const TFieldLineage& rhs) const {
            return std::tie(InputIndex, Field, Transforms) == std::tie(rhs.InputIndex, rhs.Field, rhs.Transforms);
        }

        bool operator<(const TFieldLineage& rhs) const {
            return std::tie(InputIndex, Field, Transforms) < std::tie(rhs.InputIndex, rhs.Field, rhs.Transforms);
        }
    };

    static TFieldLineage ReplaceTransforms(const TFieldLineage& src, ETransformsType newTransforms) {
        return {src.InputIndex, src.Field, (src.Transforms == ETransformsType::Copy && newTransforms == ETransformsType::Copy) ? newTransforms : ETransformsType::None};
    }
    using TFieldLineageSet = THashSet<TFieldLineage, TFieldLineage::TFieldHash, TEqualTo<TFieldLineage>, TStdIAllocator<TFieldLineage>>;

    struct TFieldsLineage {
        explicit TFieldsLineage(IAllocator* allocator)
            : Items(allocator)
            , Allocator_(allocator)
        {
        }
        TFieldLineageSet Items;
        TMaybe<THashMapLimited<TStringBuf, TFieldLineageSet>> StructItems;

        void MergeFrom(const TFieldsLineage& from) {
            Items.insert(from.Items.begin(), from.Items.end());
            if (StructItems && from.StructItems) {
                for (const auto& i : *from.StructItems) {
                    TFieldLineageSet set(Allocator_);
                    set.insert(i.second.begin(), i.second.end());
                    (*StructItems).try_emplace(i.first, set);
                }
            }
        }

    private:
        IAllocator* Allocator_;
    };
    using TFieldsLineageMap = THashMapLimited<const TExprNode*, TMaybe<TFieldsLineage>>;

    static TFieldLineageSet ReplaceTransforms(const TFieldLineageSet& src, ETransformsType newTransforms, IAllocator* allocator) {
        TFieldLineageSet ret(allocator);
        for (const auto& i : src) {
            ret.insert(ReplaceTransforms(i, newTransforms));
        }

        return ret;
    }

    static TFieldsLineage ReplaceTransforms(const TFieldsLineage& src, ETransformsType newTransforms, IAllocator* allocator) {
        TFieldsLineage ret(allocator);
        ret.Items = ReplaceTransforms(src.Items, newTransforms, allocator);
        if (src.StructItems) {
            ret.StructItems.ConstructInPlace(allocator);
            for (const auto& i : *src.StructItems) {
                (*ret.StructItems).try_emplace(i.first, ReplaceTransforms(i.second, newTransforms, allocator));
            }
        }

        return ret;
    }

    struct TLineage {
        // null - can't calculcate
        TMaybe<THashMapLimited<TStringBuf, TFieldsLineage>> Fields;
    };

    const TLineage* CollectLineage(const TExprNode& node) {
        if (auto it = Lineages_.find(&node); it != Lineages_.end()) {
            return &it->second;
        }

        auto& lineage = Lineages_[&node];
        if (auto readIt = ReadIds_.find(&node); readIt != ReadIds_.end()) {
            lineage.Fields.ConstructInPlace(Allocator_.get());
            auto type = node.GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1]->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            for (const auto& i : type->GetItems()) {
                if (i->GetName().StartsWith("_yql_sys_")) {
                    continue;
                }

                auto& v = (*lineage.Fields).try_emplace(i->GetName(), TFieldsLineage(Allocator_.get())).first->second;
                for (const auto& r : readIt->second) {
                    v.Items.insert({r, i->GetName(), ETransformsType::Copy});
                }
            }

            return &lineage;
        }

        if (!HasReads_.contains(&node)) {
            auto type = node.GetTypeAnn();
            if (type->GetKind() == ETypeAnnotationKind::List) {
                auto itemType = type->Cast<TListExprType>()->GetItemType();
                if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
                    auto structType = itemType->Cast<TStructExprType>();
                    lineage.Fields.ConstructInPlace(Allocator_.get());
                    for (const auto& i : structType->GetItems()) {
                        if (i->GetName().StartsWith("_yql_sys_")) {
                            continue;
                        }

                        (*lineage.Fields).emplace(i->GetName(), TFieldsLineage(Allocator_.get()));
                    }

                    return &lineage;
                }
            }
        }

        if (node.IsCallable({"Unordered",
                             "UnorderedSubquery",
                             "Right!",
                             "YtTableContent",
                             "Skip",
                             "Take",
                             "Sort",
                             "TopSort",
                             "AssumeSorted",
                             "SkipNullMembers"})) {
            lineage = *CollectLineage(node.Head());
            return &lineage;
        } else if (node.IsCallable("ExtractMembers")) {
            HandleExtractMembers(lineage, node);
        } else if (node.IsCallable({"FlatMap", "OrderedFlatMap"})) {
            HandleFlatMap(lineage, node);
        } else if (node.IsCallable("Aggregate")) {
            HandleAggregate(lineage, node);
        } else if (node.IsCallable({"Extend", "OrderedExtend", "Merge"})) {
            HandleExtend(lineage, node);
        } else if (node.IsCallable({"CalcOverWindow", "CalcOverSessionWindow", "CalcOverWindowGroup"})) {
            HandleWindow(lineage, node);
        } else if (node.IsCallable("EquiJoin")) {
            HandleEquiJoin(lineage, node);
        } else if (node.IsCallable("LMap")) {
            HandleLMap(lineage, node);
        } else if (node.IsCallable({"PartitionsByKeys", "PartitionByKey"})) {
            HandlePartitionByKeys(lineage, node);
        } else if (node.IsCallable({"AsList", "List", "ListIf"})) {
            HandleListLiteral(lineage, node);
        } else {
            Warning(node);
        }

        return &lineage;
    }

    void Warning(const TExprNode& node) {
        auto message = TStringBuilder() << node.Type() << " : " << node.Content() << " is not supported";
        if (Standalone_) {
            auto issue = TIssue(ExprCtx_.GetPosition(node.Pos()), message);
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_LINEAGE_INTERNAL_ERROR, issue);
            ExprCtx_.AddWarning(issue);
        } else {
            throw yexception() << message;
        }
    }

    void HandleExtractMembers(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (innerLineage.Fields.Defined()) {
            lineage.Fields.ConstructInPlace(Allocator_.get());
            for (const auto& atom : node.Child(1)->Children()) {
                TStringBuf fieldName(atom->Content());
                auto it = (*innerLineage.Fields).find(fieldName);
                if (it != (*innerLineage.Fields).end()) {
                    (*lineage.Fields).insert_or_assign(fieldName, it->second);
                } else {
                    (*lineage.Fields).insert_or_assign(fieldName, TFieldsLineage(Allocator_.get()));
                }
            }
        }
    }

    TMaybe<TFieldsLineage> ScanExprLineage(const TExprNode& node, const TExprNode* arg, const TLineage* src,
                                           TNodeMap<TMaybe<TFieldsLineage>>& visited,
                                           const TFieldsLineageMap& flattenColumns) {
        if (&node == arg) {
            return Nothing();
        }

        auto [it, inserted] = visited.emplace(&node, Nothing());
        if (!inserted) {
            return it->second;
        }

        if (auto itFlatten = flattenColumns.find(&node); itFlatten != flattenColumns.end()) {
            return it->second = itFlatten->second;
        }

        if (node.IsCallable("Member")) {
            if (&node.Head() == arg && src) {
                return it->second = (*src->Fields).at(node.Tail().Content());
            }

            if (node.Head().IsCallable("Head")) {
                auto lineage = CollectLineage(node.Head().Head());
                if (lineage && lineage->Fields) {
                    TFieldsLineage result(Allocator_.get());
                    for (const auto& f : *lineage->Fields) {
                        result.MergeFrom(f.second);
                    }

                    return it->second = result;
                }
            }

            auto inner = ScanExprLineage(node.Head(), arg, src, visited, TFieldsLineageMap(Allocator_.get()));
            if (!inner) {
                return Nothing();
            }

            if (inner->StructItems) {
                TFieldsLineage result(Allocator_.get());
                result.Items = (*inner->StructItems).at(node.Tail().Content());
                return it->second = result;
            }
        }

        if (node.IsCallable("SqlIn")) {
            auto lineage = CollectLineage(*node.Child(0));
            if (lineage && lineage->Fields) {
                TFieldsLineage result(Allocator_.get());
                for (const auto& f : *lineage->Fields) {
                    result.MergeFrom(f.second);
                }

                return it->second = result;
            }
        }

        std::vector<TFieldsLineage> results;
        TMaybe<bool> hasStructItems;
        for (ui32 index = 0; index < node.ChildrenSize(); ++index) {
            if (index != 0 && node.IsCallable("SqlIn")) {
                continue;
            }

            auto child = node.Child(index);
            if (node.IsCallable("AsStruct")) {
                child = &child->Tail();
            }

            if (!child->GetTypeAnn()->IsComputable()) {
                continue;
            }

            auto inner = ScanExprLineage(*child, arg, src, visited, TFieldsLineageMap(Allocator_.get()));
            if (!inner) {
                return Nothing();
            }

            if (!hasStructItems) {
                hasStructItems = inner->StructItems.Defined();
            } else {
                hasStructItems = *hasStructItems && inner->StructItems.Defined();
            }

            results.emplace_back(std::move(*inner));
        }

        TFieldsLineage result(Allocator_.get());
        if (hasStructItems && *hasStructItems) {
            result.StructItems.ConstructInPlace(Allocator_.get());
        }

        for (const auto& r : results) {
            result.MergeFrom(r);
        }

        return it->second = result;
    }

    void MergeLineageFromUsedFields(const TExprNode& expr, const TExprNode& arg, const TLineage& src,
                                    TFieldLineageSet& dst, const TFieldsLineageMap& flattenColumns,
                                    ETransformsType newTransforms = ETransformsType::None) {
        TNodeMap<TMaybe<TFieldsLineage>> visited;
        auto res = ScanExprLineage(expr, &arg, &src, visited, flattenColumns);
        if (!res) {
            for (const auto& f : *src.Fields) {
                for (const auto& i : f.second.Items) {
                    dst.insert(ReplaceTransforms(i, newTransforms));
                }
            }
        } else {
            for (const auto& i : res->Items) {
                dst.insert(ReplaceTransforms(i, newTransforms));
            }
        }
    }

    void MergeLineageFromUsedFields(const TExprNode& expr, const TExprNode& arg, const TLineage& src,
                                    TFieldsLineage& dst, bool produceStruct, const TFieldsLineageMap& flattenColumns,
                                    ETransformsType newTransforms = ETransformsType::None) {
        if (produceStruct) {
            auto root = &expr;
            while (root->IsCallable("Just")) {
                root = &root->Head();
            }

            if (root == &arg) {
                dst.StructItems.ConstructInPlace(Allocator_.get());
                for (const auto& f : *src.Fields) {
                    (*dst.StructItems).insert_or_assign(f.first, f.second.Items);
                }
            } else if (root->IsCallable("AsStruct")) {
                dst.StructItems.ConstructInPlace(Allocator_.get());
                for (const auto& x : root->Children()) {
                    auto fieldName = x->Head().Content();
                    auto& s = (*dst.StructItems).try_emplace(fieldName, TFieldLineageSet(Allocator_.get())).first->second;
                    MergeLineageFromUsedFields(x->Tail(), arg, src, s, flattenColumns, newTransforms);
                }
            } else if (root->IsCallable("Member") && &root->Head() == &arg) {
                auto fieldName = root->Tail().Content();
                const auto& in = (*src.Fields).at(fieldName);
                dst.StructItems = in.StructItems;
            }
        }

        MergeLineageFromUsedFields(expr, arg, src, dst.Items, flattenColumns, newTransforms);
    }

    void FillStructLineage(TLineage& lineage, const TExprNode* value, const TExprNode& arg, const TLineage& innerLineage,
                           const TTypeAnnotationNode* extType, const TFieldsLineageMap& flattenColumns) {
        TMaybe<TStringBuf> oneField;
        if (value && value->IsCallable("Member") && &value->Head() == &arg) {
            auto& f = innerLineage.Fields->at(value->Tail().Content());
            if (f.StructItems) {
                for (const auto& x : *f.StructItems) {
                    auto& res = (*lineage.Fields).try_emplace(x.first, TFieldsLineage(Allocator_.get())).first->second;
                    res.Items = x.second;
                }

                return;
            }

            // fallback
            oneField = value->Tail().Content();
        }

        if (value && value->IsCallable("If")) {
            TLineage left, right;
            left.Fields.ConstructInPlace(Allocator_.get());
            right.Fields.ConstructInPlace(Allocator_.get());
            FillStructLineage(left, value->Child(1), arg, innerLineage, extType, TFieldsLineageMap(Allocator_.get()));
            FillStructLineage(right, value->Child(2), arg, innerLineage, extType, TFieldsLineageMap(Allocator_.get()));
            for (const auto& f : *left.Fields) {
                auto& res = (*lineage.Fields).try_emplace(f.first, TFieldsLineage(Allocator_.get())).first->second;
                res.Items.insert(f.second.Items.begin(), f.second.Items.end());
            }

            for (const auto& f : *right.Fields) {
                auto& res = (*lineage.Fields).try_emplace(f.first, TFieldsLineage(Allocator_.get())).first->second;
                res.Items.insert(f.second.Items.begin(), f.second.Items.end());
            }

            return;
        }

        if (value && value->IsCallable("AsStruct")) {
            for (const auto& child : value->Children()) {
                auto& res = (*lineage.Fields).try_emplace(child->Head().Content(), TFieldsLineage(Allocator_.get())).first->second;
                const auto& expr = child->Tail();
                ETransformsType newTransforms = ETransformsType::None;
                const TExprNode* root = &expr;
                while (root->IsCallable("Just")) {
                    root = &root->Head();
                }

                if (root->IsCallable("Member") && &root->Head() == &arg) {
                    newTransforms = ETransformsType::Copy;
                }

                MergeLineageFromUsedFields(expr, arg, innerLineage, res, true, flattenColumns, newTransforms);
            }

            return;
        }

        if (extType && extType->GetKind() == ETypeAnnotationKind::Struct) {
            auto structType = extType->Cast<TStructExprType>();
            TFieldLineageSet allLineage(Allocator_.get());
            for (const auto& f : *innerLineage.Fields) {
                if (oneField && oneField != f.first) {
                    continue;
                }

                allLineage.insert(f.second.Items.begin(), f.second.Items.end());
            }

            for (const auto& i : structType->GetItems()) {
                auto& res = (*lineage.Fields).try_emplace(i->GetName(), TFieldsLineage(Allocator_.get())).first->second;
                res.Items = allLineage;
            }
        }
    }

    void HandleFlatMap(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.Fields.Defined()) {
            return;
        }

        const auto& lambda = node.Tail();
        const auto& arg = lambda.Head().Head();
        const auto& body = lambda.Tail();
        TFieldsLineageMap flattenColumns(Allocator_.get());
        const TExprNode* value = &body.Tail();
        if (body.IsCallable({"OptionalIf", "FlatListIf"})) {
            value = &body.Tail();
        } else if (body.IsCallable("Just")) {
            value = &body.Head();
        } else if (body.IsCallable({"FlatMap", "OrderedFlatMap"})) {
            if (lambda.GetTypeAnn()->GetKind() == ETypeAnnotationKind::List) {
                value = &body;
                while (value->IsCallable({"FlatMap", "OrderedFlatMap"})) {
                    TNodeMap<TMaybe<TFieldsLineage>> visited;
                    if (auto res = ScanExprLineage(value->Head(), &arg, &innerLineage, visited, TFieldsLineageMap(Allocator_.get()))) {
                        flattenColumns.emplace(value->Tail().Head().HeadPtr().Get(), res);
                    }
                    value = &value->Tail().Tail();
                }
                if (value->IsCallable("Just")) {
                    value = &value->Head();
                } else if (value->IsCallable({"OptionalIf", "FlatListIf"})) {
                    value = &value->Tail();
                }
            } else {
                value = &body.Head();
            }
        } else {
            Warning(body);
            return;
        }

        if (value == &arg) {
            lineage.Fields = *innerLineage.Fields;
            return;
        }

        lineage.Fields.ConstructInPlace(Allocator_.get());
        FillStructLineage(lineage, value, arg, innerLineage, GetSeqItemType(body.GetTypeAnn()), flattenColumns);
    }

    void HandleAggregate(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.Fields.Defined()) {
            return;
        }

        lineage.Fields.ConstructInPlace(Allocator_.get());
        for (const auto& key : node.Child(1)->Children()) {
            auto it = (*innerLineage.Fields).find(key->Content());
            if (it != (*innerLineage.Fields).end()) {
                (*lineage.Fields).insert_or_assign(key->Content(), it->second);
            } else {
                (*lineage.Fields).insert_or_assign(key->Content(), TFieldsLineage(Allocator_.get()));
            }
        }

        for (const auto& payload : node.Child(2)->Children()) {
            TVectorLimited<TStringBuf> fields(Allocator_.get());
            if (payload->Child(0)->IsList()) {
                for (const auto& child : payload->Child(0)->Children()) {
                    fields.push_back(child->Content());
                }
            } else {
                fields.push_back(payload->Child(0)->Content());
            }

            TFieldsLineage source(Allocator_.get());
            if (payload->ChildrenSize() == 3) {
                // distinct
                source = ReplaceTransforms(
                    (*innerLineage.Fields).try_emplace(payload->Child(2)->Content(), TFieldsLineage(Allocator_.get())).first->second,
                    ETransformsType::None,
                    Allocator_.get());
            } else {
                if (payload->Child(1)->IsCallable("AggregationTraits")) {
                    // merge all used fields from init/update handlers
                    auto initHandler = payload->Child(1)->Child(1);
                    auto updateHandler = payload->Child(1)->Child(2);
                    MergeLineageFromUsedFields(initHandler->Tail(),
                                               initHandler->Head().Head(),
                                               innerLineage,
                                               source,
                                               false,
                                               TFieldsLineageMap(Allocator_.get()));
                    MergeLineageFromUsedFields(updateHandler->Tail(),
                                               updateHandler->Head().Head(),
                                               innerLineage,
                                               source,
                                               false,
                                               TFieldsLineageMap(Allocator_.get()));
                } else if (payload->Child(1)->IsCallable("AggApply")) {
                    auto extractHandler = payload->Child(1)->Child(2);
                    bool produceStruct = payload->Child(1)->Head().Content() == "some";
                    MergeLineageFromUsedFields(extractHandler->Tail(),
                                               extractHandler->Head().Head(),
                                               innerLineage,
                                               source,
                                               produceStruct,
                                               TFieldsLineageMap(Allocator_.get()));
                } else {
                    Warning(*payload->Child(1));
                    lineage.Fields.Clear();
                    return;
                }
            }

            for (const auto& field : fields) {
                (*lineage.Fields).insert_or_assign(field, source);
            }
        }
    }

    void HandleLMap(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.Fields.Defined()) {
            return;
        }

        const auto& lambda = node.Tail();
        const auto& arg = lambda.Head().Head();
        const auto& body = lambda.Tail();
        if (&body == &arg) {
            lineage.Fields = *innerLineage.Fields;
            return;
        }

        lineage.Fields.ConstructInPlace(Allocator_.get());
        FillStructLineage(lineage,
                          nullptr,
                          arg,
                          innerLineage,
                          GetSeqItemType(body.GetTypeAnn()),
                          TFieldsLineageMap(Allocator_.get()));
    }

    void HandlePartitionByKeys(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.Fields.Defined()) {
            return;
        }

        const auto& lambda = node.Tail();
        const auto& arg = lambda.Head().Head();
        const auto& body = lambda.Tail();
        if (&body == &arg) {
            lineage.Fields = *innerLineage.Fields;
            return;
        }

        lineage.Fields.ConstructInPlace(Allocator_.get());
        FillStructLineage(lineage,
                          nullptr,
                          arg,
                          innerLineage,
                          GetSeqItemType(body.GetTypeAnn()),
                          TFieldsLineageMap(Allocator_.get()));
    }

    void MergeLineages(TLineage& lineage, TVectorLimited<TLineage>& inners) {
        if (inners.empty()) {
            return;
        }

        lineage.Fields.ConstructInPlace(Allocator_.get());
        for (const auto& x : *inners.front().Fields) {
            auto& res = (*lineage.Fields).try_emplace(x.first, TFieldsLineage(Allocator_.get())).first->second;
            TMaybe<bool> hasStructItems;
            for (const auto& i : inners) {
                if (auto f = (*i.Fields).FindPtr(x.first)) {
                    for (const auto& x : f->Items) {
                        res.Items.insert(x);
                    }

                    if (f->StructItems || f->Items.empty()) {
                        if (!hasStructItems) {
                            hasStructItems = true;
                        }
                    } else {
                        hasStructItems = false;
                    }
                }
            }

            if (hasStructItems && *hasStructItems) {
                res.StructItems.ConstructInPlace(Allocator_.get());
                for (const auto& i : inners) {
                    if (auto f = (*i.Fields).FindPtr(x.first)) {
                        if (f->StructItems) {
                            for (const auto& si : *f->StructItems) {
                                for (const auto& x : si.second) {
                                    (*res.StructItems).try_emplace(si.first, TFieldLineageSet(Allocator_.get())).first->second.insert(x);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    void HandleExtend(TLineage& lineage, const TExprNode& node) {
        TVectorLimited<TLineage> inners(Allocator_.get());
        for (const auto& child : node.Children()) {
            inners.push_back(*CollectLineage(*child));
            if (!inners.back().Fields.Defined()) {
                return;
            }
        }
        MergeLineages(lineage, inners);
    }

    void HandleWindow(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.Fields.Defined()) {
            return;
        }

        TExprNode::TListType frameGroups;
        if (node.IsCallable("CalcOverWindowGroup")) {
            for (const auto& g : node.Child(1)->Children()) {
                frameGroups.push_back(g->Child(2));
            }
        } else {
            frameGroups.push_back(node.Child(3));
        }

        lineage.Fields = *innerLineage.Fields;
        if (node.IsCallable("CalcOverSessionWindow")) {
            if (node.Child(5)->ChildrenSize() && !node.Child(4)->IsCallable("SessionWindowTraits")) {
                lineage.Fields.Clear();
                return;
            }

            for (const auto& sessionColumn : node.Child(5)->Children()) {
                auto& res = (*lineage.Fields).try_emplace(sessionColumn->Content(), TFieldsLineage(Allocator_.get())).first->second;
                const auto& initHandler = node.Child(4)->Child(2);
                const auto& updateHandler = node.Child(4)->Child(2);
                MergeLineageFromUsedFields(initHandler->Tail(),
                                           initHandler->Head().Head(),
                                           innerLineage,
                                           res,
                                           false,
                                           TFieldsLineageMap(Allocator_.get()));
                MergeLineageFromUsedFields(updateHandler->Tail(),
                                           updateHandler->Head().Head(),
                                           innerLineage,
                                           res,
                                           false,
                                           TFieldsLineageMap(Allocator_.get()));
            }
        }

        for (const auto& g : frameGroups) {
            for (const auto& f : g->Children()) {
                if (!f->IsCallable("WinOnRows")) {
                    lineage.Fields.Clear();
                    return;
                }

                for (ui32 i = 1; i < f->ChildrenSize(); ++i) {
                    const auto& list = f->Child(i);
                    auto field = list->Head().Content();
                    auto& res = (*lineage.Fields).try_emplace(field, TFieldsLineage(Allocator_.get())).first->second;
                    if (list->Tail().IsCallable({"RowNumber", "CumeDist", "NTile"})) {
                        continue;
                    } else if (list->Tail().IsCallable({"Lag", "Lead", "Rank", "DenseRank", "PercentRank"})) {
                        const auto& lambda = list->Tail().Child(1);
                        bool produceStruct = list->Tail().IsCallable({"Lag", "Lead"});
                        MergeLineageFromUsedFields(lambda->Tail(),
                                                   lambda->Head().Head(),
                                                   innerLineage,
                                                   res,
                                                   produceStruct,
                                                   TFieldsLineageMap(Allocator_.get()));
                    } else if (list->Tail().IsCallable("WindowTraits")) {
                        const auto& initHandler = list->Tail().Child(1);
                        const auto& updateHandler = list->Tail().Child(2);
                        MergeLineageFromUsedFields(initHandler->Tail(),
                                                   initHandler->Head().Head(),
                                                   innerLineage,
                                                   res,
                                                   false,
                                                   TFieldsLineageMap(Allocator_.get()));
                        MergeLineageFromUsedFields(updateHandler->Tail(),
                                                   updateHandler->Head().Head(),
                                                   innerLineage,
                                                   res,
                                                   false,
                                                   TFieldsLineageMap(Allocator_.get()));
                    } else {
                        lineage.Fields.Clear();
                        return;
                    }
                }
            }
        }
    }

    void HandleEquiJoin(TLineage& lineage, const TExprNode& node) {
        TVectorLimited<TLineage> inners(Allocator_.get());
        THashMapLimited<TStringBuf, ui32> inputLabels(Allocator_.get());
        for (ui32 i = 0; i < node.ChildrenSize() - 2; ++i) {
            inners.push_back(*CollectLineage(node.Child(i)->Head()));
            if (!inners.back().Fields.Defined()) {
                return;
            }

            if (node.Child(i)->Tail().IsAtom()) {
                inputLabels[node.Child(i)->Tail().Content()] = i;
            } else {
                for (const auto& label : node.Child(i)->Tail().Children()) {
                    inputLabels[label->Content()] = i;
                }
            }
        }

        THashMapLimited<TStringBuf, TStringBuf> backRename(Allocator_.get());
        for (auto setting : node.Tail().Children()) {
            if (setting->Head().Content() != "rename") {
                continue;
            }

            if (setting->Child(2)->Content().empty()) {
                continue;
            }

            backRename[setting->Child(2)->Content()] = setting->Child(1)->Content();
        }

        lineage.Fields.ConstructInPlace(Allocator_.get());
        auto structType = node.GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        THashMapLimited<TStringBuf, TMaybe<bool>> hasStructItems(Allocator_.get());
        for (const auto& field : structType->GetItems()) {
            TStringBuf originalName = field->GetName();
            if (auto it = backRename.find(originalName); it != backRename.end()) {
                originalName = it->second;
            }

            TStringBuf table, column;
            SplitTableName(originalName, table, column);
            ui32 index = inputLabels.at(table);
            auto& res = (*lineage.Fields).try_emplace(field->GetName(), TFieldsLineage(Allocator_.get())).first->second;
            auto& f = (*inners[index].Fields).at(column);
            for (const auto& i : f.Items) {
                res.Items.insert(i);
            }

            auto& h = hasStructItems[field->GetName()];
            if (f.StructItems || f.Items.empty()) {
                if (!h) {
                    h = true;
                }
            } else {
                h = false;
            }
        }

        for (const auto& field : structType->GetItems()) {
            TStringBuf originalName = field->GetName();
            if (auto it = backRename.find(originalName); it != backRename.end()) {
                originalName = it->second;
            }

            TStringBuf table, column;
            SplitTableName(originalName, table, column);
            ui32 index = inputLabels.at(table);
            auto& res = (*lineage.Fields).try_emplace(field->GetName(), TFieldsLineage(Allocator_.get())).first->second;
            auto& f = (*inners[index].Fields).at(column);
            auto& h = hasStructItems[field->GetName()];
            if (h && *h) {
                if (!res.StructItems) {
                    res.StructItems.ConstructInPlace(Allocator_.get());
                }

                if (f.StructItems) {
                    for (const auto& i : *f.StructItems) {
                        for (const auto& x : i.second) {
                            (*res.StructItems).try_emplace(i.first, TFieldLineageSet(Allocator_.get())).first->second.insert(x);
                        }
                    }
                }
            }
        }
    }

    void HandleListLiteral(TLineage& lineage, const TExprNode& node) {
        auto itemType = node.GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (itemType->GetKind() != ETypeAnnotationKind::Struct) {
            return;
        }

        auto structType = itemType->Cast<TStructExprType>();
        lineage.Fields.ConstructInPlace(Allocator_.get());
        ui32 startIndex = 0;
        if (node.IsCallable({"List", "ListIf"})) {
            startIndex = 1;
        }

        for (ui32 i = startIndex; i < node.ChildrenSize(); ++i) {
            auto child = node.Child(i);
            if (child->IsCallable("AsStruct")) {
                for (const auto& f : child->Children()) {
                    TNodeMap<TMaybe<TFieldsLineage>> visited;
                    auto res = ScanExprLineage(f->Tail(),
                                               nullptr,
                                               nullptr,
                                               visited,
                                               TFieldsLineageMap(Allocator_.get()));
                    if (res) {
                        auto name = f->Head().Content();
                        (*lineage.Fields).try_emplace(name, TFieldsLineage(Allocator_.get())).first->second.MergeFrom(*res);
                    }
                }
            } else {
                TNodeMap<TMaybe<TFieldsLineage>> visited;
                auto res = ScanExprLineage(*child,
                                           nullptr,
                                           nullptr,
                                           visited,
                                           TFieldsLineageMap(Allocator_.get()));
                if (res) {
                    for (const auto& i : structType->GetItems()) {
                        if (i->GetName().StartsWith("_yql_sys_")) {
                            continue;
                        }

                        (*lineage.Fields).try_emplace(i->GetName(), TFieldsLineage(Allocator_.get())).first->second.MergeFrom(*res);
                    }
                }
            }
        }
    }

    void WriteLineage(NYson::TYsonWriter& writer, const TLineage& lineage) {
        // TODO: remove Standalone_ after fixing all failed tests, see YQL-20445
        if (Standalone_ && !lineage.Fields.Defined()) {
            YQL_ENSURE(!GetEnv("YQL_DETERMINISTIC_MODE"), "Can't calculate lineage");
            writer.OnEntity();
            return;
        }

        writer.OnBeginMap();
        TVectorLimited<TStringBuf> fields(Allocator_.get());
        for (const auto& f : *lineage.Fields) {
            fields.push_back(f.first);
        }

        Sort(fields);
        for (const auto& f : fields) {
            writer.OnKeyedItem(f);
            writer.OnBeginList();
            TVectorLimited<TFieldLineage> items(Allocator_.get());
            for (const auto& i : lineage.Fields->at(f).Items) {
                items.push_back(i);
            }

            Sort(items);
            for (const auto& i : items) {
                writer.OnListItem();
                writer.OnBeginMap();
                writer.OnKeyedItem("Input");
                writer.OnInt64Scalar(i.InputIndex);
                writer.OnKeyedItem("Field");
                writer.OnStringScalar(i.Field);
                writer.OnKeyedItem("Transforms");
                switch (i.Transforms) {
                    case ETransformsType::Copy:
                        writer.OnStringScalar("Copy");
                        break;
                    default:
                        writer.OnEntity();
                }
                writer.OnEndMap();
            }
            writer.OnEndList();
        }

        writer.OnEndMap();
    }

private:
    const TExprNode& Root_;
    const TTypeAnnotationContext& Ctx_;
    TExprContext& ExprCtx_;
    std::unique_ptr<IAllocator> Allocator_;
    TNodeMapLimited<IDataProvider*> Reads_, Writes_;
    ui32 NextReadId_ = 0;
    ui32 NextWriteId_ = 0;
    TNodeMapLimited<TVectorLimited<ui32>> ReadIds_;
    TNodeMapLimited<TLineage> Lineages_;
    TNodeSetLimited HasReads_;
    bool Standalone_;
};

template <typename Compare, typename Fun>
void IterateTwoLists(NYT::TNode::TListType& listFirst, NYT::TNode::TListType& listSecond, Compare comp, Fun action)
{
    if (listFirst.size() != listSecond.size()) {
        throw yexception() << "Iterate over two lists with different sizes";
    }

    TVector<NYT::TNode::TListType::iterator> itFirst;
    for (auto it = listFirst.begin(); it != listFirst.end(); ++it) {
        itFirst.push_back(it);
    }
    Sort(itFirst, comp);

    TVector<NYT::TNode::TListType::iterator> itSecond;
    for (auto it = listSecond.begin(); it != listSecond.end(); ++it) {
        itSecond.push_back(it);
    }
    Sort(itSecond, comp);

    for (size_t i = 0; i < itFirst.size(); ++i) {
        action(*itFirst[i], *itSecond[i]);
    }
}

} // namespace

TString CalculateLineage(const TExprNode& root, const TTypeAnnotationContext& ctx, TExprContext& exprCtx, bool standalone) {
    TLineageScanner scanner(root, ctx, exprCtx, standalone);
    return scanner.Process();
}

void ValidateLineage(const TString& lineageStr) {
    const auto& lineageNode = NYT::NodeFromYsonString(lineageStr);
    const auto& writeSection = lineageNode.AsMap().at("Writes").AsList();
    ForEach(writeSection.begin(),
            writeSection.end(),
            [](auto& it) { YQL_ENSURE(it["Lineage"].IsMap()); });
}

void CheckEquvalentLineages(const TString& lineageFirst, const TString& lineageSecond) {
    auto lineageNode1 = NYT::NodeFromYsonString(lineageFirst);
    auto lineageNode2 = NYT::NodeFromYsonString(lineageSecond);

    THashMap<i64, NYT::TNode> idToPath1, idToPath2;
    IterateTwoLists(lineageNode1["Reads"].AsList(),
                    lineageNode2["Reads"].AsList(),
                    // clang-format off
                    [](NYT::TNode::TListType::iterator it1, NYT::TNode::TListType::iterator it2) {
                        return it1->AsMap()["Name"].AsString() > it2->AsMap()["Name"].AsString();
                    },
                    // clang-format on
                    [&idToPath1, &idToPath2](auto& it1, auto& it2) {
                        idToPath1[it1["Id"].AsInt64()] = it1["Name"];
                        idToPath2[it2["Id"].AsInt64()] = it2["Name"];
                        it1["Id"] = it1["Name"], it2["Id"] = it2["Name"];
                        if (NodeToCanonicalYsonString(it1) != NodeToCanonicalYsonString(it2)) {
                            throw yexception() << "'Reads' sections are different";
                        } });

    IterateTwoLists(lineageNode1["Writes"].AsList(),
                    lineageNode2["Writes"].AsList(),
                    // clang-format off
                    [](NYT::TNode::TListType::iterator it1, NYT::TNode::TListType::iterator it2) {
                        return it1->AsMap()["Name"].AsString() > it2->AsMap()["Name"].AsString();
                    },
                    // clang-format on
                    [&idToPath1, &idToPath2](auto& it1, auto& it2) {
                        it1["Id"] = it1["Name"], it2["Id"] = it2["Name"];
                        if (it1.AsMap().size() != it2.AsMap().size()) {
                            throw yexception() << "Keys in 'Writes' section are different";
                        }
                        for (auto& [key, value] : it1.AsMap()) {
                            if (key == "Lineage") {
                                if (it1["Lineage"].AsMap().size() != it2["Lineage"].AsMap().size()) {
                                    throw yexception() << "Numbers of output fields 'Lineage' section are different";
                                }
                                for (auto& [fieldName, fieldLineage] : it1["Lineage"].AsMap()) {
                                    ForEach(fieldLineage.AsList().begin(),
                                            fieldLineage.AsList().end(),
                                            [&idToPath1](auto& it) {
                                                it["Input"] = idToPath1[it["Input"].AsInt64()];
                                            });
                                    ForEach(it2["Lineage"][fieldName].AsList().begin(),
                                            it2["Lineage"][fieldName].AsList().end(),
                                            [&idToPath2](auto& it) {
                                                it["Input"] = idToPath2[it["Input"].AsInt64()];
                                            });
                                    IterateTwoLists(fieldLineage.AsList(),
                                                    it2["Lineage"].AsMap()[fieldName].AsList(),
                                                    [](NYT::TNode::TListType::iterator it1, NYT::TNode::TListType::iterator it2) {
                                                        if (it1->AsMap()["Field"].AsString() == it2->AsMap()["Field"].AsString()) {
                                                            if (it1->AsMap()["Input"].AsString() == it2->AsMap()["Input"].AsString()) {
                                                                const auto& transforms1 = it1->AsMap()["Transforms"].IsNull() ? "#" : it1->AsMap()["Transforms"].AsString();
                                                                const auto& transforms2 = it2->AsMap()["Transforms"].IsNull() ? "#" : it2->AsMap()["Transforms"].AsString();
                                                                return transforms1 > transforms2;
                                                            } else {
                                                                return it1->AsMap()["Input"].AsString() > it2->AsMap()["Input"].AsString();
                                                            }
                                                        }
                                                        return it1->AsMap()["Field"].AsString() > it2->AsMap()["Field"].AsString();
                                                    },
                                                    [fieldName](auto& itt1, auto& itt2) {
                                                        if (NodeToCanonicalYsonString(itt1) != NodeToCanonicalYsonString(itt2)) {
                                                            throw yexception() << "Lineage for '" << fieldName << "' are different";
                                                        } });
                                }
                            } else {
                                if (NodeToCanonicalYsonString(it1[key]) != NodeToCanonicalYsonString(it2[key])) {
                                    throw yexception() << "'Writes' sections are different for '" << key << "'";
                                }
                            }
                        } });
}

} // namespace NYql
