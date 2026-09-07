#include "yql_lineage.h"
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_join.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <yql/essentials/utils/limiting_allocator.h>
#include <yql/essentials/utils/std_allocator.h>

#include <library/cpp/yson/node/node_io.h>
#include <util/system/env.h>

#include <variant>

static constexpr TStringBuf YqlSysPrefix = "_yql_sys_";
static constexpr TStringBuf YqlTimeColumn = "_yql_time";

namespace NYql {

namespace {

bool IsYqlSystemField(TStringBuf name) {
    return name.StartsWith(YqlSysPrefix) || name == YqlTimeColumn;
}

enum class ETransformsType {
    Copy,
    Math,
    None
};

struct TFieldLineage {
    TStringBuf Field;
    ui32 InputIndex;
    ETransformsType Transforms;

    struct TFieldHash {
        std::size_t operator()(const TFieldLineage& x) const noexcept {
            return CombineHashes(
                CombineHashes(std::hash<ui32>()(x.InputIndex), THash<TStringBuf>()(x.Field)),
                std::hash<ETransformsType>()(x.Transforms));
        }
    };

    bool operator==(const TFieldLineage& rhs) const {
        return std::tie(Field, InputIndex, Transforms) == std::tie(rhs.Field, rhs.InputIndex, rhs.Transforms);
    }

    bool operator<(const TFieldLineage& rhs) const {
        return std::tie(Field, InputIndex, Transforms) < std::tie(rhs.Field, rhs.InputIndex, rhs.Transforms);
    }
};

using TFieldLineageSet = std::unordered_set<TFieldLineage, TFieldLineage::TFieldHash, TEqualTo<TFieldLineage>, TStdIAllocator<TFieldLineage>>;

struct TSchemaHash {
    size_t operator()(const NYql::TStructExprType* type) const {
        if (type == nullptr) {
            return 0;
        }
        ui64 hash = NYql::TypeHashMagic | (ui64)NYql::ETypeAnnotationKind::Struct;
        size_t count = 0;
        for (const auto* item : type->GetItems()) {
            if (IsYqlSystemField(item->GetName())) {
                continue;
            }
            hash = NYql::StreamHash(item->GetHash(), hash);
            ++count;
        }
        hash = NYql::StreamHash(count, hash);
        return hash;
    }
};

struct TSchemaEqualTo {
    bool operator()(const NYql::TStructExprType* lhs, const NYql::TStructExprType* rhs) const {
        if (lhs == rhs) {
            return true;
        }
        if (!lhs || !rhs) {
            return false;
        }
        const auto& lItems = lhs->GetItems();
        const auto& rItems = rhs->GetItems();
        for (auto li = lItems.begin(), ri = rItems.begin(); li != lItems.end() || ri != rItems.end();) {
            if (li != lItems.end() && IsYqlSystemField((*li)->GetName())) {
                ++li;
                continue;
            }
            if (ri != rItems.end() && IsYqlSystemField((*ri)->GetName())) {
                ++ri;
                continue;
            }
            if (li == lItems.end() && ri == rItems.end()) {
                return true;
            }
            if (li == lItems.end() || ri == rItems.end()) {
                return false;
            }
            if (*li != *ri) {
                return false;
            }
            ++li, ++ri;
        }
        return true;
    }
};

template <class TValue>
using TVectorLimited = TVector<TValue, TStdIAllocator<const TValue>>;

using TOrderedLineageVector = TVectorLimited<TFieldLineage>;

template <class TValue>
using TSetLimited = TSet<TValue, TLess<TValue>, TStdIAllocator<TValue>>;

struct TLineageHash {
    size_t operator()(const TOrderedLineageVector& vec) const {
        size_t hash = 0;
        for (const auto& elem : vec) {
            hash = CombineHashes(hash, NYql::TFieldLineage::TFieldHash{}(elem));
        }
        return hash;
    }
};

struct TLineageEqualTo {
    bool operator()(const TOrderedLineageVector& lhs, const TOrderedLineageVector& rhs) const {
        if (lhs.size() != rhs.size()) {
            return false;
        }
        for (size_t i = 0; i < lhs.size(); ++i) {
            if (!(lhs[i].InputIndex == rhs[i].InputIndex &&
                  lhs[i].Field == rhs[i].Field &&
                  lhs[i].Transforms == rhs[i].Transforms)) {
                return false;
            }
        }
        return true;
    }
};

struct TIndexHash {
    size_t operator()(const TSetLimited<ui32>& set) const {
        size_t hash = 0;
        for (const auto& elem : set) {
            hash = CombineHashes(hash, THash<ui32>{}(elem));
        }
        return hash;
    }
};

template <typename T>
using TNodeMapLimited = std::unordered_map<const TExprNode*, T, std::hash<const TExprNode*>, std::equal_to<const TExprNode*>, TStdIAllocator<std::pair<const TExprNode* const, T>>>;

using TNodeSetLimited = std::unordered_set<const TExprNode*, std::hash<const TExprNode*>, std::equal_to<>, TStdIAllocator<const TExprNode*>>;

template <class TKey,
          class TValue>
using THashMapLimited = std::unordered_map<TKey, TValue, THash<TKey>, TEqualTo<TKey>, TStdIAllocator<std::pair<const TKey, TValue>>>;

using TSchemaMapLimited = std::unordered_map<const TStructExprType*, ui32, TSchemaHash, TSchemaEqualTo, TStdIAllocator<std::pair<const TStructExprType* const, ui32>>>;

template <class TKey,
          class TValue>
using TMapLimited = TMap<TKey, TValue, TLess<TKey>, TStdIAllocator<std::pair<const TKey, TValue>>>;

template <class TValue>
using THashSetLimited = THashSet<TValue, THash<TValue>, TEqualTo<TValue>, TStdIAllocator<TValue>>;

using NodeDataProviderPair = std::pair<const TExprNode*, IDataProvider*>;

const TStringBuf ZeroString = {};

using TLineageMapLimited = std::unordered_map<TOrderedLineageVector, ui32, TLineageHash, TLineageEqualTo, TStdIAllocator<std::pair<const TOrderedLineageVector, ui32>>>;

using TIndexMapLimited = std::unordered_map<TSetLimited<ui32>, ui32, TIndexHash, TEqualTo<TSetLimited<ui32>>, TStdIAllocator<std::pair<const TSetLimited<ui32>, ui32>>>;

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
    TLineageScanner(const TExprNode& root, TTypeAnnotationContext& ctx, TExprContext& exprCtx, const TLineageRunOptions& options)
        : Root_(root)
        , Ctx_(ctx)
        , ExprCtx_(exprCtx)
        , Options_(options)
        , Allocator_(MakeLimitingAllocator(ctx.LineageSettings.LineageMemoryLimit, TDefaultAllocator::Instance()))
        , Reads_(Allocator_.get())
        , Writes_(Allocator_.get())
        , ReadIds_(Allocator_.get())
        , Lineages_(Allocator_.get())
        , HasReads_(Allocator_.get())
        , StringPool_(4096, TMemoryPool::TExpGrow::Instance(), Allocator_.get())
        , Strings_(Allocator_.get())
        , TableIds_(Allocator_.get())
        , SchemaSets_(Allocator_.get())
        , LineageSets_(Allocator_.get())
        , IndexSets_(Allocator_.get())
        , SchemaRefs_(Allocator_.get())
        , LineageRefs_(Allocator_.get())
    {
        if (Options_.Version != 1 && Options_.Version != 2) {
            throw yexception() << "Unsupported LineageVersion is provided: " << Options_.Version;
        }
    }

    TString Process() {
        auto startTime = TInstant::Now();
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

        TLimitedStringStream s(Ctx_.LineageSettings.LineageOutputLimit);
        NYson::TYsonWriter writer(&s, NYson::EYsonFormat::Binary);
        CollectResults();
        writer.OnBeginMap();
        if (Options_.Version > 1) {
            writer.OnKeyedItem("Version");
            writer.OnInt64Scalar(Options_.Version);
            WriteSchemaSet(writer);
            WriteLineageSet(writer);
        }
        writer.OnKeyedItem("Reads");
        writer.OnBeginList();
        for (const auto& [tableName, tableId] : TableIds_) {
            writer.OnListItem();
            writer.OnBeginMap();
            writer.OnKeyedItem("Id");
            writer.OnInt64Scalar(tableId);
            writer.OnKeyedItem("Name");
            writer.OnStringScalar(tableName);
            WriteSchemaRef(writer, tableName);
            WriteProviderSchemaRef(writer, tableName);
            writer.OnEndMap();
        }
        writer.OnEndList();
        writer.OnKeyedItem("Writes");
        writer.OnBeginList();
        TMapLimited<TStringBuf, TVectorLimited<NodeDataProviderPair>> writeTables(Allocator_.get());
        for (const auto& w : Writes_) {
            TVector<TPinInfo> outputs;
            w.second->GetPlanFormatter().GetOutputs(*w.first, outputs, /* withLimits */ false);
            YQL_ENSURE(outputs.size() == 1);
            writeTables.try_emplace(AppendString(outputs.front().DisplayName), TVectorLimited<NodeDataProviderPair>(Allocator_.get())).first->second.push_back(w);
        }
        for (const auto& w : writeTables) {
            writer.OnListItem();
            writer.OnBeginMap();
            writer.OnKeyedItem("Id");
            writer.OnInt64Scalar(++NextWriteId_);
            writer.OnKeyedItem("Name");
            writer.OnStringScalar(w.first);
            WriteSchemaRef(writer, w.first);
            WriteProviderSchemaRef(writer, w.first);
            WriteLineageRef(writer, w.first);
            writer.OnEndMap();
        }
        writer.OnEndList();
        if (Options_.Version > 1) {
            WriteReadSet(writer);
        }
        writer.OnEndMap();
        Ctx_.LineageStats.Duration = (TInstant::Now() - startTime).MicroSeconds();
        Ctx_.LineageStats.Memory = Allocator_->GetAllocatedSize();
        return s.Str();
    }

private:
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

    using TColumnsLineage = THashMapLimited<TStringBuf, TFieldsLineage>;

    class TLineage {
    public:
        // Data with named columns, each column tracked separately.
        TColumnsLineage& InitColumns(IAllocator* allocator) {
            return Data_.emplace<TColumnsLineage>(allocator);
        }

        // A sequence of non-struct items (e.g. List<Int32>): there are no columns to track
        // separately, the whole item has one lineage. Consumers that reference such an item can only
        // depend on all of it, see CollectAllItems.
        TFieldsLineage& InitColumnless(IAllocator* allocator) {
            return Data_.emplace<TFieldsLineage>(allocator);
        }

        void Reset() {
            Data_.emplace<std::monostate>();
        }

        bool IsCalculated() const {
            return !std::holds_alternative<std::monostate>(Data_);
        }

        bool IsColumnBased() const {
            return std::holds_alternative<TColumnsLineage>(Data_);
        }

        TColumnsLineage& Columns() {
            return std::get<TColumnsLineage>(Data_);
        }

        const TColumnsLineage& Columns() const {
            return std::get<TColumnsLineage>(Data_);
        }

        const TFieldsLineage& Columnless() const {
            return std::get<TFieldsLineage>(Data_);
        }

    private:
        std::variant<std::monostate, TColumnsLineage, TFieldsLineage> Data_;
    };

    void CollectAllItems(const TLineage& src, TFieldLineageSet& dst,
                         ETransformsType newTransforms = ETransformsType::Copy) {
        YQL_ENSURE(src.IsCalculated(), "Nothing to collect from an uncalculated lineage");
        if (!src.IsColumnBased()) {
            for (const auto& i : src.Columnless().Items) {
                dst.insert(ReplaceTransforms(i, newTransforms));
            }
            return;
        }

        for (const auto& f : src.Columns()) {
            for (const auto& i : f.second.Items) {
                dst.insert(ReplaceTransforms(i, newTransforms));
            }
        }
    }

    void CollectResults() {
        TVectorLimited<std::tuple<TStringBuf, const TExprNode*, const TExprNode*>> readTables(Allocator_.get());
        for (const auto& r : Reads_) {
            TVector<TPinInfo> inputs;
            auto& formatter = r.second->GetPlanFormatter();
            formatter.GetInputs(*r.first, inputs, /*withLimits=*/false);
            for (const auto& i : inputs) {
                const TStringBuf& tableName = AppendString(i.DisplayName);
                readTables.emplace_back(tableName, r.first, i.Key);
            }
        }
        SortBy(readTables, [](const auto& x) { return std::get<0>(x); });
        for (const auto& r : readTables) {
            const auto& tableName = std::get<0>(r);
            const auto* readNode = std::get<1>(r);
            const auto* key = std::get<2>(r);
            auto [it, inserted] = TableIds_.try_emplace(tableName, 0);
            if (inserted) {
                it->second = ++NextReadId_;
                const TStructExprType& itemType = *key->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                AddSchemaRef(&itemType, tableName);
            }
            ReadIds_.try_emplace(readNode, TVectorLimited<ui32>(Allocator_.get())).first->second.push_back(it->second);
        }
        THashMapLimited<TStringBuf, TVectorLimited<NodeDataProviderPair>> writeTables(Allocator_.get());
        for (const auto& w : Writes_) {
            TVector<TPinInfo> outputs;
            w.second->GetPlanFormatter().GetOutputs(*w.first, outputs, /* withLimits */ false);
            YQL_ENSURE(outputs.size() == 1);
            const TStringBuf& tableName = AppendString(outputs.front().DisplayName);
            writeTables.try_emplace(tableName, TVectorLimited<NodeDataProviderPair>(Allocator_.get())).first->second.push_back(w);
        }
        for (const auto& w : writeTables) {
            auto data = w.second[0].first->Child(3);
            const auto& itemType = *GetSeqItemType(*data->GetTypeAnn()).Cast<TStructExprType>();
            AddSchemaRef(&itemType, w.first);
            if (w.second.size() == 1) {
                AddLineageRef(*CollectLineage(*data), w.first);
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
                AddLineageRef(lineage, w.first);
            }
        }
    }

    void AddSchemaRef(const TStructExprType* itemType, const TStringBuf& tableName) {
        auto [it, inserted] = SchemaSets_.try_emplace(itemType, 1);
        if (!inserted) {
            it->second += 1;
        }
        SchemaRefs_[tableName] = it->first;
    }

    void AddLineageRef(const TLineage& lineage, const TStringBuf& tableName) {
        if (!lineage.IsCalculated()) {
            YQL_ENSURE(!GetEnv("YQL_LINEAGE_CHECK"), "Can't calculate lineage for " << tableName);
            return;
        }
        YQL_ENSURE(lineage.IsColumnBased(), "Columnless lineage for write target " << tableName << " that already has columns");
        auto& lineageFields = LineageRefs_.emplace(tableName, TMapLimited<TStringBuf, const TVectorLimited<TFieldLineage>*>(Allocator_.get())).first->second;
        for (const auto& fi : lineage.Columns()) {
            TVectorLimited<TFieldLineage> items(Allocator_.get());
            for (const auto& i : lineage.Columns().at(fi.first).Items) {
                items.push_back(i);
            }
            Sort(items);
            auto [it, inserted] = LineageSets_.try_emplace(items, 1);
            if (!inserted) {
                it->second += 1;
            }
            lineageFields.emplace(fi.first, &it->first);
        }
    }

    IPlanFormatter* GetFormatter() {
        if (!Writes_.empty()) {
            return &Writes_.begin()->second->GetPlanFormatter();
        } else if (!Reads_.empty()) {
            return &Reads_.begin()->second->GetPlanFormatter();
        }
        return nullptr;
    }

    void WriteSchemaSet(NYson::TYsonWriter& writer) {
        bool isDuplicate = false;
        size_t index = 1;
        TVectorLimited<std::pair<const TStructExprType*, ui32>> items(Allocator_.get());
        items.reserve(SchemaSets_.size());
        for (auto& [value, count] : SchemaSets_) {
            if (count > 1) {
                count = ++index;
                isDuplicate = true;
                items.push_back({value, count});
            }
        }
        if (Options_.Version > 1 && isDuplicate) {
            writer.OnKeyedItem("SchemaSets");
            writer.OnBeginList();
            for (auto& [value, ind] : items) {
                writer.OnListItem();
                writer.OnBeginMap();
                writer.OnKeyedItem("Id");
                writer.OnInt64Scalar(ind - 1);
                writer.OnKeyedItem("Schema");
                WriteSchema(writer, *value, /*formatter=*/nullptr);
                writer.OnEndMap();
            }
            writer.OnEndList();
            writer.OnKeyedItem("YtSchemaSets");
            writer.OnBeginList();
            for (auto& [value, ind] : items) {
                writer.OnListItem();
                writer.OnBeginMap();
                writer.OnKeyedItem("Id");
                writer.OnInt64Scalar(ind - 1);
                writer.OnKeyedItem("YtSchema");
                WriteSchema(writer, *value, GetFormatter());
                writer.OnEndMap();
            }
            writer.OnEndList();
        }
    }

    void WriteLineageSet(NYson::TYsonWriter& writer) {
        bool isDuplicate = false;
        size_t index = 1;
        TVectorLimited<std::pair<const TVectorLimited<TFieldLineage>*, ui32>> items(Allocator_.get());
        for (auto& [value, count] : LineageSets_) {
            if (count > 1) {
                count = ++index;
                isDuplicate = true;
                items.push_back({&value, count});
            }
        }
        if (Options_.Version > 1 && isDuplicate) {
            SortBy(items, [](const auto& x) { return x.second; });
            writer.OnKeyedItem("LineageSets");
            writer.OnBeginList();
            for (auto& [value, ind] : items) {
                writer.OnListItem();
                writer.OnBeginMap();
                writer.OnKeyedItem("Id");
                writer.OnInt64Scalar(ind - 1);
                writer.OnKeyedItem("Lineage");
                WriteLineageSection(writer, value);
                writer.OnEndMap();
            }
            writer.OnEndList();
        }
    }

    void WriteReadSet(NYson::TYsonWriter& writer) {
        if (!IndexSets_.empty()) {
            TVectorLimited<std::pair<TSetLimited<ui32>, ui32>> items(Allocator_.get());
            items.reserve(IndexSets_.size());
            Copy(IndexSets_.begin(), IndexSets_.end(), std::back_inserter(items));
            SortBy(items, [](const auto& x) { return x.second; });
            writer.OnKeyedItem("ReadSets");
            writer.OnBeginList();
            for (const auto& [key, value] : items) {
                writer.OnListItem();
                writer.OnBeginMap();
                writer.OnKeyedItem("Id");
                writer.OnInt64Scalar(value);
                writer.OnKeyedItem("Inputs");
                TVectorLimited<ui32> inputs(Allocator_.get());
                inputs.assign(key.begin(), key.end());
                Sort(inputs);
                writer.OnBeginList();
                for (const auto& el : inputs) {
                    writer.OnListItem();
                    writer.OnInt64Scalar(el);
                }
                writer.OnEndList();
                writer.OnEndMap();
            }
            writer.OnEndList();
        }
    }

    void WriteLineageRef(NYson::TYsonWriter& writer, const TStringBuf& tableName) {
        writer.OnKeyedItem("Lineage");
        auto it = LineageRefs_.find(tableName);
        if (it == LineageRefs_.end()) {
            if (Options_.Standalone) {
                writer.OnEntity();
                return;
            }
            throw yexception() << TStringBuilder() << "Lineage can't be calculated for " << tableName << " table";
        }
        writer.OnBeginMap();
        const auto& lineageRefs = it->second;
        if (lineageRefs.empty()) {
            writer.OnEndMap();
            return;
        }
        for (const auto& [fieldName, lineage] : lineageRefs) {
            writer.OnKeyedItem(fieldName);
            if (Options_.Version > 1 && LineageSets_.at(*lineage) > 1) {
                writer.OnBeginList();
                writer.OnListItem();
                writer.OnBeginMap();
                writer.OnKeyedItem("Ref");
                writer.OnInt64Scalar(LineageSets_.at(*lineage) - 1);
                writer.OnEndMap();
                writer.OnEndList();
            } else {
                WriteLineageSection(writer, lineage);
            }
        }
        writer.OnEndMap();
    }

    void WriteLineageSection(NYson::TYsonWriter& writer, const TVectorLimited<TFieldLineage>* fieldLineage) {
        TMapLimited<std::pair<TStringBuf, ETransformsType>, TSetLimited<ui32>> inputSets(Allocator_.get());
        if (Options_.Version > 1) {
            // if several indices have the save (Field, Transforms), replace them with new syntetic index which is stored into IndexSets_
            for (const auto& i : *fieldLineage) {
                inputSets.try_emplace(make_pair(i.Field, i.Transforms), TSetLimited<ui32>(Allocator_.get())).first->second.insert(i.InputIndex);
            }
            for (const auto& inputs : inputSets) {
                if (inputs.second.size() > 1) {
                    if (IndexSets_.find(inputs.second) == IndexSets_.end()) {
                        IndexSets_[inputs.second] = ++NextReadId_;
                    }
                }
            }
        }
        THashSetLimited<std::pair<TStringBuf, ETransformsType>> checkedItems(Allocator_.get());
        TVectorLimited<TFieldLineage> items(Allocator_.get());
        for (const auto& i : *fieldLineage) {
            ui32 inputIndex = i.InputIndex;
            if (Options_.Version > 1) {
                if (checkedItems.contains(make_pair(TString(i.Field), i.Transforms))) {
                    continue;
                }
                auto it = inputSets.find(make_pair(TString(i.Field), i.Transforms));
                if (it->second.size() > 1) {
                    auto itt = IndexSets_.find(it->second);
                    inputIndex = itt->second;
                }
            }
            items.push_back({.Field = i.Field, .InputIndex = inputIndex, .Transforms = i.Transforms});
            checkedItems.insert(make_pair(i.Field, i.Transforms));
        }
        Sort(items);
        writer.OnBeginList();
        for (const auto& i : items) {
            ui32 inputIndex = i.InputIndex;
            writer.OnListItem();
            writer.OnBeginMap();
            writer.OnKeyedItem("Input");
            writer.OnInt64Scalar(inputIndex);
            writer.OnKeyedItem("Field");
            writer.OnStringScalar(i.Field);
            writer.OnKeyedItem("Transforms");
            switch (i.Transforms) {
                case ETransformsType::Copy:
                    writer.OnStringScalar("Copy");
                    break;
                case ETransformsType::Math:
                    writer.OnStringScalar("Math");
                    break;
                default:
                    writer.OnEntity();
            }
            writer.OnEndMap();
        }
        writer.OnEndList();
    }

    void WriteSchemaRef(NYson::TYsonWriter& writer, const TStringBuf& tableName) {
        const auto schema = SchemaRefs_[tableName];
        if (Options_.Version > 1 && SchemaSets_[schema] > 1) {
            writer.OnKeyedItem("SchemaRef");
            writer.OnInt64Scalar(SchemaSets_[schema] - 1);
        } else {
            writer.OnKeyedItem("Schema");
            WriteSchema(writer, *schema, /*formatter=*/nullptr);
        }
    }

    void WriteProviderSchemaRef(NYson::TYsonWriter& writer, const TStringBuf& tableName) {
        const auto schema = SchemaRefs_[tableName];
        if (Options_.Version > 1 && SchemaSets_[schema] > 1) {
            writer.OnKeyedItem("YtSchemaRef");
            writer.OnInt64Scalar(SchemaSets_[schema] - 1);
        } else {
            writer.OnKeyedItem("YtSchema");
            WriteSchema(writer, *schema, GetFormatter());
        }
    }

    void WriteSchema(NYson::TYsonWriter& writer, const TStructExprType& structType, IPlanFormatter* formatter) {
        writer.OnBeginMap();
        for (const auto& i : structType.GetItems()) {
            if (IsYqlSystemField(i->GetName())) {
                continue;
            }

            writer.OnKeyedItem(i->GetName());
            if (formatter) {
                formatter->WriteTypeDetails(writer, *i->GetItemType());
            } else {
                if (Options_.Standalone && Options_.YsonTypeFormat) {
                    NCommon::WriteTypeToYson(writer, i->GetItemType());
                } else {
                    writer.OnStringScalar(FormatType(i->GetItemType()));
                }
            }
        }

        writer.OnEndMap();
    }

    static bool IsMathCallable(const TExprNode& node) {
        if (node.IsCallable({"+", "-", "*", "/", "%",
                             "Add", "Sub", "Mul", "Div", "Mod",
                             "Plus", "Minus", "Abs", "Increment", "Decrement",
                             "BitAnd", "BitOr", "BitXor", "BitNot", "ShiftLeft", "ShiftRight", "CountBits"})) {
            return true;
        }
        return node.IsCallable("Apply") && node.Head().IsCallable("Udf") &&
               node.Head().Head().Content().StartsWith("Math.");
    }

    static ETransformsType GetValueTransforms(const TExprNode& expr, const TExprNode& arg) {
        const TExprNode* root = &expr;
        while (root->IsCallable({"Just", "AsTagged"})) {
            root = &root->Head();
        }

        if (root->IsCallable("Member") && &root->Head() == &arg) {
            return ETransformsType::Copy;
        }

        if (IsMathCallable(*root)) {
            return ETransformsType::Math;
        }

        return ETransformsType::None;
    }

    static TFieldLineage ReplaceTransforms(const TFieldLineage& src, ETransformsType newTransforms) {
        const ETransformsType result = (newTransforms == ETransformsType::Copy)
                                           ? src.Transforms
                                           : newTransforms;
        return {.Field = src.Field, .InputIndex = src.InputIndex, .Transforms = result};
    }

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

    template <typename TCallback>
    static void ProcessReadItem(const TTypeAnnotationNode* itemType, const TCallback& callback) {
        if (itemType->GetKind() == ETypeAnnotationKind::Variant) {
            for (const auto* alt : itemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>()->GetItems()) {
                for (const auto* i : alt->Cast<TStructExprType>()->GetItems()) {
                    callback(*i);
                }
            }
        } else {
            for (const auto* i : itemType->Cast<TStructExprType>()->GetItems()) {
                callback(*i);
            }
        }
    }

    const TLineage* CollectLineage(const TExprNode& node) {
        if (auto it = Lineages_.find(&node); it != Lineages_.end()) {
            return &it->second;
        }

        auto& lineage = Lineages_[&node];
        if (auto readIt = ReadIds_.find(&node); readIt != ReadIds_.end()) {
            auto& columns = lineage.InitColumns(Allocator_.get());
            auto type = node.GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1]->Cast<TListExprType>()->GetItemType();
            ProcessReadItem(type, [&](const TItemExprType& i) {
                if (IsYqlSystemField(i.GetName())) {
                    return;
                }

                auto& v = columns.try_emplace(i.GetName(), TFieldsLineage(Allocator_.get())).first->second;
                for (const auto& r : readIt->second) {
                    v.Items.insert({i.GetName(), r, ETransformsType::Copy});
                }
            });

            return &lineage;
        }

        if (!HasReads_.contains(&node)) {
            auto type = node.GetTypeAnn();
            if (type->GetKind() == ETypeAnnotationKind::List) {
                auto itemType = type->Cast<TListExprType>()->GetItemType();
                if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
                    auto structType = itemType->Cast<TStructExprType>();
                    auto& columns = lineage.InitColumns(Allocator_.get());
                    for (const auto& i : structType->GetItems()) {
                        if (IsYqlSystemField(i->GetName())) {
                            continue;
                        }

                        columns.emplace(i->GetName(), TFieldsLineage(Allocator_.get()));
                    }

                    return &lineage;
                }
            }
        }

        if (IsNonStructSequence(*node.GetTypeAnn())) {
            CollectColumnlessLineage(lineage, node);
            return &lineage;
        }

        if (node.IsCallable({"Unordered",
                             "UnorderedSubquery",
                             "Right!",
                             "YtTableContent",
                             "Skip",
                             "Take",
                             "Sort",
                             "TopSort",
                             "Top",
                             "Nth",
                             "Demux",
                             "AssumeSorted", "AssumeUnique", "AssumeDistinct", "AssumeUniqueHint", "AssumeDistinctHint",
                             "AssumeChopped", "AssumeConstraints", "AssumeStrict", "AssumeNonStrict",
                             "SkipNullMembers", "FilterNullMembers",
                             "Iterator", "ToFlow", "FromFlow", "ToStream", "ForwardList", "Just", "WithWorld"})) {
            lineage = *CollectLineage(node.Head());
            return &lineage;
        } else if (node.IsCallable("YtMaterialize!")) {
            lineage = *CollectLineage(*node.Child(2));
            return &lineage;
        } else if (node.IsCallable("ExtractMembers")) {
            HandleExtractMembers(lineage, node);
        } else if (node.IsCallable({"FlatMap", "OrderedFlatMap"})) {
            HandleFlatMap(lineage, node);
        } else if (node.IsCallable("Aggregate")) {
            HandleAggregate(lineage, node);
        } else if (node.IsCallable({"Extend", "OrderedExtend", "Merge"})) {
            HandleExtend(lineage, node);
        } else if (node.IsCallable("Mux")) {
            HandleMux(lineage, node);
        } else if (node.IsCallable({"CalcOverWindow", "CalcOverSessionWindow", "CalcOverWindowGroup"})) {
            HandleWindow(lineage, node);
        } else if (node.IsCallable("EquiJoin")) {
            HandleEquiJoin(lineage, node);
        } else if (node.IsCallable({"LMap", "OrderedLMap"})) {
            HandleLMap(lineage, node);
        } else if (node.IsCallable({"PartitionsByKeys", "PartitionByKey"})) {
            HandlePartitionByKeys(lineage, node);
        } else if (node.IsCallable("Chopper")) {
            HandleChopper(lineage, node);
        } else if (node.IsCallable("Condense1")) {
            HandleCondense1(lineage, node);
        } else if (node.IsCallable("CombineByKey")) {
            HandleCombineByKey(lineage, node);
        } else if (node.IsCallable({"AsList", "List", "ListIf"})) {
            HandleListLiteral(lineage, node);
        } else if (node.IsCallable("AsStruct")) {
            HandleAsStruct(lineage, node);
        } else {
            Warning(node);
        }

        return &lineage;
    }

    static bool IsNonStructSequence(const TTypeAnnotationNode& type) {
        switch (type.GetKind()) {
            case ETypeAnnotationKind::List:
            case ETypeAnnotationKind::Stream:
            case ETypeAnnotationKind::Flow:
                break;
            default:
                return false;
        }

        const auto itemKind = GetSeqItemType(type).GetKind();
        return itemKind != ETypeAnnotationKind::Struct && itemKind != ETypeAnnotationKind::Variant;
    }

    void CollectColumnlessLineage(TLineage& lineage, const TExprNode& node) {
        YQL_ENSURE(!lineage.IsColumnBased(), "Columnless lineage for data that already has columns");
        TNodeMap<TMaybe<TFieldsLineage>> visited;
        auto res = ScanExprLineage(node, /*arg=*/nullptr, /*src=*/nullptr, visited, TFieldsLineageMap(Allocator_.get()));
        auto& item = lineage.InitColumnless(Allocator_.get());
        if (res) {
            item.MergeFrom(*res);
        }
    }

    void Warning(const TExprNode& node) {
        auto message = TStringBuilder() << node.Type() << " : " << node.Content() << " is not supported";
        if (Options_.Standalone) {
            auto issue = TIssue(ExprCtx_.GetPosition(node.Pos()), message);
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_LINEAGE_INTERNAL_ERROR, issue);
            ExprCtx_.AddWarning(issue);
        } else {
            throw yexception() << message;
        }
    }

    void HandleExtractMembers(TLineage& lineage, const TExprNode& node) {
        FilterColumns(lineage, *CollectLineage(node.Head()), *node.Child(1));
    }

    void FilterColumns(TLineage& lineage, const TLineage& innerLineage, const TExprNode& members) {
        if (!innerLineage.IsColumnBased()) {
            return;
        }

        auto& columns = lineage.InitColumns(Allocator_.get());
        for (const auto& atom : members.Children()) {
            TStringBuf fieldName(atom->Content());
            auto it = innerLineage.Columns().find(fieldName);
            if (it != innerLineage.Columns().end()) {
                columns.insert_or_assign(fieldName, it->second);
            } else {
                columns.insert_or_assign(fieldName, TFieldsLineage(Allocator_.get()));
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
            if (&node.Head() == arg && src && src->IsColumnBased()) {
                if (IsYqlSystemField(node.Tail().Content())) {
                    return it->second = TFieldsLineage(Allocator_.get());
                }
                return it->second = src->Columns().at(node.Tail().Content());
            }

            if (node.Head().IsCallable("Head")) {
                auto lineage = CollectLineage(node.Head().Head());
                if (lineage && lineage->IsColumnBased()) {
                    TFieldsLineage result(Allocator_.get());
                    for (const auto& f : lineage->Columns()) {
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
                if (IsYqlSystemField(node.Tail().Content())) {
                    return it->second = TFieldsLineage(Allocator_.get());
                }
                TFieldsLineage result(Allocator_.get());
                result.Items = (*inner->StructItems).at(node.Tail().Content());
                return it->second = result;
            }
        }

        bool sqlInTableSource = false;
        if (node.IsCallable("SqlIn")) {
            sqlInTableSource = HasSetting(*node.Child(2), "tableSource");
            if (sqlInTableSource) {
                auto lineage = CollectLineage(*node.Child(0));
                if (lineage && lineage->IsCalculated()) {
                    TFieldsLineage result(Allocator_.get());
                    CollectAllItems(*lineage, result.Items);
                    return it->second = result;
                }
            }
        }

        std::vector<TFieldsLineage> results;
        TMaybe<bool> hasStructItems;
        for (ui32 index = 0; index < node.ChildrenSize(); ++index) {
            if (index == 0 && sqlInTableSource) {
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
            CollectAllItems(src, dst, newTransforms);
        } else {
            for (const auto& i : res->Items) {
                dst.insert(ReplaceTransforms(i, newTransforms));
            }
        }
    }

    void MergeLineageFromUsedFields(const TExprNode& expr, const TExprNode& arg, const TLineage& src,
                                    TFieldsLineage& dst, bool produceStruct, const TFieldsLineageMap& flattenColumns,
                                    ETransformsType newTransforms = ETransformsType::None) {
        if (produceStruct && src.IsColumnBased()) {
            auto root = &expr;
            while (root->IsCallable("Just")) {
                root = &root->Head();
            }

            if (root == &arg) {
                dst.StructItems.ConstructInPlace(Allocator_.get());
                for (const auto& f : src.Columns()) {
                    (*dst.StructItems).insert_or_assign(f.first, f.second.Items);
                }
            } else if (root->IsCallable("AsStruct")) {
                dst.StructItems.ConstructInPlace(Allocator_.get());
                for (const auto& x : root->Children()) {
                    auto fieldName = x->Head().Content();
                    auto& s = (*dst.StructItems).try_emplace(fieldName, TFieldLineageSet(Allocator_.get())).first->second;
                    MergeLineageFromUsedFields(x->Tail(), arg, src, s, flattenColumns,
                                               GetValueTransforms(x->Tail(), arg));
                }
            } else if (root->IsCallable("Member") && &root->Head() == &arg) {
                auto fieldName = root->Tail().Content();
                if (!IsYqlSystemField(fieldName)) {
                    dst.StructItems = src.Columns().at(fieldName).StructItems;
                }
            }
        }

        MergeLineageFromUsedFields(expr, arg, src, dst.Items, flattenColumns, newTransforms);
    }

    void FillStructLineage(TLineage& lineage, const TExprNode* value, const TExprNode& arg, const TLineage& innerLineage,
                           const TTypeAnnotationNode* extType, const TFieldsLineageMap& flattenColumns) {
        while (value && value->IsCallable("Variant")) {
            value = &value->Head();
        }

        if (value == &arg) {
            lineage = innerLineage;
            return;
        }

        TMaybe<TStringBuf> oneField;
        if (value && value->IsCallable("Member") && &value->Head() == &arg && innerLineage.IsColumnBased()) {
            const auto fieldName = value->Tail().Content();
            if (!IsYqlSystemField(fieldName)) {
                auto& f = innerLineage.Columns().at(fieldName);
                if (f.StructItems) {
                    for (const auto& x : *f.StructItems) {
                        auto& res = lineage.Columns().try_emplace(x.first, TFieldsLineage(Allocator_.get())).first->second;
                        res.Items = x.second;
                    }
                    return;
                }
            }

            // fallback
            oneField = fieldName;
        }

        if (value && value->IsCallable("If")) {
            TLineage left;
            TLineage right;
            left.InitColumns(Allocator_.get());
            right.InitColumns(Allocator_.get());
            FillStructLineage(left, value->Child(1), arg, innerLineage, extType, TFieldsLineageMap(Allocator_.get()));
            FillStructLineage(right, value->Child(2), arg, innerLineage, extType, TFieldsLineageMap(Allocator_.get()));
            for (const auto& f : left.Columns()) {
                auto& res = lineage.Columns().try_emplace(f.first, TFieldsLineage(Allocator_.get())).first->second;
                res.Items.insert(f.second.Items.begin(), f.second.Items.end());
            }

            for (const auto& f : right.Columns()) {
                auto& res = lineage.Columns().try_emplace(f.first, TFieldsLineage(Allocator_.get())).first->second;
                res.Items.insert(f.second.Items.begin(), f.second.Items.end());
            }

            return;
        }

        if (value && value->IsCallable("AsStruct")) {
            for (const auto& child : value->Children()) {
                auto& res = lineage.Columns().try_emplace(child->Head().Content(), TFieldsLineage(Allocator_.get())).first->second;
                const auto& expr = child->Tail();
                MergeLineageFromUsedFields(expr, arg, innerLineage, res, /*produceStruct=*/true, flattenColumns,
                                           GetValueTransforms(expr, arg));
            }

            return;
        }

        if (extType && (extType->GetKind() == ETypeAnnotationKind::Struct || extType->GetKind() == ETypeAnnotationKind::Variant)) {
            TFieldLineageSet allLineage(Allocator_.get());
            if (oneField) {
                for (const auto& f : innerLineage.Columns()) {
                    if (oneField == f.first) {
                        allLineage.insert(f.second.Items.begin(), f.second.Items.end());
                    }
                }
            } else {
                CollectAllItems(innerLineage, allLineage);
            }

            ProcessReadItem(extType, [&](const TItemExprType& i) {
                if (IsYqlSystemField(i.GetName())) {
                    return;
                }

                auto& res = lineage.Columns().try_emplace(i.GetName(), TFieldsLineage(Allocator_.get())).first->second;
                res.Items = allLineage;
            });
        }
    }

    void HandleFlatMap(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.IsCalculated()) {
            return;
        }

        const auto& lambda = node.Tail();
        CollectLambdaBodyLineage(lineage, lambda.Tail(), lambda.Head().Head(), innerLineage);
    }

    const TExprNode* CollectFlattenChain(const TExprNode& body, const TExprNode& arg, const TLineage& innerLineage,
                                         TFieldsLineageMap& flattenColumns) {
        const TExprNode* value = &body;
        while (value->IsCallable({"FlatMap", "OrderedFlatMap"})) {
            TNodeMap<TMaybe<TFieldsLineage>> visited;
            auto res = ScanExprLineage(value->Head(), &arg, &innerLineage, visited, TFieldsLineageMap(Allocator_.get()));
            if (!res) {
                TFieldsLineage all(Allocator_.get());
                CollectAllItems(innerLineage, all.Items);
                res = std::move(all);
            }
            flattenColumns.emplace(value->Tail().Head().HeadPtr().Get(), res);
            value = &value->Tail().Tail();
        }
        if (value->IsCallable("Just")) {
            value = &value->Head();
        } else if (value->IsCallable({"OptionalIf", "FlatListIf", "ListIf"})) {
            value = &value->Tail();
        }
        return value;
    }

    void CollectLambdaBodyLineage(TLineage& lineage, const TExprNode& body, const TExprNode& arg,
                                  const TLineage& innerLineage, bool strict = true) {
        if (body.IsCallable({"Extend", "OrderedExtend", "Merge"})) {
            TVectorLimited<TLineage> inners(Allocator_.get());
            for (const auto& child : body.Children()) {
                CollectLambdaBodyLineage(inners.emplace_back(), *child, arg, innerLineage, strict);
                if (!inners.back().IsColumnBased()) {
                    lineage.Reset();
                    return;
                }
            }
            MergeLineages(lineage, inners);
            return;
        }

        if (body.IsCallable({"AsList", "List"})) {
            const auto* itemType = GetSeqItemType(body.GetTypeAnn());
            const ui32 firstItem = body.IsCallable("List") ? 1 : 0;
            TVectorLimited<TLineage> inners(Allocator_.get());
            for (ui32 i = firstItem; i < body.ChildrenSize(); ++i) {
                auto& inner = inners.emplace_back();
                inner.InitColumns(Allocator_.get());
                FillStructLineage(inner, body.Child(i), arg, innerLineage, itemType,
                                  TFieldsLineageMap(Allocator_.get()));
            }

            if (inners.empty()) {
                auto& columns = lineage.InitColumns(Allocator_.get());
                ProcessReadItem(itemType, [&](const TItemExprType& i) {
                    columns.emplace(i.GetName(), TFieldsLineage(Allocator_.get()));
                });
                return;
            }

            MergeLineages(lineage, inners);
            return;
        }

        if (body.IsCallable("ExtractMembers")) {
            TLineage inner;
            CollectLambdaBodyLineage(inner, body.Head(), arg, innerLineage, strict);
            FilterColumns(lineage, inner, *body.Child(1));
            return;
        }

        if (body.IsCallable("If")) {
            TVectorLimited<TLineage> inners(Allocator_.get());
            bool untrackable = false;
            const auto collectBranch = [&](const TExprNode& branch) {
                if (untrackable || IsEmptyContainer(branch)) {
                    return;
                }
                CollectLambdaBodyLineage(inners.emplace_back(), branch, arg, innerLineage, strict);
                untrackable = !inners.back().IsColumnBased();
            };

            const auto count = body.ChildrenSize();
            for (ui32 i = 1; i + 1 < count; i += 2) {
                collectBranch(*body.Child(i));
            }
            collectBranch(body.Tail());

            if (untrackable || inners.empty()) {
                lineage.Reset();
                return;
            }

            MergeLineages(lineage, inners);
            return;
        }

        if (body.IsCallable("FlatOptionalIf")) {
            CollectLambdaBodyLineage(lineage, body.Tail(), arg, innerLineage, strict);
            return;
        }

        TFieldsLineageMap flattenColumns(Allocator_.get());
        const TExprNode* value = &body.Tail();
        if (body.IsCallable({"OptionalIf", "FlatListIf", "ListIf"})) {
            value = &body.Tail();
        } else if (body.IsCallable({"Just", "ToStream"})) {
            value = &body.Head();
        } else if (body.IsCallable({"FlatMap", "OrderedFlatMap"})) {
            switch (body.GetTypeAnn()->GetKind()) {
                case ETypeAnnotationKind::List:
                    value = CollectFlattenChain(body, arg, innerLineage, flattenColumns);
                    break;
                case ETypeAnnotationKind::Optional: {
                    TFieldsLineageMap chainColumns(Allocator_.get());
                    const auto* chained = CollectFlattenChain(body, arg, innerLineage, chainColumns);
                    if (chained == &arg || chained->IsCallable("AsStruct")) {
                        value = chained;
                        flattenColumns = std::move(chainColumns);
                    } else {
                        value = &body.Head();
                    }
                    break;
                }
                default:
                    value = &body.Head();
                    break;
            }
        } else if (body.IsCallable("GroupByKey")) {
            value = &body;
        } else if (!strict) {
            lineage.InitColumns(Allocator_.get());
            FillStructLineage(lineage, /*value=*/nullptr, arg, innerLineage, GetSeqItemType(body.GetTypeAnn()),
                              TFieldsLineageMap(Allocator_.get()));
            return;
        } else {
            Warning(body);
            return;
        }

        if (value == &arg) {
            lineage = innerLineage;
            return;
        }

        lineage.InitColumns(Allocator_.get());
        FillStructLineage(lineage, value, arg, innerLineage, GetSeqItemType(body.GetTypeAnn()), flattenColumns);
    }

    void HandleAggregate(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.IsColumnBased()) {
            return;
        }

        auto& columns = lineage.InitColumns(Allocator_.get());
        for (const auto& key : node.Child(1)->Children()) {
            auto it = innerLineage.Columns().find(key->Content());
            if (it != innerLineage.Columns().end()) {
                columns.insert_or_assign(key->Content(), it->second);
            } else {
                columns.insert_or_assign(key->Content(), TFieldsLineage(Allocator_.get()));
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
                    innerLineage.Columns().try_emplace(payload->Child(2)->Content(), TFieldsLineage(Allocator_.get())).first->second,
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
                                               /*produceStruct=*/false,
                                               TFieldsLineageMap(Allocator_.get()));
                    MergeLineageFromUsedFields(updateHandler->Tail(),
                                               updateHandler->Head().Head(),
                                               innerLineage,
                                               source,
                                               /*produceStruct=*/false,
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
                    lineage.Reset();
                    return;
                }
            }

            for (const auto& field : fields) {
                columns.insert_or_assign(field, source);
            }
        }

        if (const TExprNode::TPtr outputColumnsSetting = GetSetting(*node.Child(3), "output_columns")) {
            TSetLimited<TStringBuf> outMembers(Allocator_.get());
            const auto& settingsList = outputColumnsSetting->ChildPtr(1)->ChildrenList();
            Transform(settingsList.begin(),
                      settingsList.end(),
                      std::inserter(outMembers, outMembers.begin()),
                      [](const auto& x) { return x->Content(); });
            EraseNodesIf(columns, [&outMembers](auto& iter) {
                return !outMembers.contains(iter.first);
            });
        }
    }

    void HandleGroupHandler(TLineage& lineage, const TExprNode& node, const TExprNode& arg) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.IsColumnBased()) {
            return;
        }

        const auto& body = node.Tail().Tail();
        if (&body == &arg) {
            lineage = innerLineage;
            return;
        }

        if (body.IsCallable({"FlatMap", "OrderedFlatMap"}) && &body.Head() == &arg) {
            const auto& lambda = body.Tail();
            CollectLambdaBodyLineage(lineage, lambda.Tail(), lambda.Head().Head(), innerLineage, /*strict=*/false);
            return;
        }

        lineage.InitColumns(Allocator_.get());
        FillStructLineage(lineage,
                          /*value=*/nullptr,
                          arg,
                          innerLineage,
                          GetSeqItemType(body.GetTypeAnn()),
                          TFieldsLineageMap(Allocator_.get()));
    }

    void HandleLMap(TLineage& lineage, const TExprNode& node) {
        HandleGroupHandler(lineage, node, node.Tail().Head().Head());
    }

    void HandlePartitionByKeys(TLineage& lineage, const TExprNode& node) {
        HandleGroupHandler(lineage, node, node.Tail().Head().Head());
    }

    void HandleChopper(TLineage& lineage, const TExprNode& node) {
        HandleGroupHandler(lineage, node, node.Tail().Head().Tail());
    }

    void HandleCondense1(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.IsColumnBased()) {
            lineage.InitColumns(Allocator_.get());
            return;
        }

        auto stateType = GetSeqItemType(node.GetTypeAnn());
        if (!stateType || stateType->GetKind() != ETypeAnnotationKind::Struct) {
            lineage.InitColumns(Allocator_.get());
            return;
        }

        const auto& initHandler = *node.Child(1);
        const auto& updateHandler = *node.Child(3);

        TFieldsLineage source(Allocator_.get());
        MergeLineageFromUsedFields(initHandler.Tail(), initHandler.Head().Head(), innerLineage, source,
                                   /*produceStruct=*/false, TFieldsLineageMap(Allocator_.get()));
        MergeLineageFromUsedFields(updateHandler.Tail(), updateHandler.Head().Head(), innerLineage, source,
                                   /*produceStruct=*/false, TFieldsLineageMap(Allocator_.get()));

        auto& columns = lineage.InitColumns(Allocator_.get());
        for (const auto& i : stateType->Cast<TStructExprType>()->GetItems()) {
            columns.insert_or_assign(i->GetName(), source);
        }
    }

    void HandleCombineByKey(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.IsColumnBased()) {
            return;
        }

        auto itemType = GetSeqItemType(node.GetTypeAnn());
        if (!itemType || itemType->GetKind() != ETypeAnnotationKind::Struct) {
            return;
        }

        const auto& preMapHandler = *node.Child(1);
        const auto& preMapArg = preMapHandler.Head().Head();
        const TExprNode* preMapValue = &preMapHandler.Tail();
        while (preMapValue->IsCallable({"Just", "ToStream"})) {
            preMapValue = &preMapValue->Head();
        }

        TLineage mappedLineage;
        if (preMapValue == &preMapArg) {
            mappedLineage = innerLineage;
        } else {
            mappedLineage.InitColumns(Allocator_.get());
            FillStructLineage(mappedLineage, preMapValue, preMapArg, innerLineage,
                              GetSeqItemType(preMapHandler.GetTypeAnn()), TFieldsLineageMap(Allocator_.get()));
        }

        if (!mappedLineage.IsColumnBased()) {
            return;
        }

        const auto& keyExtractor = *node.Child(2);
        const auto& initHandler = *node.Child(3);
        const auto& updateHandler = *node.Child(4);

        TFieldsLineage source(Allocator_.get());
        MergeLineageFromUsedFields(keyExtractor.Tail(), keyExtractor.Head().Head(), mappedLineage, source,
                                   /*produceStruct=*/false, TFieldsLineageMap(Allocator_.get()));
        MergeLineageFromUsedFields(initHandler.Tail(), *initHandler.Head().Child(1), mappedLineage, source,
                                   /*produceStruct=*/false, TFieldsLineageMap(Allocator_.get()));
        MergeLineageFromUsedFields(updateHandler.Tail(), *updateHandler.Head().Child(1), mappedLineage, source,
                                   /*produceStruct=*/false, TFieldsLineageMap(Allocator_.get()));

        auto& columns = lineage.InitColumns(Allocator_.get());
        for (const auto& i : itemType->Cast<TStructExprType>()->GetItems()) {
            columns.insert_or_assign(i->GetName(), source);
        }
    }

    void MergeLineages(TLineage& lineage, TVectorLimited<TLineage>& inners) {
        if (inners.empty()) {
            return;
        }

        auto& columns = lineage.InitColumns(Allocator_.get());
        for (const auto& x : inners.front().Columns()) {
            auto& res = columns.try_emplace(x.first, TFieldsLineage(Allocator_.get())).first->second;
            TMaybe<bool> hasStructItems;
            for (const auto& i : inners) {
                if (auto it = i.Columns().find(x.first); it != i.Columns().end()) {
                    auto f = &it->second;
                    for (const auto& x : f->Items) {
                        res.Items.insert(x);
                    }

                    if (f->StructItems) {
                        if (!hasStructItems) {
                            hasStructItems = true;
                        }
                    } else if (!f->Items.empty()) {
                        hasStructItems = false;
                    }
                }
            }

            if (hasStructItems && *hasStructItems) {
                res.StructItems.ConstructInPlace(Allocator_.get());
                for (const auto& i : inners) {
                    if (auto it = i.Columns().find(x.first); it != i.Columns().end()) {
                        auto f = &it->second;
                        if (f->StructItems) {
                            for (const auto& si : *f->StructItems) {
                                auto& items = (*res.StructItems).try_emplace(si.first, TFieldLineageSet(Allocator_.get())).first->second;
                                items.insert(si.second.begin(), si.second.end());
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
            if (!inners.back().IsColumnBased()) {
                return;
            }
        }
        MergeLineages(lineage, inners);
    }

    void HandleMux(TLineage& lineage, const TExprNode& node) {
        TVectorLimited<TLineage> inners(Allocator_.get());
        for (const auto& child : node.Head().Children()) {
            inners.push_back(*CollectLineage(*child));
            if (!inners.back().IsColumnBased()) {
                return;
            }
        }
        MergeLineages(lineage, inners);
    }

    bool HandleSessionColumns(TLineage& lineage, const TLineage& innerLineage,
                              const TExprNode& sessionSpec, const TExprNode& sessionColumns) {
        if (sessionColumns.ChildrenSize() == 0) {
            return true;
        }
        if (!sessionSpec.IsCallable("SessionWindowTraits")) {
            lineage.Reset();
            return false;
        }
        const auto& initHandler = sessionSpec.Child(2);
        const auto& updateHandler = sessionSpec.Child(3);
        for (const auto& sessionColumn : sessionColumns.Children()) {
            auto& res = lineage.Columns().try_emplace(sessionColumn->Content(), TFieldsLineage(Allocator_.get())).first->second;
            MergeLineageFromUsedFields(initHandler->Tail(),
                                       initHandler->Head().Head(),
                                       innerLineage,
                                       res,
                                       /*produceStruct=*/false,
                                       TFieldsLineageMap(Allocator_.get()));
            MergeLineageFromUsedFields(updateHandler->Tail(),
                                       updateHandler->Head().Head(),
                                       innerLineage,
                                       res,
                                       /*produceStruct=*/false,
                                       TFieldsLineageMap(Allocator_.get()));
        }
        return true;
    }

    void HandleWindow(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.IsColumnBased()) {
            return;
        }

        TExprNode::TListType frameGroups;
        if (node.IsCallable("CalcOverWindowGroup")) {
            for (const auto& g : node.Child(1)->Children()) {
                frameGroups.emplace_back(g->Child(2));
            }
        } else {
            frameGroups.emplace_back(node.Child(3));
        }

        lineage = innerLineage;
        if (node.IsCallable("CalcOverSessionWindow")) {
            if (!HandleSessionColumns(lineage, innerLineage, *node.Child(4), *node.Child(5))) {
                return;
            }
        } else if (node.IsCallable("CalcOverWindowGroup")) {
            for (const auto& g : node.Child(1)->Children()) {
                if (!HandleSessionColumns(lineage, innerLineage, *g->Child(3), *g->Child(4))) {
                    return;
                }
            }
        }

        for (const auto& g : frameGroups) {
            for (const auto& f : g->Children()) {
                if (f->IsCallable("WinFilter")) {
                    continue;
                }

                if (!f->IsCallable("WinOnRows")) {
                    lineage.Reset();
                    return;
                }

                for (ui32 i = 1; i < f->ChildrenSize(); ++i) {
                    const auto& list = f->Child(i);
                    auto field = list->Head().Content();
                    auto& res = lineage.Columns().try_emplace(field, TFieldsLineage(Allocator_.get())).first->second;
                    if (list->ChildrenSize() == 3) {
                        res = ReplaceTransforms(
                            innerLineage.Columns().try_emplace(list->Tail().Content(), TFieldsLineage(Allocator_.get())).first->second,
                            ETransformsType::None,
                            Allocator_.get());
                    } else if (list->Tail().IsCallable({"RowNumber", "CumeDist", "NTile"})) {
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
                                                   /*produceStruct=*/false,
                                                   TFieldsLineageMap(Allocator_.get()));
                        MergeLineageFromUsedFields(updateHandler->Tail(),
                                                   updateHandler->Head().Head(),
                                                   innerLineage,
                                                   res,
                                                   /*produceStruct=*/false,
                                                   TFieldsLineageMap(Allocator_.get()));
                    } else {
                        lineage.Reset();
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
            if (!inners.back().IsColumnBased()) {
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

        auto& columns = lineage.InitColumns(Allocator_.get());
        auto structType = node.GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        THashMapLimited<TStringBuf, TMaybe<bool>> hasStructItems(Allocator_.get());
        for (const auto& field : structType->GetItems()) {
            TStringBuf originalName = field->GetName();
            if (auto it = backRename.find(originalName); it != backRename.end()) {
                originalName = it->second;
            }

            TStringBuf table;
            TStringBuf column;
            SplitTableName(originalName, table, column);
            ui32 index = inputLabels.at(table);
            auto& res = columns.try_emplace(field->GetName(), TFieldsLineage(Allocator_.get())).first->second;
            auto& f = inners[index].Columns().at(column);
            for (const auto& i : f.Items) {
                res.Items.insert(i);
            }

            auto& h = hasStructItems[field->GetName()];
            if (f.StructItems) {
                if (!h) {
                    h = true;
                }
            } else if (!f.Items.empty()) {
                h = false;
            }
        }

        for (const auto& field : structType->GetItems()) {
            TStringBuf originalName = field->GetName();
            if (auto it = backRename.find(originalName); it != backRename.end()) {
                originalName = it->second;
            }

            TStringBuf table;
            TStringBuf column;
            SplitTableName(originalName, table, column);
            ui32 index = inputLabels.at(table);
            auto& res = columns.try_emplace(field->GetName(), TFieldsLineage(Allocator_.get())).first->second;
            auto& f = inners[index].Columns().at(column);
            auto& h = hasStructItems[field->GetName()];
            if (h && *h) {
                if (!res.StructItems) {
                    res.StructItems.ConstructInPlace(Allocator_.get());
                }

                if (f.StructItems) {
                    for (const auto& i : *f.StructItems) {
                        auto& items = (*res.StructItems).try_emplace(i.first, TFieldLineageSet(Allocator_.get())).first->second;
                        items.insert(i.second.begin(), i.second.end());
                    }
                }
            }
        }
    }

    void HandleAsStruct(TLineage& lineage, const TExprNode& node) {
        auto& columns = lineage.InitColumns(Allocator_.get());
        for (const auto& f : node.Children()) {
            TNodeMap<TMaybe<TFieldsLineage>> visited;
            auto res = ScanExprLineage(f->Tail(),
                                       /*arg=*/nullptr,
                                       /*src=*/nullptr,
                                       visited,
                                       TFieldsLineageMap(Allocator_.get()));
            if (res) {
                auto name = f->Head().Content();
                columns.try_emplace(name, TFieldsLineage(Allocator_.get())).first->second.MergeFrom(*res);
            }
        }
    }

    void HandleListLiteral(TLineage& lineage, const TExprNode& node) {
        auto itemType = node.GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (itemType->GetKind() != ETypeAnnotationKind::Struct) {
            return;
        }

        auto structType = itemType->Cast<TStructExprType>();
        auto& columns = lineage.InitColumns(Allocator_.get());
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
                                               /*arg=*/nullptr,
                                               /*src=*/nullptr,
                                               visited,
                                               TFieldsLineageMap(Allocator_.get()));
                    if (res) {
                        auto name = f->Head().Content();
                        columns.try_emplace(name, TFieldsLineage(Allocator_.get())).first->second.MergeFrom(*res);
                    }
                }
            } else {
                TNodeMap<TMaybe<TFieldsLineage>> visited;
                auto res = ScanExprLineage(*child,
                                           /*arg=*/nullptr,
                                           /*src=*/nullptr,
                                           visited,
                                           TFieldsLineageMap(Allocator_.get()));
                if (res) {
                    for (const auto& i : structType->GetItems()) {
                        if (IsYqlSystemField(i->GetName())) {
                            continue;
                        }

                        columns.try_emplace(i->GetName(), TFieldsLineage(Allocator_.get())).first->second.MergeFrom(*res);
                    }
                }
            }
        }
    }

    TStringBuf AppendString(const TStringBuf& buf) {
        if (buf.empty()) {
            return ZeroString;
        }

        auto it = Strings_.find(buf);
        if (it != Strings_.end()) {
            return *it;
        }

        auto newBuf = StringPool_.AppendString(buf);
        Strings_.insert(it, newBuf);
        return newBuf;
    }

    const TExprNode& Root_;
    TTypeAnnotationContext& Ctx_;
    TExprContext& ExprCtx_;
    TLineageRunOptions Options_;
    std::unique_ptr<ILimitingAllocator> Allocator_;
    TNodeMapLimited<IDataProvider*> Reads_, Writes_;
    ui32 NextReadId_ = 0;
    ui32 NextWriteId_ = 0;
    TNodeMapLimited<TVectorLimited<ui32>> ReadIds_;
    TNodeMapLimited<TLineage> Lineages_;
    TNodeSetLimited HasReads_;
    TMemoryPool StringPool_;
    THashSetLimited<TStringBuf> Strings_;
    TMapLimited<TStringBuf, ui32> TableIds_;
    // Sets: value -> duplication's count
    TSchemaMapLimited SchemaSets_;
    TLineageMapLimited LineageSets_;
    TIndexMapLimited IndexSets_;
    // Refs: tableName -> value
    THashMapLimited<TStringBuf, const TStructExprType*> SchemaRefs_;
    THashMapLimited<TStringBuf, TMapLimited<TStringBuf, const TOrderedLineageVector*>> LineageRefs_;
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

TString CalculateLineage(const TExprNode& root, TTypeAnnotationContext& ctx, TExprContext& exprCtx, const TLineageRunOptions& options) {
    TLineageScanner scanner(root, ctx, exprCtx, options);
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

    THashMap<i64, NYT::TNode> idToPath1;
    THashMap<i64, NYT::TNode> idToPath2;
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
