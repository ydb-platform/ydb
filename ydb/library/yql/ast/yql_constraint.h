#pragma once

#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/containers/sorted_vector/sorted_vector.h>
#include <library/cpp/json/json_writer.h>

#include <util/stream/output.h>

#include <deque>
#include <unordered_map>

namespace NYql {

struct TExprContext;
class TTypeAnnotationNode;
class TStructExprType;
class TVariantExprType;

class TConstraintNode {
protected:
    TConstraintNode(TExprContext& ctx, std::string_view name);
    TConstraintNode(TConstraintNode&& constr);
public:
    using TPathType = std::deque<std::string_view>;
    using TSetType = NSorted::TSimpleSet<TPathType>;
    using TListType = std::vector<const TConstraintNode*>;
    using TPathFilter = std::function<bool(const TPathType&)>;
    using TPathReduce = std::function<std::vector<TPathType>(const TPathType&)>;

    struct THash {
        size_t operator()(const TConstraintNode* node) const {
            return node->GetHash();
        }
    };

    struct TEqual {
        bool operator()(const TConstraintNode* one, const TConstraintNode* two) const {
            return one->Equals(*two);
        }
    };

    struct TCompare {
        inline bool operator()(const TConstraintNode* l, const TConstraintNode* r) const {
            return l->GetName() < r->GetName();
        }

        inline bool operator()(const std::string_view name, const TConstraintNode* r) const {
            return name < r->GetName();
        }

        inline bool operator()(const TConstraintNode* l, const std::string_view name) const {
            return l->GetName() < name;
        }
    };

    virtual ~TConstraintNode() = default;

    ui64 GetHash() const {
        return Hash_;
    }

    virtual bool Equals(const TConstraintNode& node) const = 0;
    virtual bool Includes(const TConstraintNode& node) const {
        return Equals(node);
    }
    virtual void Out(IOutputStream& out) const;
    virtual void ToJson(NJson::TJsonWriter& out) const = 0;

    virtual bool IsApplicableToType(const TTypeAnnotationNode&) const { return true; }
    virtual const TConstraintNode* OnlySimpleColumns(TExprContext&) const { return this; }

    template <typename T>
    const T* Cast() const {
        static_assert(std::is_base_of<TConstraintNode, T>::value,
                      "Should be derived from TConstraintNode");

        const auto ret = dynamic_cast<const T*>(this);
        YQL_ENSURE(ret, "Cannot cast '" << Name_ << "' constraint to " << T::Name());
        return ret;
    }

    const std::string_view& GetName() const {
        return Name_;
    }

    static const TTypeAnnotationNode* GetSubTypeByPath(const TPathType& path, const TTypeAnnotationNode& type);
protected:
    ui64 Hash_;
    std::string_view Name_;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TConstraintSet {
public:
    TConstraintSet() = default;
    TConstraintSet(const TConstraintSet&) = default;
    TConstraintSet(TConstraintSet&&) = default;

    TConstraintSet& operator =(const TConstraintSet&) = default;
    TConstraintSet& operator =(TConstraintSet&&) = default;

    template <class TConstraintType>
    const TConstraintType* GetConstraint() const {
        auto res = GetConstraint(TConstraintType::Name());
        return res ? res->template Cast<TConstraintType>() : nullptr;
    }

    template <class TConstraintType>
    const TConstraintType* RemoveConstraint() {
        auto res = RemoveConstraint(TConstraintType::Name());
        return res ? res->template Cast<TConstraintType>() : nullptr;
    }

    const TConstraintNode::TListType& GetAllConstraints() const {
        return Constraints_;
    }

    void Clear() {
        Constraints_.clear();
    }

    explicit operator bool() const {
        return !Constraints_.empty();
    }

    bool operator ==(const TConstraintSet& s) const {
        return Constraints_ == s.Constraints_;
    }

    bool operator !=(const TConstraintSet& s) const {
        return Constraints_ != s.Constraints_;
    }

    const TConstraintNode* GetConstraint(std::string_view name) const;
    void AddConstraint(const TConstraintNode* node);
    const TConstraintNode* RemoveConstraint(std::string_view name);

    using TPredicate = std::function<bool(const std::string_view& name)>;
    bool FilterConstraints(const TPredicate& predicate);

    void Out(IOutputStream& out) const;
    void ToJson(NJson::TJsonWriter& writer) const;
private:
    TConstraintNode::TListType Constraints_;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TSortedConstraintNode final: public TConstraintNode {
public:
    using TContainerType = TSmallVec<std::pair<TSetType, bool>>;
    using TFullSetType = NSorted::TSimpleSet<TSetType>;
private:
    friend struct TExprContext;

    TSortedConstraintNode(TExprContext& ctx, TContainerType&& content);
    TSortedConstraintNode(TSortedConstraintNode&& constr);
public:
    static constexpr std::string_view Name() {
        return "Sorted";
    }

    const TContainerType& GetContent() const {
        return Content_;
    }

    TFullSetType GetAllSets() const;

    bool Equals(const TConstraintNode& node) const override;
    bool Includes(const TConstraintNode& node) const override;
    void Out(IOutputStream& out) const override;
    void ToJson(NJson::TJsonWriter& out) const override;

    bool IsPrefixOf(const TSortedConstraintNode& node) const;
    bool StartsWith(const TSetType& prefix) const;

    const TSortedConstraintNode* CutPrefix(size_t newPrefixLength, TExprContext& ctx) const;

    void FilterUncompleteReferences(TSetType& references) const;

    static const TSortedConstraintNode* MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx);
    const TSortedConstraintNode* MakeCommon(const TSortedConstraintNode* other, TExprContext& ctx) const;

    const TSortedConstraintNode* FilterFields(TExprContext& ctx, const TPathFilter& predicate) const;
    const TSortedConstraintNode* RenameFields(TExprContext& ctx, const TPathReduce& reduce) const;

    bool IsApplicableToType(const TTypeAnnotationNode& type) const override;
    const TConstraintNode* OnlySimpleColumns(TExprContext& ctx) const override;
private:
    TContainerType Content_;
};

class TChoppedConstraintNode final: public TConstraintNode {
public:
    using TFullSetType = NSorted::TSimpleSet<TSetType>;
private:
    friend struct TExprContext;

    TChoppedConstraintNode(TExprContext& ctx, TFullSetType&& sets);
    TChoppedConstraintNode(TExprContext& ctx, const TSetType& keys);
    TChoppedConstraintNode(TChoppedConstraintNode&& constr);
public:
    static constexpr std::string_view Name() {
        return "Chopped";
    }

    const TFullSetType& GetAllSets() const { return Sets_; }

    bool Equals(const TConstraintNode& node) const override;
    bool Includes(const TConstraintNode& node) const override;
    void Out(IOutputStream& out) const override;
    void ToJson(NJson::TJsonWriter& out) const override;

    bool Equals(const TSetType& prefix) const;

    void FilterUncompleteReferences(TSetType& references) const;

    static const TChoppedConstraintNode* MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx);
    const TChoppedConstraintNode* MakeCommon(const TChoppedConstraintNode* other, TExprContext& ctx) const;

    const TChoppedConstraintNode* FilterFields(TExprContext& ctx, const TPathFilter& predicate) const;
    const TChoppedConstraintNode* RenameFields(TExprContext& ctx, const TPathReduce& reduce) const;

    bool IsApplicableToType(const TTypeAnnotationNode& type) const override;
    const TConstraintNode* OnlySimpleColumns(TExprContext& ctx) const override;
private:
    TFullSetType Sets_;
};

template<bool Distinct>
class TUniqueConstraintNodeBase final: public TConstraintNode {
public:
    using TFullSetType = NSorted::TSimpleSet<TSetType>;
protected:
    friend struct TExprContext;

    TUniqueConstraintNodeBase(TExprContext& ctx, const std::vector<std::string_view>& columns);
    TUniqueConstraintNodeBase(TExprContext& ctx, TFullSetType&& sets);
    TUniqueConstraintNodeBase(TUniqueConstraintNodeBase&& constr);
public:
    static constexpr std::string_view Name() {
        return Distinct ? "Distinct" : "Unique";
    }

    const TFullSetType& GetAllSets() const { return Sets_; }

    bool Equals(const TConstraintNode& node) const override;
    bool Includes(const TConstraintNode& node) const override;
    void Out(IOutputStream& out) const override;
    void ToJson(NJson::TJsonWriter& out) const override;

    bool IsOrderBy(const TSortedConstraintNode& sorted) const;
    bool ContainsCompleteSet(const std::vector<std::string_view>& columns) const;

    void FilterUncompleteReferences(TSetType& references) const;

    static const TUniqueConstraintNodeBase* MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx);
    const TUniqueConstraintNodeBase* FilterFields(TExprContext& ctx, const TPathFilter& predicate) const;
    const TUniqueConstraintNodeBase* RenameFields(TExprContext& ctx, const TPathReduce& reduce) const;
    const TUniqueConstraintNodeBase* MakeCommon(const TUniqueConstraintNodeBase* other, TExprContext& ctx) const;

    static const TUniqueConstraintNodeBase* Merge(const TUniqueConstraintNodeBase* one, const TUniqueConstraintNodeBase* two, TExprContext& ctx);

    const TUniqueConstraintNodeBase* GetComplicatedForType(const TTypeAnnotationNode& type, TExprContext& ctx) const;
    const TUniqueConstraintNodeBase* GetSimplifiedForType(const TTypeAnnotationNode& type, TExprContext& ctx) const;

    bool IsApplicableToType(const TTypeAnnotationNode& type) const override;
    const TConstraintNode* OnlySimpleColumns(TExprContext& ctx) const override;
private:
    static TSetType ColumnsListToSet(const std::vector<std::string_view>& columns);
    static TFullSetType DedupSets(TFullSetType&& sets);

    TFullSetType Sets_;
};

using TUniqueConstraintNode = TUniqueConstraintNodeBase<false>;
using TDistinctConstraintNode = TUniqueConstraintNodeBase<true>;

template<class TOriginalConstraintNode>
class TPartOfConstraintNode : public TConstraintNode {
public:
    using TMainConstraint = TOriginalConstraintNode;
    using TPartType = NSorted::TSimpleMap<TPathType, TPathType>;
    using TReversePartType = NSorted::TSimpleMap<TPathType, NSorted::TSimpleSet<TPathType>>;
    using TMapType = std::unordered_map<const TMainConstraint*, TPartType>;
private:
    friend struct TExprContext;

    TPartOfConstraintNode(TPartOfConstraintNode&& constr);
    TPartOfConstraintNode(TExprContext& ctx, TMapType&& mapping);
public:
    static constexpr std::string_view Name();

    const TMapType& GetColumnMapping() const;
    TMapType GetColumnMapping(const std::string_view& asField) const;
    TMapType GetColumnMapping(TExprContext& ctx, const std::string_view& prefix) const;

    bool Equals(const TConstraintNode& node) const override;
    bool Includes(const TConstraintNode& node) const override;
    void Out(IOutputStream& out) const override;
    void ToJson(NJson::TJsonWriter& out) const override;

    const TPartOfConstraintNode* ExtractField(TExprContext& ctx, const std::string_view& field) const;
    const TPartOfConstraintNode* FilterFields(TExprContext& ctx, const TPathFilter& predicate) const;
    const TPartOfConstraintNode* RenameFields(TExprContext& ctx, const TPathReduce& reduce) const;
    const TPartOfConstraintNode* CompleteOnly(TExprContext& ctx) const;

    static const TPartOfConstraintNode* MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx);

    static TMapType GetCommonMapping(const TMainConstraint* complete, const TPartOfConstraintNode* incomplete = nullptr, const std::string_view& field = {});
    static void UniqueMerge(TMapType& output, TMapType&& input);
    static TMapType ExtractField(const TMapType& mapping, const std::string_view& field);

    static const TMainConstraint* MakeComplete(TExprContext& ctx, const TMapType& mapping, const TMainConstraint* original, const std::string_view& field = {});
    static const TMainConstraint* MakeComplete(TExprContext& ctx, const TPartOfConstraintNode* partial, const TMainConstraint* original, const std::string_view& field = {});

    bool IsApplicableToType(const TTypeAnnotationNode& type) const override;
private:
    TMapType Mapping_;
};

using TPartOfSortedConstraintNode = TPartOfConstraintNode<TSortedConstraintNode>;
using TPartOfChoppedConstraintNode = TPartOfConstraintNode<TChoppedConstraintNode>;
using TPartOfUniqueConstraintNode = TPartOfConstraintNode<TUniqueConstraintNode>;
using TPartOfDistinctConstraintNode = TPartOfConstraintNode<TDistinctConstraintNode>;

template<>
constexpr std::string_view TPartOfSortedConstraintNode::Name() {
    return "PartOfSorted";
}

template<>
constexpr std::string_view TPartOfChoppedConstraintNode::Name() {
    return "PartOfChopped";
}

template<>
constexpr std::string_view TPartOfUniqueConstraintNode::Name() {
    return "PartOfUnique";
}

template<>
constexpr std::string_view TPartOfDistinctConstraintNode::Name() {
    return "PartOfDistinct";
}

class TPassthroughConstraintNode final: public TConstraintNode {
public:
    using TPartType = NSorted::TSimpleMap<TPathType, std::string_view>;
    using TMapType = std::unordered_map<const TPassthroughConstraintNode*, TPartType>;
    using TReverseMapType = NSorted::TSimpleMap<std::string_view, std::string_view>;
private:
    friend struct TExprContext;

    TPassthroughConstraintNode(TExprContext& ctx, const TStructExprType& itemType);
    TPassthroughConstraintNode(TExprContext& ctx, const ui32 width);
    TPassthroughConstraintNode(TPassthroughConstraintNode&& constr);
    TPassthroughConstraintNode(TExprContext& ctx, TMapType&& mapping);
public:
    static constexpr std::string_view Name() {
        return "Passthrough";
    }

    const TMapType& GetColumnMapping() const;
    TMapType GetColumnMapping(const std::string_view& asField) const;
    TMapType GetColumnMapping(TExprContext& ctx, const std::string_view& prefix) const;

    TReverseMapType GetReverseMapping() const;

    bool Equals(const TConstraintNode& node) const override;
    bool Includes(const TConstraintNode& node) const override;
    void Out(IOutputStream& out) const override;
    void ToJson(NJson::TJsonWriter& out) const override;

    static void UniqueMerge(TMapType& output, TMapType&& input);
    const TPassthroughConstraintNode* ExtractField(TExprContext& ctx, const std::string_view& field) const;
    const TPassthroughConstraintNode* FilterFields(TExprContext& ctx, const TPathFilter& predicate) const;
    const TPassthroughConstraintNode* RenameFields(TExprContext& ctx, const TPathReduce& reduce) const;

    static const TPassthroughConstraintNode* MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx);
    const TPassthroughConstraintNode* MakeCommon(const TPassthroughConstraintNode* other, TExprContext& ctx) const;
private:
    TMapType Mapping_;
};

class TEmptyConstraintNode final: public TConstraintNode {
protected:
    friend struct TExprContext;

    TEmptyConstraintNode(TExprContext& ctx);
    TEmptyConstraintNode(TEmptyConstraintNode&& constr);

public:
    static constexpr std::string_view Name() {
        return "Empty";
    }

    bool Equals(const TConstraintNode& node) const override;
    void ToJson(NJson::TJsonWriter& out) const override;

    static const TEmptyConstraintNode* MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx);
};

class TVarIndexConstraintNode final: public TConstraintNode {
public:
    using TMapType = NSorted::TSimpleMap<ui32, ui32>;

protected:
    friend struct TExprContext;

    TVarIndexConstraintNode(TExprContext& ctx, const TMapType& mapping);
    TVarIndexConstraintNode(TExprContext& ctx, const TVariantExprType& itemType);
    TVarIndexConstraintNode(TExprContext& ctx, size_t mapItemsCount);
    TVarIndexConstraintNode(TVarIndexConstraintNode&& constr);

public:
    static constexpr std::string_view Name() {
        return "VarIndex";
    }

    // multimap: result index -> {original indices}
    const TMapType& GetIndexMapping() const {
        return Mapping_;
    }

    // original index -> {result indices}
    TMapType GetReverseMapping() const;

    bool Equals(const TConstraintNode& node) const override;
    bool Includes(const TConstraintNode& node) const override;
    void Out(IOutputStream& out) const override;
    void ToJson(NJson::TJsonWriter& out) const override;

    static const TVarIndexConstraintNode* MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx);

private:
    TMapType Mapping_;
};

class TMultiConstraintNode final: public TConstraintNode {
public:
    using TMapType = NSorted::TSimpleMap<ui32, TConstraintSet>;

public:
    TMultiConstraintNode(TExprContext& ctx, const TMapType& items);
    TMultiConstraintNode(TExprContext& ctx, ui32 index, const TConstraintSet& constraints);
    TMultiConstraintNode(TMultiConstraintNode&& constr);

public:
    static constexpr std::string_view Name() {
        return "Multi";
    }

    const TMapType& GetItems() const {
        return Items_;
    }

    const TConstraintSet* GetItem(ui32 index) const {
        return Items_.FindPtr(index);
    }

    bool Equals(const TConstraintNode& node) const override;
    bool Includes(const TConstraintNode& node) const override;
    void Out(IOutputStream& out) const override;
    void ToJson(NJson::TJsonWriter& out) const override;

    static const TMultiConstraintNode* MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx);

    const TMultiConstraintNode* FilterConstraints(TExprContext& ctx, const TConstraintSet::TPredicate& predicate) const;

    bool FilteredIncludes(const TConstraintNode& node, const THashSet<TString>& blacklist) const;
    const TConstraintNode* OnlySimpleColumns(TExprContext& ctx) const override;
private:
    TMapType Items_;
};

} // namespace NYql

