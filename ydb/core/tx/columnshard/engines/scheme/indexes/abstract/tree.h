#pragma once
#include "like.h"

#include <ydb/core/formats/arrow/program/abstract.h>
#include <ydb/core/tx/program/program.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <library/cpp/json/writer/json_value.h>
#include <yql/essentials/core/arrow_kernels/request/request.h>

namespace NKikimr::NOlap::NIndexes::NRequest {

enum class ENodeType : ui32 {
    Aggregation,
    OriginalColumn,
    Root,
    Operation,
    Constant
};

class TNodeId {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY(ui32, GenerationId, 0);
    YDB_READONLY(ENodeType, NodeType, ENodeType::OriginalColumn);

    static inline TAtomicCounter Counter = 0;

    TNodeId(const ui32 columnId, const ui32 generationId, const ENodeType type)
        : ColumnId(columnId)
        , GenerationId(generationId)
        , NodeType(type) {
    }

public:
    bool operator==(const TNodeId& item) const {
        return ColumnId == item.ColumnId && GenerationId == item.GenerationId && NodeType == item.NodeType;
    }

    TNodeId BuildCopy() const {
        return TNodeId(ColumnId, Counter.Inc(), NodeType);
    }

    TString ToString() const;

    static TNodeId RootNodeId() {
        return TNodeId(0, 0, ENodeType::Root);
    }

    static TNodeId Constant(const ui32 columnId) {
        return TNodeId(columnId, Counter.Inc(), ENodeType::Constant);
    }

    static TNodeId Original(const ui32 columnId);

    static TNodeId Aggregation() {
        return TNodeId(0, Counter.Inc(), ENodeType::Aggregation);
    }

    static TNodeId Operation(const ui32 columnId) {
        return TNodeId(columnId, Counter.Inc(), ENodeType::Operation);
    }

    bool operator<(const TNodeId& item) const {
        return std::tie(ColumnId, GenerationId, NodeType) < std::tie(item.ColumnId, item.GenerationId, item.NodeType);
    }
};

class IRequestNode {
protected:
    TNodeId NodeId;
    std::vector<std::shared_ptr<IRequestNode>> Children;
    IRequestNode* Parent = nullptr;
    virtual bool DoCollapse() = 0;

    virtual NJson::TJsonValue DoSerializeToJson() const = 0;
    virtual std::shared_ptr<IRequestNode> DoCopy() const = 0;

public:
    template <class T>
    T* FindFirst() const {
        for (auto&& c : Children) {
            if (auto* result = c->As<T>()) {
                return result;
            }
        }
        return nullptr;
    }

    std::shared_ptr<IRequestNode> Copy() const;

    const std::vector<std::shared_ptr<IRequestNode>>& GetChildren() const {
        return Children;
    }

    IRequestNode(const TNodeId& nodeId)
        : NodeId(nodeId) {
    }

    virtual ~IRequestNode() = default;

    template <class T>
    bool Is() const {
        return dynamic_cast<const T*>(this);
    }

    template <class T>
    T* As() {
        return dynamic_cast<T*>(this);
    }

    void RemoveChildren(const TNodeId nodeId);

    const TNodeId& GetNodeId() const {
        return NodeId;
    }

    virtual bool Collapse() {
        for (auto&& i : Children) {
            if (i->Collapse()) {
                return true;
            }
        }
        if (DoCollapse()) {
            return true;
        }
        return false;
    }

    void Attach(const std::vector<std::shared_ptr<IRequestNode>>& children) {
        auto copy = children;
        for (auto&& c : copy) {
            Attach(c);
        }
    }

    void Attach(const std::shared_ptr<IRequestNode>& children);

    void Exchange(const TNodeId& nodeId, const std::shared_ptr<IRequestNode>& children);

    NJson::TJsonValue SerializeToJson() const;
};

class TConstantNode: public IRequestNode {
private:
    using TBase = IRequestNode;
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, Constant);

protected:
    virtual NJson::TJsonValue DoSerializeToJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "const");
        result.InsertValue("const", Constant->ToString());
        return result;
    }
    virtual bool DoCollapse() override {
        return false;
    }
    virtual std::shared_ptr<IRequestNode> DoCopy() const override {
        return std::make_shared<TConstantNode>(GetNodeId().GetColumnId(), Constant);
    }

public:
    TConstantNode(const ui32 columnId, const std::shared_ptr<arrow::Scalar>& constant)
        : TBase(TNodeId::Constant(columnId))
        , Constant(constant) {
    }
};

class TRootNode: public IRequestNode {
private:
    using TBase = IRequestNode;

protected:
    virtual bool DoCollapse() override {
        return false;
    }
    virtual NJson::TJsonValue DoSerializeToJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "ROOT");
        return result;
    }

    virtual std::shared_ptr<IRequestNode> DoCopy() const override {
        return nullptr;
    }

public:
    TRootNode()
        : TBase(TNodeId::RootNodeId()) {
    }
};

class TOriginalColumn: public IRequestNode {
private:
    using TBase = IRequestNode;

protected:
    virtual bool DoCollapse() override {
        return false;
    }
    virtual NJson::TJsonValue DoSerializeToJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "column");
        return result;
    }
    virtual std::shared_ptr<IRequestNode> DoCopy() const override {
        return std::make_shared<TOriginalColumn>(GetNodeId().GetColumnId());
    }

public:
    TOriginalColumn(const ui32 columnId)
        : TBase(TNodeId::Original(columnId)) {
    }
};

class TPackAnd: public IRequestNode {
private:
    using TBase = IRequestNode;
    THashMap<ui32, std::shared_ptr<arrow::Scalar>> Equals;
    THashMap<ui32, TLikeDescription> Likes;
    bool IsEmptyFlag = false;

protected:
    virtual bool DoCollapse() override {
        return false;
    }
    virtual NJson::TJsonValue DoSerializeToJson() const override;
    virtual std::shared_ptr<IRequestNode> DoCopy() const override {
        return std::make_shared<TPackAnd>(*this);
    }

public:
    TPackAnd(const TPackAnd&) = default;

    TPackAnd(const ui32 columnId, const std::shared_ptr<arrow::Scalar>& value)
        : TBase(TNodeId::Aggregation()) {
        AddEqual(columnId, value);
    }

    TPackAnd(const ui32 columnId, const TLikePart& part)
        : TBase(TNodeId::Aggregation()) {
        AddLike(columnId, TLikeDescription(part));
    }

    const THashMap<ui32, std::shared_ptr<arrow::Scalar>>& GetEquals() const {
        return Equals;
    }

    const THashMap<ui32, TLikeDescription>& GetLikes() const {
        return Likes;
    }

    bool IsEmpty() const {
        return IsEmptyFlag;
    }
    void AddEqual(const ui32 columnId, const std::shared_ptr<arrow::Scalar>& value);
    void AddLike(const ui32 columnId, const TLikeDescription& value) {
        auto it = Likes.find(columnId);
        if (it == Likes.end()) {
            Likes.emplace(columnId, value);
        } else {
            it->second.Merge(value);
        }
    }
    void Merge(const TPackAnd& add) {
        for (auto&& i : add.Equals) {
            AddEqual(i.first, i.second);
        }
        for (auto&& i : add.Likes) {
            AddLike(i.first, i.second);
        }
    }
};

class TOperationNode: public IRequestNode {
private:
    using TBase = IRequestNode;
    NYql::TKernelRequestBuilder::EBinaryOp Operation;

protected:
    virtual NJson::TJsonValue DoSerializeToJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "operation");
        result.InsertValue("operation", ::ToString(Operation));
        return result;
    }

    virtual bool DoCollapse() override;
    virtual std::shared_ptr<IRequestNode> DoCopy() const override {
        std::vector<std::shared_ptr<IRequestNode>> children;
        return std::make_shared<TOperationNode>(GetNodeId().GetColumnId(), Operation, children);
    }

public:
    NYql::TKernelRequestBuilder::EBinaryOp GetOperation() const {
        return Operation;
    }

    TOperationNode(
        const ui32 columnId, const NYql::TKernelRequestBuilder::EBinaryOp& operation, const std::vector<std::shared_ptr<IRequestNode>>& args)
        : TBase(TNodeId::Operation(columnId))
        , Operation(operation) {
        for (auto&& i : args) {
            Attach(i);
        }
    }
};

class TNormalForm {
private:
    std::map<ui32, std::shared_ptr<IRequestNode>> Nodes;
    std::map<ui32, std::shared_ptr<IRequestNode>> NodesGlobal;

public:
    TNormalForm() = default;

    bool Add(const NArrow::NSSA::IResourceProcessor& processor, const TProgramContainer& program);

    std::shared_ptr<TRootNode> GetRootNode();
};

}   // namespace NKikimr::NOlap::NIndexes::NRequest
