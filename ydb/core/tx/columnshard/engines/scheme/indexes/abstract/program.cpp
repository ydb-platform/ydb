#include "program.h"
#include "composite.h"
#include <ydb/library/yql/core/arrow_kernels/request/request.h>

namespace NKikimr::NOlap::NIndexes::NRequest {

class IRequestNode {
protected:
    TString Name;
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

    std::shared_ptr<IRequestNode> Copy() const {
        auto selfCopy = DoCopy();
        selfCopy->Parent = nullptr;
        selfCopy->Name = GetNextId(Name);
        AFL_VERIFY(selfCopy);
        for (auto&& i : Children) {
            selfCopy->Children.emplace_back(i->Copy());
        }
        for (auto&& i : selfCopy->Children) {
            i->Parent = selfCopy.get();
        }
        return selfCopy;
    }

    const TString& GetName() const {
        return Name;
    }
    const std::vector<std::shared_ptr<IRequestNode>>& GetChildren() const {
        return Children;
    }

    static TString GetNextId(const TString& originalName) {
        static TAtomic Counter = 0;
        TStringBuf sb(originalName.data(), originalName.size());
        TStringBuf left;
        TStringBuf right;
        if (sb.TrySplit('$', left, right)) {
            return TString(left.data(), left.size()) + "$" + ::ToString(AtomicIncrement(Counter));
        } else {
            return originalName + "$" + ::ToString(AtomicIncrement(Counter));
        }
    }

    IRequestNode(const TString& name)
        : Name(name) {

    }

    IRequestNode(const std::string& name)
        : Name(name.data(), name.size()) {

    }

    IRequestNode(const char* name)
        : Name(name) {

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

    void RemoveChildren(const TString& name) {
        auto nameCopy = name;
        const auto pred = [nameCopy](const std::shared_ptr<IRequestNode>& child) {
            if (child->GetNodeName() == nameCopy) {
                child->Parent = nullptr;
                return true;
            } else {
                return false;
            }
        };
        const ui32 sizeBefore = Children.size();
        Children.erase(std::remove_if(Children.begin(), Children.end(), pred), Children.end());
        AFL_VERIFY(sizeBefore == Children.size() + 1);
    }

    const TString& GetNodeName() const {
        return Name;
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

    void Attach(const std::shared_ptr<IRequestNode>& children) {
        auto copy = children;
        if (copy->Parent) {
            copy->Parent->RemoveChildren(copy->GetNodeName());
        }
        copy->Parent = this;
        for (auto&& i : Children) {
            AFL_VERIFY(i->GetName() != copy->GetName());
        }
        Children.emplace_back(copy);
    }

    void Exchange(const TString& name, const std::shared_ptr<IRequestNode>& children) {
        auto copy = children;
        for (auto&& i : Children) {
            if (i->GetName() == name) {
                i = copy;
                i->Parent = this;
                return;
            }
        }
        AFL_VERIFY(false);
    }

    NJson::TJsonValue SerializeToJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue(Name, DoSerializeToJson());
        if (Children.size()) {
            auto& childrenJson = result.InsertValue("children", NJson::JSON_ARRAY);
            for (auto&& i : Children) {
                childrenJson.AppendValue(i->SerializeToJson());
            }
        }
        return result;
    }
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
        return std::make_shared<TConstantNode>(GetName(), Constant);
    }
public:
    TConstantNode(const std::string& name, const std::shared_ptr<arrow::Scalar>& constant)
        : TBase(name)
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
        : TBase("ROOT") {

    }
};

class TOriginalColumn: public IRequestNode {
private:
    using TBase = IRequestNode;
    YDB_READONLY_DEF(TString, ColumnName);
protected:
    virtual bool DoCollapse() override {
        return false;
    }
    virtual NJson::TJsonValue DoSerializeToJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "column");
        result.InsertValue("column_name", ColumnName);
        return result;
    }
    virtual std::shared_ptr<IRequestNode> DoCopy() const override {
        return std::make_shared<TOriginalColumn>(GetName());
    }
public:
    TOriginalColumn(const std::string& columnName)
        : TBase(GetNextId(TString(columnName.data(), columnName.size())))
        , ColumnName(columnName.data(), columnName.size()) {

    }
};

class TPackAnd: public IRequestNode {
private:
    using TBase = IRequestNode;
    THashMap<TString, std::shared_ptr<arrow::Scalar>> Conditions;
    bool IsEmptyFlag = false;
protected:
    virtual bool DoCollapse() override {
        return false;
    }
    virtual NJson::TJsonValue DoSerializeToJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "pack_and");
        if (IsEmptyFlag) {
            result.InsertValue("empty", true);
        }
        auto& arrJson = result.InsertValue("conditions", NJson::JSON_ARRAY);
        for (auto&& i : Conditions) {
            auto& jsonCondition = arrJson.AppendValue(NJson::JSON_MAP);
            jsonCondition.InsertValue(i.first, i.second->ToString());
        }
        return result;
    }
    virtual std::shared_ptr<IRequestNode> DoCopy() const override {
        return std::make_shared<TPackAnd>(*this);
    }
public:
    TPackAnd(const TPackAnd&) = default;
    TPackAnd(const TString& cName, const std::shared_ptr<arrow::Scalar>& value)
        : TBase(GetNextId("PackAnd")) {
        AddCondition(cName, value);
    }

    const THashMap<TString, std::shared_ptr<arrow::Scalar>>& GetEquals() const {
        return Conditions;
    }

    bool IsEmpty() const {
        return IsEmptyFlag;
    }
    void AddCondition(const TString& cName, const std::shared_ptr<arrow::Scalar>& value) {
        AFL_VERIFY(value);
        auto it = Conditions.find(cName);
        if (it == Conditions.end()) {
            Conditions.emplace(cName, value);
        } else if (it->second->Equals(*value)) {
            return;
        } else {
            IsEmptyFlag = true;
        }
    }
    void Merge(const TPackAnd& add) {
        for (auto&& i : add.Conditions) {
            AddCondition(i.first, i.second);
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

    virtual bool DoCollapse() override {
        if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::Coalesce) {
            AFL_VERIFY(Children.size() == 2);
            AFL_VERIFY(Children[1]->Is<TConstantNode>());
            Parent->Attach(Children[0]);
            Parent->RemoveChildren(GetNodeName());
            return true;
        }
        if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::Equals && Children.size() == 2 && Children[1]->Is<TConstantNode>() && Children[0]->Is<TOriginalColumn>()) {
            Parent->Exchange(GetNodeName(), std::make_shared<TPackAnd>(Children[0]->As<TOriginalColumn>()->GetColumnName(), Children[1]->As<TConstantNode>()->GetConstant()));
            return true;
        }
        if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::And) {
            if (Parent->Is<TOperationNode>() && Parent->As<TOperationNode>()->Operation == NYql::TKernelRequestBuilder::EBinaryOp::And) {
                Parent->Attach(Children);
                Parent->RemoveChildren(GetNodeName());
                return true;
            }
        }
        if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::Or) {
            if (Parent->Is<TOperationNode>() && Parent->As<TOperationNode>()->Operation == NYql::TKernelRequestBuilder::EBinaryOp::Or) {
                Parent->Attach(Children);
                Parent->RemoveChildren(GetNodeName());
                return true;
            }
        }
        if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::And) {
            auto copy = Children;
            TPackAnd* baseSet = nullptr;
            bool changed = false;
            for (auto&& c : copy) {
                if (c->Is<TPackAnd>()) {
                    if (baseSet) {
                        baseSet->Merge(*c->As<TPackAnd>());
                        RemoveChildren(c->GetNodeName());
                        changed = true;
                    } else {
                        baseSet = c->As<TPackAnd>();
                    }
                }
            }
            if (changed) {
                return true;
            }
        }

        if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::And && Children.size() == 1) {
            AFL_VERIFY(Children.front()->Is<TPackAnd>());
            Parent->Exchange(GetNodeName(), Children.front());
            return true;
        }

        if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::And) {
            std::vector<std::shared_ptr<IRequestNode>> newNodes;
            std::set<TString> cNames;
            for (auto&& i : Children) {
                if (i->Is<TOperationNode>() && i->As<TOperationNode>()->Operation == NYql::TKernelRequestBuilder::EBinaryOp::Or) {
                    auto orNode = i;
                    RemoveChildren(i->GetNodeName());
                    auto copy = orNode->GetChildren();
                    auto copyChildren = Children;
                    for (auto&& orNodeChildren : copy) {
                        std::vector<std::shared_ptr<IRequestNode>> producedChildren;
                        for (auto&& c : copyChildren) {
                            producedChildren.emplace_back(c->Copy());
                        }
                        producedChildren.emplace_back(orNodeChildren->Copy());
                        newNodes.emplace_back(std::make_shared<TOperationNode>(GetNextId(Name), NYql::TKernelRequestBuilder::EBinaryOp::And, producedChildren));
                    }
                    Parent->Exchange(GetNodeName(), std::make_shared<TOperationNode>(GetNextId(orNode->GetName()), NYql::TKernelRequestBuilder::EBinaryOp::Or, newNodes));
                    return true;
                }
            }
        }
        return false;
    }
    virtual std::shared_ptr<IRequestNode> DoCopy() const override {
        std::vector<std::shared_ptr<IRequestNode>> children;
        return std::make_shared<TOperationNode>(GetName(), Operation, children);
    }
public:
    NYql::TKernelRequestBuilder::EBinaryOp GetOperation() const {
        return Operation;
    }

    TOperationNode(const std::string& name, const NYql::TKernelRequestBuilder::EBinaryOp& operation, const std::vector<std::shared_ptr<IRequestNode>>& args)
        : TBase(name)
        , Operation(operation) {
        for (auto&& i : args) {
            Attach(i);
        }
    }
};

class TNormalForm {
private:
    std::map<std::string, std::shared_ptr<IRequestNode>> Nodes;
public:
    TNormalForm() = default;

    bool Add(const NSsa::TAssign& assign, const TProgramContainer& program) {
        std::vector<std::shared_ptr<IRequestNode>> argNodes;
        for (auto&& arg : assign.GetArguments()) {
            if (arg.IsGenerated()) {
                auto it = Nodes.find(arg.GetColumnName());
                if (it == Nodes.end()) {
                    AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "program_arg_is_missing")("program", program.DebugString());
                    return false;
                }
                argNodes.emplace_back(it->second);
            } else {
                argNodes.emplace_back(std::make_shared<TOriginalColumn>(arg.GetColumnName()));
            }
        }
        for (auto&& i : argNodes) {
            Nodes.erase(i->GetNodeName());
        }

        if (assign.IsConstant()) {
            AFL_VERIFY(argNodes.size() == 0);
            Nodes.emplace(assign.GetName(), std::make_shared<TConstantNode>(assign.GetName(), assign.GetConstant()));
        } else if (!!assign.GetYqlOperationId()) {
            Nodes.emplace(assign.GetName(), std::make_shared<TOperationNode>(assign.GetName(), (NYql::TKernelRequestBuilder::EBinaryOp)*assign.GetYqlOperationId(), argNodes));
        } else {
            return false;
        }
        return true;
    }

    std::shared_ptr<TRootNode> GetRootNode() {
        if (Nodes.empty()) {
            return nullptr;
        }
        AFL_VERIFY(Nodes.size() == 1);
        auto result = std::make_shared<TRootNode>();
        result->Attach(Nodes.begin()->second);
        return result;
    }
};

std::shared_ptr<TDataForIndexesCheckers> TDataForIndexesCheckers::Build(const TProgramContainer& program) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("program", program.DebugString());
    auto fStep = program.GetSteps().front();
    TNormalForm nForm;
    for (auto&& s : fStep->GetAssignes()) {
        if (!nForm.Add(s, program)) {
            return nullptr;
        }
    }
    auto rootNode = nForm.GetRootNode();
    if (!rootNode) {
        return nullptr;
    }
    while (rootNode->Collapse()) {
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("collapsed_program", rootNode->SerializeToJson());
    if (rootNode->GetChildren().size() != 1) {
        return nullptr;
    }
    std::shared_ptr<TDataForIndexesCheckers> result = std::make_shared<TDataForIndexesCheckers>();
    if (auto* orNode = rootNode->GetChildren().front()->As<TOperationNode>()) {
        if (orNode->GetOperation() == NYql::TKernelRequestBuilder::EBinaryOp::Or) {
            for (auto&& i : orNode->GetChildren()) {
                if (auto* andPackNode = i->As<TPackAnd>()) {
                    result->AddBranch(andPackNode->GetEquals());
                } else if (auto* operationNode = i->As<TOperationNode>()) {
                    if (operationNode->GetOperation() == NYql::TKernelRequestBuilder::EBinaryOp::And) {
                        TPackAnd* pack = operationNode->FindFirst<TPackAnd>();
                        if (!pack) {
                            return nullptr;
                        }
                        result->AddBranch(pack->GetEquals());
                    }
                } else {
                    return nullptr;
                }
            }
        }
    } else if (auto* andPackNode = rootNode->GetChildren().front()->As<TPackAnd>()) {
        result->AddBranch(andPackNode->GetEquals());
    } else {
        return nullptr;
    }
    return result;
}

TIndexCheckerContainer TDataForIndexesCheckers::GetCoverChecker() const {
    std::vector<std::shared_ptr<IIndexChecker>> andCheckers;
    for (auto&& i : Branches) {
        auto andChecker = i->GetAndChecker();
        if (!andChecker) {
            return TIndexCheckerContainer();
        }
        andCheckers.emplace_back(andChecker);
    }
    if (andCheckers.size() == 0) {
        return TIndexCheckerContainer();
    } else if (andCheckers.size() == 1) {
        return andCheckers.front();
    } else {
        return TIndexCheckerContainer(std::make_shared<TOrIndexChecker>(andCheckers));
    }
}

std::shared_ptr<NKikimr::NOlap::NIndexes::IIndexChecker> TBranchCoverage::GetAndChecker() const {
    if (Indexes.empty()) {
        return nullptr;
    }
    return std::make_shared<TAndIndexChecker>(Indexes);
}

}   // namespace NKikimr::NOlap::NIndexes::NRequest