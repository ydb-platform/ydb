#pragma once

#include "node_visitor.h"
#include "group.h"
#include <functional>

namespace NScheduling {

template <class TAcc, class TGrp>
class TApplyVisitor
        : public IVisitorBase
        , public IVisitor<TAcc>
        , public IVisitor<TGrp>
{
private:
    std::function<void (TAcc*)> AccFunc;
    std::function<void (TGrp*)> GrpFunc;
public:
    template <class AF, class GF>
    TApplyVisitor(AF af, GF gf)
        : AccFunc(af)
        , GrpFunc(gf)
    {}

    void Visit(TAcc* node) override
    {
        AccFunc(node);
    }

    void Visit(TGrp* node) override
    {
        GrpFunc(node);
    }
};

template <class TNode>
class TApplySimpleVisitor
        : public IVisitorBase
        , public IVisitor<TNode>
{
private:
    std::function<void (TNode*)> NodeFunc;
public:
    template <class NF>
    TApplySimpleVisitor(NF nf)
        : NodeFunc(nf)
    {}

    void Visit(TNode* node) override
    {
        NodeFunc(node);
    }
};

template <class TAcc, class TGrp>
class TRecursiveApplyVisitor
        : public IVisitorBase
        , public IVisitor<TAcc>
        , public IVisitor<TGrp>
{
private:
    std::function<void (TAcc*)> AccFunc;
    std::function<void (TGrp*)> GrpFunc;
public:
    template <class AF, class GF>
    TRecursiveApplyVisitor(AF af, GF gf)
        : AccFunc(af)
        , GrpFunc(gf)
    {}

    void Visit(TAcc* node) override
    {
        AccFunc(node);
    }

    void Visit(TGrp* node) override
    {
        GrpFunc(node);
        node->AcceptInChildren(this);
    }
};

template <class TAcc, class TGrp, class T, class AF, class GF>
void ApplyTo(T* group, AF af, GF gf)
{
    TApplyVisitor<TAcc, TGrp> v(af, gf);
    group->AcceptInChildren(&v);
}

template <class TNode, class T, class NF>
void ApplyTo(T* group, NF nf)
{
    TApplySimpleVisitor<TNode> v(nf);
    group->AcceptInChildren(&v);
}

template <class TAcc, class TGrp, class T, class AF, class GF>
void RecursiveApplyTo(T* group, AF af, GF gf)
{
    TRecursiveApplyVisitor<TAcc, TGrp> v(af, gf);
    group->Accept(&v);
}

}
