#pragma once

#include <ydb/library/planner/base/defs.h>
#include <ydb/library/planner/base/visitor.h>

namespace NScheduling {

class TShareNode;
class TShareAccount;
class TShareGroup;

class INodeVisitor
        : public IVisitorBase
        , public IVisitor<TShareAccount>
        , public IVisitor<TShareGroup>
{
public:
    using IVisitor<TShareAccount>::Visit;
    using IVisitor<TShareGroup>::Visit;
};

class IConstNodeVisitor
        : public IVisitorBase
        , public IVisitor<const TShareAccount>
        , public IVisitor<const TShareGroup>
{
public:
    using IVisitor<const TShareAccount>::Visit;
    using IVisitor<const TShareGroup>::Visit;
};

class ISimpleNodeVisitor
        : public IVisitorBase
        , public IVisitor<TShareNode>
{};

class IConstSimpleNodeVisitor
        : public IVisitorBase
        , public IVisitor<const TShareNode>
{};

}
