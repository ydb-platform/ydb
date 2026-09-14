#pragma once

#include "node_visitor.h"
#include "shareplanner.h"

namespace NScheduling {

class TStepper : public INodeVisitor
{
public:
    void Visit(TShareAccount* node) override
    {
        node->Step();
    }

    void Visit(TShareGroup* node) override
    {
        node->AcceptInChildren(this);
        node->Step();
    }
};

}
