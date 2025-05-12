#pragma once

#include <ydb/library/planner/share/node_visitor.h>
#include <ydb/library/planner/share/protos/shareplanner_sensors.pb.h>

namespace NScheduling {

class IContext;
class IModel;
class TShareNode;
class TSharePlanner;

class IContext: public IVisitable {
public:
    SCHEDULING_DEFINE_VISITABLE(IVisitable);
protected:
    IModel* Model;
public:
    explicit IContext(IModel* model)
        : Model(model)
    {}

    virtual ~IContext() {}
    virtual FWeight GetWeight() const = 0;
    virtual void FillSensors(TShareNodeSensors&) const { }
    IModel* GetModel() { return Model; }
    const IModel* GetModel() const { return Model; }
};

class IModel: public INodeVisitor
{
protected:
    TSharePlanner* Planner;
public:
    explicit IModel(TSharePlanner* planner)
        : Planner(planner)
    {}

    virtual void Run(TShareGroup* root)
    {
        Visit(root);
    }
    virtual void OnAttach(TShareNode* node) = 0;
    virtual void OnDetach(TShareNode* node) = 0;
};

}
