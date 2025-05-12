#pragma once

#include <util/generic/cast.h>
#include <util/system/type_name.h>

namespace NScheduling {

class IVisitable;

class IVisitorBase {
public:
    virtual ~IVisitorBase() {}
    virtual void VisitUnkown(IVisitable* o) { VisitFailed(o); }
    virtual void VisitUnkown(const IVisitable* o) { VisitFailed(o); }
private:
    inline void VisitFailed(const IVisitable* o);
};

template <class T>
class IVisitor {
public:
    virtual void Visit(T* node) = 0;
};

class IVisitable {
public:
    virtual ~IVisitable() {}
    virtual void Accept(IVisitorBase* v) { v->VisitUnkown(this); }
    virtual void Accept(IVisitorBase* v) const { v->VisitUnkown(this); }
protected:
    template <class TDerived>
    static bool AcceptImpl(TDerived* d, IVisitorBase* v)
    {
        if (auto* p = dynamic_cast<IVisitor<TDerived>*>(v)) {
            p->Visit(d);
            return true;
        } else {
            return false;
        }
    }
private:
};

#define SCHEDULING_DEFINE_VISITABLE(TBase) \
    void Accept(::NScheduling::IVisitorBase* v) override { if (!AcceptImpl(this, v)) { TBase::Accept(v); } } \
    void Accept(::NScheduling::IVisitorBase* v) const override { if (!AcceptImpl(this, v)) { TBase::Accept(v); } } \
    /**/


void IVisitorBase::VisitFailed(const IVisitable* o)
{
    Y_ABORT("visitor of type '%s' cannot visit class of type '%s'", TypeName(*this).c_str(), TypeName(*o).c_str());
}

}
