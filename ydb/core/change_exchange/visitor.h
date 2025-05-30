#pragma once

#include "fwd.h"

#include <util/generic/fwd.h>
#include <util/system/yassert.h>

namespace NKikimr::NChangeExchange {

class IVisitor {
public:
    virtual ~IVisitor() = default;

    virtual void Visit(const NDataShard::TChangeRecord& record) = 0;
    virtual void Visit(const NReplication::NService::TChangeRecord& record) = 0;
    virtual void Visit(const NBackup::NImpl::TChangeRecord& record) = 0;
};

class TBaseVisitor: public IVisitor {
public:
    void Visit(const NDataShard::TChangeRecord&) override { Y_ABORT("not implemented"); }
    void Visit(const NReplication::NService::TChangeRecord&) override { Y_ABORT("not implemented"); }
    void Visit(const NBackup::NImpl::TChangeRecord&) override { Y_ABORT("not implemented"); }
};

class IPartitionResolverVisitor: public IVisitor {
public:
    virtual ui64 GetPartitionId() const = 0;
};

class TBasePartitionResolver: public IPartitionResolverVisitor {
    ui64 PartitionId = 0;

protected:
    inline void SetPartitionId(ui64 value) {
        PartitionId = value;
    }

public:
    ui64 GetPartitionId() const override {
        return PartitionId;
    }

    void Visit(const NDataShard::TChangeRecord&) override { Y_ABORT("not implemented"); }
    void Visit(const NReplication::NService::TChangeRecord&) override { Y_ABORT("not implemented"); }
    void Visit(const NBackup::NImpl::TChangeRecord&) override { Y_ABORT("not implemented"); }
};

}
