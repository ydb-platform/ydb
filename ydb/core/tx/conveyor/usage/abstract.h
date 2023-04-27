#pragma once
#include <memory>
#include <ydb/library/accessor/accessor.h>
#include <util/generic/string.h>

namespace NKikimr::NConveyor {

class ITask {
public:
    enum EPriority: ui32 {
        High = 1000,
        Normal = 500,
        Low = 0
    };
private:
    YDB_READONLY_DEF(TString, ErrorMessage);
    YDB_ACCESSOR(EPriority, Priority, EPriority::Normal);
protected:
    ITask& SetErrorMessage(const TString& message) {
        ErrorMessage = message;
        return *this;
    }
    virtual bool DoExecute() = 0;
public:
    using TPtr = std::shared_ptr<ITask>;
    virtual ~ITask() = default;

    bool HasError() const {
        return !!ErrorMessage;
    }

    bool Execute();
};

}
