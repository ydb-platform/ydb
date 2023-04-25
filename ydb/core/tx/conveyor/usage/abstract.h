#pragma once
#include <memory>
#include <ydb/library/accessor/accessor.h>
#include <util/generic/string.h>

namespace NKikimr::NConveyor {

class ITask {
private:
    TString ErrorMessage;
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

    const TString& GetErrorMessage() const {
        return ErrorMessage;
    }

    bool Execute();
};

}
