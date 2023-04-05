#pragma once
#include <memory>

namespace NKikimr::NConveyor {

class ITask {
protected:
    virtual bool DoExecute() = 0;
public:
    using TPtr = std::shared_ptr<ITask>;
    virtual ~ITask() = default;
    bool Execute() {
        return DoExecute();
    }
};

}
