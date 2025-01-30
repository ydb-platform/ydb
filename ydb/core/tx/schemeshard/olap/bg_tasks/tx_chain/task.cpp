#include "task.h"
#include "session.h"

namespace NKikimr::NSchemeShard::NOlap::NBackground {

std::shared_ptr<NKikimr::NOlap::NBackground::ISessionLogic> TTxChainTask::DoBuildSession() const {
    return std::make_shared<TTxChainSession>(TxData);
}

}