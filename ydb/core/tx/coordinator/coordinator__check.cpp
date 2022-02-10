#include "coordinator__check.h"

namespace NKikimr {
namespace NFlatTxCoordinator {

ITransaction* TTxCoordinator::CreateTxConsistencyCheck() {
    return new TTxConsistencyCheck(this);
}

}
}
