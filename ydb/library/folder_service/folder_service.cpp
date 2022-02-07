#include "folder_service.h"

namespace NKikimr::NFolderService {

NActors::TActorId FolderServiceActorId() {
    constexpr TStringBuf name = "FLDRSRVS";
    return NActors::TActorId(0, name);
}

} // namespace NKikimr::NFolderService