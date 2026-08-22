#include "vdisk_priorities.h"


namespace NKikimr {

TString PriToString(ui8 pri) {
#define CASE_OUT(x) case x: return #x
    switch (pri) {
        CASE_OUT(NPriRead::SyncLog);
        CASE_OUT(NPriRead::HullComp);
        CASE_OUT(NPriRead::HullOnlineRt);
        CASE_OUT(NPriRead::HullOnlineOther);
        CASE_OUT(NPriRead::HullLoad);
        CASE_OUT(NPriRead::HullLow);

        CASE_OUT(NPriWrite::SyncLog);
        CASE_OUT(NPriWrite::HullFresh);
        CASE_OUT(NPriWrite::HullHugeAsyncBlob);
        CASE_OUT(NPriWrite::HullHugeUserData);
        CASE_OUT(NPriWrite::HullComp);

        CASE_OUT(NPriInternal::LogRead);
        CASE_OUT(NPriInternal::LogWrite);
        CASE_OUT(NPriInternal::Other);
        CASE_OUT(NPriInternal::Trim);
        default: return "unknown";
    }
#undef CASE_OUT
}
}
