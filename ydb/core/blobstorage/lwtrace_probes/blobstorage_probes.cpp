#include "blobstorage_probes.h"
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_request_id.h>
#include <ydb/core/base/blobstorage.h>

LWTRACE_DEFINE_PROVIDER(BLOBSTORAGE_PROVIDER);
LWTRACE_DEFINE_PROVIDER(FAIL_INJECTION_PROVIDER);

namespace NKikimr { namespace NPDisk {


void TRequestTypeField::ToString(ui32 value, TString *out) {
    switch(ERequestType(value)) {
    case ERequestType::RequestLogRead:      *out = "LogRead"; break;
    case ERequestType::RequestLogWrite:     *out = "LogWrite"; break;
    case ERequestType::RequestChunkRead:    *out = "ChunkRead"; break;
    case ERequestType::RequestChunkWrite:   *out = "ChunkWrite"; break;
    case ERequestType::RequestChunkTrim:    *out = "ChunkTrim"; break;
    case ERequestType::RequestYardInit:     *out = "YardInit"; break;
    case ERequestType::RequestCheckSpace:   *out = "CheckSpace"; break;
    case ERequestType::RequestHarakiri:     *out = "Harakiri"; break;
    case ERequestType::RequestChunkReserve: *out = "ChunkReserve"; break;
    case ERequestType::RequestYardControl:  *out = "YardControl"; break;
    default: *out = "Unknown";
    }
    *out += "(" + ::ToString(value) + ")";
}

}}

namespace NKikimr {

void TBlobPutTactics::ToString(ui64 value, TString *out) {
    *out = TEvBlobStorage::TEvPut::TacticName(TEvBlobStorage::TEvPut::ETactic(value));
}

void TEventTypeField::ToString(ui64 value, TString* out) {
#define CASE(EVENT) case TEvBlobStorage::EVENT: *out = #EVENT; break;
    switch(TEvBlobStorage::EEv(value)) {
    CASE(EvPatch);
    CASE(EvPut);
    CASE(EvVPut);
    CASE(EvGet);
    CASE(EvVGet);
    CASE(EvDiscover);
    default: *out = "Unknown";
    }
    *out += "(" + ::ToString(value) + ")";
#undef CASE
}

}
