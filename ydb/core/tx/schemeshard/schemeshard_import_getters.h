#pragma once

#include "defs.h"
#include "schemeshard_import.h"
#include "schemeshard_info_types.h"

namespace NKikimr {
namespace NSchemeShard {

IActor* CreateSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx, TMaybe<NBackup::TEncryptionIV> iv);

IActor* CreateSchemaMappingGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo);

IActor* CreateListObjectsInS3ExportGetter(TEvImport::TEvListObjectsInS3ExportRequest::TPtr&& ev);

} // NSchemeShard
} // NKikimr
