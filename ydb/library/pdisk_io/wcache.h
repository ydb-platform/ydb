#pragma once
#include <ydb/library/pdisk_io/drivedata.h>

#include <util/stream/str.h>
#include <util/system/file.h>

#include <optional>

namespace NKikimr {
namespace NPDisk {

// FHANDLE file is used for the operation.
// const TString &path is used for debugging and messages in outDetails.
// TStringSteram *outDetails can be nullptr or a pointer to a TStringStream that will receive error details.
// Return value is true in case of success.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum EWriteCacheResult {
    WriteCacheResultOk = 0,
    WriteCacheResultErrorTemporary = 1,
    WriteCacheResultErrorPersistent = 2
};

EWriteCacheResult FlushWriteCache(FHANDLE file, const TString &path, TStringStream *outDetails);

// outIsEnabled must be a valid bool pointer.
EWriteCacheResult GetWriteCache(FHANDLE file, const TString &path, TDriveData *outDriveData,
        TStringStream *outDetails);

std::optional<TDriveData> GetDriveData(const TString &path, TStringStream *outDetails);


// Attention! You should flush the cache before disabling it.
EWriteCacheResult SetWriteCache(FHANDLE file, const TString &path, bool isEnable, TStringStream *outDetails);

} // NPDisk
} // NKikimr

