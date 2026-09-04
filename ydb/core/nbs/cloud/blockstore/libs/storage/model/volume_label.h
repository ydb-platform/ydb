#pragma once

#include <util/generic/string.h>

#include <tuple>

namespace NYdb::NBS::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString DiskIdToPathDeprecated(const TString& diskId);

TString DiskIdToPath(const TString& diskId);

TString PathNameToDiskId(const TString& pathName);

// Converts diskId relative to rootDir to a pair of (dir, name) for the volume.
std::tuple<TString, TString> DiskIdToVolumeDirAndNameDeprecated(
    const TString& rootDir,
    const TString& diskId);

// Converts diskId relative to rootDir to a pair of (dir, name) for the volume.
std::tuple<TString, TString> DiskIdToVolumeDirAndName(
    const TString& rootDir,
    const TString& diskId);

// Adds a suffix to the disk name so that the disk and its copy always differ in
// the same way.
TString GetSecondaryDiskId(const TString& diskId);

// Remove suffix of secondary disk if needed.
TString GetLogicalDiskId(const TString& diskId);

// Returns whether the diskId has a secondary disk suffix.
bool IsSecondaryDiskId(const TString& diskId);

// Returns the next name for the disk copy by adding or removing the "-copy"
// suffix from the given diskId
TString GetNextDiskId(const TString& diskId);

}   // namespace NYdb::NBS::NBlockStore::NStorage
