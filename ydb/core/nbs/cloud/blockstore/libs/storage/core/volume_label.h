#pragma once

#include <util/generic/string.h>

#include <tuple>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString DiskIdToPath(const TString& diskId);

TString PathNameToDiskId(const TString& pathName);

// Converts diskId relative to rootDir to a pair of (dir, name) for the volume.
std::tuple<TString, TString> DiskIdToVolumeDirAndName(const TString& rootDir,
                                                      const TString& diskId);

}   // namespace NYdb::NBS::NStorage
