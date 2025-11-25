// local_paths.h

#pragma once

#include <util/folder/path.h>
#include <util/generic/string.h>

namespace NYdb::NConsoleClient::NLocalPaths {

extern const TString YdbBinaryName;

TFsPath GetConfigDir();
TFsPath GetStateDir();
TFsPath GetCacheDir();
TFsPath GetCanonicalBinaryPath();
TFsPath GetLegacyBinaryPath();
TFsPath GetLegacyPathHelperScript();

TFsPath GetProfilesFile();
TFsPath GetImportProgressDir();
TFsPath GetUpdateStateFile();
TFsPath GetUpdateCacheDir();

}

