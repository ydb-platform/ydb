#pragma once

#include <ydb/core/protos/console.pb.h>
#include <ydb/core/protos/console_config.pb.h>

namespace NKikimr::NConsole {

struct TDatabaseConfigPatch {
    TString Config;
    bool Modified = false;
    NKikimrConsole::TDatabaseFeatureFlagsChange Audit;
};

// Captures the explicit values of recognized settings before validating a patch.
// This also supplies the unchanged values for auditing a rejected request.
NKikimrConsole::TDatabaseFeatureFlagsChange DescribeDatabaseFeatureFlagPatch(
    const TString& config,
    const NKikimrConsole::TPatchDatabaseConfigRequest& request);

// Patches the editable YAML document, preserving unrelated fields and tags.
// Throws on unsupported flags or a mapping that would discard inherited flags.
TDatabaseConfigPatch PatchDatabaseFeatureFlags(
    const TString& config,
    const NKikimrConsole::TPatchDatabaseConfigRequest& request);

} // namespace NKikimr::NConsole
