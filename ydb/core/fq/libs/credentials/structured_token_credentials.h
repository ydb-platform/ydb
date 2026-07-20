#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

namespace NFq {

// Wraps the given factory and adds HasIamAuth support, which is resolved via
// AppData(). Must only be used in KiKiMR actor context where AppData is available.
NYql::IStructuredTokenCredentialsFactory::TPtr CreateKikimrStructuredTokenCredentialsFactoryOverFactory(
    NYql::IStructuredTokenCredentialsFactory::TPtr factory);

// Creates an IStructuredTokenCredentialsProvider that supports all token types
// including HasIamAuth, which is resolved via AppData(). Must only be used in
// KiKiMR actor context where AppData is available.
NYql::IStructuredTokenCredentialsFactory::TPtr CreateKikimrStructuredTokenCredentialsFactory(
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr saFactory);

} // namespace NFq
