#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace NYdb {

//! Checks the following environment variables and creates TDriverConfig with the first appeared:
//! YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=<path-to-file> — service account key file,
//! YDB_ANONYMOUS_CREDENTIALS="1" — uses anonymous access (used for test installation),
//! YDB_METADATA_CREDENTIALS="1" — uses metadata service,
//! YDB_ACCESS_TOKEN_CREDENTIALS=<access-token> — access token (for example, IAM-token).
//! YDB_OAUTH2_KEY_FILE=<path-to-file> - OAuth 2.0 RFC8693 token exchange credentials parameters json file
//! If grpcs protocol is given in endpoint (or protocol is empty), enables SSL and uses
//! certificate from resourses and user cert from env variable "YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
TDriverConfig CreateFromEnvironment(const TStringType& connectionString = "");
//! Creates TDriverConfig from cloud service account key file
TDriverConfig CreateFromSaKeyFile(const TStringType& saKeyFile, const TStringType& connectionString = "");

}
