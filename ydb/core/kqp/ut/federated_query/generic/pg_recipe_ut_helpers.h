#pragma once
#include <library/cpp/testing/unittest/registar.h>

#include <pqxx/pqxx>

namespace NTestUtils {

    TString GetPgHost();
    ui32 GetPgPort();
    TString GetPgUser();
    TString GetPgDatabase();
    TString GetPgPassword();

    pqxx::connection CreatePostgresqlConnection();

} // namespace NTestUtils
