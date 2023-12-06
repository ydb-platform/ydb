#pragma once
#include <library/cpp/testing/unittest/registar.h>

#include <pqxx/pqxx>

namespace NTestUtils {

    TString GetPgHost();
    ui32 GetPgPort();
    TString GetPgUser();
    TString GetPgDatabase();

    pqxx::connection CreatePostgresqlConnection();

} // namespace NTestUtils
