#include <yql/essentials/fuzz_targets/sql_feature_parse_common.h>

using namespace NYql::NFuzzSql;

namespace {

TString BuildStatement(FuzzedDataProvider& provider) {
    const TString lit = QuoteSqlString(provider.ConsumeBytesAsString(provider.ConsumeIntegralInRange<size_t>(0, 64)));
    const TString id = Identifier(provider.ConsumeBytesAsString(provider.ConsumeIntegralInRange<size_t>(0, 24)), "obj");
    const TString type = PickType(provider);

    TStringBuilder out;
    switch (provider.ConsumeIntegralInRange<ui8>(0, 14)) {
        case 0:
            out << "PRAGMA " << id << " = " << lit << ";\nSELECT 1;";
            break;
        case 1:
            out << "USE plato;\nDECLARE $p AS " << type << ";\nSELECT $p;";
            break;
        case 2:
            out << "$x = SELECT key, value FROM plato.Input;\nSELECT * FROM $x;";
            break;
        case 3:
            out << "DEFINE ACTION $act($value) AS\n"
                << "    SELECT $value AS value;\n"
                << "END DEFINE;\n"
                << "DO $act(" << lit << ");";
            break;
        case 4:
            out << "DEFINE SUBQUERY $sq($limit) AS\n"
                << "    SELECT key FROM plato.Input LIMIT $limit;\n"
                << "END DEFINE;\n"
                << "SELECT * FROM $sq(" << provider.ConsumeIntegralInRange<ui32>(0, 16) << ");";
            break;
        case 5:
            out << "VALUES (1, " << lit << "), (2, " << QuoteSqlString("b") << ");";
            break;
        case 6:
            out << "INSERT INTO plato.Output SELECT key, value FROM plato.Input;";
            break;
        case 7:
            out << "UPSERT INTO plato.Output SELECT key, value FROM plato.Input;";
            break;
        case 8:
            out << "UPDATE plato.Output SET value = " << lit << " WHERE key = " << QuoteSqlString("k1") << ";";
            break;
        case 9:
            out << "DELETE FROM plato.Output WHERE amount < " << SmallInt(provider) << ";";
            break;
        case 10:
            out << "CREATE TABLE " << id << " (key String, value " << type << ", PRIMARY KEY(key));\n"
                << "DROP TABLE " << id << ";";
            break;
        case 11:
            out << "CREATE VIEW " << id << " WITH (security_invoker = TRUE) AS SELECT key FROM plato.Input;\n"
                << "DROP VIEW " << id << ";";
            break;
        case 12:
            out << "COMMIT;\nDISCARD SELECT key FROM plato.Input;";
            break;
        case 13:
            out << "IMPORT " << QuoteSqlString("lib") << " SYMBOLS $foo;\n"
                << "EXPORT $foo SYMBOLS " << lit << ";\n"
                << "SELECT 1;";
            break;
        case 14:
            out << "CREATE EXTERNAL DATA SOURCE " << id
                << " WITH (SOURCE_TYPE = " << QuoteSqlString("ObjectStorage")
                << ", LOCATION = " << QuoteSqlString("https://example.invalid") << ");";
            break;
    }
    return Limit(out);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    if (size > MaxInputSize) {
        return 0;
    }

    const TString raw(reinterpret_cast<const char*>(data), size);
    ExerciseSqlWithFreshSettings(raw, data, size);

    FuzzedDataProvider provider(data, size);
    TStringBuilder program;
    const size_t statements = provider.ConsumeIntegralInRange<size_t>(1, 3);
    for (size_t i = 0; i < statements; ++i) {
        program << BuildStatement(provider) << '\n';
    }
    ExerciseSqlWithFreshSettings(Limit(program), data, size);
    return 0;
}
