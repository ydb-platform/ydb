#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimr::NKqp {

// Replaces only the string literals that the YQL grammar marks as secret
// values with '***removed***'. Other strings, numbers, identifiers,
// keywords and statement structure are preserved as-is. The masking is
// not a generic "obfuscate all strings" pass — it deliberately targets
// grammar rules whose payload is, by definition, secret material:
//
//   * password_value         — CREATE/ALTER USER ... PASSWORD '...'
//   * hash_option            — CREATE/ALTER USER ... HASH '...'
//   * object_feature_value   — CREATE/ALTER/UPSERT OBJECT (TYPE SECRET)
//                              WITH (key = '...'); also covers external
//                              data source credentials (AWS keys,
//                              MySQL/PG/CH passwords, ...)
//
// If YQL grammar (yql/essentials/sql/v1/SQLv1Antlr4.g.in) introduces a
// new rule that carries secret material via STRING_VALUE, add a matching
// handler in kqp_mask_literals.cpp and a test in
// kqp_mask_literals_ut.cpp.
//
// Returns Nothing() if parsing fails. Callers MUST treat this as a hard
// failure and not log the original query: the function is only invoked
// when the query is already known to carry secrets, so falling back to
// the unmasked text would defeat the masking. Suppress the query text
// (e.g. log a placeholder) on Nothing().
TMaybe<TString> MaskSecretValueLiterals(const TString& query);

} // namespace NKikimr::NKqp
