#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimr::NKqp {

// Replaces every string literal in the SQL query with '***removed***',
// keeping numbers, identifiers, keywords and statement structure intact.
// The function does NOT try to decide which literal is "the password" —
// it masks all of them, on the assumption that the caller has already
// established the query is suspect (e.g. a CREATE/ALTER USER, CREATE
// OBJECT TYPE SECRET, etc.) and any string in such a query can carry
// secret material.
//
// Implementation is grammar-driven: handlers in kqp_mask_literals.cpp
// cover the YQL grammar rules where strings appear (literal_value,
// pragma_value, password_value, hash_option, object_feature_value).
// If YQL grammar (yql/essentials/sql/v1/SQLv1Antlr4.g.in) gains a new
// rule that consumes STRING_VALUE without going through literal_value
// and may carry secrets, add a matching handler here and a test in
// kqp_mask_literals_ut.cpp.
//
// Returns Nothing() if parsing fails. Callers MUST treat this as a hard
// failure and not log the original query: the function is only invoked
// when the query is already known to carry secrets, so falling back to
// the unmasked text would defeat the masking. Suppress the query text
// (e.g. log a placeholder) on Nothing().
TMaybe<TString> MaskSecretValueLiterals(const TString& query);

} // namespace NKikimr::NKqp
