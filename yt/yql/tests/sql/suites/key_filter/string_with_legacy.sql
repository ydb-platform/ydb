/* syntax version 1 */
/* postgres can not */
pragma yt.UseNewPredicateExtraction="false";

SELECT key FROM plato.Input WHERE StartsWith(key, String("150")) ORDER BY key;

SELECT key FROM plato.Input WHERE StartsWith(key, Utf8("15")) OR StartsWith(key, Utf8("150")) ORDER BY key;

SELECT key FROM plato.Input WHERE StartsWith(key, Utf8("тест")) OR StartsWith(key, String("тест\xff")) ORDER BY key;

SELECT key FROM plato.Input WHERE StartsWith(key, String("\xff")) ORDER BY key;
