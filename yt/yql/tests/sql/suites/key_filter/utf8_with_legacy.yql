/* syntax version 1 */
/* postgres can not */
pragma yt.UseNewPredicateExtraction="false";

SELECT key FROM plato.Input WHERE StartsWith(key, String("150")) ORDER BY key;

SELECT key FROM plato.Input WHERE StartsWith(key, String("15")) OR StartsWith(key, String("150")) ORDER BY key;

SELECT key FROM plato.Input WHERE StartsWith(key, String("\xf5")) ORDER BY key;

SELECT key FROM plato.Input WHERE StartsWith(key, Utf8("тест\xf4\x8f\xbf\xbf")) ORDER BY key;

SELECT key FROM plato.Input WHERE StartsWith(key, Utf8("тест")) OR StartsWith(key, Utf8("тест\xf4\x8f\xbf\xbf")) ORDER BY key;

SELECT key FROM plato.Input WHERE StartsWith(key, Utf8("\xf4\x8f\xbf\xbf")) ORDER BY key;
