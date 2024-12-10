PRAGMA DqEngine = "disable";
PRAGMA yt.InferSchema;
PRAGMA yt.UseNativeYtTypes = "1";
PRAGMA yt.DefaultMaxJobFails = "1";

SELECT
    boobee,
    DictLookup(_other, 'boobee')
FROM
    plato.Input
ORDER BY
    boobee
;
