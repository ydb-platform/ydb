/* postgres can not */
use plato;

pragma yt.UseNativeYtTypes="1";
pragma yt.NativeYtTypeCompatibility="null;void";

INSERT INTO @tmp
SELECT null AS ttt, Yql::Void AS v
;

COMMIT;
SELECT * FROM @tmp
