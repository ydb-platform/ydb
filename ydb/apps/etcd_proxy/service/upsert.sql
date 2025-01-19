DEFINE ACTION $upsert($revision, $key, $value, $lease) AS

INSERT INTO `verhaal`
SELECT
    $key AS `key`,
    SOME(`created`) ?? $revision AS `created`,
    $revision AS `modified`,
    (SOME(`version`) ?? 0L) + 1L AS `version`,
    $value AS `value`,
    $lease AS `lease`
FROM `huidig` WHERE `key` = $key;

UPSERT INTO `huidig`
SELECT
    $key AS `key`,
    SOME(`created`) ?? $revision AS `created`,
    $revision AS `modified`,
    (SOME(`version`) ?? 0L) + 1L AS `version`,
    $value AS `value`,
    $lease AS `lease`
FROM `huidig` WHERE `key` = $key;

END DEFINE;
