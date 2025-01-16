DEFINE ACTION $upsert($revision, $key, $value, $lease) AS

UPSERT INTO `huidig`
SELECT * FROM (
    SELECT
        SOME(<|`key`:$key, `created`:`created`, `modified`:$revision, `version`:`version` + 1L, `value`:$value, `lease`:`lease`|>) ??
             <|`key`:$key, `created`:$revision, `modified`:$revision, `version`: 1L, `value`:$value, `lease`:$lease|>
    FROM `huidig` WHERE `key` = $key
) FLATTEN COLUMNS;

INSERT INTO `verhaal`
SELECT * FROM (
    SELECT
        SOME(<|`key`:$key, `created`:`created`, `modified`:$revision, `version`:`version` + 1L, `value`:$value, `lease`:`lease`|>) ??
             <|`key`:$key, `created`:$revision, `modified`:$revision, `version`: 1L, `value`:$value, `lease`:$lease|>
    FROM `huidig` WHERE `key` = $key
) FLATTEN COLUMNS;

END DEFINE;
