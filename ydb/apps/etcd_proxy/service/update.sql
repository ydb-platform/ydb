DEFINE ACTION $update($revision, $key, $value, $lease) AS

UPDATE `huidig` ON
SELECT `key` AS `key`, `created` AS `created`, $revision AS `modified`, `version` + 1L AS `version`, NVL($value,`value`) AS `value`, NVL($lease,`lease`) AS `lease`
FROM `huidig` WHERE `key` = $key;

INSERT INTO `verhaal`
SELECT `key` AS `key`, `created` AS `created`, $revision AS `modified`, `version` + 1L AS `version`, NVL($value,`value`) AS `value`, NVL($lease,`lease`) AS `lease`
FROM `huidig` WHERE `key` = $key;

END DEFINE;
