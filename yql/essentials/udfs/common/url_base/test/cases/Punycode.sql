$input = AsList(
    <|value: "abæcdöef"|>,
    <|value: "яндекс.ру"|>,
    <|value: "yandex.ru"|>,
    <|value: "xn--d1acpjx3f.xn--p1ag"|>,
);

SELECT
    value,
    Url::PunycodeToHostName(value) AS hostname_utf,
    Url::HostNameToPunycode(Url::PunycodeToHostName(value)) as punycode_hostname,
    Url::ForcePunycodeToHostName(value) AS hostname_utf_forced,
    Url::ForceHostNameToPunycode(Url::ForcePunycodeToHostName(value)) as punycode_hostname_forced,
    Url::CanBePunycodeHostName(value) as can_be_punycode
FROM AS_TABLE($input);
