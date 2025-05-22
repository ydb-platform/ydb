/* syntax version 1 */
SELECT
    value,
    Url::PunycodeToHostName(value) AS hostname_utf,
    Url::HostNameToPunycode(Url::PunycodeToHostName(value)) as punycode_hostname,
    Url::ForcePunycodeToHostName(value) AS hostname_utf_forced,
    Url::ForceHostNameToPunycode(Url::ForcePunycodeToHostName(value)) as punycode_hostname_forced,
    Url::CanBePunycodeHostName(value) as can_be_punycode
FROM Input;
