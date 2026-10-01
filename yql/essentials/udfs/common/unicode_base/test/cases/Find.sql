$input = [
    <|value: Just("lästig, möchten, ausführlich, später, können, natürlich, universität, öffentlich, rückwärts, kämpfen, mögen, überall, regelmäßig"u)|>,
    <|value: Just("lästig, möchten, ausführlich, später, können, natürlich, universität, öffentlich, rückwärts, kämpfen, mögen, überall, regelmäßig"u)|>,
];

SELECT
    value as value,
    Unicode::Substring(value, Unicode::Find(value, "ä"u), Unicode::RFind(value, "ä"u) - Unicode::Find(value, "ä"u)),
    Unicode::Substring(value, Unicode::Find(value, "ö"u), Unicode::RFind(value, "ö"u) - Unicode::Find(value, "ö"u)),
    Unicode::Substring(value, Unicode::Find(value, "ü"u), Unicode::RFind(value, "ü"u) - Unicode::Find(value, "ü"u)),
    Unicode::Substring(value, Unicode::Find(value, "ä"u, 30ul), Unicode::RFind(value, "ä"u, 123ul) - Unicode::Find(value, "ä"u, 30ul)),
    Unicode::Substring(value, Unicode::Find(value, "ö"u, 9ul), Unicode::RFind(value, "ö"u, 103ul) - Unicode::Find(value, "ö"u, 9ul)),
    Unicode::Substring(value, Unicode::Find(value, "ü"u, 45ul), Unicode::RFind(value, "ü"u, 83ul) - Unicode::Find(value, "ü"u, 45ul))
FROM AS_TABLE($input)
