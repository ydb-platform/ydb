$text ="lästig, möchten, ausführlich, später, können, natürlich, universität, öffentlich, rückwärts, kämpfen, mögen, überall, regelmäßig"u;

SELECT
    Unicode::Substring($text, Unicode::Find($text, "ä"u), Unicode::RFind($text, "ä"u) - Unicode::Find($text, "ä"u)),
    Unicode::Substring($text, Unicode::Find($text, "ö"u), Unicode::RFind($text, "ö"u) - Unicode::Find($text, "ö"u)),
    Unicode::Substring($text, Unicode::Find($text, "ü"u), Unicode::RFind($text, "ü"u) - Unicode::Find($text, "ü"u));


SELECT
    Unicode::Substring($text, Unicode::Find($text, "ä"u, 30ul), Unicode::RFind($text, "ä"u, 123ul) - Unicode::Find($text, "ä"u, 30ul)),
    Unicode::Substring($text, Unicode::Find($text, "ö"u, 9ul), Unicode::RFind($text, "ö"u, 103ul) - Unicode::Find($text, "ö"u, 9ul)),
    Unicode::Substring($text, Unicode::Find($text, "ü"u, 45ul), Unicode::RFind($text, "ü"u, 83ul) - Unicode::Find($text, "ü"u, 45ul));

