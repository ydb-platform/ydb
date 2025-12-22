PRAGMA config.flags('NamedArgsIgnoreCase');

SELECT
    Yson::Parse('[', Yson::Options(FALSE AS strict))
;
