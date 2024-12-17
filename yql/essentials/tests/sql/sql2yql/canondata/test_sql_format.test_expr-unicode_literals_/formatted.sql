PRAGMA WarnUntypedStringLiterals;
PRAGMA UnicodeLiterals;

$f = () -> {
    RETURN (
        "a"s,
        "b"u,
        'c'
    );
};

SELECT
    $f()
;

PRAGMA DisableWarnUntypedStringLiterals;
PRAGMA DisableUnicodeLiterals;

$g = () -> {
    RETURN (
        "a"s,
        "b"u,
        'c'
    );
};

SELECT
    $g()
;
