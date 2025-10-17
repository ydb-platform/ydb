PRAGMA Layer = @@{
    "name": "abc"
}@@;

$bar = @@{
    "name": "@@
    || '123' || '321' || @@",
    "parent": "abc"
}@@;

PRAGMA Layer = $bar;

SELECT
    $bar
;
