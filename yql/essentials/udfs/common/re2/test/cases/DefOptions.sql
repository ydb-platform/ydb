/* syntax version 1 */
select Re2::Options(
    true as `Utf8`,
    false as PosixSyntax,
    false as LongestMatch,
    true as LogErrors,
    8<<20 as MaxMem,
    false as Literal,
    false as NeverNl,
    false as DotNl,
    false as NeverCapture,
    true as CaseSensitive,
    false as PerlClasses,
    false as WordBoundary,
    false as OneLine
);

select Re2::Options(
);
