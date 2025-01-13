/* postgres can not */
/* syntax version 1 */
SELECT
    FormatCode(
        ListCode(
            AtomCode('1'),
            AsList(AtomCode('2'), AtomCode('3')),
            FuncCode(
                'Func',
                AtomCode('4'),
                AsList(AtomCode('5'), AtomCode('6'))
            ),
            LambdaCode(
                () -> {
                    RETURN AtomCode('7');
                }
            ),
            LambdaCode(
                ($x) -> {
                    RETURN FuncCode('-', $x);
                }
            ),
            LambdaCode(
                ($x, $y) -> {
                    RETURN FuncCode('*', $x, $y);
                }
            ),
            LambdaCode(
                2, ($args) -> {
                    RETURN FuncCode('+', Unwrap($args[0]), Unwrap($args[1]));
                }
            ),
        )
    )
;
