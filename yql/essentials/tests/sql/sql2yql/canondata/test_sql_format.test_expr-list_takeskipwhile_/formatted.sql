/* postgres can not */
/* syntax version 1 */
$a = AsList(1, 2, 3, 1, 2, 3);

SELECT
    ListTakeWhile(
        $a, ($x) -> {
            RETURN $x < 3;
        }
    ),
    ListSkipWhile(
        $a, ($x) -> {
            RETURN $x < 3;
        }
    ),
    Yql::Collect(
        YQL::TakeWhile(
            Yql::Iterator($a, Yql::DependsOn(1)), ($x) -> {
                RETURN $x < 3;
            }
        )
    ),
    Yql::Collect(
        YQL::SkipWhile(
            Yql::Iterator($a, Yql::DependsOn(2)), ($x) -> {
                RETURN $x < 3;
            }
        )
    ),
    Yql::TakeWhile(
        Just(1), ($x) -> {
            RETURN $x < 3;
        }
    ),
    Yql::SkipWhile(
        Just(1), ($x) -> {
            RETURN $x < 3;
        }
    )
;
