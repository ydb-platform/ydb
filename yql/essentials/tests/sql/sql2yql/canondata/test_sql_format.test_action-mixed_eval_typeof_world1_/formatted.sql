/* syntax version 1 */
/* postgres can not */
/* custom error:Anonymous table "@Output1" must be materialized*/
USE plato;

$force_remove_members = ($struct, $to_remove) -> {
    $remover = EvaluateCode(
        LambdaCode(
            ($st) -> {
                $to_keep = ListFlatMap(
                    StructTypeComponents(TypeHandle(TypeOf($struct))), ($x) -> {
                        RETURN IF($x.Name NOT IN $to_remove, $x.Name);
                    }
                );
                RETURN FuncCode(
                    'AsStruct',
                    ListMap(
                        $to_keep,
                        ($x) -> {
                            RETURN ListCode(AtomCode($x), FuncCode('Member', $st, AtomCode($x)));
                        }
                    )
                );
            }
        )
    );
    RETURN $remover($struct);
};

DEFINE ACTION $func($input, $output) AS
    $jname = $output;

    INSERT INTO @$jname WITH truncate
    SELECT
        *
    FROM
        $input AS input
    ;
    COMMIT;

    INSERT INTO $output WITH truncate
    SELECT
        AGG_LIST(
            $force_remove_members(
                TableRow(),
                ['']
            )
        )
    FROM
        @$jname
    ;
    COMMIT;
END DEFINE;

$exps = [('Input', 'Output1'), ('Input', 'Output2'), ('Input', 'Output3')];

EVALUATE FOR $exp_name IN $exps DO BEGIN
    $input = $exp_name.0;
    $output = $exp_name.1;
    DO
        $func($input, $output)
    ;
END DO;
