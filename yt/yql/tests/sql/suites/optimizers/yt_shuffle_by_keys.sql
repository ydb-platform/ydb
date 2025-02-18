--Test, that YT optimizer can rewrite ShuffleByKeys with PartitionsByKes
USE plato;

$input = PROCESS Input;

SELECT YQL::ShuffleByKeys(
            $input, 
            ($_)->("dsdsa"), 
            ($_)->([1]) -- list
        );

SELECT YQL::ShuffleByKeys(
            $input, 
            ($_)->(12), 
            ($_)->(Just(2)) -- optional
        );
        
SELECT YQL::ShuffleByKeys(
            $input, 
            ($_)->(true), 
            ($_)->(YQL::ToStream([3])) -- stream
        );

