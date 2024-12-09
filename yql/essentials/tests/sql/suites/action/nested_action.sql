/* syntax version 1 */
/* postgres can not */
define action $action($b,$c) as
    define action $aaa() as
        select $b;
    end define;
    define action $bbb() as
        select $c;
    end define;    
    do $aaa();
    do $bbb();
end define;

do $action(1,2);
