/* postgres can not */
select 
    Untag(AsTagged(1,'a'),'a'),
    Untag(Just(AsTagged(1,'a')),'a'),
    Untag(Nothing(Tagged<Int32,'a'>?),'a'),
    Untag(NULL,'a');

