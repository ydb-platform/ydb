$foo1 = YQL::"(lambda '(item) (Concat (String '\"foo\\\"\") item))";
$foo2 = YQL::'(lambda \'(item) (Concat (String \'"foo\\\'") item))';

SELECT
    $foo1('bar'),
    $foo2('bar')
;
