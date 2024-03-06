SELECT '?' LIKE '%??%' ESCAPE '?',
       'x_' LIKE '%xxx_' ESCAPE 'x',
       '[' LIKE '[' ESCAPE '!',
       '.' LIKE '..' ESCAPE '.',
       '[' LIKE '[[' ESCAPE '[',
       'a%b' LIKE '.a.%.b' ESCAPE '.',
       'x' LIKE '..' ESCAPE '.';