$p = 'http_test://' || 'foo.txt';

PRAGMA file('foo.txt', $p);

SELECT
    FileContent('foo.txt')
;
