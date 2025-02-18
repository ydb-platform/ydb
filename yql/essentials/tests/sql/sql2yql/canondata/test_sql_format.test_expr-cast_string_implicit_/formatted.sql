SELECT
    AsList('aaa', 'aaa'u)
; -- List<String>

SELECT
    AsList('aaa', '[1, 2, 3]'j)
; -- List<String>

SELECT
    AsList('aaa', '[1; 2; 3]'y)
; -- List<String>

SELECT
    AsList('aaa'u, 'aaa')
; -- List<String>

SELECT
    AsList('aaa'u, '[1, 2, 3]'j)
; -- List<Utf8>

SELECT
    AsList('aaa'u, '[1; 2; 3]'y)
; -- List<String>

SELECT
    AsList('[1, 2, 3]'j, 'aaa')
; -- List<String>

SELECT
    AsList('[1, 2, 3]'j, 'aaa'u)
; -- List<Utf8>

SELECT
    AsList('[1, 2, 3]'j, '[1; 2; 3]'y)
; -- List<String>

SELECT
    AsList('[1; 2; 3]'y, 'aaa')
; -- List<String>

SELECT
    AsList('[1; 2; 3]'y, 'aaa'u)
; -- List<String>

SELECT
    AsList('[1; 2; 3]'y, '[1, 2, 3]'j)
; -- List<String>
