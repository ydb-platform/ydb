SELECT
    StablePickle({{1, 2}, {3, 4}}) == StablePickle({{4, 3}, {2, 1}})
;

SELECT
    StablePickle({{pgdate('2020-01-01'), pgdate('2020-01-02')}, {pgdate('2020-01-03'), pgdate('2020-01-04')}}) == StablePickle({{pgdate('2020-01-04'), pgdate('2020-01-03')}, {pgdate('2020-01-02'), pgdate('2020-01-01')}})
;

SELECT
    StablePickle({{pgbit('0001'), pgbit('0010')}, {pgbit('0100'), pgbit('1000')}}) == StablePickle({{pgbit('1000'), pgbit('0100')}, {pgbit('0010'), pgbit('0001')}})
;
