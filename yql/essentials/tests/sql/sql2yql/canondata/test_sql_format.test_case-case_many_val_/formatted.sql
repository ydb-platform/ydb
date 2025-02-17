/* syntax version 1 */
/* yt can not */
$switch = ($x) -> {
    $res = CASE $x
        WHEN 0 THEN 1
        WHEN 1 THEN 2
        WHEN 2 THEN 3
        WHEN 3 THEN 4
        WHEN 4 THEN 5
        WHEN 5 THEN 6
        WHEN 6 THEN 7
        WHEN 7 THEN 8
        WHEN 8 THEN 9
        WHEN 9 THEN 10
        WHEN 10 THEN 11
        WHEN 11 THEN 12
        WHEN 12 THEN 13
        WHEN 13 THEN 14
        WHEN 14 THEN 15
        WHEN 15 THEN 16
        WHEN 16 THEN 17
        WHEN 17 THEN 18
        WHEN 18 THEN 19
        WHEN 19 THEN 20
        WHEN 20 THEN 21
        WHEN 21 THEN 22
        WHEN 22 THEN 23
        WHEN 23 THEN 24
        WHEN 24 THEN 25
        WHEN 25 THEN 26
        WHEN 26 THEN 27
        WHEN 27 THEN 28
        WHEN 28 THEN 29
        WHEN 29 THEN 30
        WHEN 30 THEN 31
        WHEN 31 THEN 32
        WHEN 32 THEN 33
        WHEN 33 THEN 34
        WHEN 34 THEN 35
        WHEN 35 THEN 36
        WHEN 36 THEN 37
        WHEN 37 THEN 38
        WHEN 38 THEN 39
        WHEN 39 THEN 40
        WHEN 40 THEN 41
        WHEN 41 THEN 42
        WHEN 42 THEN 43
        WHEN 43 THEN 44
        WHEN 44 THEN 45
        WHEN 45 THEN 46
        WHEN 46 THEN 47
        WHEN 47 THEN 48
        WHEN 48 THEN 49
        WHEN 49 THEN 50
        WHEN 50 THEN 51
        WHEN 51 THEN 52
        WHEN 52 THEN 53
        WHEN 53 THEN 54
        WHEN 54 THEN 55
        WHEN 55 THEN 56
        WHEN 56 THEN 57
        WHEN 57 THEN 58
        WHEN 58 THEN 59
        WHEN 59 THEN 60
        WHEN 60 THEN 61
        WHEN 61 THEN 62
        WHEN 62 THEN 63
        WHEN 63 THEN 64
        WHEN 64 THEN 65
        WHEN 65 THEN 66
        WHEN 66 THEN 67
        WHEN 67 THEN 68
        WHEN 68 THEN 69
        WHEN 69 THEN 70
        WHEN 70 THEN 71
        WHEN 71 THEN 72
        WHEN 72 THEN 73
        WHEN 73 THEN 74
        WHEN 74 THEN 75
        WHEN 75 THEN 76
        WHEN 76 THEN 77
        WHEN 77 THEN 78
        WHEN 78 THEN 79
        WHEN 79 THEN 80
        WHEN 80 THEN 81
        WHEN 81 THEN 82
        WHEN 82 THEN 83
        WHEN 83 THEN 84
        WHEN 84 THEN 85
        WHEN 85 THEN 86
        WHEN 86 THEN 87
        WHEN 87 THEN 88
        WHEN 88 THEN 89
        WHEN 89 THEN 90
        WHEN 90 THEN 91
        WHEN 91 THEN 92
        WHEN 92 THEN 93
        WHEN 93 THEN 94
        WHEN 94 THEN 95
        WHEN 95 THEN 96
        WHEN 96 THEN 97
        WHEN 97 THEN 98
        WHEN 98 THEN 99
        ELSE 100
    END;
    RETURN $res;
};

$expected = ListFromRange(1, 101);

SELECT
    ListMap(ListFromRange(0, 100), $switch) == $expected
;
