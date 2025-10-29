/* postgres can not */

$pt = AsTuple(1, 2, 3);
$et = Nothing(ParseType("Tuple<Int32, Int32, Int32>?"));

SELECT
    1     IN $pt, -- true
    100   IN $pt, -- false
    1/1   IN $pt, -- Just(true)
    1/0   IN $pt, -- Nothing(bool)
    NULL  IN $pt, -- Nothing(bool)

    1     IN Just($pt), -- Just(true)
    100   IN Just($pt), -- Just(false)
    1/1   IN Just($pt), -- Just(true)
    1/0   IN Just($pt), -- Nothing(bool)
    NULL  IN Just($pt), -- Nothing(bool)

    1     IN $et, -- Nothing(bool) starting from here
    100   IN $et,
    1/1   IN $et,
    1/0   IN $et,
    NULL  IN $et,

    1     IN NULL,
    100   IN NULL,
    1/1   IN NULL,
    1/0   IN NULL,
    NULL  IN NULL;
