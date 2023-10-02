/* postgres can not */
/* syntax version 1 */
SELECT CAST(DyNumber("-21.33") AS String), CAST(DyNumber("0") AS Utf8),
    CAST(["-21.33E2","3.14","42","","bad"] AS List<DyNumber>);

SELECT ListMap(["\x00\x80\x65","\x00\x81\x66","\x00\x82\x56","\x01","\x02\x84\x9A","\x02\x85\x99","\x02\x86\xA9"], ($i)->(FromBytes($i, DyNumber)));
