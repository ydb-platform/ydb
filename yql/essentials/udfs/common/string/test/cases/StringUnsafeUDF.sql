$input = AsList(
    <|value:"EAQCAILRO5SSA4TUPEQCAIDVNFXXAIC3EBOSI==="|>,
    <|value:"ICAgIXF3ZSBydHkgICB1aW9wIFsgXSQ="|>,
    <|value:"202020217177652072747920202075696F70205B205D24"|>,
    <|value:"IBQXGIBAEAQCAIBAMRTGO2BANJVWYXDOHMTSKIBA"|>,
    <|value:"QGFzICAgICAgIGRmZ2ggamtsXG47JyUgIA,,"|>,
    <|value:"4061732020202020202064666768206A6B6C5C6E3B27252020"|>
);

SELECT
    String::Base32Decode(value) as b32dec,
    String::Base32StrictDecode(value) AS b32sdec,
    String::Base64Decode(value) as b64dec,
    String::Base64StrictDecode(value) AS b64sdec,
    String::HexDecode(value) as xdec,
FROM AS_TABLE($input)
