SELECT
    String::Base32Decode(value) as b32dec,
    String::Base32StrictDecode(value) AS b32sdec,
    String::Base64Decode(value) as b64dec,
    String::Base64StrictDecode(value) AS b64sdec,
    String::HexDecode(value) as xdec,
FROM Input
