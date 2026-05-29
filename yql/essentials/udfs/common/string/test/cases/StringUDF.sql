$input = AsList(
    <|value:"   !qwe rty   uiop [ ]$"|>,
    <|value:"@as       dfgh jkl\\n;'\%  "|>,
    <|value:"   #zxc\tvbn \t\n\b m,./?^   "|>,
    <|value:"1!2@3#4$5%6^7&8*9(0)-_=+,<.>"|>
);

SELECT
    String::Base32Encode(value) as b32enc,
    String::Base64Encode(value) as b64enc,
    String::Base64EncodeUrl(value) as b64encu,
    String::EscapeC(value) as cesc,
    String::UnescapeC(value) as cunesc,
    String::HexEncode(value) as xenc,
    String::EncodeHtml(value) as henc,
    String::DecodeHtml(value) as hdec,
    String::CgiEscape(value) as cgesc,
    String::CgiUnescape(value) as cgunesc,
    String::Collapse(value) as clps,
    String::Strip(value) as strp,
    String::CollapseText(value, 9) as clpst,
FROM AS_TABLE($input)
