$input = AsList(
    <|value:"qwertyui", biguint:1234567890ul, negint:-123l|>,
    <|value:"asdfghjl", biguint:9876543210ul, negint:-456l|>,
    <|value:"zxcvbnm?", biguint:9999999999ul, negint:-789l|>,
    <|value:"12345678", biguint:0ul, negint:0l|>,
    <|value:"!@#$%^&*", biguint:9182737465ul, negint:-999l|>
);

SELECT
    value,
    String::RightPad(value, 20) AS right_pad,
    String::LeftPad(value, 20) AS left_pad,
    String::RightPad(value, 20, "0") AS right_pad_zero,
    String::LeftPad(value, 20, "0") AS left_pad_zero,
    String::Hex(biguint) AS hex,
    String::SHex(negint) AS shex,
    String::Bin(biguint) AS bin,
    String::SBin(negint) AS sbin,
    String::HexText(value) AS hex_text,
    String::BinText(value) AS bin_text,
    String::HumanReadableDuration(biguint) AS duration,
    String::HumanReadableQuantity(biguint) AS quantity,
    String::HumanReadableBytes(biguint) AS bytes,
    String::Prec(negint / 12345.6789, 4) AS prec
FROM AS_TABLE($input);
