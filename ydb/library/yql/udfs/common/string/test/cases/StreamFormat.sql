/* syntax version 1 */
SELECT
    value,
    String::RightPad(value, 20) AS right_pad,
    String::LeftPad(value, 20) AS left_pad,
    String::RightPad(value, 20, "0") AS right_pad_zero,
    String::LeftPad(value, 20, "0") AS left_pad_zero,
    String::Hex(LENGTH(value)) AS hex,
    String::SHex(-LENGTH(value)) AS shex,
    String::Bin(LENGTH(value)) AS bin,
    String::SBin(-LENGTH(value)) AS sbin,
    String::HexText(value) AS hex_text,
    String::BinText(value) AS bin_text,
    String::HumanReadableDuration(LENGTH(value) * 123456789) AS duration,
    String::HumanReadableQuantity(LENGTH(value) * 123456789) AS quantity,
    String::HumanReadableBytes(LENGTH(value) * 123456789) AS bytes,
    String::Prec(LENGTH(value) / 12345.6789, 4) AS prec
FROM Input;
