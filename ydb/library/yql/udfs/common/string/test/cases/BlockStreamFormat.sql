/* XXX: Enable UseBlocks pragma and provide input to trigger block execution. */
PRAGMA UseBlocks;

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
FROM Input;
