$input = [
    <|value: "\xC3\xA9"u|>,
    <|value: "e\xCC\x81"u|>,
    <|value: "\xC2\xB5"u|>,
    <|value: "\xE2\x84\x8C"u|>,
];

SELECT
    value AS value,
    Unicode::Normalize(value) AS normalize,
    Unicode::NormalizeNFD(value) AS normalize_nfd,
    Unicode::NormalizeNFC(value) AS normalize_nfc,
    Unicode::NormalizeNFKD(value) AS normalize_nfkd,
    Unicode::NormalizeNFKC(value) AS normalize_nfkc
FROM AS_TABLE($input)
