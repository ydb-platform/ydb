/* syntax version 1 */

pragma UseBlocks;

SELECT
    value AS value,
    Unicode::Normalize(value) AS normalize,
    Unicode::NormalizeNFD(value) AS normalize_nfd,
    Unicode::NormalizeNFC(value) AS normalize_nfc,
    Unicode::NormalizeNFKD(value) AS normalize_nfkd,
    Unicode::NormalizeNFKC(value) AS normalize_nfkc
FROM Input

