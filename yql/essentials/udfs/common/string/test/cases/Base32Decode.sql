/* syntax version 1 */
SELECT
    value,
    String::Base32StrictDecode(value) AS strict_decoded,
    String::Base32Decode(value) AS decoded
FROM Input;
