/* syntax version 1 */
SELECT
    value,
    String::Base32Encode(value) AS encoded
FROM Input;
