/* syntax version 1 */
SELECT
    Re2::IsValidRegexp("*") AS invalid_star_at_start,
    Re2::IsValidRegexp("[") AS invalid_unclosed_bracket,
    Re2::IsValidRegexp(".") AS valid_dot_metachar,
    Re2::IsValidRegexp("abc") AS valid_literal_string,
    Re2::IsValidRegexp("\xff") AS valid_byte_default_encoding,
    Re2::IsValidRegexp("\xff", Re2::Options(true as Utf8)) AS invalid_byte_utf8_encoding,
    Re2::IsValidRegexp("\xff", Re2::Options(false as Utf8)) AS valid_byte_latin1_encoding,

    Re2posix::IsValidRegexp("(?:abc)") AS posix_invalid_non_capturing_group,
    Re2::IsValidRegexp("(?:abc)") AS re2_default_valid_non_capturing_group,

    Re2::IsValidRegexp(null) as null_string,
    Re2::IsValidRegexp(".", null) AS null_options;
