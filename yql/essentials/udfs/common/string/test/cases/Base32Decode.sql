$input = AsList(
    <|value:"ORSXG5A="|>,
    <|value:"KRSXG5CUMVZXI==="|>,
    <|value:"MFYHA3DF"|>,
    <|value:"hmmmm===hmmmm"|>
);

SELECT
    value,
    String::Base32StrictDecode(value) AS strict_decoded,
    String::Base32Decode(value) AS decoded
FROM AS_TABLE($input);
