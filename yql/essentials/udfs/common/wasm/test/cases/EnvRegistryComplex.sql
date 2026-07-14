/* syntax version 1 */
SELECT
    Base64::base64_encode("hello world") AS encoded,
    Base64::base64_decode(Base64::base64_encode("hello world")) AS round_trip;
