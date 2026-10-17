$input = [
    <|key: "127.0.0.1"|>,
    <|key: "::1"|>,
    <|key: "213.180.193.3"|>,
    <|key: "2a02:6b8::3"|>,
    <|key: "2400:cb00:2048:1::681c:1b65"|>,
    <|key: "fe80::215:b2ff:fea9:67ce"|>,
    <|key: "::ffff:77.75.155.3"|>,
    <|key: "sdfsdfsdf"|>,
    <|key: "0.0.0.0"|>,
];

SELECT
    internal_representation AS internal_representation,
    Ip::Ipv4ToUint32(internal_representation) AS uint32_repr,
    Ip::Ipv4FromUint32(Ip::Ipv4ToUint32(internal_representation)) AS internal_repr_uint32,
FROM (
    SELECT Ip::FromString(key) AS internal_representation FROM AS_TABLE($input)
);
