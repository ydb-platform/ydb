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
    Ip::ToString(internal_representation) AS round_trip,
    Ip::IsIPv4(internal_representation) AS is_ipv4,
    Ip::IsIPv6(internal_representation) AS is_ipv6,
    Ip::IsEmbeddedIPv4(internal_representation) AS is_embedded_ipv4,
    Ip::ToString(Ip::ConvertToIPv6(internal_representation)) AS all_ipv6,
    Ip::ToString(Ip::GetSubnet(internal_representation)) AS default_subnet,
    Ip::ToString(Ip::GetSubnet(internal_representation, 125)) AS small_subnet,
    Ip::ToString(Ip::GetSubnet(internal_representation, 16)) AS large_subnet,
    Ip::ToString(Ip::GetSubnet(internal_representation, 32)) AS single_subnet4,
    Ip::ToString(Ip::GetSubnet(internal_representation, 128)) AS single_subnet6
FROM (
    SELECT Ip::FromString(key) AS internal_representation FROM AS_TABLE($input)
);
