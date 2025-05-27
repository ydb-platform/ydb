/* syntax version 1 */
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
    SELECT Ip::FromString(key) AS internal_representation FROM Input
);
