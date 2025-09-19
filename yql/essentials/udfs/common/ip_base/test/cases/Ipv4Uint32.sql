/* syntax version 1 */
SELECT
    internal_representation AS internal_representation,
    Ip::Ipv4ToUint32(internal_representation) AS uint32_repr,
    Ip::Ipv4FromUint32(Ip::Ipv4ToUint32(internal_representation)) AS internal_repr_uint32,
FROM (
    SELECT Ip::FromString(key) AS internal_representation FROM Input
);
