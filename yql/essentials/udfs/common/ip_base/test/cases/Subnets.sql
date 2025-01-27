/* syntax version 1 */
SELECT
    subnet1 AS internal1,
    Ip::SubnetToString(subnet1) AS string1,
    Ip::SubnetMatch(subnet1, subnet2) AS subnet1_subnet2_match,
    Ip::SubnetMatch(subnet1, ip1) AS subnet1_ip1_match,
    Ip::SubnetMatch(subnet2, ip1) AS subnet2_ip1_match,
    Ip::ToString(Ip::GetSubnetByMask(ip1, ip2)) AS ip1_ip2_mask_subnet
FROM (
    SELECT
        Ip::SubnetFromString(subnet1) AS subnet1,
        Ip::SubnetFromString(subnet2) AS subnet2,
        Ip::FromString(ip1) AS ip1,
        Ip::FromString(ip2) AS ip2
    FROM Input
);