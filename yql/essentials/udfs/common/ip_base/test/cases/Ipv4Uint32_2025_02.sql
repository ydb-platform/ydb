SELECT
    Ip::Ipv4ToUint32(Ip::FromString("127.0.0.1")) AS uint32_repr,
    Ip::Ipv4FromUint32(0x7F000001U) AS internal_repr_uint32;
