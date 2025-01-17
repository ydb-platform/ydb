SELECT
    Digest::Crc64(key, 777), Digest::Crc64(key, 777 AS Init),
    Digest::Fnv32(key, 777), Digest::Fnv32(key, 777 AS Init),
    Digest::Fnv64(key, 777), Digest::Fnv64(key, 777 AS Init),
    Digest::MurMurHash(key, 777), Digest::MurMurHash(key, 777 AS Init),
    Digest::MurMurHash32(key, 777), Digest::MurMurHash32(key, 777 AS Init),
    Digest::MurMurHash2A(key, 777), Digest::MurMurHash2A(key, 777 AS Init),
    Digest::MurMurHash2A32(key, 777), Digest::MurMurHash2A32(key, 777 AS Init),
    Digest::CityHash(key, 777), Digest::CityHash(key, 777 AS Init),
    
FROM Input;