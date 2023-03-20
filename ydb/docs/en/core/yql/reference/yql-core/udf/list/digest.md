# Digest

A set of commonly used hash functions.

**List of functions**

* ```Digest::Crc32c(String{Flags::AutoMap}) -> Uint32```
* ```Digest::Fnv32(String{Flags::AutoMap}) -> Uint32```
* ```Digest::Fnv64(String{Flags::AutoMap}) -> Uint64```
* ```Digest::MurMurHash(String{Flags:AutoMap}) -> Uint64```
* ```Digest::MurMurHash32(String{Flags:AutoMap}) -> Uint32```
* ```Digest::MurMurHash2A(String{Flags:AutoMap}) -> Uint64```
* ```Digest::MurMurHash2A32(String{Flags:AutoMap}) -> Uint32```
* ```Digest::CityHash(String{Flags:AutoMap}) -> Uint64```
* ```Digest::CityHash128(String{Flags:AutoMap}) -> Tuple<Uint64,Uint64>```
* ```Digest::NumericHash(Uint64{Flags:AutoMap}) -> Uint64```
* ```Digest::Md5Hex(String{Flags:AutoMap}) -> String```
* ```Digest::Md5Raw(String{Flags:AutoMap}) -> String```
* ```Digest::Md5HalfMix(String{Flags:AutoMap}) -> Uint64```: MD5 coarsening option (yabs_md5)
* ```Digest::Argon2(String{Flags:AutoMap},String{Flags:AutoMap}) -> String```: The second argument is the salt
* ```Digest::Blake2B(String{Flags:AutoMap},[String?]) -> String```: The second optional argument is the key
* ```Digest::SipHash(Uint64,Uint64,String{Flags:AutoMap}) -> Uint64```
* ```Digest::HighwayHash(Uint64,Uint64,Uint64,Uint64,String{Flags:AutoMap}) -> Uint64```
* ```Digest::FarmHashFingerprint(Uint64{Flags:AutoMap}) -> Uint64```
* ```Digest::FarmHashFingerprint2(Uint64{Flags:AutoMap}, Uint64{Flags:AutoMap}) -> Uint64```
* ```Digest::FarmHashFingerprint32(String{Flags:AutoMap}) -> Uint32```
* ```Digest::FarmHashFingerprint64(String{Flags:AutoMap}) -> Uint64```
* ```Digest::FarmHashFingerprint128(String{Flags:AutoMap}) -> Tuple<Uint64,Uint64>```
* ```Digest::SuperFastHash(String{Flags:AutoMap}) -> Uint32```
* ```Digest::Sha1(String{Flags:AutoMap}) -> String```
* ```Digest::Sha256(String{Flags:AutoMap}) -> String```
* ```Digest::IntHash64(Uint64{Flags:AutoMap}) -> Uint64```
* ```Digest::XXH3(String{Flags:AutoMap}) -> Uint64```
* ```Digest::XXH3_128(String{Flags:AutoMap}) -> Tuple<Uint64,Uint64>```

**Examples**

```sql
SELECT Digest::Md5Hex("YQL");  -- "1a0c1b56e9d617688ee345da4030da3c"
SELECT Digest::NumericHash(123456789); -- 1734215268924325803
```

