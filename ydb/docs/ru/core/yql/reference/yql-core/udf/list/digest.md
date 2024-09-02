# Digest

Модуль Digest вычисляют хеши(aka fingerprint'ы) от исходных сообщений. Хеши представляют собой (обычно) некоторую строку небольшого / фиксированного размера. Фактический размер хеша зависит от используемого алгоритма. Результат представляет собой последовательность бит.
Модуль содержит как криптографические, так и некриптографические функции.

Важным свойством алгоритмов  является то, что хеш, вероятно, изменится, если сообщение каким-то образом изменится. Другим свойством является то, что функции дайджеста являются односторонними функциями, то есть должно быть трудно найти сообщение, соответствующее некоторому заданному дайджесту.

**Список функций**

* ```Digest::Crc32c(String{Flags::AutoMap}) -> Uint32```
* ```Digest::Fnv32(String{Flags::AutoMap}) -> Uint32```
* ```Digest::Fnv64(String{Flags::AutoMap}) -> Uint64```
* ```Digest::MurMurHash(String{Flags:AutoMap}) -> Uint64```
* ```Digest::MurMurHash32(String{Flags:AutoMap}) -> Uint32```
* ```Digest::MurMurHash2A(String{Flags:AutoMap}) -> Uint64```
* ```Digest::MurMurHash2A32(String{Flags:AutoMap}) -> Uint32```
* ```Digest::CityHash(String{Flags:AutoMap}) -> Uint64```
* ```Digest::CityHash128(String{Flags:AutoMap}) -> Tuple<Uint64,Uint64>```

CityHash функция для байтовой строки с результатом типа uint128. Результат представлен как пара из двух uint64 чисел <low, high>
```sql
select Digest::CityHash128("Who set this ancient quarrel new abroach?"); -- (11765163929838407746,2460323961016211789)
```

* ```Digest::NumericHash(Uint64{Flags:AutoMap}) -> Uint64```

``` sql
SELECT Digest::NumericHash(123456789); -- 1734215268924325803
```

* ```Digest::Md5Hex(String{Flags:AutoMap}) -> String```
* ```Digest::Md5Raw(String{Flags:AutoMap}) -> String```
* ```Digest::Md5HalfMix(String{Flags:AutoMap}) -> Uint64```

MD5 - широко распространенный, 128-битный алгоритм хеширования.

Md5Hex - возращает хеш в виде ASCII  строки закодировавнной в 16 битной кодировке

Md5Raw - возращает хеш в виде байтовой строки

Md5HalfMix - вариант огрубления MD5 до размера Uint64

``` sql
select
    Digest::Md5Hex("Who set this ancient quarrel new abroach?"), -- "644e98bae764871650f2d93e14c6488d"
    Digest::Md5Raw("Who set this ancient quarrel new abroach?"), -- Binary String: 64 4e 98 ba e7 64 87 16 50 f2 d9 3e 14 c6 48 8d
    Digest::Md5HalfMix("Who set this ancient quarrel new abroach?"); -- 17555822562955248004
```

* ```Digest::Argon2(string:String{Flags:AutoMap}, salt:String{Flags:AutoMap}) -> String```

Argon2 - функция формирования ключа, направленная на высокую скорость заполнения памяти и эффективное использование нескольких вычислительных блоков.

Параметры:
- string - исходная строка
- salt - строка которая будет использоавться в качестве соли для хеш функции
```sql
select Digest::Argon2("Who set this ancient quarrel new abroach?", "zcIvVcuHEIL8"); -- Binary String: fa 50 34 d3 c3 23 a4 de 22 c7 7c e1 9c 65 64 88 25 b3 59 75 c5 b8 8c 73 da 88 eb 79 31 70 e8 f1
select Digest::Argon2("Who set this ancient quarrel new abroach?", "M78P42R8HA=="); -- Binary String: d2 0e f1 3e 72 5a e9 32 65 ed 28 4b 12 1f 39 70 e5 10 aa 1a 15 67 6d 96 5d e8 19 b3 bd d5 04 e9
```
* ```Digest::Blake2B(string:String{Flags:AutoMap},[key:String?]) -> String```

BLAKE2 — криптографическая хеш-функция, создана как альтернатива MD5 и SHA-1 алгоритмам, в которых были найдены уязвимости.

Параметры:
- string - исходная строка
- key - ключ, используемый для шифроавния исходной строки (может быть использован как разделяемый секрет между источником и приемником)
```sql
select Digest::Blake2B("Who set this ancient quarrel new abroach?"); -- Binary String: 62 21 91 d8 11 5a da ad 5e 7c 86 47 41 02 7f 8f a8 a6 82 07 47 d8 f8 30 ab b4 c3 00 db 9c 24 2f
```
* ```Digest::SipHash(low:Uint64,high:Uint64,string:String{Flags:AutoMap}) -> Uint64```

Функция для хеширования исходного сообщения (```string```) с помощью 128 битного ключа. Ключ представлен парой uint64 чисел low, high

```sql
select Digest::SipHash(0,0,"Who set this ancient quarrel new abroach?"); -- 14605466535756698285
```

* ```Digest::HighwayHash(key0:Uint64,key1:Uint64,key2:Uint64,key3:Uint64,string:String{Flags:AutoMap}) -> Uint64```

Функция для хеширования исходного сообщения (```string```) с ключом длиной в 256 бит.

Ключ формируется как:

    - key0 - первые 8 байт ключа
    - key1 - последующие 8 байт ключа
    - key2 - последующие 8 байт ключа
    - key3 - последние 8 байт ключа

* ```Digest::FarmHashFingerprint(source:Uint64{Flags:AutoMap}) -> Uint64```
* ```Digest::FarmHashFingerprint2(low:Uint64{Flags:AutoMap}, high:Uint64{Flags:AutoMap}) -> Uint64```

FarmHash функция для 128 битного числа. 128 битное число получается путем объединения битов из двух uit64 чисел.

* ```Digest::FarmHashFingerprint32(string:String{Flags:AutoMap}) -> Uint32```
* ```Digest::FarmHashFingerprint64(string:String{Flags:AutoMap}) -> Uint64```
* ```Digest::FarmHashFingerprint128(string:String{Flags:AutoMap}) -> Tuple<Uint64,Uint64>```

FarmHash функция для байтовой строки с результатом типа uint128. Результат представлен как пара из двух uint64 чисел <low, high>

```sql
select Digest::FarmHashFingerprint2(31880,6990065); -- 237693065644851126
select Digest::FarmHashFingerprint128("Who set this ancient quarrel new abroach?"); -- (17165761740714960035, 5559728965407786337)
```

* ```Digest::SuperFastHash(String{Flags:AutoMap}) -> Uint32```
* ```Digest::Sha1(String{Flags:AutoMap}) -> String```
* ```Digest::Sha256(String{Flags:AutoMap}) -> String```
* ```Digest::IntHash64(Uint64{Flags:AutoMap}) -> Uint64```

* ```Digest::XXH3(String{Flags:AutoMap}) -> Uint64```
* ```Digest::XXH3_128(String{Flags:AutoMap}) -> Tuple<Uint64,Uint64>```

XXH3 - некриптографическая хеш-функция из семейста xxxHash. XXH3_128 генерит 128 битный хеш, результат представлен как пара из двух uint64 чисел <low, high>
```sql
select Digest::XXH3_128("Who set this ancient quarrel new abroach?"); -- (17117571879768798812, 14282600258804776266)
```
