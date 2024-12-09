/* syntax version 1 */

/*
syntax='proto3';

message Test {
  message InnerInner {
    string a = 1;
  }
  message Inner {
    InnerInner i = 1;
  }
  Inner inner = 1;
}
*/

$config = @@{"name":"Test","syntax":{"aware":true},"lists":{"optional":false},"format":"json","meta":"H4sIAAAAAAAAA+PqZuSSyyrNy9Yv081NzDY2LtMvKMovydcvSS0u0QMzlRK5WEKAPCEpLtbMvLzUIglGBUYNbiNuPZConidISEqciwvMABNCnFyMiWBVnFIqXKwQMWkuxkyoTgEknWAiiQ1skzEAZSMFuY4AAAA="}@@;

$udf = Udf(Protobuf::Parse, $config as TypeConfig);

SELECT $udf(TestField) AS Profile
FROM plato.Input;

