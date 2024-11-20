/* syntax version 1 */

/*
message Test {
  enum Letters {
    A = 0;
    B = 1;
    Z = 25;
  }
  message Inner {
    repeated Letters alphabet = 1;
    Letters l = 2;
  }
  Inner inner = 1;
}
*/

$config = @@{"name":"Test","view":{"enum":"full_name"},"syntax":{"aware":true},"lists":{"optional":false},"format":"json","meta":"H4sIAAAAAAAAA+PayMgll1Wal61fppubmG1sXKZfUJRfkq9fklpcogdmKrUxcrGEALlCUlysmXl5qUUSjAqMGtxG3HogUT1PkJCUExcrmCEkz8WRmFOQkZiUWgJUx6zBZ8QLUeeTWlKSWlQsJMHFmCPBBDQBXUZJjosdpoiVi9FRgAFEOQkwgqgoAckkNrB7jAFNZK4ztAAAAA=="}@@;

$udf = Udf(Protobuf::Parse, $config as TypeConfig);

SELECT $udf(TestField) AS Profile
FROM plato.Input;

