/* syntax version 1 */

$config = @@{"name":"Test","syntax":{"aware":true},"lists":{"optional":false},"format":"prototext","meta":"H4sIAAAAAAAAA+NK5FLOKs3L1i/TzU3MNjYu0y8oyi/Jjy9JLS7RBxF6YL6SERdLCJAnxMnFmCjBqMCowQpiJkkwAZnMIGayBDOQyQlipkiwKDBrsCaxgbUaAwBc3r8mYwAAAA=="}@@;

$udf = Udf(Protobuf::Parse, $config as TypeConfig);
$udf2 = Udf(Protobuf::Serialize, $config as TypeConfig);

SELECT $udf($udf2($udf(TestField))) AS Profile
FROM plato.Input;

