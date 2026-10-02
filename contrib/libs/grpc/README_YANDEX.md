# Ниже описанны доработки, привнесённые в патчах от Яндекса.
## Поддержка zstd
В отличии от других алгоритмов сжатия, он не включён по умолчанию. Для использования этого алгоритма пользователь должен явно включить его поддержку как на клиентской, так и на серверной стороне.
Включение алгоритма на клиентской стороне:
```cpp
auto channelArgs = grpc::ChannelArguments();
// Говорим что хотим отправлять сообщения, сжатые zstd.
channelArgs.SetCompressionAlgorithm(grpc_compression_algorithm::GRPC_COMPRESS_ZSTD);
auto channel = grpc::CreateCustomChannel(target, crets, channelArgs);
// ...
```
ВАЖНО:
Даже, если алгоритм сжатия отключён на клиенте (через `channelArgs.SetInt(GRPC_COMPRESSION_CHANNEL_ENABLED_ALGORITHMS_BITSET,...)`) он всё равно может принимать ответы с ним
(это относится не только к zstd но и к gzip). В случае такого отключения логах клиента будут видны сообщения вида
```
Cancel error=UNIMPLEMENTED: Compression algorithm 'gzip' is disabled. [type.googleapis.com/grpc.status.int.grpc_status='12']
```
однако ответы программа продолжает получать.

Включение алгоритма на серверной стороне:
```cpp
ServerBuilder builder;
// Говорим, что готовы принимать сжатые zstd сообщения от клиентов (иначе клиенты будут получать ошибку).
builder.SetCompressionAlgorithmSupportStatus(grpc_compression_algorithm::GRPC_COMPRESS_ZSTD, true);
// Говорим, что хотим отправлять сжатые zstd ответы.
builder.SetDefaultCompressionAlgorithm(grpc_compression_algorithm::GRPC_COMPRESS_ZSTD);

// ...
std::unique_ptr<Server> server(builder.BuildAndStart());
```

## Регулирование степени сжатия deflate
- Регулируется переменной окружения `GRPC_ZLEVEL`.
- Значение переменной считывается каждый раз заново при сжатии сообщения.
- Если переменная есть, в ней должно быть строковое представление целого числа - уровня сжатия (подробнее см. документацию параметра `level` в библиотеке zlib).
- Если переменная не выставлена, то поведение не меняется (используется уровень сжатия `Z_DEFAULT_COMPRESSION`).
