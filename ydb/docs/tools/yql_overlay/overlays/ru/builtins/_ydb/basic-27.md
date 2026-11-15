Создает экзмепляр гомогенного варианта (т.е. содержащего поля/элементы одного типа), причем индекс или поле варианта можно задавать динамически. При несуществующем индексе или имени поля будет возвращен `NULL`.
Обратная функция - [VariantItem](#variantitem).

### Сигнатура

```yql
DynamicVariant(item:T,index:Uint32?,Variant<T, T, ...>)->Optional<Variant<T, T, ...>>
DynamicVariant(item:T,index:Utf8?,Variant<key1: T, key2: T, ...>)->Optional<Variant<key1: T, key2: T, ...>>
```

### Пример
