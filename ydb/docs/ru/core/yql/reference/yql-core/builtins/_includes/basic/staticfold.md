## StaticFold, StaticFold1 {#staticfold}

```
StaticFold(obj:Struct/Tuple, initVal, updateLambda)
StaticFold1(obj:Struct/Tuple, initLambda, updateLambda)
```

Статическая свертка структуры или кортежа.

- `obj` - объект, элементы которого нужно свернуть
- `initVal` - _(для StaticFold)_ исходное состояние свертки
- `initLambda` - _(для StaticFold1)_ функция для получения исходного состояния по первому элементу
- `updateLambda` - функция обновления состояния (принимает в аргументах предыдущее состояние и следующий элемент объекта)


`StaticFold(<|key_1:$el_1, key_2:$el_2, ..., key_n:$el_n|>, $init, $f)` преобразуется в свертку:
```yql
$f(...$f($f($init, el_1), $el_2)..., $el_n)

```
`StaticFold1(<|key_1:$el_1, key_2:$el_2, ..., key_n:$el_n|>, $f0, $f)`:
```yql
$f(...$f($f0(el_1), $el_2)..., $el_n)

```
`StaticFold1(<||>, $f0, $f)` вернет Null

Аналогично работает и с кортежами.
