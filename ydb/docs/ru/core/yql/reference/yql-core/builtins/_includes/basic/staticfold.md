## StaticFold, StaticFold1 {#staticfold}

```
StaticFold(obj:Struct/Tuple, initVal, updateLambda)
StaticFold1(obj:Struct/Tuple, initLambda, updateLambda)
```

Статическая левоассоциативная свертка структуры или кортежа.
Для кортежей свертка производится в порядке от меньшего индекса к большему, для структур порядок не гарантируется.

- `obj` - объект, элементы которого нужно свернуть
- `initVal` - _(для StaticFold)_ исходное состояние свертки
- `initLambda` - _(для StaticFold1)_ функция для получения исходного состояния по первому элементу
- `updateLambda` - функция обновления состояния (принимает в аргументах следующий элемент объекта и предыдущее состояние)


`StaticFold(<|key_1:$el_1, key_2:$el_2, ..., key_n:$el_n|>, $init, $f)` преобразуется в свертку:
```yql
$f($el_n, ...$f($el_2, $f($init, el_1))...)
```
`StaticFold1(<|key_1:$el_1, key_2:$el_2, ..., key_n:$el_n|>, $f0, $f)`:
```yql
$f($el_n, ...$f($el_2, $f($f0($init), el_1))...)
```

`StaticFold1(<||>, $f0, $f)` вернет `NULL`.

Аналогично работает и с кортежами.
