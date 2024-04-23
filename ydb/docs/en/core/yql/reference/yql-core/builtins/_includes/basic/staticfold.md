## StaticFold, StaticFold1 {#staticfold}

```
StaticFold(obj:Struct/Tuple, initVal, updateLambda)
StaticFold1(obj:Struct/Tuple, initLambda, updateLambda)
```

Fold over struct/tuple elements.

- `obj` - object to fold
- `initVal` - _(StaticFold)_ initial fold state
- `initLambda` - _(StaticFold1)_ lambda that produces initial fold state by first element
- `updateLambda` - lambda that produces the new state (arguments are the next element and the previous state)


`StaticFold(<|key_1:$el_1, key_2:$el_2, ..., key_n:$el_n|>, $init, $f)` transforms into:
```yql
$f($el_n, ...$f($el_2, $f($init, el_1))...)
```
`StaticFold1(<|key_1:$el_1, key_2:$el_2, ..., key_n:$el_n|>, $f0, $f)`:
```yql
$f($el_n, ...$f($el_2, $f($f0($init), el_1))...)
```
`StaticFold1(<||>, $f0, $f)` is Null

Works with tuples in the same way.