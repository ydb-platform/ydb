## Установка

Ubuntu
```
sudo apt install spin gcc
```

MacOS
```
brew install spin gcc
```

## Запуск

Поиск ошибок в инварианте
```
spin -a queue_slow.pml && gcc -O2 -w -o pan pan.c
./pan -a -I -m<max search depth> -N <name of invariant>
```

Если инвариант только один, его можно не указывать

Пример инварианта в коде модели:
```
ltl no_overflow {
    [] ((queue.tail - queue.head <= QUEUE_SIZE + CONSUMERS) && 
        (queue.tail <= ELEMENTS + lagged_pops) && 
        (queue.head <= ELEMENTS + lagged_pops) &&
        (lagged_pops <= (ELEMENTS/ (QUEUE_SIZE + CONSUMERS - 1)) * (CONSUMERS - 1)))
}
```

Поиск циклов
```
spin -a queue_slow.pml -DWITH_BLOCKING && gcc -O2 -w -o pan pan.c -DNP
./pan -l -I -m<max search depth>
```

Если нашлась ошибка, то появился файл queue_slow.pml.trail
Проиграть путь с ошибкой можно так:
```
./pan -r
```

Либо если нужны только printf
```
./pan -r -S
```
