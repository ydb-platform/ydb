# Actor Model Assignment

Вам необходимо реализовать программу которая из stdin получает на вход числа и вычисляет сумму наибольших простых делителей этих чисел. Для этого нужно будет создать три актора:
* TReadActor - считывает значения из stdin и передает их TMaximumPrimeDevisorActor актору
* TMaximumPrimeDevisorActor - занимается подсчетом наибольших простых делителей
* TWriteActor - считает сумму посчитанных наибольших простых делителей и печатает это число в stdout

## Процедура сдачи
1. Вам нужно сделать fork этого репозитория (upstream)
2. После этого разрабатываетесь в удобной для вас ветке
3. Когда решение готово, то вы создаете merge request из своего репозитория в репозиторий из которого вы cделали fork (upstream)

## Основные файлы
* main.cpp - в этом файле вы зарегистрируете ваши акторы и раскоментируете код для ожидания их выполнения
* actors.cpp/actors.h - содержат подробное описания поведения акторов TReadActor/TMaximumPrimeDevisorActor/TWriteActor
* events.h - сюда добавляете все необходимые события которые будут передаваться между акторами

## Пример сборки

1. Необходимые зависимости можно найти здесь https://github.com/ydb-platform/ydb/blob/main/BUILD.md#how-to-build
2. Далее запускаете cmake:
```(bash)
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain ../ydb
```
3. После этого можете компилировать и запускать ваш код с помощью команд:
```(bash)
ninja tools/actor_model/all
echo '1' | ./tools/actor_model/actor_model
```

## Примеры запуска

```(bash)
|> echo '1' | ./tools/actor_model/actor_model
1
```

```(bash)
|> echo '1 2 3' | ./executor
6
```

```(bash)
|> echo '1 2 3 4' | ./executor
8
```

```(bash)
|> echo '' | ./executor
0
```

