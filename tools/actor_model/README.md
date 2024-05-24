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
В проекте есть докерфайл, с помощью которого можно осуществлять сборку. Протестирован на windows и macOS (работает только на процессорах intell).

1. Клоним текущий репозиторий  
```(bash)
git clone -u ydb_assignment
```  
2. Переходим в него и собираем контейнер
```(bash)
sudo docker build -t actor_model:latest .
``` 
3. Далее запускаем контейнер(не меняйте пути, там будет папка для билда). При запуске в контейнере прогоняется прогоняется cmake и ninja, поэтому собираться будет долго. Лучше сначала запустить контейнер а потом редактировать код.
```(bash)
sudo docker run -v $(pwd):/home/ydbwork/ydb --rm -it actor_model
``` 
4. После запуска контейнера вы будете в папку build, от туда можно компилировать и запускать ваш код с помощью команд:
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

## Если планируете запускать проект несколько раз, а каждый раз ждать не хотите

По сути самая долгая часть в запуске контейнера это запуск cmake и проверки от ninja. Если сохранить лоакльно папку build, то далее запуск контейнера будет сильно быстрее, но понадобятся дополнительные манипуляции локально.

1. Создадим папку ydbwork и в ней папку build, после чего перейдем в нее
```(bash)
mkdir -p ydbwork
cd ydbwork
```
2. Склоним проект, ветка ydb_assignment
```(bash)
git clone -b ydb_assignment <путь к вашему форку>
```
3. Далее в репозитории ydb изменим Dockerfile, закоментим соедующие строчки (должно быть так):
```(bash)
#RUN mkdir -p /home/ydbwork/build

#CMD cd /home/ydbwork/build && cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain ../ydb && ninja tools/actor_model/all && bash

```
4. Оставаясь в директории ydb соберем контейнер
```(bash)
cd ydb
sudo docker build -t actor_model:latest .
```
5. Вернемся в папку ydbwork и запустим контейнер
```
sudo docker run -v $(pwd):/home/ydbwork --rm -it actor_model
```
6. Внтури контейнера перейдем в папку /home/ydbwork/build, внутри нее прогоним cmake и ninja
```
cd /home/build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain ../ydb
ninja tools/actor_model/all
```
Все, далее запуская контейнер из папки ydbwork можно сразу переходить в папку home/ydbwork/build внутри контейнера и компилировать код через ninja, первая проверка тоже займет время но сильно меньше чем cmake+ninja при инициализации.