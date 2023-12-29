# Сборка и тестирование YDB с использованием Ya Make

**Ya Make** - это система сборки и тестирования, исторически используемая для разработки YDB. Изначально разработанная для C++, теперь она поддерживает ряд языков программирования, включая Java, Go и Python.

Язык конфигурации сборки **Ya Make** является основным для YDB, с файлом `ya.make` в каждом каталоге, представляющем таргеты Ya Make.

Настройте среду разработки, как описано в статье [Работа над изменениями - Настройка окружения](suggest-change.md#envsetup) для работы с `Ya Make`.

## Выполнение ваших команд {#run_ya}

В корневом каталоге репозитория YDB расположен скрипт `ya` для запуска команд `Ya Make` из командной строки. Его можно добавить в переменную окружения PATH, чтобы он запускался без указания полного пути. Для Linux/Bash и репозитория GitHub, склонированного в `~/ydbwork/ydb`, можно использовать следующую команду:

```
echo "alias ya='~/ydbwork/ydb/ya'" >> ~/.bashrc
source ~/.bashrc
```

Запуск `ya` без параметров выводит справку:

```
$ ya
Yet another build tool.

Usage: ya [--precise] [--profile] [--error-file ERROR_FILE] [--keep-tmp] [--no-logs] [--no-report] [--no-tmp-dir] [--print-path] [--version] [-v] [--diag] [--help] <SUBCOMMAND> [OPTION]...

Options:
...

Available subcommands:
...
```

Подробная справка по любой подкоманде может быть получена её запуском с флагом `--help`, например:

```
$ ya make --help
Build and run tests
To see more help use -hh/-hhh

Usage:
  ya make [OPTION]... [TARGET]...

Examples:
  ya make -r               Build current directory in release mode
  ya make -t -j16 library  Build and test library with 16 threads
  ya make --checkout -j0   Checkout absent directories without build

Options:
...
```

Скрипт `ya` загружает при запуске необходимые артефакты, зависящие от платформы, и кэширует их локально. Периодически скрипт обновляется ссылками на новые версии артефактов.

## Настройка IDE {#ide}

При использовании IDE для разработки доступна команда `ya ide`, которая помогает создать проект с преднастроенными инструментами. Поддерживаются следующие IDE: goland, idea, pycharm, venv, vscode (multilanguage, clangd, go, py).

Перейдите в каталог в исходном коде, который должен быть корневым для вашего проекта. Запустите команду `ya ide`, указав имя IDE, и целевой каталог для записи конфигурации проекта IDE в параметре `-P`. Например, для работы с изменениями библиотеки YQL в vscode вы можете запустить следующую команду:

```
cd ~/ydbwork/ydb/библиотека/yql
ya ide vscode -P=~/ydbwork/vscode/yqllib
```

Теперь можно открыть `~/ydbwork/vscode/yqllib/ide.code-workspace` из vscode.

## Сборка таргета {#make}

В `Ya Make` есть 3 основных типа таргетов: программа, тест и библиотека. Чтобы собрать таргет, запустите `ya make` с именем каталога. Например, для сборки YDB CLI запустите:

```
cd ~/ydbwork/ydb
ya make ydb/apps/ydb
```

Вы также можете запустить `ya make` из каталога таргета без параметров:

```
cd ~/ydbwork/ydb/ydb/apps/ydb
ya make
```

## Запуск тестов {#test}

Запуск команды `ya test` в каком-либо каталоге создаст все бинарные файлы тестов, расположенные внутри его подкаталогов, и запустит тесты.

Например, для запуска small тестов YDB Core запустите:

```
cd ~/ydbwork/ydb
ya test ydb/core
```

Для запуска medium и large тестов добавьте опции `-tt` и `-ttt` к вызову `ya test` соответственно.