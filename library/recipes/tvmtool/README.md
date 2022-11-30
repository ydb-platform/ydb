tvmtool recipe
---

Этот рецепт позволяет в тестах поднять [tvmtool](https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/), который в проде разворачивается на localhost.

Демон слушает на порте из файла `tvmtool.port`. Для запросов к нему следует использовать AUTHTOKEN из `tvmtool.authtoken`. См. [пример](https://a.yandex-team.ru/arc/trunk/arcadia/library/recipes/tvmtool/ut/test.py).

Варианты подключения:
 1) `recipe_with_default_cfg.inc` - для запуска демона с дефолтным [конфигом](https://a.yandex-team.ru/arc/trunk/arcadia/library/recipes/tvmtool/tvmtool.default.conf). Например:
    ```
    INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmtool/recipe_with_default_cfg.inc)
    ```
    [Пример](https://a.yandex-team.ru/arc_vcs/library/recipes/tvmtool/examples/ut_simple)
2) `recipe.inc` - для запуска демона со своим конфигом. Например
    ```
    INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmtool/recipe.inc)

    USE_RECIPE(
        library/recipes/tvmtool/tvmtool
        foo/tvmtool.conf
    )
    ```
    [Пример](https://a.yandex-team.ru/arc_vcs/library/recipes/tvmtool/examples/ut_with_custom_config)
3) `recipe.inc` + `--with-roles-dir` - запуск со своим конфигом и с поддержкой ролей
    ```
    INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmtool/recipe.inc)

    USE_RECIPE(
        library/recipes/tvmtool/tvmtool
        foo/tvmtool.conf
        --with_roles_dir foo/roles
    )
    ```
    В каталоге `foo/` ожидается наличие файлов с именами вида `{slug}.json` - для всех slug из tvmtool.conf.
    [Пример](https://a.yandex-team.ru/arc_vcs/library/recipes/tvmtool/examples/ut_with_roles)
4) `recipe.inc` + `--with-tvmapi` - для запуска демона, который будет ходить в tvm-api (тоже рецепт). Например:
    ```
    # start tvm-api
    INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmapi/recipe.inc)

    # start tvmtool
    INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmtool/recipe.inc)
    USE_RECIPE(
        library/recipes/tvmtool/tvmtool
        foo/tvmtool.conf
        --with-tvmapi
    )
    ```
    [Пример](https://a.yandex-team.ru/arc_vcs/library/recipes/tvmtool/examples/ut_with_tvmapi)
5) `recipe.inc` + `--with-tvmapi` + `--with-tirole` - для запуска демона, который будет ходить в tvm-api и tirole (тоже рецепты). Например:
    ```
    # start tvm-api
    INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmapi/recipe.inc)

    # start tirole
    INCLUDE(${ARCADIA_ROOT}/library/recipes/tirole/recipe.inc)
    USE_RECIPE(
        library/recipes/tirole/tirole
        --roles-dir library/recipes/tirole/ut_simple/roles_dir
    )

    # start tvmtool
    INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmtool/recipe.inc)
    USE_RECIPE(
        library/recipes/tvmtool/tvmtool
        foo/tvmtool.conf
        --with-tvmapi
        --with-tirole
    )
    ```
    [Пример](https://a.yandex-team.ru/arc_vcs/library/recipes/tvmtool/examples/ut_with_tvmapi_and_tirole)

Варианты 1, 2 и 3 запустят tvmtool с флагом `--unittest`. Это значит, что:
  * в конфиге можно указывать какие угодно tvm_id
  * в конфиге секрет может быть пустым или равен строке "fake_secret"

Вариант 4 и 5 запустит tvmtool, который будет ходить в tvm-api. Это значит, он сможет работать только с теми приложениями и их секретами, которые есть в [базе](https://a.yandex-team.ru/arc/trunk/arcadia/library/recipes/tvmapi/clients/clients.json) tvm-api. В этом варианте можно получать ServiceTicket в tvm-api и проверять в tvmtool.

Любой из этих вариантов позволяет проверять ServiceTicket'ы/UserTicket'ы, сгенерированные через `tvmknife unittest`.

Вопросы можно писать в [PASSPORTDUTY](https://st.yandex-team.ru/createTicket?queue=PASSPORTDUTY&_form=77618)
