# orjson --- обновление сборки в arc

Предполагается, что у вас корень аркадии находится в `~/arcadia`, кроме того установлен [rustup](https://rustup.rs/) и 
необходимый rust-toolchain.

Одна из сложностей обновления в том, что нужно собрать бинарь liborjson.a, в котором нужно переименовать все символы,
чтобы не было дублей. Для этого используется инструмент [ym2](https://docs.yandex-team.ru/contrib-automation/templates),
исходники которого лежат [здесь](https://a.yandex-team.ru/arcadia/devtools/yamaker/libym2). Впрочем, это --- справочная
информация.

## Обновляем контриб:

(укажите нужную версию [отсюда](https://github.com/ijl/orjson/releases), помните про двухнедельный карантин)

```bash
cd ~/arcadia/contrib/python/orjson
ya tool yamaker pypi --contrib-path ~/arcadia/contrib/python/orjson -v 3.10.12 
```

Теперь ровно ту же версию (например, 3.10.12) нужно прописать в файле `/.yandex_meta/build.ym` в строчке

```
{% block current_version %}3.10.12{% endblock %}
```

Теперь нужно запустить кросс-сборку бинарей:

```bash
ya tool yamaker ym2 reimport -t ~/arcadia/contrib/python/orjson/.yandex_meta/build.ym
```

Если повезёт, то всё отработает правильно :)


## Ну, теперь осталось залить в аркадию

Свою версию только используйте :) 
Ну или тикет добавьте.

```bash
arc checkout -b bump-orjson-to-v-3-10-12
arc add ~/arcadia/contrib/python/orjson
arc add ~/arcadia/contrib/python/orjson/a -f
arc commit -m "bump-orjson-to-v-3-10-12"
```

Теперь хорошо бы провести проверку на реимпорт:

```bash
ya tool yamaker pypi --contrib-path ~/arcadia/contrib/python/orjson -v 3.10.12 
arc status
```

Изменений быть не должно. Если их нет, то поехали в арк:

```bash
arc status
arc pr create -m "bump-orjson-to-v-3-10-12"
```

## Если не повезло, то билд в аркадии быстро упадёт с ошибкой вида 
`ld.lld: error: undefined symbol: PyInit_6orjson6orjson`
Ну что же, вы неудачник :)
Сначала посмотрите, что экспортируется во всех ваших бинарях:

```bash
llvm-nm --extern-only --defined-only -A a/aarch64-apple-darwin/release/liborjson.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
llvm-nm --extern-only --defined-only -A a/x86_64-apple-darwin/release/liborjson.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
llvm-nm --extern-only --defined-only -A a/aarch64-unknown-linux-gnu/release/liborjson.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
llvm-nm --extern-only --defined-only -A a/x86_64-unknown-linux-musl/release/liborjson.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
llvm-nm --extern-only --defined-only -A a/x86_64-unknown-linux-gnu/release/liborjson.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
```

Если где-то нет того символа, на нехватку которого есть жалоба, то ищите `redefine-sym` в `/.yandex_meta/build.ym`
