# pydantic-core --- обновление сборки в arc

Предполагается, что у вас корень аркадии находится в `~/arcadia`, кроме того установлен [rustup](https://rustup.rs/) и 
необходимый rust-toolchain.

Одна из сложностей обновления в том, что нужно собрать бинарь lib_pydantic_core.a, в котором нужно переименовать все символы,
чтобы не было дублей. Для этого используется инструмент [ym2](https://docs.yandex-team.ru/contrib-automation/templates),
исходники которого лежат [здесь](https://a.yandex-team.ru/arcadia/devtools/yamaker/libym2). Впрочем, это --- справочная
информация.

## Обновляем контриб:

(укажите нужную версию [отсюда](https://github.com/pydantic/pydantic-core/releases), помните про двухнедельный карантин)

```bash
cd ~/arcadia/contrib/python/pydantic-core
ya tool yamaker pypi --contrib-path ~/arcadia/contrib/python/pydantic-core -v 2.16.3 
```

Теперь ровно ту же версию (например, 2.16.3) нужно прописать в файле `/.yandex_meta/build.ym` в строчке

```
{% block current_version %}2.16.3{% endblock %}
```

Теперь нужно запустить кросс-сборку бинарей:

```bash
ya tool yamaker ym2 reimport -t ~/arcadia/contrib/python/pydantic-core/.yandex_meta/build.ym
```

Если повезёт, то всё отработает правильно :)


## Ну, теперь осталось залить в аркадию

Свою версию только используйте :) 
Ну или тикет добавьте.

```bash
arc checkout -b bump-pydantic-core-to-v-2.16.3
arc add ~/arcadia/contrib/python/pydantic-core
arc add ~/arcadia/contrib/python/pydantic-core/a -f
arc commit -m "bump-pydantic-core-to-v-2.16.3"
```

Теперь хорошо бы провести проверку на реимпорт:

```bash
ya tool yamaker pypi --contrib-path ~/arcadia/contrib/python/pydantic-core -v 2.16.3 
arc status
```

Изменений быть не должно. Если их нет, то поехали в арк:

```bash
arc status
arc pr create -m "bump-pydantic-core-to-v-2.16.3"
```

## Если не повезло, то билд в аркадии быстро упадёт с ошибкой вида 
`ld.lld: error: undefined symbol: PyInit_13pydantic_core14_pydantic_core`
Ну что же, вы неудачник :)
Сначала посмотрите, что экспортируется во всех ваших бинарях:

```bash
llvm-nm --extern-only --defined-only -A a/aarch64-apple-darwin/release/lib_pydantic_core.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
llvm-nm --extern-only --defined-only -A a/x86_64-apple-darwin/release/lib_pydantic_core.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
llvm-nm --extern-only --defined-only -A a/aarch64-unknown-linux-gnu/release/lib_pydantic_core.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
llvm-nm --extern-only --defined-only -A a/x86_64-unknown-linux-musl/release/lib_pydantic_core.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
llvm-nm --extern-only --defined-only -A a/x86_64-unknown-linux-gnu/release/lib_pydantic_core.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
```

Если где-то нет того символа, на нехватку которого есть жалоба, то ищите `redefine-sym` в `/.yandex_meta/build.ym`


## Обновление этой инструкции

Если хочется обновить эту инструкцию, это нужно просто отредактировать этот файл
