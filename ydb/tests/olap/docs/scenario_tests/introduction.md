# Цели и задачи

Исполнение на произвольном кластере произвольных сценариев
Составление отчетов о результатах запусков
Генерация разнопрофильной нагрузки на кластер
Выявление ошибок и проблем

# Технологии

* Система регулярного запуска задач
  `Arcadia Ci + Sandbox`
* Среда для написания тестов
  `Pytest + ya make`
* Инструмент для построения визуальных отчетов
  `Allure + ya make`
* Система хранения и представления данных
  `YDB + DataLens`

# Запуски

Запускается на регулярной основе в [Arcadia CI](https://a.yandex-team.ru/projects/kikimr/ci/actions/launches?dir=kikimr%2Ftests%2Facceptance%2Folap%2Fyatests&id=run_scenario_tests_auto).
Описание запусков в [a.yaml](https://a.yandex-team.ru/arcadia/kikimr/tests/acceptance/olap/yatests/a.yaml?rev=r13836177#L102).

# Написание сценариев

## Общий подход

Код лежит в [Аркадии](https://a.yandex-team.ru/arcadia/kikimr/tests/acceptance/olap/yatests/scenario)
Сценарии пишутся на языке `Python 3` в файлах, имена которых начинаются с префикса `test_`. Файлы нужно прописать в [ya.make](https://a.yandex-team.ru/arcadia/kikimr/tests/acceptance/olap/yatests/scenario/ya.make) в секции `TEST_SRCS`.

В качестве **наборов сценариев** принимаются классы с именами, начинающимися с `Test`.

В качестве сценариев воспринимаются функции-члены классов-наборов сценариев, имена которых начинаются на `scenario_`.

В классе-наборе сценариев можно определить методы `setup_class` и `teardown_class`, которые будут выполняться соответственно перед и после исполнения набора сценариев.

Для написания сценариев используются [Хелперы](./scenario.helpers.md)

{% cut "Пример сценария" %}

```python
class TestSchenarioSimple(BaseTestSet):
    @classmethod
    def setup_class(cls):
        sth.drop_if_exist(['testTable'], DropTable)

    @classmethod
    def teardown_class(cls):
        sth.drop_if_exist(['testTable'], DropTable)

    schema1 = (
        sth.Schema()
        .with_column(name='id', type=PrimitiveType.Int32, not_null=True)
        .with_column(name='level', type=PrimitiveType.Uint32)
        .with_key_columns('id')
    )

    def scenario_table(self):
        table_name = 'testTable'
        sth.execute_scheme_query(CreateTable(table_name).with_schema(self.schema1))
        sth.bulk_upsert_data(
            table_name,
            self.schema1,
            [
                {'id': 1, 'level': 3},
                {'id': 2, 'level': None},
            ],
            comment="with ok scheme",
        )
        assert sth.get_table_rows_count(table_name) == 2
        sth.bulk_upsert(table_name, dg.DataGeneratorPerColumn(self.schema1, 100), comment="100 sequetial ids")
        sth.execute_scheme_query(DropTable(table_name))
```

{% endcut %}

