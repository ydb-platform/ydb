# Goals and objectives

Execution of arbitrary scenarios on an arbitrary cluster
Compiling reports on launch results
Generating a multi-profile load on a cluster
Identifying errors and problems

# Technologies

* Regular task launch system
   `Arcadia Ci + Sandbox`
* Environment for writing tests
   `Pytest + ya make`
* Tool for creating visual reports
   `Allure + ya make`
* Data storage and presentation system
   `YDB+DataLens`

# Launches

Runs on a regular basis in [Arcadia CI](https://a.yandex-team.ru/projects/kikimr/ci/actions/launches?dir=kikimr%2Ftests%2Facceptance%2Folap%2Fyatests&id=run_scenario_tests_auto).
Description of launches in [a.yaml](https://a.yandex-team.ru/arcadia/kikimr/tests/acceptance/olap/yatests/a.yaml?rev=r13836177#L102).

# Scenarios writing

## General approach

The code is in [Github](https://github.com/ydb-platform/ydb/tree/main/ydb/tests/olap/scenario)
Scenarios are written in `Python 3` in files whose names begin with the `test_` prefix. The files need to be registered in [ya.make](https://github.com/ydb-platform/ydb/tree/main/ydb/tests/olap/scenario/ya.make) in the `TEST_SRCS` section.

Classes with names starting with `Test` are accepted as **sets of scenarios**.

Scenarios are considered to be member functions of scenario classes whose names begin with `scenario_`.

In a scenario set class, you can define `setup_class` and `teardown_class` methods that will be executed before and after the scenario set execution, respectively.

To write scenarios, use [Helpers](./scenario.helpers.md)

{% cut "Sample scenario" %}

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

