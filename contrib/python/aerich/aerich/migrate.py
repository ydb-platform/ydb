from __future__ import annotations

import contextlib
import functools
import importlib
import inspect
import pkgutil
import re
from collections.abc import Awaitable, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Literal, cast, overload

import asyncclick as click
from dictdiffer import diff
from tortoise import BaseDBAsyncClient, Model, Tortoise
from tortoise.exceptions import OperationalError
from tortoise.indexes import Index

from aerich._compat import tortoise_version_less_than
from aerich.coder import load_index
from aerich.ddl import BaseDDL
from aerich.enums import Color
from aerich.models import MAX_VERSION_LENGTH, Aerich
from aerich.utils import (
    decompress_dict,
    get_app_connection,
    get_dict_diff_by_key,
    get_formatted_compressed_data,
    get_models_describe,
    import_py_file,
    import_py_module,
    is_default_function,
    py_module_path,
    run_async,
)

MIGRATE_TEMPLATE = """from tortoise import BaseDBAsyncClient

RUN_IN_TRANSACTION = True


async def upgrade(db: BaseDBAsyncClient) -> str:
    return \"\"\"
        {upgrade_sql}\"\"\"


async def downgrade(db: BaseDBAsyncClient) -> str:
    return \"\"\"
        {downgrade_sql}\"\"\"


MODELS_STATE = (
    {models_state}
)
"""


@dataclass
class MigrationFile:
    upgrade: Callable[[BaseDBAsyncClient], Awaitable[str]]
    downgrade: Callable[[BaseDBAsyncClient], Awaitable[str]]
    models_state: dict[str, Any] | None  # e.g.: aerich.utils.get_models_describe(app='models')


class Migrate:
    upgrade_operators: list[str] = []
    downgrade_operators: list[str] = []
    _upgrade_fk_m2m_index_operators: list[str] = []
    _downgrade_fk_m2m_index_operators: list[str] = []
    _upgrade_m2m: list[str] = []
    _downgrade_m2m: list[str] = []
    _aerich = Aerich.__name__
    _rename_fields: dict[str, dict[str, str]] = {}  # {'model': {'old_field': 'new_field'}}

    ddl: BaseDDL
    ddl_class: type[BaseDDL]
    _last_version_content: dict | None = None
    app: str
    migrate_location: Path
    dialect: str
    _db_version: str | None = None

    @staticmethod
    def get_field_by_name(name: str, fields: list[dict]) -> dict:
        return next(filter(lambda x: x.get("name") == name, fields))

    @classmethod
    def get_all_version_modules(cls) -> list[pkgutil.ModuleInfo]:
        def get_file_version(module: pkgutil.ModuleInfo) -> str:
            return module.name.split("_")[0]

        def is_version_file(module: pkgutil.ModuleInfo) -> bool:
            if "_" not in module.name:
                return False
            return get_file_version(module).isdigit()

        files = filter(is_version_file, pkgutil.iter_modules([str(cls.migrate_location)]))
        return sorted(files, key=lambda x: int(get_file_version(x)))

    @classmethod
    def get_all_version_files(cls) -> list[str]:
        return [m.name + ".py" for m in cls.get_all_version_modules()]

    @classmethod
    def _get_model(cls, model: str) -> type[Model]:
        return Tortoise.apps[cls.app].get(model)  # type: ignore

    @classmethod
    async def get_last_version(cls) -> Aerich | None:
        try:
            return await Aerich.filter(app=cls.app).first()
        except OperationalError:
            return None

    @classmethod
    def get_last_version_file(cls) -> str | None:
        if m := cls.get_last_version_module():
            return m.name
        return None

    @classmethod
    def get_last_version_module(cls) -> pkgutil.ModuleInfo | None:
        if ms := cls.get_all_version_modules():
            return ms[-1]
        return None

    @classmethod
    def get_migration_info_for_file(cls, file_path: str | pkgutil.ModuleInfo) -> MigrationFile:
        module = (
            import_py_module(file_path)
            if isinstance(file_path, pkgutil.ModuleInfo)
            else import_py_file(cls.migrate_location / file_path)
        )
        model_state_str = getattr(module, "MODELS_STATE", None)

        models_state = decompress_dict(model_state_str) if model_state_str else None

        return MigrationFile(
            upgrade=module.upgrade, downgrade=module.downgrade, models_state=models_state
        )

    @classmethod
    async def _get_db_version(cls, connection: BaseDBAsyncClient) -> None:
        if cls.dialect == "mysql":
            sql = "select version() as version"
            ret = await connection.execute_query(sql)
            cls._db_version = ret[1][0].get("version")

    @classmethod
    async def load_ddl_class(cls) -> type[BaseDDL]:
        ddl_dialect_module = importlib.import_module(f"aerich.ddl.{cls.dialect}")
        return getattr(ddl_dialect_module, f"{cls.dialect.capitalize()}DDL")

    @classmethod
    async def init(cls, config: dict, app: str, location: str, offline: bool = False) -> None:
        if not Tortoise._inited:
            # TODO: init tortoise without create db connection for offline mode
            await Tortoise.init(config=config)
        cls.app = app
        cls.migrate_location = Path(location, app)
        if last_version_module := cls.get_last_version_module():
            try:
                last_version_info = cls.get_migration_info_for_file(last_version_module)
            except AttributeError:
                # Skip invalid migration file
                pass
            else:
                if not last_version_info.models_state:
                    raise RuntimeError(
                        "Old format of migration file detected, run `aerich fix-migrations` to upgrade format"
                    )
                if offline:
                    cls._last_version_content = last_version_info.models_state
            if not offline and (last_version := await cls.get_last_version()):
                cls._last_version_content = cast(dict, last_version.content)
        connection = get_app_connection(config, app)
        cls.dialect = connection.schema_generator.DIALECT
        cls.ddl_class = await cls.load_ddl_class()
        cls.ddl = cls.ddl_class(connection)
        await cls._get_db_version(connection)

    @classmethod
    async def _get_last_version_num(cls, offline: bool = False) -> int | None:
        last_version = cls.get_last_version_file() if offline else (await cls.get_last_version())
        if not last_version:
            return None
        version = getattr(last_version, "version", str(last_version))
        return int(version.split("_", 1)[0])

    @classmethod
    async def generate_version(cls, name: str | None = None, offline: bool = False) -> str:
        now = datetime.now().strftime("%Y%m%d%H%M%S").replace("/", "")
        last_version_num = await cls._get_last_version_num(offline=offline)
        if last_version_num is None:
            return f"0_{now}_init.py"
        version = f"{last_version_num + 1}_{now}_{name}.py"
        if len(version) > MAX_VERSION_LENGTH:
            raise ValueError(f"Version name exceeds maximum length ({MAX_VERSION_LENGTH})")
        return version

    @classmethod
    async def _generate_diff_py(
        cls, name, no_input: bool = False, offline: bool = False
    ) -> str | None:
        content = cls._get_diff_file_content()
        version = await cls.generate_version(name, offline=offline)  # '<num>_<date>_<name>.py'
        conflict_modules = (
            []
            if offline
            else [
                version_module
                for version_module in cls.get_all_version_modules()
                if version_module.name.startswith(version.split("_")[0])
            ]
        )
        if conflict_modules:
            if len(conflict_modules) == 1:
                file = py_module_path(conflict_modules[0])
                tip = f"Migration file exists({file}). Do you want to remove it?"
            else:
                tip = f"Migration file exists({[py_module_path(m) for m in conflict_modules]}). Do you want to remove them?"
            overwrite = no_input
            if not overwrite:
                confirm = functools.partial(
                    click.prompt,
                    tip,
                    default=False,
                    type=bool,
                    show_choices=True,
                )
                if inspect.iscoroutinefunction(click.prompt):
                    # For asyncclick>=8.3
                    overwrite = await confirm()
                else:
                    overwrite = bool(confirm())
                if not overwrite:
                    return None
            # delete same version files
            for version_module in conflict_modules:
                py_module_path(version_module).unlink()

        Path(cls.migrate_location, version).write_text(content, encoding="utf-8")
        return version

    @classmethod
    def _exclude_extra_field_types(cls, diffs) -> list[tuple]:
        # Exclude changes of db_field_types that is not about the current dialect, e.g.:
        # {"db_field_types": {
        #   "oracle": "VARCHAR(255)" --> "oracle": "NVARCHAR2(255)"
        # }}
        return [
            c
            for c in diffs
            if not (
                len(c) == 3
                and c[1] == "db_field_types"
                and not ({i[0] for i in c[2]} & {cls.dialect, ""})
            )
        ]

    @overload
    @classmethod
    async def migrate(
        cls, name: str, empty: bool, no_input: Literal[True], offline: bool = False
    ) -> str: ...

    @overload
    @classmethod
    async def migrate(
        cls, name: str, empty: bool, no_input: bool = False, offline: bool = False
    ) -> str | None: ...

    @classmethod
    async def migrate(
        cls, name: str, empty: bool, no_input: bool = False, offline: bool = False
    ) -> str | None:
        """
        diff old models and new models to generate diff content
        :param name: str name for migration
        :param empty: bool if True generates empty migration
        :param no_input: bool if True skip click.prompt and cast return of it as true
        :param offline: bool if True generates migration without connecting to db
        :return:
        """
        if empty:
            return await cls._generate_diff_py(name, no_input=no_input, offline=offline)
        new_version_content = get_models_describe(cls.app)
        last_version = cast(dict, cls._last_version_content)
        cls.diff_models(last_version, new_version_content, no_input=no_input)
        cls.diff_models(new_version_content, last_version, False, no_input=no_input)

        cls._merge_operators()

        if not cls.upgrade_operators:
            return ""

        return await cls._generate_diff_py(name, no_input=no_input, offline=offline)

    @classmethod
    def _get_diff_file_content(cls) -> str:
        """
        builds content for diff file from template
        """

        def join_lines(lines: list[str]) -> str:
            if not lines:
                return ""
            return ";\n        ".join(lines) + ";"

        return cls.build_migration_file_text(
            upgrade_sql=join_lines(cls.upgrade_operators),
            downgrade_sql=join_lines(cls.downgrade_operators),
            models_state=get_models_describe(cls.app),
        )

    @classmethod
    def _add_operator(cls, operator: str, upgrade: bool = True, fk_m2m_index: bool = False) -> None:
        """
        add operator,differentiate fk because fk is order limit
        :param operator:
        :param upgrade:
        :param fk_m2m_index:
        :return:
        """
        operator = operator.rstrip(";")
        if upgrade:
            if fk_m2m_index:
                cls._upgrade_fk_m2m_index_operators.append(operator)
            else:
                cls.upgrade_operators.append(operator)
        else:
            if fk_m2m_index:
                cls._downgrade_fk_m2m_index_operators.append(operator)
            else:
                cls.downgrade_operators.append(operator)

    @classmethod
    def _handle_indexes(cls, model: type[Model], indexes: list[tuple[str] | Index]) -> list:
        if not tortoise_version_less_than("0.23.0"):
            # tortoise>=0.23.0 have __eq__/__hash__ with Index class since 313ee76.
            return indexes
        if index_classes := set(index.__class__ for index in indexes if isinstance(index, Index)):
            # Leave magic patch here to compare with older version of tortoise-orm
            # TODO: limit tortoise>0.22.2 in pyproject.toml and remove this function when v0.10.0 released
            for index_cls in index_classes:
                if index_cls(fields=("id",)) != index_cls(fields=("id",)):

                    def _hash(self) -> int:
                        return hash((tuple(sorted(self.fields)), self.name, self.expressions))

                    def _eq(self, other) -> bool:
                        return type(self) is type(other) and self.__dict__ == other.__dict__

                    setattr(index_cls, "__hash__", _hash)
                    setattr(index_cls, "__eq__", _eq)
        return indexes

    @classmethod
    def _get_indexes(cls, model, model_describe: dict) -> set[Index | tuple[str, ...]]:
        indexes: set[Index | tuple[str, ...]] = set()
        for x in cls._handle_indexes(model, model_describe.get("indexes", [])):
            if isinstance(x, Index):
                indexes.add(x)
            elif isinstance(x, dict):
                indexes.add(load_index(x))
            else:
                indexes.add(cast("tuple[str, ...]", tuple(x)))
        return indexes

    @staticmethod
    def _validate_custom_m2m_through(field: dict) -> None:
        # TODO: Check whether field includes required fk columns
        pass

    @classmethod
    def _handle_m2m_fields(
        cls, old_model_describe: dict, new_model_describe: dict, model, new_models, upgrade=True
    ) -> None:
        old_m2m_fields = cast("list[dict]", old_model_describe.get("m2m_fields", []))
        new_m2m_fields = cast("list[dict]", new_model_describe.get("m2m_fields", []))
        new_tables: dict[str, dict] = {
            field["table"]: field
            for field in new_models.values()
            if field.get("managed") is not False
        }
        for action, option, change in get_dict_diff_by_key(old_m2m_fields, new_m2m_fields):
            if action == "change":
                # Example:: action = 'change'; option = [0, 'unique']; change = (False, True)
                attr = option[-1]
                if attr == "indexed":
                    # Ignore changing of indexed, as it usually changed by unique
                    continue
                elif attr == "nullable":
                    # nullable of m2m relation is constrainted by orm framework, not by db
                    continue
                elif attr in ("unique", "db_constraint"):
                    # TODO: handle 'unique'
                    if upgrade:
                        click.secho(
                            f"Aerich does not handle {attr!r} attribution for m2m field. You may need to change the constraints in db manually.",
                            fg=Color.yellow,
                        )
                    continue
            with contextlib.suppress(TypeError, KeyError):
                if change[0][0] == "db_constraint":
                    continue
            new_value = change[0][1]
            if isinstance(new_value, str):
                for new_m2m_field in new_m2m_fields:
                    if new_m2m_field["name"] == new_value:
                        table = cast(str, new_m2m_field.get("through"))
                        break
            else:
                table = new_value.get("through")
            if action == "add":
                add = False
                if upgrade:
                    if field := new_tables.get(table):
                        cls._validate_custom_m2m_through(field)
                    elif table not in cls._upgrade_m2m:
                        cls._upgrade_m2m.append(table)
                        add = True
                else:
                    if table not in cls._downgrade_m2m:
                        cls._downgrade_m2m.append(table)
                        add = True
                if add:
                    ref_desc = cast(dict, new_models.get(new_value.get("model_name")))
                    cls._add_operator(
                        cls.create_m2m(model, new_value, ref_desc),
                        upgrade,
                        fk_m2m_index=True,
                    )
            elif action == "remove":
                add = False
                if upgrade and table not in cls._upgrade_m2m:
                    cls._upgrade_m2m.append(table)
                    add = True
                elif not upgrade and table not in cls._downgrade_m2m:
                    cls._downgrade_m2m.append(table)
                    add = True
                if add:
                    cls._add_operator(cls.drop_m2m(table), upgrade, True)

    @classmethod
    def _handle_relational(
        cls,
        key: str,
        old_model_describe: dict,
        new_model_describe: dict,
        model: type[Model],
        old_models: dict,
        new_models: dict,
        upgrade=True,
    ) -> None:
        old_fk_fields = cast("list[dict]", old_model_describe.get(key))
        new_fk_fields = cast("list[dict]", new_model_describe.get(key))

        old_fk_fields_name: list[str] = [i.get("name", "") for i in old_fk_fields]
        new_fk_fields_name: list[str] = [i.get("name", "") for i in new_fk_fields]

        # add
        for new_fk_field_name in set(new_fk_fields_name).difference(set(old_fk_fields_name)):
            fk_field = cls.get_field_by_name(new_fk_field_name, new_fk_fields)
            if fk_field.get("db_constraint"):
                ref_describe = cast(dict, new_models[fk_field["python_type"]])
                sql = cls._add_fk(model, fk_field, ref_describe)
                cls._add_operator(sql, upgrade, fk_m2m_index=True)
        # drop
        for old_fk_field_name in set(old_fk_fields_name).difference(set(new_fk_fields_name)):
            old_fk_field = cls.get_field_by_name(
                old_fk_field_name, cast("list[dict]", old_fk_fields)
            )
            if old_fk_field.get("db_constraint"):
                ref_describe = cast(dict, old_models[old_fk_field["python_type"]])
                sql = cls._drop_fk(model, old_fk_field, ref_describe)
                cls._add_operator(sql, upgrade, fk_m2m_index=True)

    @classmethod
    def _handle_fk_fields(
        cls,
        old_model_describe: dict,
        new_model_describe: dict,
        model: type[Model],
        old_models: dict,
        new_models: dict,
        upgrade=True,
    ) -> None:
        key = "fk_fields"
        cls._handle_relational(
            key, old_model_describe, new_model_describe, model, old_models, new_models, upgrade
        )

    @classmethod
    def _handle_o2o_fields(
        cls,
        old_model_describe: dict,
        new_model_describe: dict,
        model: type[Model],
        old_models: dict,
        new_models: dict,
        upgrade=True,
    ) -> None:
        key = "o2o_fields"
        cls._handle_relational(
            key, old_model_describe, new_model_describe, model, old_models, new_models, upgrade
        )

    @classmethod
    def _is_unique_constraint(cls, model: type[Model], index_name: str) -> bool:
        if cls.dialect != "postgres":
            return False
        # For postgresql, if a unique_together was created when generating the table, it is
        # a constraint. And if it was created after table generated, it will be a unique index.
        migrate_files = cls.get_all_version_modules()
        if len(migrate_files) < 2:
            return True
        pattern = re.compile(rf' "?{index_name}"? ')
        for filename in reversed(migrate_files[1:]):
            module = import_py_module(filename)
            upgrade_sql = run_async(module.upgrade, None)
            if pattern.search(upgrade_sql):
                line = [i for i in upgrade_sql.splitlines() if pattern.search(i)][0]
                prefix_words = pattern.split(line)[0].lower().split()
                if "drop" in prefix_words:
                    # The migrate file may be generated by `aerich migrate` without applied by `aerich upgrade`
                    continue
                return "constraint" in prefix_words
        return True

    @classmethod
    def _handle_add_models(
        cls, upgrade: bool, new_models, new_table_items: list[tuple[str, dict, type[Model]]]
    ) -> None:
        sql_fks: list[tuple[str, set[str]]] = []
        for new_model_str, new_model_describe, model in new_table_items:
            if not upgrade:
                # we can't find origin model when downgrade, so skip
                continue
            sql = cls.add_model(model)
            fk_model_names: set[str] = {
                i.get("python_type", "")
                for i in (new_model_describe["fk_fields"] + new_model_describe["o2o_fields"])
            }
            item = (sql, fk_model_names)
            name = new_model_describe["name"]
            for index, (_, fks) in enumerate(sql_fks):
                if name in fks:
                    sql_fks.insert(index, item)
                    break
            else:
                sql_fks.append(item)
            cls._handle_m2m_fields({}, new_model_describe, model, new_models, upgrade)
        for sql, _ in sql_fks:
            cls._add_operator(sql, upgrade)

    @classmethod
    def diff_models(
        cls, old_models: dict[str, dict], new_models: dict[str, dict], upgrade=True, no_input=False
    ) -> None:
        """
        diff models and add operators
        :param old_models:
        :param new_models:
        :param upgrade:
        :return:
        """
        _aerich = f"{cls.app}.{cls._aerich}"
        try:
            old_models.pop(_aerich, None)
        except AttributeError:
            # Invalid use when app migration directory exists but aerich table not exist
            raise click.UsageError(
                "You may need to run `aerich init-db` first to initialize the database."
            ) from None
        new_models.pop(_aerich, None)
        models_with_rename_field: set[str] = set()  # models that trigger the click.prompt

        new_table_items: list[tuple[str, dict, type[Model]]] = []
        other_items: list[tuple[str, dict, type[Model]]] = []
        for new_model_str, new_model_describe in new_models.items():
            if upgrade and new_model_describe.get("managed") is False:
                continue
            model = cls._get_model(new_model_describe["name"].split(".")[1])
            item = (new_model_str, new_model_describe, model)
            if new_model_str not in old_models:
                new_table_items.append(item)
            else:
                other_items.append(item)
        cls._handle_add_models(upgrade, new_models, new_table_items)
        for new_model_str, new_model_describe, model in other_items:
            old_model_describe = cast(dict, old_models.get(new_model_str))
            if not upgrade and old_model_describe.get("managed") is False:
                continue
            # rename table
            new_table = cast(str, new_model_describe.get("table"))
            old_table = cast(str, old_model_describe.get("table"))
            if new_table != old_table:
                cls._add_operator(cls.rename_table(model, old_table, new_table), upgrade)
            _old_uniques = cast("list[Iterable[str]]", old_model_describe.get("unique_together"))
            _new_uniques = cast("list[Iterable[str]]", new_model_describe.get("unique_together"))
            old_unique_together = set(map(lambda x: tuple(x), _old_uniques))
            new_unique_together = set(map(lambda x: tuple(x), _new_uniques))
            old_indexes = cls._get_indexes(model, old_model_describe)
            new_indexes = cls._get_indexes(model, new_model_describe)
            # pk field
            cls._handle_pk_field_alter(model, old_model_describe, new_model_describe, upgrade)
            # fk fields
            args = (old_model_describe, new_model_describe, model, old_models, new_models)
            cls._handle_fk_fields(*args, upgrade=upgrade)
            # o2o fields
            cls._handle_o2o_fields(*args, upgrade=upgrade)
            old_o2o_columns = [i["raw_field"] for i in old_model_describe.get("o2o_fields", [])]
            new_o2o_columns = [i["raw_field"] for i in new_model_describe.get("o2o_fields", [])]
            # m2m fields
            cls._handle_m2m_fields(
                old_model_describe, new_model_describe, model, new_models, upgrade
            )
            # add unique_together
            for index in new_unique_together.difference(old_unique_together):
                cls._add_operator(cls._add_index(model, index, True), upgrade, True)
            # remove unique_together
            for index in old_unique_together.difference(new_unique_together):
                index_name = cls._unique_index_name(model, index)
                if upgrade and cls._is_unique_constraint(model, index_name):
                    cls._add_operator(
                        cls.ddl.drop_unique_constraint(model, index_name), upgrade, True
                    )
                else:
                    cls._add_operator(cls.ddl.drop_index_by_name(model, index_name), upgrade, True)
            # add indexes
            for idx in new_indexes.difference(old_indexes):
                cls._add_operator(cls._add_index(model, idx), upgrade, fk_m2m_index=True)
            # remove indexes
            for idx in old_indexes.difference(new_indexes):
                cls._add_operator(cls._drop_index(model, idx), upgrade, fk_m2m_index=True)
            old_data_fields = list(
                filter(
                    lambda x: x.get("db_field_types") is not None,
                    cast("list[dict]", old_model_describe.get("data_fields")),
                )
            )
            new_data_fields = list(
                filter(
                    lambda x: x.get("db_field_types") is not None,
                    cast("list[dict]", new_model_describe.get("data_fields")),
                )
            )

            old_data_fields_name = cast("list[str]", [i.get("name") for i in old_data_fields])
            new_data_fields_name = cast("list[str]", [i.get("name") for i in new_data_fields])

            # add fields or rename fields
            for new_data_field_name in set(new_data_fields_name).difference(
                set(old_data_fields_name)
            ):
                new_data_field = cls.get_field_by_name(new_data_field_name, new_data_fields)
                is_rename = False
                field_type = new_data_field.get("field_type")
                db_column = new_data_field.get("db_column")
                new_name = set(new_data_field_name)
                for old_data_field in sorted(
                    old_data_fields,
                    key=lambda f: (
                        f.get("field_type") != field_type,
                        # old field whose name have more same characters with new field's
                        # should be put in front of the other
                        len(new_name.symmetric_difference(set(f.get("name", "")))),
                    ),
                ):
                    changes = cls._exclude_extra_field_types(diff(old_data_field, new_data_field))
                    old_data_field_name = cast(str, old_data_field.get("name"))
                    if len(changes) == 2:
                        # rename field
                        name_diff = (old_data_field_name, new_data_field_name)
                        column_diff = (old_data_field.get("db_column"), db_column)
                        if (
                            changes[0] == ("change", "name", name_diff)
                            and changes[1] == ("change", "db_column", column_diff)
                            and old_data_field_name not in new_data_fields_name
                        ):
                            if upgrade:
                                if (rename_fields := cls._rename_fields.get(new_model_str)) and (
                                    old_data_field_name in rename_fields
                                    or new_data_field_name in rename_fields.values()
                                ):
                                    continue
                                prefix = f"({new_model_str}) "
                                if new_model_str not in models_with_rename_field:
                                    if models_with_rename_field:
                                        # When there are multi rename fields with different models,
                                        # print a empty line to warn that is another model
                                        prefix = "\n" + prefix
                                    models_with_rename_field.add(new_model_str)
                                if not (is_rename := no_input):
                                    tip = f"{prefix}Rename {old_data_field_name} to {new_data_field_name}?"
                                    confirm = functools.partial(
                                        click.prompt,
                                        default=True,
                                        type=bool,
                                        show_choices=True,
                                    )
                                    if inspect.iscoroutinefunction(click.prompt):
                                        # For asyncclick>=8.3
                                        is_rename = run_async(confirm, tip)
                                    elif isinstance(r := confirm(tip), bool):
                                        is_rename = r
                                if is_rename:
                                    if rename_fields is None:
                                        rename_fields = cls._rename_fields[new_model_str] = {}
                                    rename_fields[old_data_field_name] = new_data_field_name
                            else:
                                is_rename = False
                                if rename_to := cls._rename_fields.get(new_model_str, {}).get(
                                    new_data_field_name
                                ):
                                    is_rename = True
                                    if rename_to != old_data_field_name:
                                        continue
                            if is_rename:
                                # only MySQL8+ has rename syntax
                                if (
                                    cls.dialect == "mysql"
                                    and cls._db_version
                                    and cls._db_version.startswith("5.")
                                ):
                                    cls._add_operator(
                                        cls._change_field(model, old_data_field, new_data_field),
                                        upgrade,
                                    )
                                else:
                                    cls._add_operator(
                                        cls._rename_field(model, *changes[1][2]),
                                        upgrade,
                                    )
                if not is_rename:
                    cls._add_operator(cls._add_field(model, new_data_field), upgrade)
                    if (
                        new_data_field["indexed"]
                        and new_data_field["db_column"] not in new_o2o_columns
                    ):
                        unique = new_data_field["unique"]
                        if not unique or cls.ddl.should_add_unique_index_when_adding_column():
                            cls._add_operator(
                                cls._add_index(model, (new_data_field["db_column"],), unique),
                                upgrade,
                                True,
                            )
            # remove fields
            rename_fields = cls._rename_fields.get(new_model_str)
            for old_data_field_name in set(old_data_fields_name).difference(
                set(new_data_fields_name)
            ):
                # don't remove field if is renamed
                if rename_fields and (
                    (upgrade and old_data_field_name in rename_fields)
                    or (not upgrade and old_data_field_name in rename_fields.values())
                ):
                    continue
                old_data_field = cls.get_field_by_name(old_data_field_name, old_data_fields)
                db_column = cast(str, old_data_field["db_column"])
                cls._add_operator(
                    cls._remove_field(model, db_column),
                    upgrade,
                )
                if old_data_field["indexed"] and old_data_field["db_column"] not in old_o2o_columns:
                    is_unique_field = old_data_field.get("unique")
                    cls._add_operator(
                        cls._drop_index(model, {db_column}, is_unique_field),
                        upgrade,
                        True,
                    )

            # change fields
            for field_name in set(new_data_fields_name).intersection(set(old_data_fields_name)):
                cls._handle_field_changes(
                    model, field_name, old_data_fields, new_data_fields, upgrade
                )

        if post_hook_sql := cls.ddl.schema_generator._post_table_hook().strip():
            cls._add_operator(post_hook_sql, upgrade)
        dropped_m2m_tables: set[str] = set()
        for old_model in old_models.keys() - new_models.keys():
            if not upgrade and old_models[old_model].get("managed") is False:
                continue
            model_describe = old_models[old_model]
            for field_describe in model_describe.get("m2m_fields", []):
                if (through := field_describe["through"]) not in dropped_m2m_tables:
                    cls._add_operator(cls.drop_m2m(through), upgrade)
                    dropped_m2m_tables.add(through)
            cls._add_operator(cls.drop_model(model_describe["table"]), upgrade)

    @classmethod
    def _handle_pk_field_alter(
        cls,
        model: type[Model],
        old_model_describe: dict[str, dict],
        new_model_describe: dict[str, dict],
        upgrade: bool,
    ) -> None:
        old_pk_field = old_model_describe.get("pk_field", {})
        new_pk_field = new_model_describe.get("pk_field", {})
        changes = cls._exclude_extra_field_types(diff(old_pk_field, new_pk_field))
        sqls: list[str] = []
        for action, option, change in changes:
            if action != "change":
                continue
            if option == "db_column":
                # rename pk
                sql = cls._rename_field(model, *change)
            elif option == "constraints.max_length":
                sql = cls._modify_field(model, new_pk_field)
            elif option == "field_type":
                # Only support change field type between int fields, e.g.: IntField -> BigIntField
                if not all(field_type.endswith("IntField") for field_type in change):
                    if upgrade:
                        model_name = model._meta.full_name.split(".")[-1]
                        field_name = new_pk_field.get("name", "")
                        msg = (
                            f"Does not support change primary_key({model_name}.{field_name}) field type,"
                            " you may need to do it manually."
                        )
                        click.secho(msg, fg=Color.yellow)
                    return
                sql = cls._modify_field(model, new_pk_field)
            else:
                # Skip option like 'constraints.ge', 'constraints.le', 'db_field_types.'
                continue
            sqls.append(sql)
        for sql in sorted(sqls, key=lambda x: "RENAME" not in x):
            # TODO: alter references field in m2m table
            cls._add_operator(sql, upgrade)

    @classmethod
    def _handle_field_changes(
        cls,
        model: type[Model],
        field_name: str,
        old_data_fields: list[dict],
        new_data_fields: list[dict],
        upgrade: bool,
    ) -> None:
        old_data_field = cls.get_field_by_name(field_name, old_data_fields)
        new_data_field = cls.get_field_by_name(field_name, new_data_fields)
        changes = cls._exclude_extra_field_types(diff(old_data_field, new_data_field))
        options = {c[1] for c in changes}
        modified = False
        for change in changes:
            _, option, old_new = change
            if option == "indexed":
                # change index
                if old_new[0] is False and old_new[1] is True:
                    unique = new_data_field.get("unique")
                    cls._add_operator(cls._add_index(model, (field_name,), unique), upgrade, True)
                else:
                    unique = old_data_field.get("unique")
                    if unique:
                        for sql in cls._drop_unique_index(model, field_name):
                            cls._add_operator(sql, upgrade, True)
                    else:
                        cls._add_operator(cls._drop_index(model, (field_name,)), upgrade, True)
            elif option == "db_field_types.":
                if new_data_field.get("field_type") == "DecimalField":
                    # modify column
                    cls._add_operator(cls._modify_field(model, new_data_field), upgrade)
            elif option == "default":
                if not (is_default_function(old_new[0]) or is_default_function(old_new[1])):
                    # change column default
                    cls._add_operator(cls._alter_default(model, new_data_field), upgrade)
            elif option == "unique":
                if "indexed" in options:
                    # indexed include it
                    continue
                # Change unique for indexed field, e.g.: `db_index=True, unique=False` --> `db_index=True, unique=True`
                drop_unique = old_new[0] is True and old_new[1] is False
                for sql in cls.ddl.alter_indexed_column_unique(model, field_name, drop_unique):
                    cls._add_operator(sql, upgrade, True)
            elif option == "nullable":
                # change nullable
                cls._add_operator(cls._alter_null(model, new_data_field), upgrade)
            elif option == "description":
                # change comment
                cls._add_operator(cls._set_comment(model, new_data_field), upgrade)
            else:
                if modified:
                    continue
                # modify column
                cls._add_operator(cls._modify_field(model, new_data_field), upgrade)
                modified = True

    @classmethod
    def rename_table(cls, model: type[Model], old_table_name: str, new_table_name: str) -> str:
        return cls.ddl.rename_table(model, old_table_name, new_table_name)

    @classmethod
    def add_model(cls, model: type[Model]) -> str:
        return cls.ddl.create_table(model)

    @classmethod
    def drop_model(cls, table_name: str) -> str:
        return cls.ddl.drop_table(table_name)

    @classmethod
    def create_m2m(
        cls, model: type[Model], field_describe: dict, reference_table_describe: dict
    ) -> str:
        return cls.ddl.create_m2m(model, field_describe, reference_table_describe)

    @classmethod
    def drop_m2m(cls, table_name: str) -> str:
        return cls.ddl.drop_m2m(table_name)

    @classmethod
    def _resolve_fk_fields_name(cls, model: type[Model], fields_name: Iterable[str]) -> list[str]:
        ret = []
        for field_name in fields_name:
            try:
                field = model._meta.fields_map[field_name]
            except KeyError:
                # field dropped or to be add
                pass
            else:
                if field.source_field:
                    field_name = field.source_field
                elif field_name in model._meta.fk_fields:
                    field_name += "_id"
            ret.append(field_name)
        return ret

    @classmethod
    def _unique_index_name(cls, model: type[Model], fields_name: Iterable[str]) -> str:
        field_names = cls._resolve_fk_fields_name(model, fields_name)
        return cls.ddl._index_name(True, model, field_names)

    @classmethod
    def _drop_index(
        cls, model: type[Model], fields_name: Iterable[str] | Index, unique=False
    ) -> str:
        if isinstance(fields_name, Index):
            if cls.dialect == "mysql":
                # schema_generator of MySQL return a empty index sql
                if hasattr(fields_name, "field_names"):
                    # tortoise>=0.24
                    fields = fields_name.field_names
                else:
                    # TODO: remove else when drop support for tortoise<0.24
                    if not (fields := fields_name.fields):
                        fields = [getattr(i, "get_sql")() for i in fields_name.expressions]
                return cls.ddl.drop_index(model, fields, unique, name=fields_name.name)
            return cls.ddl.drop_index_by_name(
                model, fields_name.index_name(cls.ddl.schema_generator, model)
            )
        field_names = cls._resolve_fk_fields_name(model, fields_name)
        return cls.ddl.drop_index(model, field_names, unique)

    @classmethod
    def _drop_unique_index(cls, model: type[Model], field_name: str) -> list[str]:
        field_name, *_ = cls._resolve_fk_fields_name(model, (field_name,))
        if hasattr(cls.ddl, "drop_unique_index"):
            return cls.ddl.drop_unique_index(model, field_name)
        return [cls.ddl.drop_index(model, [field_name], unique=True)]

    @classmethod
    def _add_index(
        cls, model: type[Model], fields_name: Iterable[str] | Index, unique=False
    ) -> str:
        if isinstance(fields_name, Index):
            if cls.dialect == "mysql":
                # schema_generator of MySQL return a empty index sql
                if hasattr(fields_name, "field_names"):
                    # tortoise>=0.24
                    fields = fields_name.field_names
                else:
                    # TODO: remove else when drop support for tortoise<0.24
                    if not (fields := fields_name.fields):
                        fields = [getattr(i, "get_sql")() for i in fields_name.expressions]
                return cls.ddl.add_index(
                    model,
                    fields,
                    name=fields_name.name,
                    index_type=fields_name.INDEX_TYPE,
                    extra=fields_name.extra,
                )
            sql = fields_name.get_sql(cls.ddl.schema_generator, model, safe=True)
            if tortoise_version_less_than("0.24.0"):
                sql = sql.replace("  ", " ")
                if cls.dialect == "postgres" and (exists := "IF NOT EXISTS ") not in sql:
                    idx = " INDEX "
                    sql = sql.replace(idx, idx + exists)
            return sql
        field_names = cls._resolve_fk_fields_name(model, fields_name)
        return cls.ddl.add_index(model, field_names, unique)

    @classmethod
    def _add_field(cls, model: type[Model], field_describe: dict, is_pk: bool = False) -> str:
        return cls.ddl.add_column(model, field_describe, is_pk)

    @classmethod
    def _alter_default(cls, model: type[Model], field_describe: dict) -> str:
        return cls.ddl.alter_column_default(model, field_describe)

    @classmethod
    def _alter_null(cls, model: type[Model], field_describe: dict) -> str:
        return cls.ddl.alter_column_null(model, field_describe)

    @classmethod
    def _set_comment(cls, model: type[Model], field_describe: dict) -> str:
        return cls.ddl.set_comment(model, field_describe)

    @classmethod
    def _modify_field(cls, model: type[Model], field_describe: dict) -> str:
        return cls.ddl.modify_column(model, field_describe)

    @classmethod
    def _drop_fk(
        cls, model: type[Model], field_describe: dict, reference_table_describe: dict
    ) -> str:
        return cls.ddl.drop_fk(model, field_describe, reference_table_describe)

    @classmethod
    def _remove_field(cls, model: type[Model], column_name: str) -> str:
        return cls.ddl.drop_column(model, column_name)

    @classmethod
    def _rename_field(cls, model: type[Model], old_field_name: str, new_field_name: str) -> str:
        return cls.ddl.rename_column(model, old_field_name, new_field_name)

    @classmethod
    def _change_field(
        cls, model: type[Model], old_field_describe: dict, new_field_describe: dict
    ) -> str:
        db_field_types = cast(dict, new_field_describe.get("db_field_types"))
        return cls.ddl.change_column(
            model,
            cast(str, old_field_describe.get("db_column")),
            cast(str, new_field_describe.get("db_column")),
            cast(str, db_field_types.get(cls.dialect) or db_field_types.get("")),
        )

    @classmethod
    def _add_fk(
        cls, model: type[Model], field_describe: dict, reference_table_describe: dict
    ) -> str:
        """
        add fk
        :param model:
        :param field_describe:
        :param reference_table_describe:
        :return:
        """
        return cls.ddl.add_fk(model, field_describe, reference_table_describe)

    @classmethod
    def _merge_operators(cls) -> None:
        """
        fk/m2m/index must be last when add,first when drop
        :return:
        """
        for _upgrade_fk_m2m_operator in cls._upgrade_fk_m2m_index_operators:
            if "ADD" in _upgrade_fk_m2m_operator or "CREATE" in _upgrade_fk_m2m_operator:
                cls.upgrade_operators.append(_upgrade_fk_m2m_operator)
                if m := re.search(r'CREATE TABLE "(\w+?)"', _upgrade_fk_m2m_operator):
                    table_name = m.group(1)
                    pattern = re.compile(rf'COMMENT ON TABLE "{table_name}"')
                    # Comment of postgresql m2m table may set before creation of it
                    for index, sql in enumerate(cls.upgrade_operators[:-1]):
                        if pattern.search(sql):
                            sqls = sql.split(";")
                            for i, s in enumerate(sqls):
                                if pattern.search(s):
                                    break
                            comment_sql = s.strip()
                            sqls.pop(i)
                            cls.upgrade_operators[index] = ";".join(sqls)
                            # Put comment of this table behind the create sql
                            cls.upgrade_operators.append(comment_sql)
                            break
            else:
                cls.upgrade_operators.insert(0, _upgrade_fk_m2m_operator)

        for _downgrade_fk_m2m_operator in cls._downgrade_fk_m2m_index_operators:
            if "ADD" in _downgrade_fk_m2m_operator or "CREATE" in _downgrade_fk_m2m_operator:
                cls.downgrade_operators.append(_downgrade_fk_m2m_operator)
            else:
                cls.downgrade_operators.insert(0, _downgrade_fk_m2m_operator)

    @classmethod
    async def fix_migrations(cls, config: dict[str, Any]) -> list[str] | None:
        """
        Fix old migration files to include MODELS_STATE for aerich 0.9.2+

        :return: List of updated migration file paths
        """
        version_modules = cls.get_all_version_modules()
        if not version_modules:
            click.echo(f"No migration file found for app {cls.app!r}, nothing to do.")
            return None
        unfixed_file_modules: list[tuple[str, ModuleType]] = []
        for version_module in version_modules:
            # Check if file already has MODELS_STATE
            migration_info = import_py_module(version_module)
            if getattr(migration_info, "MODELS_STATE", None):
                # File is already in the new format
                continue
            file_name = version_module.name + ".py"
            unfixed_file_modules.append((file_name, migration_info))
        if not unfixed_file_modules:
            return []

        if not Tortoise._inited:
            await Tortoise.init(config=config)
        connection = get_app_connection(config, cls.app)

        try:
            fid = await Aerich.first().values("id")
        except OperationalError:
            click.secho(
                "⚠️ Warning: Aerich table not found. "
                "fix-migrations can only be applied by using "
                "existing database with all migrations applied.",
                fg=Color.yellow,
            )
            return None
        if not fid:  # Aerich.all().count() == 0
            click.secho(
                "⚠️ Warning: Aerich table is empty. "
                "fix-migrations can only be applied by using "
                "existing database with all migrations applied.",
                fg=Color.yellow,
            )
            return None

        updated_files: list[str] = []

        # Get model state from Aerich table for each migration
        for file_name, migration_info in unfixed_file_modules:
            file_path = cls.migrate_location / file_name
            # Find the corresponding record in the Aerich table
            aerich_obj = await Aerich.filter(version=file_name, app=cls.app).first()
            if aerich_obj is None:
                click.secho(
                    f"⚠️ Warning: No matching record for migration {file_name} in Aerich table. Skipping.",
                    fg=Color.yellow,
                )
                continue

            # Get models state from the content column
            models_state = aerich_obj.content
            if not models_state:
                click.secho(
                    f"⚠️ Warning: No content found for migration {file_name}. Skipping.",
                    fg=Color.yellow,
                )
                continue

            upgrade_sql = await migration_info.upgrade(connection)
            downgrade_sql = await migration_info.downgrade(connection)

            # Generate new content with the template
            new_content = cls.build_migration_file_text(
                upgrade_sql=upgrade_sql,
                downgrade_sql=downgrade_sql,
                models_state=models_state,
            )

            # Write the new content to the file
            file_path.write_text(new_content, encoding="utf-8")
            updated_files.append(str(file_path))

        return updated_files

    @classmethod
    def build_migration_file_text(
        cls, upgrade_sql: str, models_state: str | dict, downgrade_sql=""
    ) -> str:
        if isinstance(models_state, dict):
            models_state = get_formatted_compressed_data(models_state)
        return MIGRATE_TEMPLATE.format(
            upgrade_sql=upgrade_sql.strip(),
            downgrade_sql=downgrade_sql.strip(),
            models_state=models_state,
        )
