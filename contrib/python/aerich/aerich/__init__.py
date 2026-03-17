from __future__ import annotations

import pkgutil
import warnings
from collections.abc import Generator
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, overload

from tortoise import BaseDBAsyncClient, Tortoise, connections
from tortoise.exceptions import OperationalError
from tortoise.transactions import in_transaction
from tortoise.utils import generate_schema_for_client, get_schema_sql

from aerich._compat import _init_asyncio_patch, _init_tortoise_0_24_1_patch
from aerich.exceptions import DowngradeError, NotInitedError
from aerich.inspectdb.mysql import InspectMySQL
from aerich.inspectdb.postgres import InspectPostgres
from aerich.inspectdb.sqlite import InspectSQLite
from aerich.migrate import Migrate
from aerich.models import Aerich
from aerich.utils import (
    decompress_dict,
    file_module_info,
    get_app_connection,
    get_app_connection_name,
    get_models_describe,
    import_py_file,
    import_py_module,
    load_tortoise_config,
    py_module_path,
)
from aerich.version import __version__

if TYPE_CHECKING:
    from aerich._compat import Self
    from aerich.inspectdb import Inspect


_init_asyncio_patch()  # Change event_loop_policy for Windows
_init_tortoise_0_24_1_patch()  # Patch m2m table generator for tortoise-orm==0.24.1
__all__ = ("Command", "TortoiseContext", "__version__")


class TortoiseContext(AbstractAsyncContextManager):
    def __init__(self, tortoise_config: dict | None = None) -> None:
        if tortoise_config is None:
            tortoise_config = load_tortoise_config()
        self.tortoise_config = tortoise_config
        self._init_when_aenter = True

    async def init(self) -> None:
        await Tortoise.init(config=self.tortoise_config)

    async def __aenter__(self) -> Self:
        if self._init_when_aenter:
            await self.init()
        return self

    def __await__(self) -> Generator[Any, None, Self]:
        # To support `command = await Command(tortoise_config)`
        async def _self() -> Self:
            return await self.__aenter__()

        return _self().__await__()

    @staticmethod
    async def aclose() -> None:
        """Close tortoise connections if it was inited"""
        if Tortoise._inited:
            await connections.close_all()

    async def __aexit__(self, *args, **kw) -> None:
        await self.aclose()


class Command(TortoiseContext):
    def __init__(
        self,
        tortoise_config: dict,
        app: str = "models",
        location: str = "./migrations",
        inspectdb_fields: dict[str, str] | None = None,
    ) -> None:
        super().__init__(tortoise_config)
        self.app = app
        self.location = location
        self._inspectdb_fields = inspectdb_fields
        Migrate.app = app

    async def init(self, offline: bool = False) -> None:
        await Migrate.init(self.tortoise_config, self.app, self.location, offline=offline)

    async def close(self) -> None:
        warnings.warn(
            "`Command.close()` is deprecated, please use Command.aclose() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        await self.aclose()

    async def _upgrade(
        self,
        conn: BaseDBAsyncClient,
        version_file: str,
        fake: bool = False,
        version_module: pkgutil.ModuleInfo | None = None,
    ) -> None:
        if version_module is not None:
            m = import_py_module(version_module)
        else:
            m = import_py_file(Path(Migrate.migrate_location, version_file))
        upgrade = m.upgrade
        if not fake:
            await conn.execute_script(await upgrade(conn))

        model_state_str = getattr(m, "MODELS_STATE", None)
        models_state = (
            decompress_dict(model_state_str) if model_state_str else get_models_describe(self.app)
        )
        await Aerich.create(version=version_file, app=self.app, content=models_state)

    async def upgrade(self, run_in_transaction: bool = True, fake: bool = False) -> list[str]:
        migrated = []
        for version_module in Migrate.get_all_version_modules():
            version_file = version_module.name + ".py"
            try:
                exists = await Aerich.exists(version=version_file, app=self.app)
            except OperationalError:
                exists = False
            if not exists:
                app_conn_name = get_app_connection_name(self.tortoise_config, self.app)
                m = import_py_module(version_module)
                migration_run_in_transaction = getattr(m, "RUN_IN_TRANSACTION", run_in_transaction)
                if migration_run_in_transaction:
                    async with in_transaction(app_conn_name) as conn:
                        await self._upgrade(conn, version_file, fake, version_module)
                else:
                    app_conn = get_app_connection(self.tortoise_config, self.app)
                    await self._upgrade(app_conn, version_file, fake, version_module)
                migrated.append(version_file)
        return migrated

    async def downgrade(self, version: int, delete: bool, fake: bool = False) -> list[str]:
        ret: list[str] = []
        if version == -1:
            specified_version = await Migrate.get_last_version()
        else:
            specified_version = await Aerich.filter(
                app=self.app, version__startswith=f"{version}_"
            ).first()
        if not specified_version:
            raise DowngradeError("No specified version found")
        if version == -1:
            versions = [specified_version]
        else:
            versions = await Aerich.filter(app=self.app, pk__gte=specified_version.pk)
        for version_obj in versions:
            file = version_obj.version
            async with in_transaction(
                get_app_connection_name(self.tortoise_config, self.app)
            ) as conn:
                module_info = file_module_info(Migrate.migrate_location, Path(file).stem)
                m = import_py_module(module_info)
                downgrade = m.downgrade
                downgrade_sql = await downgrade(conn)
                if not downgrade_sql.strip():
                    raise DowngradeError("No downgrade items found")
                if not fake:
                    await conn.execute_script(downgrade_sql)
                await version_obj.delete()
                if delete:
                    py_module_path(module_info).unlink()
                ret.append(file)
        return ret

    async def heads(self) -> list[str]:
        ret = []
        versions = Migrate.get_all_version_files()
        for version in versions:
            if not await Aerich.exists(version=version, app=self.app):
                ret.append(version)
        return ret

    async def history(self) -> list[str]:
        versions = Migrate.get_all_version_files()
        return [version for version in versions]

    async def inspectdb(self, tables: list[str] | None = None) -> str:
        connection = get_app_connection(self.tortoise_config, self.app)
        dialect = connection.schema_generator.DIALECT
        if dialect == "mysql":
            cls: type[Inspect] = InspectMySQL
        elif dialect == "postgres":
            cls = InspectPostgres
        elif dialect == "sqlite":
            cls = InspectSQLite
        else:
            raise NotImplementedError(f"{dialect} is not supported")
        inspect = cls(connection, tables)
        if self._inspectdb_fields:
            inspect._special_fields = self._inspectdb_fields
        return await inspect.inspect()

    @overload
    async def migrate(
        self,
        name: str = "update",
        empty: bool = False,
        no_input: Literal[True] = True,
        offline: bool = False,
    ) -> str: ...

    @overload
    async def migrate(
        self,
        name: str = "update",
        empty: bool = False,
        no_input: bool = False,
        offline: bool = False,
    ) -> str | None: ...

    async def migrate(
        self,
        name: str = "update",
        empty: bool = False,
        no_input: bool = False,
        offline: bool = False,
    ) -> str | None:
        # return None if same version migration file already exists, and new one not generated
        try:
            return await Migrate.migrate(name, empty, no_input, offline)
        except NotInitedError as e:
            raise NotInitedError("You have to call .init() first before migrate") from e

    async def init_db(self, safe: bool, pre_sql: str | None = None) -> None:
        await self._do_init(safe, pre_sql)

    async def _do_init(self, safe: bool, pre_sql: str | None = None, offline: bool = False) -> None:
        location = self.location
        app = self.app
        config = self.tortoise_config

        await Tortoise.init(config=config)
        connection = get_app_connection(config, app)
        if offline:
            await Migrate.init(config, app, location, offline=True)
        elif pre_sql:
            await connection.execute_script(pre_sql)

        dirname = Path(location, app)
        if not dirname.exists():
            dirname.mkdir(parents=True)
        else:
            # If directory is empty, go ahead, otherwise raise FileExistsError
            for unexpected_file in dirname.glob("*"):
                raise FileExistsError(str(unexpected_file))
        schema = get_schema_sql(connection, safe)

        version = await Migrate.generate_version(offline=offline)
        aerich_content = get_models_describe(app)
        version_file = Path(dirname, version)
        content = Migrate.build_migration_file_text(upgrade_sql=schema, models_state=aerich_content)
        version_file.write_text(content, encoding="utf-8")
        Migrate._last_version_content = aerich_content
        if not offline:
            await generate_schema_for_client(connection, safe)
            await Aerich.create(version=version, app=app, content=aerich_content)

    async def init_migrations(self, safe: bool) -> None:
        await self._do_init(safe, offline=True)

    async def fix_migrations(self) -> list[str] | None:
        """
        Fix migration files to include models state for aerich 0.6.0+
        :return: List of updated migration files (if no migration file or no aerich objects will return None)
        """
        Migrate.app = self.app
        Migrate.migrate_location = Path(self.location, self.app)
        return await Migrate.fix_migrations(self.tortoise_config)
