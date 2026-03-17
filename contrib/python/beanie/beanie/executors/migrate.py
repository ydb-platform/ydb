import asyncio
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import click
import toml

from beanie.migrations import template
from beanie.migrations.database import DBHandler
from beanie.migrations.models import RunningDirections, RunningMode
from beanie.migrations.runner import MigrationNode

logging.basicConfig(format="%(message)s", level=logging.INFO)


class MigrationSettings:
    def __init__(self, **kwargs: Any):
        self.direction = (
            kwargs.get("direction")
            or self.get_env_value("direction")
            or self.get_from_toml("direction")
            or RunningDirections.FORWARD
        )

        self.distance = int(
            kwargs.get("distance")
            or self.get_env_value("distance")
            or self.get_from_toml("distance")
            or 0
        )
        self.connection_uri = str(
            kwargs.get("connection_uri")
            or self.get_env_value("connection_uri")
            or self.get_from_toml("connection_uri")
        )
        self.database_name = str(
            kwargs.get("database_name")
            or self.get_env_value("database_name")
            or self.get_from_toml("database_name")
        )
        self.path = Path(
            kwargs.get("path")
            or self.get_env_value("path")
            or self.get_from_toml("path")
        )
        self.allow_index_dropping = bool(
            kwargs.get("allow_index_dropping")
            or self.get_env_value("allow_index_dropping")
            or self.get_from_toml("allow_index_dropping")
            or False
        )
        self.use_transaction = bool(kwargs.get("use_transaction"))

    @staticmethod
    def get_env_value(field_name) -> Any:
        if field_name == "connection_uri":
            value = (
                os.environ.get("BEANIE_URI")
                or os.environ.get("BEANIE_CONNECTION_URI")
                or os.environ.get("BEANIE_CONNECTION_STRING")
                or os.environ.get("BEANIE_MONGODB_DSN")
                or os.environ.get("BEANIE_MONGODB_URI")
                or os.environ.get("beanie_uri")
                or os.environ.get("beanie_connection_uri")
                or os.environ.get("beanie_connection_string")
                or os.environ.get("beanie_mongodb_dsn")
                or os.environ.get("beanie_mongodb_uri")
            )
        elif field_name == "database_name":
            value = (
                os.environ.get("BEANIE_DB")
                or os.environ.get("BEANIE_DB_NAME")
                or os.environ.get("BEANIE_DATABASE_NAME")
                or os.environ.get("beanie_db")
                or os.environ.get("beanie_db_name")
                or os.environ.get("beanie_database_name")
            )
        else:
            value = os.environ.get(
                f"BEANIE_{field_name.upper()}"
            ) or os.environ.get(f"beanie_{field_name.lower()}")
        return value

    @staticmethod
    def get_from_toml(field_name) -> Any:
        path = Path("pyproject.toml")
        if path.is_file():
            val = (
                toml.load(path)
                .get("tool", {})
                .get("beanie", {})
                .get("migrations", {})
            )
        else:
            val = {}
        return val.get(field_name)


@click.group()
def migrations():
    pass


async def run_migrate(settings: MigrationSettings):
    DBHandler.set_db(settings.connection_uri, settings.database_name)
    root = await MigrationNode.build(settings.path)
    mode = RunningMode(
        direction=settings.direction, distance=settings.distance
    )
    await root.run(
        mode=mode,
        allow_index_dropping=settings.allow_index_dropping,
        use_transaction=settings.use_transaction,
    )


@migrations.command()
@click.option(
    "--forward",
    "direction",
    required=False,
    flag_value="FORWARD",
    help="Roll the migrations forward. This is default",
)
@click.option(
    "--backward",
    "direction",
    required=False,
    flag_value="BACKWARD",
    help="Roll the migrations backward",
)
@click.option(
    "-d",
    "--distance",
    required=False,
    help="How many migrations should be done since the current? "
    "0 - all the migrations. Default is 0",
)
@click.option(
    "-uri",
    "--connection-uri",
    required=False,
    type=str,
    help="MongoDB connection URI",
)
@click.option(
    "-db", "--database_name", required=False, type=str, help="DataBase name"
)
@click.option(
    "-p",
    "--path",
    required=False,
    type=str,
    help="Path to the migrations directory",
)
@click.option(
    "--allow-index-dropping/--forbid-index-dropping",
    required=False,
    default=False,
    help="if allow-index-dropping is set, Beanie will drop indexes from your collection",
)
@click.option(
    "--use-transaction/--no-use-transaction",
    required=False,
    default=True,
    help="Enable or disable the use of transactions during migration. "
    "When enabled (--use-transaction), Beanie uses transactions for migration, "
    "which necessitates a replica set. When disabled (--no-use-transaction), "
    "migrations occur without transactions.",
)
def migrate(
    direction,
    distance,
    connection_uri,
    database_name,
    path,
    allow_index_dropping,
    use_transaction,
):
    settings_kwargs = {}
    if direction:
        settings_kwargs["direction"] = direction
    if distance:
        settings_kwargs["distance"] = distance
    if connection_uri:
        settings_kwargs["connection_uri"] = connection_uri
    if database_name:
        settings_kwargs["database_name"] = database_name
    if path:
        settings_kwargs["path"] = path
    if allow_index_dropping:
        settings_kwargs["allow_index_dropping"] = allow_index_dropping
    settings_kwargs["use_transaction"] = use_transaction
    settings = MigrationSettings(**settings_kwargs)

    asyncio.run(run_migrate(settings))


@migrations.command()
@click.option("-n", "--name", required=True, type=str, help="Migration name")
@click.option(
    "-p",
    "--path",
    required=True,
    type=str,
    help="Path to the migrations directory",
)
def new_migration(name, path):
    path = Path(path)
    ts_string = datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"{ts_string}_{name}.py"

    shutil.copy(template.__file__, path / file_name)


if __name__ == "__main__":
    migrations()
