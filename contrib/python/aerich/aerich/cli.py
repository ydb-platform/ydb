from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import cast

import asyncclick as click
from asyncclick import Context, UsageError
from tortoise.backends.base.config_generator import expand_db_url

from aerich import Command
from aerich._compat import imports_tomlkit, tomllib
from aerich.enums import Color
from aerich.exceptions import DowngradeError
from aerich.migrate import Migrate
from aerich.utils import (
    CONFIG_DEFAULT_VALUES,
    _load_tortoise_aerich_config,
    add_src_path,
    get_models_describe,
    get_tortoise_config,
    import_py_module,
)
from aerich.version import __version__


def _check_aerich_models_included(tortoise_config: dict) -> None:
    # e.g.: tortoise_config = {'apps': {'app_1': {'models': ['models']}}}
    apps: dict[str, dict[str, list]] = tortoise_config.get("apps", {})
    app_values: list[dict[str, list[str]]] = list(apps.values())
    all_models: set[str] = {m for model in app_values for m in model.get("models", [])}
    if not all_models:
        return
    aerich_item = "aerich.models"
    if aerich_item in all_models:
        return
    if len(apps) == 1:
        # Auto add 'aerich.models' if there is only one app
        apps[list(apps)[0]]["models"].append(aerich_item)
        return
    raise UsageError(f"You have to add {aerich_item!r} in the models of your tortoise config")


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, "-V", "--version")
@click.option("-c", "--config", default="pyproject.toml", show_default=True, help="Config file.")
@click.option("--app", required=False, help="Tortoise-ORM app name.")
@click.pass_context
async def cli(ctx: Context, config: str, app: str) -> None:
    if any(opt in sys.argv for opt in ctx.help_option_names):
        # Skip init to print help message
        return
    ctx.ensure_object(dict)
    ctx.obj["config_file"] = config

    invoked_subcommand = ctx.invoked_subcommand
    if invoked_subcommand != "init":
        config_path = Path(config)
        if not config_path.exists():
            raise UsageError(
                "You need to run `aerich init` first to create the config file.", ctx=ctx
            )
        tortoise_config, aerich_config = _load_tortoise_aerich_config(
            ctx=ctx, config_file=config_path
        )
        try:
            location = aerich_config["location"]
        except KeyError as e:
            raise UsageError(
                "You need run `aerich init` again when upgrading to aerich 0.6.0+."
            ) from e
        if not app:
            try:
                apps_config = cast(dict, tortoise_config["apps"])
            except KeyError:
                raise UsageError('Config must define "apps" section') from None
            app = list(apps_config.keys())[0]
        command = Command(tortoise_config=tortoise_config, app=app, location=location)
        if inspectdb_fields := aerich_config.get("inspectdb"):
            command._inspectdb_fields = cast(dict[str, str], inspectdb_fields)
        # The 'init-db' subcommand requires it to not init when aenter
        command._init_when_aenter = False
        # Call ``command.__aexit__()`` when the context is popped
        ctx.obj["command"] = await ctx.with_async_resource(command)
        _check_aerich_models_included(tortoise_config)
        if invoked_subcommand not in ("init-db", "init-migrations", "fix-migrations"):
            if not Path(location, app).exists():
                raise UsageError(
                    "You need to run `aerich init-db` first to initialize the database.", ctx=ctx
                )
            await command.init(offline="--offline" in sys.argv)


@cli.command(help="Generate a migration file for the current state of the models.")
@click.option("--name", default="update", show_default=True, help="Migration name.")
@click.option("--empty", default=False, is_flag=True, help="Generate an empty migration file.")
@click.option("--no-input", default=False, is_flag=True, help="Do not ask for prompt.")
@click.option(
    "--offline", default=False, is_flag=True, help="Generate migration without connecting to db."
)
@click.pass_context
async def migrate(ctx: Context, name: str, empty: bool, no_input: bool, offline: bool) -> None:
    command = ctx.obj["command"]
    ret = await command.migrate(name, empty, no_input, offline)
    if ret is None:
        return click.secho(
            "Aborted! You may need to run `aerich heads` to list avaliable unapplied migrations.",
            fg=Color.yellow,
        )
    if not ret:
        if not offline:
            # Auto fill MODELS_STATE to old style migration file
            all_migrations = Migrate.get_all_version_modules()
            last_one = all_migrations[-1]
            module = import_py_module(last_one)
            if not getattr(module, "MODELS_STATE", None):
                upgrade = await module.upgrade(None)
                downgrade = await module.downgrade(None)
                models_state = get_models_describe(command.app)
                content = Migrate.build_migration_file_text(
                    upgrade, models_state=models_state, downgrade_sql=downgrade
                )
                file = Path(Migrate.migrate_location, last_one.name + ".py")
                file.write_text(content, encoding="utf-8")
                click.echo(f"Filled `MODELS_STATE` to migration file {file.name}")
        return click.secho("No changes detected", fg=Color.yellow)
    click.secho(f"Success creating migration file {ret}", fg=Color.green)


@cli.command(help="Upgrade to specified migration version.")
@click.option(
    "--in-transaction",
    "-i",
    default=True,
    type=bool,
    help="Make migrations in a single transaction or not. Can be helpful for large migrations or creating concurrent indexes. Overwrites the value in a migration file",
)
@click.option(
    "--fake",
    default=False,
    is_flag=True,
    help="Mark migrations as run without actually running them.",
)
@click.pass_context
async def upgrade(ctx: Context, in_transaction: bool, fake: bool) -> None:
    command = ctx.obj["command"]
    migrated = await command.upgrade(run_in_transaction=in_transaction, fake=fake)
    if not migrated:
        return click.secho("No upgrade items found", fg=Color.yellow)
    for version_file in migrated:
        if fake:
            click.echo(f"Upgrading to {version_file}... " + click.style("FAKED", fg=Color.green))
        else:
            click.secho(f"Success upgrading to {version_file}", fg=Color.green)


@cli.command(help="Downgrade to specified version.")
@click.option(
    "-v",
    "--version",
    default=-1,
    type=int,
    show_default=False,
    help="Specified version, default to last migration.",
)
@click.option(
    "-d",
    "--delete",
    is_flag=True,
    default=False,
    show_default=True,
    help="Also delete the migration files.",
)
@click.option(
    "--fake",
    default=False,
    is_flag=True,
    help="Mark migrations as run without actually running them.",
)
@click.pass_context
@click.confirmation_option(
    prompt="Downgrade is dangerous: you might lose your data! Are you sure?",
)
async def downgrade(ctx: Context, version: int, delete: bool, fake: bool) -> None:
    command = ctx.obj["command"]
    try:
        files = await command.downgrade(version, delete, fake=fake)
    except DowngradeError as e:
        return click.secho(str(e), fg=Color.yellow)
    for file in files:
        if fake:
            click.echo(f"Downgrading to {file}... " + click.style("FAKED", fg=Color.green))
        else:
            click.secho(f"Success downgrading to {file}", fg=Color.green)


@cli.command(help="Show currently available heads (unapplied migrations).")
@click.pass_context
async def heads(ctx: Context) -> None:
    command = ctx.obj["command"]
    head_list = await command.heads()
    if not head_list:
        return click.secho("No available heads.", fg=Color.green)
    for version in head_list:
        click.secho(version, fg=Color.green)


@cli.command(help="List all migrations.")
@click.pass_context
async def history(ctx: Context) -> None:
    command = ctx.obj["command"]
    versions = await command.history()
    if not versions:
        return click.secho("No migrations created yet.", fg=Color.green)
    for version in versions:
        click.secho(version, fg=Color.green)


def _write_config(config_path: Path, doc: dict, table: dict) -> None:
    tomlkit = imports_tomlkit()

    try:
        doc["tool"]["aerich"] = table
    except KeyError:
        doc["tool"] = {"aerich": table}
    config_path.write_text(tomlkit.dumps(doc))


@cli.command(help="Initialize aerich config and create migrations folder.")
@click.option(
    "-t",
    "--tortoise-orm",
    required=True,
    help="Tortoise-ORM config dict location, like `settings.TORTOISE_ORM`.",
)
@click.option(
    "--location",
    default="./migrations",
    show_default=True,
    help="Migrations folder.",
)
@click.option(
    "-s",
    "--src_folder",
    default=CONFIG_DEFAULT_VALUES["src_folder"],
    show_default=False,
    help="Folder of the source, relative to the project root.",
)
@click.pass_context
async def init(ctx: Context, tortoise_orm: str, location: str, src_folder: str) -> None:
    config_file = ctx.obj["config_file"]

    if os.path.isabs(src_folder):
        src_folder = os.path.relpath(os.getcwd(), src_folder)
    # Add ./ so it's clear that this is relative path
    if not src_folder.startswith("./"):
        src_folder = "./" + src_folder

    # check that we can find the configuration, if not we can fail before the config file gets created
    add_src_path(src_folder)
    get_tortoise_config(ctx=ctx, tortoise_orm=tortoise_orm)
    config_path = Path(config_file)
    table = {"tortoise_orm": tortoise_orm, "location": location, "src_folder": src_folder}
    if not config_path.exists():
        text = "[tool.aerich]" + "".join(f'{os.linesep}{k} = "{v}"' for k, v in table.items())
        config_path.write_text(text, encoding="utf-8")
        click.secho(f"Success writing aerich config to {config_file}", fg=Color.green)
    else:
        content = config_path.read_text("utf-8")
        doc: dict = tomllib.loads(content)
        if (aerich_config := doc.get("tool", {}).get("aerich")) and all(
            aerich_config.get(k) == v for k, v in table.items()
        ):
            click.echo(f"Aerich config {config_file} already inited.")
            if Path(location).exists():
                return
        else:
            item_title = "[tool.aerich]"
            lines = content.splitlines()
            if not (linesep := content[len(content.rstrip()) :].replace(" ", "")):
                linesep = os.linesep
                for sep in ("\n", "\r\n", "\r"):
                    if sep.join(lines).strip() == content.strip():
                        linesep = sep
                        break
            if aerich_config is None or item_title not in content:
                # Add aerich config item
                newlines = [item_title, *[f'{k} = "{v}"' for k, v in table.items()]]
                with config_path.open("a") as f:
                    f.write(linesep)
                    f.writelines([i + linesep for i in newlines])
            else:
                # Modify aerich config
                if "#" not in content:
                    _write_config(config_path, doc, table)
                else:
                    item_index = 0
                    for index, line in enumerate(lines):
                        if line.strip().startswith(item_title):
                            item_index = index
                            break
                    for index in range(item_index + 1, len(lines) + 1):
                        slim = lines[index].strip()
                        if slim.startswith("#"):
                            continue
                        if slim.startswith("["):
                            break
                        for key in table:
                            if re.match(rf"{key}\s*=", slim):
                                lines[index] = f'{key} = "{table.pop(key)}"'
                                break
                        else:
                            continue
                        if not table:
                            break
                    for key, value in table.items():
                        lines.insert(item_index, f'{key} = "{value}"')
                    text = linesep.join(lines)
                    if end := content[len(linesep.join(content.splitlines())) :]:
                        text += end[len(end.rstrip()) :].replace(" ", "")
                    config_path.write_text(text, encoding="utf-8")

            click.secho(f"Success writing aerich config to {config_file}", fg=Color.green)

    Path(location).mkdir(parents=True, exist_ok=True)
    click.secho(f"Success creating migrations folder {location}", fg=Color.green)


@cli.command(help="Generate schema and generate app migration folder.")
@click.option(
    "-s",
    "--safe",
    type=bool,
    is_flag=True,
    default=True,
    help="Create tables only when they do not already exist.",
    show_default=True,
)
@click.option("--pre", required=False, help="SQL to execute before generating schemas.")
@click.pass_context
async def init_db(ctx: Context, safe: bool, pre: str) -> None:
    await _init_app(ctx, safe, pre)


async def _init_app(ctx: Context, safe: bool, pre: str = "", offline: bool = False) -> None:
    command = ctx.obj["command"]
    app = command.app
    dirname = Path(command.location, app)
    try:
        if offline:
            await command.init_migrations(safe)
        else:
            await command.init_db(safe, pre)
        click.secho(f"Success creating app migration folder {dirname}", fg=Color.green)
        click.secho(f'Success generating initial migration file for app "{app}"', fg=Color.green)
        if not offline:
            default_connection = (
                command.tortoise_config["apps"].get(app, {}).get("default_connection", "default")
            )
            db = command.tortoise_config["connections"].get(default_connection, "")
            if isinstance(db, str):
                db = expand_db_url(db)
            if credentials := db.get("credentials"):
                db = credentials.get("database", "") or credentials.get("file_path", "")
            click.secho(f'Success writing schemas to database "{db}"', fg=Color.green)
    except FileExistsError:
        click.secho(
            f"App {app!r} is already initialized. Delete {dirname} and try again.", fg=Color.yellow
        )


@cli.command(help="Generate app migration folder and your first migration.")
@click.option(
    "-s",
    "--safe",
    type=bool,
    is_flag=True,
    default=True,
    help="Create tables only when they do not already exist.",
    show_default=True,
)
@click.pass_context
async def init_migrations(ctx: Context, safe: bool) -> None:
    await _init_app(ctx, safe, offline=True)


@cli.command(help="Prints the current database tables to stdout as Tortoise-ORM models.")
@click.option(
    "-t",
    "--table",
    help="Which tables to inspect.",
    multiple=True,
    required=False,
)
@click.pass_context
async def inspectdb(ctx: Context, table: list[str]) -> None:
    command = ctx.obj["command"]
    ret = await command.inspectdb(table)
    click.secho(ret)


@cli.command(help="Fix migration files to include models state for aerich 0.6.0+.")
@click.pass_context
async def fix_migrations(ctx: Context) -> None:
    command = ctx.obj["command"]
    updated_files = await command.fix_migrations()
    if updated_files:
        count = len(updated_files)
        click.secho(f"Updated {count} migration file{'s' * (count > 1)}:", fg=Color.green)
        for file in updated_files:
            click.echo(f"  - {file}")
    elif updated_files is not None:
        click.secho(
            "No migration files to update. All files are already in the correct format.",
            fg=Color.green,
        )


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
