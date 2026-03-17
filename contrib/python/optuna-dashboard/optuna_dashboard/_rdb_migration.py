from __future__ import annotations

import threading

from bottle import Bottle
from bottle import redirect
from bottle import request
from bottle import SimpleTemplate
from optuna.storages import BaseStorage
from optuna.storages import RDBStorage
from optuna.version import __version__ as optuna_ver

from ._bottle_util import BottleViewReturn


rdb_schema_migrate_lock = threading.Lock()
rdb_schema_needs_migrate = False
rdb_schema_unsupported = False
rdb_schema_template = SimpleTemplate(
    """<!DOCTYPE html>
<html lang="en">
<head>
<title>Incompatible RDB Schema Error - Optuna Dashboard</title>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
body {
    padding: 0;
    height: 100vh;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
}
.wrapper {
    padding: 64px;
    width: 600px;
    background-color: rgb(255, 255, 255);
    box-shadow: rgba(0, 0, 0, 0.08) 0 8px 24px;
    margin: 0px auto;
    border-radius: 8px;
}
</style>
</head>
<body>
    <div class="wrapper">
    <h1>Error: Incompatible RDB Schema</h1>
% if rdb_schema_unsupported:
    <p>Your Optuna version {{ optuna_ver }} seems outdated against the storage version. Please try updating optuna to the latest version by `$ pip install -U optuna`.</p>
% elif rdb_schema_needs_migrate:
    <p>The runtime optuna version {{ optuna_ver }} is no longer compatible with the table schema. Please execute `$ optuna storage upgrade --storage $STORAGE_URL` or press the following button for upgrading the storage.</p>
    <form action="/incompatible-rdb-schema" method="post">
    <button>Migrate</button>
    </form>
% end
    </div>
</body>
</html>"""  # noqa: E501
)


def update_schema_compatibility_flags(storage: RDBStorage) -> None:
    global rdb_schema_needs_migrate, rdb_schema_unsupported

    with rdb_schema_migrate_lock:
        current_version = storage.get_current_version()
        head_version = storage.get_head_version()
        rdb_schema_needs_migrate = current_version != head_version
        rdb_schema_unsupported = current_version not in storage.get_all_versions()


def is_incompatible() -> bool:
    return rdb_schema_needs_migrate or rdb_schema_unsupported


def register_rdb_migration_route(app: Bottle, storage: BaseStorage) -> None:
    if isinstance(storage, RDBStorage):
        update_schema_compatibility_flags(storage)

    @app.get("/incompatible-rdb-schema")
    def get_incompatible_rdb_schema() -> BottleViewReturn:
        if not is_incompatible() or not isinstance(storage, RDBStorage):
            return redirect("/dashboard", 302)
        return rdb_schema_template.render(
            rdb_schema_needs_migrate=rdb_schema_needs_migrate,
            rdb_schema_unsupported=rdb_schema_unsupported,
            optuna_ver=optuna_ver,
        )

    @app.post("/incompatible-rdb-schema")
    def post_incompatible_rdb_schema() -> BottleViewReturn:
        if not isinstance(storage, RDBStorage):
            return redirect("/dashboard", 302)

        global rdb_schema_needs_migrate
        assert not rdb_schema_unsupported
        with rdb_schema_migrate_lock:
            storage.upgrade()
            rdb_schema_needs_migrate = False
        return redirect("/dashboard", 302)

    @app.hook("before_request")
    def check_schema_compatibility() -> None:
        if not isinstance(storage, RDBStorage):
            return

        if request.path != "/" and not request.path.startswith("/dashboard"):
            return

        update_schema_compatibility_flags(storage)
        if is_incompatible():
            return redirect("/incompatible-rdb-schema", 302)
