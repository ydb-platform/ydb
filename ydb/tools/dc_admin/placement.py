"""Online migration of State Storage ring groups using versioned YDB CLI configs."""

import shlex
import subprocess
import tempfile
from pathlib import Path

import yaml

SERVICES = {
    "StateStorageConfig": "explicit_state_storage_config",
    "StateStorageBoardConfig": "explicit_state_storage_board_config",
    "SchemeBoardConfig": "explicit_scheme_board_config",
}


def run(argv, progress):
    if progress:
        progress("$ " + shlex.join(argv))
    result = subprocess.run(argv, capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise ValueError(result.stderr.strip() or f"CLI завершился с кодом {result.returncode}")
    if "Unable to bump" in result.stderr:
        raise ValueError(result.stderr.strip())
    return result.stdout


def fetch(cli, explicit, progress=None):
    argv = [*cli(), "admin", "cluster", "config", "fetch"]
    if explicit:
        argv.append("--v2-explicit-sections")
    try:
        doc = yaml.safe_load(run(argv, progress))
    except yaml.YAMLError as error:
        raise ValueError("Ожидался один YAML-документ MainConfig.") from error
    if not isinstance(doc, dict) or doc.get("metadata", {}).get("kind") != "MainConfig":
        raise ValueError("Ожидался версионированный MainConfig.")
    return doc


def services(doc):
    domains = doc["config"]["domains_config"]
    return {name: domains[key] for name, key in SERVICES.items()}


def snapshot(cli):
    return services(fetch(cli, True))


def rings(config):
    return [config["ring"]] if "ring" in config else config["ring_groups"]


def nodes(group):
    # The stand uses a separate ring per static node.
    if set(group) - {"ring", "nto_select", "write_only", "ring_group_actor_id_offset"} or any(
        set(r) != {"node"} or len(r["node"]) != 1 for r in group["ring"]
    ):
        raise ValueError("Ожидается конфигурация с одним узлом на кольцо.")
    return sorted(r["node"][0] for r in group["ring"])


def describe(configs, locations):
    return {
        key: [
            {"dcs": sorted({locations[n] for n in nodes(g)}), "writeOnly": g.get("write_only", False)}
            for g in rings(config)
        ]
        for key, config in configs.items()
    }


def next_step(config, target):
    groups = rings(config)
    if len(groups) == 1:
        (old,) = groups
        if nodes(old) == target and old["nto_select"] == len(target):
            return None
        new = {
            "ring": [{"node": [n]} for n in target],
            "nto_select": len(target),
            "ring_group_actor_id_offset": int(not old.get("ring_group_actor_id_offset", 0)),
            "write_only": True,
        }
        groups = [old, new]
    elif len(groups) == 2:
        first, second = groups
        if nodes(second) == target and second["nto_select"] == len(target):
            if second.get("write_only"):
                groups = [first, {k: v for k, v in second.items() if k != "write_only"}]
            else:
                groups = [second, dict(first, write_only=True)]
        elif nodes(first) == target and first["nto_select"] == len(target) and second.get("write_only"):
            groups = [first]
        else:
            raise ValueError("Идёт миграция на другой набор узлов.")
    else:
        raise ValueError("Неизвестная конфигурация ring groups.")
    return {**{k: v for k, v in config.items() if k not in ("ring", "ring_groups")}, "ring_groups": groups}


def migrate(cli, locations, datacenters, cancel, progress):
    if not set(datacenters) <= set(locations.values()):
        raise ValueError("Выбранный DC отсутствует в топологии стенда.")
    target = sorted(n for n, dc in locations.items() if dc in datacenters)
    previous = None
    with tempfile.TemporaryDirectory(prefix="dc-admin-placement-") as directory:
        for index in range(1, 6):
            live = fetch(cli, True, progress)
            configs = services(live)
            if previous and any(rings(configs[k]) != rings(v) for k, v in previous.items()):
                raise ValueError("Размещение изменилось. Проверьте состояние перед продолжением.")
            changes = {name: next_step(cfg, target) for name, cfg in configs.items()}
            if not any(changes.values()):
                progress("Размещение подтверждено: " + ", ".join(datacenters))
                return
            if index == 5:
                break
            if any(len(rings(cfg)) > 1 for cfg in configs.values()):
                if cancel.wait(60):
                    raise ValueError("Миграция остановлена. Для продолжения выберите те же DC.")
                fresh = fetch(cli, True, progress)
                if services(fresh) != configs:
                    raise ValueError("Размещение изменилось во время ожидания.")
                live = fresh
            # The enriched export also materializes BlobStorage. Use the plain export as the write base.
            doc = fetch(cli, False, progress)
            if doc["metadata"] != live["metadata"] or not isinstance(doc["metadata"].get("version"), int):
                raise ValueError("Версия YAML изменилась. Повторите операцию.")
            before = Path(directory) / f"before-{index}.yaml"
            before.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True))
            domains = doc["config"].setdefault("domains_config", {})
            legacy = domains.get("state_storage", [])
            if len(legacy) > 1:
                raise ValueError("Ожидалась одна конфигурация StateStorage для домена стенда.")
            domains.pop("state_storage", None)
            previous = {}
            for name, key in SERVICES.items():
                previous[name] = changes[name] or configs[name]
                domains[key] = previous[name]
            path = Path(directory) / f"step-{index}.yaml"
            path.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True))
            args = ["diff", "-U0", str(before), str(path)]
            progress("$ " + shlex.join(args))
            diff = subprocess.run(args, capture_output=True, text=True, timeout=30)
            if diff.returncode not in (0, 1):
                raise ValueError(diff.stderr.strip() or "Не удалось сравнить конфиги.")
            progress(diff.stdout.rstrip() or "Файлы идентичны.")
            if cancel.is_set():
                raise ValueError("Миграция остановлена.")
            output = run([*cli(), "admin", "storage", "replace", "-f", str(path)], progress)
            if output.strip():
                progress(output.strip())
    raise ValueError("Не удалось подтвердить размещение.")
