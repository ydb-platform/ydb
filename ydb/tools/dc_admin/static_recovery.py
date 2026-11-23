"""Offline recovery of static group 0 on the three-DC systemd test stand."""

import os
import json
from pathlib import Path
import shlex
import subprocess
import tempfile


def require(condition, message):
    if not condition:
        raise ValueError(message)


def second_dc(cfg, dc, devices):
    ss = cfg["BlobStorageConfig"]["ServiceSet"]
    (group,) = ss["Groups"]
    require(
        group["GroupID"] == 0 and group.get("Mirror3dcState") in ("DC_FAILURE_INV1", "DC_FAILURE_INV2"),
        "Требуется статическая группа 0 в подтверждённом DC_FAILURE_INV1 или DC_FAILURE_INV2.",
    )
    locations = {n["NodeId"]: n["Location"]["DataCenter"] for n in cfg["AllNodes"]}
    disks = {(p["NodeID"], p["PDiskID"]): p for p in ss["PDisks"]}
    roles = {}
    for ring in group["Rings"]:
        members = [v for domain in ring["FailDomains"] for v in domain["VDiskLocations"]]
        names = {locations[v["NodeID"]] for v in members}
        require(len(names) == 1 and len(members) == 3, "Неизвестная топология статической группы.")
        (name,) = names
        require(name not in roles, "Несколько realm в одном DC.")
        roles[name] = ring.get("State", "INCLUDED")
        if name != dc:
            continue
        paths = {Path(disks[v["NodeID"], v["PDiskID"]]["Path"]).resolve() for v in members}
        require(paths == devices.get(name), "Диски сервиса не совпадают с расположением статической группы.")
    require(
        sorted(roles.values()) == ["EXCLUDED", "INCLUDED", "INCLUDED"] and roles.get(dc) == "INCLUDED",
        "Выживший DC должен быть INCLUDED в исходной группе.",
    )
    return next(name for name, role in roles.items() if role == "INCLUDED" and name != dc)


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save(path, value):
    with path.with_suffix(".tmp").open("w") as f:
        json.dump(value, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    path.with_suffix(".tmp").replace(path)
    sync_dir(path.parent)


def run_recovery(dc, inventory, ydbd, archive, cancel=None, progress=None, state_dcs=None):
    selected = sorted(state_dcs) if state_dcs is not None else [dc]
    mode = "state-storage" if state_dcs is not None else "group"
    require(selected and len(set(selected)) == len(selected), "Нужен список работающих DC.")
    require(
        len(inventory) == 9 and all(sum(h["dc"] == name for h in inventory.values()) == 3 for name in selected),
        "Требуется по три ноды в каждом выбранном DC.",
    )
    units = sorted(u for u, h in inventory.items() if h["dc"] in selected)
    paths = {u: Path("/dev/mapper") / (u[: -len(".service")] + "_pdisk") for u in units}
    devices = {}
    for u, path in paths.items():
        require(path.is_block_device(), f"Устройство {path} недоступно.")
        devices.setdefault(inventory[u]["dc"], set()).add(path.resolve())
    require(
        len({p.resolve() for p in paths.values()}) == len(units), "Пути PDisk должны указывать на разные устройства."
    )
    log = []

    def publish(line):
        log.append(line)
        if progress:
            progress("\n".join(log))

    def command(argv, capture=False, rollback=False, quiet=False):
        if cancel and cancel.is_set() and not rollback:
            raise ValueError("Операция остановлена между командами.")
        argv = list(map(str, argv))
        display = "$ " + shlex.join(argv)
        if not quiet:
            publish(display)
        result = subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=180)
        if result.returncode:
            raise ValueError(f"{display}: {(result.stderr or result.stdout).strip()[-1500:] or result.returncode}")
        if not capture and result.stdout.strip():
            publish(result.stdout.strip())
        return result.stdout

    def states():
        return {
            unit: command(["systemctl", "show", "-p", "ActiveState", "--value", unit], True, quiet=True).strip()
            for unit in inventory
        }

    def read(unit):
        return json.loads(command([ydbd, "admin", "bs", "disk", "metadata", "read", "--json", paths[unit]], True))

    archive.mkdir(mode=0o700, parents=True, exist_ok=True)
    pending = archive / "pending"
    resuming = pending.exists()
    status = states()
    require(
        all(status[u] in ("inactive", "failed") for u in inventory if u not in units),
        "Список работающих DC изменился. Обновите окно.",
    )
    ctl = ["sudo", "-n", "systemctl"]
    if resuming:
        folder = pending.resolve(strict=True)
        operation = json.loads((folder / "operation.json").read_text())
        require(
            operation["mode"] == mode and operation["datacenters"] == selected, "Продолжите ранее начатую операцию."
        )
        restart = operation["restart"]
        require(restart and set(restart) <= set(units), "Некорректный список сервисов в архиве.")
    else:
        require(
            all(status[u] in ("active", "inactive", "failed") for u in units),
            "Дождитесь завершения запуска или остановки нод.",
        )
        restart = [u for u in units if status[u] == "active"]
        require({inventory[u]["dc"] for u in restart} == set(selected), "Список работающих DC изменился.")
        if mode == "group":
            require(restart == units, "Все три ноды выбранного DC должны быть запущены.")
        folder = Path(tempfile.mkdtemp(dir=archive, prefix="group0-"))
        with (folder / "dc").open("w") as f:
            f.write(",".join(selected))
            f.flush()
            os.fsync(f.fileno())
        save(folder / "operation.json", {"mode": mode, "datacenters": selected, "restart": restart})
        sync_dir(folder)
        pending.symlink_to(folder.resolve())
        sync_dir(archive)

    target_path = folder / "target.json"
    complete = folder / "verified.json"
    try:
        if not complete.exists():
            command([*ctl, "stop", *units])
            require(all(v in ("inactive", "failed") for v in states().values()), "Не все ноды остановлены.")
            records = {u: read(u) for u in units}
            if not target_path.exists():
                newest = max(units, key=lambda u: int(records[u]["CommittedStorageConfig"]["Generation"]))
                source = records[newest]["CommittedStorageConfig"]
                original = folder / (newest + ".committed.json")
                references = {}
                for unit in units:
                    snapshot = folder / (unit + ".committed.json")
                    save(snapshot, records[unit]["CommittedStorageConfig"])
                    reference = references.setdefault(inventory[unit]["dc"], snapshot)
                    if snapshot != reference:
                        command(["diff", "-q", reference, snapshot])
                publish("Исходные конфиги сверены внутри каждого выбранного DC.")
                for u, record in records.items():
                    cfg = record["CommittedStorageConfig"]
                    require(
                        source.get("SelfAssemblyUUID") and cfg.get("SelfAssemblyUUID") == source["SelfAssemblyUUID"],
                        "PDisk принадлежит другому кластеру.",
                    )
                    require(
                        int(cfg["Generation"]) < int(source["Generation"]) or cfg == source,
                        "Обнаружен более новый или конфликтующий конфиг.",
                    )
                    prop = record.get("ProposedStorageConfig")
                    require(
                        not prop or prop == source or int(prop["Generation"]) < int(source["Generation"]),
                        "Найдена незавершённая конфигурация.",
                    )
                    save(folder / (u + ".json"), record)
                arguments = [second_dc(source, dc, devices), dc] if mode == "group" else selected
                helper = archive.parent / "dc-admin-prepare-solo"
                prepared = command([helper, mode, folder / (newest + ".json"), *arguments], True)
                if mode == "state-storage" and all(
                    record["CommittedStorageConfig"] == json.loads(prepared)["CommittedStorageConfig"]
                    for record in records.values()
                ):
                    command([*ctl, "start", *restart])
                    pending.unlink()
                    sync_dir(archive)
                    return {
                        "command": "StateStorage · " + ", ".join(selected),
                        "exitCode": 0,
                        "output": "\n".join(log + ["Размещение уже соответствует работающим DC."]),
                    }
                prepared_path = folder / "prepared.json"
                save(prepared_path, json.loads(prepared))
                candidate = folder / "candidate.json"
                publish("$ " + shlex.join(["cat", str(prepared_path)]) + " > " + shlex.quote(str(candidate)))
                with candidate.open("wb") as output:
                    subprocess.run(["cat", str(prepared_path)], stdout=output, check=True, timeout=30)
                    os.fsync(output.fileno())
                new_config = folder / "new.committed.json"
                save(new_config, json.loads(candidate.read_text())["CommittedStorageConfig"])
                diff_path = folder / "config.diff"
                args = ["diff", "-u", str(original), str(new_config)]
                publish("$ " + shlex.join(args) + " > " + shlex.quote(str(diff_path)))
                with diff_path.open("wb") as output:
                    diff = subprocess.run(args, stdout=output, stderr=subprocess.PIPE, timeout=30)
                    os.fsync(output.fileno())
                require(diff.returncode in (0, 1), "Не удалось получить различия старого и нового конфигов.")
                publish(diff_path.read_text().rstrip())
                candidate.replace(target_path)
                sync_dir(folder)
            elif (folder / "config.diff").exists():
                publish(f"Сохранённый diff: {folder / 'config.diff'}")
            target = json.loads(target_path.read_text())
            for u in units:
                backup = json.loads((folder / (u + ".json")).read_text())
                require(records[u] == backup or (u in units and records[u] == target), f"Метаданные {u} изменились.")
            for u in units:
                if records[u] != target:
                    command(
                        [
                            ydbd,
                            "admin",
                            "bs",
                            "disk",
                            "metadata",
                            "write",
                            "--validate-config",
                            "--from-json",
                            target_path,
                            paths[u],
                        ]
                    )
                require(read(u) == target, f"Проверка записи {u} не прошла.")
            save(complete, True)
        command([*ctl, "start", *restart])
    except Exception as error:
        if not target_path.exists():
            command([*ctl, "start", *restart], rollback=True)
            pending.unlink()
            sync_dir(archive)
        raise ValueError(f"{error}. Каталог операции: {folder}") from error
    pending.unlink()
    sync_dir(archive)
    return {
        "command": f"Группа 0 → Solo DC ({dc})" if mode == "group" else "StateStorage · " + ", ".join(selected),
        "exitCode": 0,
        "output": "\n".join(log + [f"Проверено PDisk: {len(units)}. Резервные копии: {folder}"]),
    }
