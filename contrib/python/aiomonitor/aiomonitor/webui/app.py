from __future__ import annotations

import asyncio
import dataclasses
import importlib.resources
import sys
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Mapping, Tuple

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum

import trafaret as t
from aiohttp import web
from jinja2 import Environment, PackageLoader, select_autoescape

from .utils import APIParams, check_params

if TYPE_CHECKING:
    from ..monitor import Monitor


@dataclasses.dataclass
class WebUIContext:
    monitor: Monitor
    jenv: Environment


class TaskTypes(StrEnum):
    RUNNING = "running"
    TERMINATED = "terminated"


@dataclasses.dataclass
class TaskTypeParams(APIParams):
    task_type: TaskTypes

    @classmethod
    def get_checker(cls):
        return t.Dict({
            t.Key("task_type", default=TaskTypes.RUNNING): t.Enum(
                TaskTypes.RUNNING,
                TaskTypes.TERMINATED,
            ),
        })


@dataclasses.dataclass
class TaskIdParams(APIParams):
    task_id: str

    @classmethod
    def get_checker(cls) -> t.Trafaret:
        return t.Dict({
            t.Key("task_id"): t.String,
        })


@dataclasses.dataclass
class ListFilterParams(APIParams):
    filter: str
    persistent: bool

    @classmethod
    def get_checker(cls) -> t.Trafaret:
        return t.Dict({
            t.Key("filter", default=""): t.String(allow_blank=True),
            t.Key("persistent", default=False): t.ToBool,
        })


@dataclasses.dataclass
class NavigationItem:
    title: str
    current: bool


nav_menus: Mapping[str, NavigationItem] = {
    "/": NavigationItem(
        title="Dashboard",
        current=False,
    ),
    "/about": NavigationItem(
        title="About",
        current=False,
    ),
}


def get_navigation_info(
    route: str,
) -> Tuple[NavigationItem, Mapping[str, NavigationItem]]:
    nav_items: Dict[str, NavigationItem] = {}
    current_item = None
    for path, item in nav_menus.items():
        is_current = path == route
        nav_items[path] = NavigationItem(item.title, is_current)
        if is_current:
            current_item = item
    if current_item is None:
        raise web.HTTPNotFound
    return current_item, nav_items


async def show_list_page(request: web.Request) -> web.Response:
    ctx: WebUIContext = request.app["ctx"]
    nav_info, nav_items = get_navigation_info(request.path)
    template = ctx.jenv.get_template("index.html")
    async with check_params(request, TaskTypeParams) as params:
        output = template.render(
            navigation=nav_items,
            page={
                "title": nav_info.title,
            },
            current_list_type=params.task_type,
            list_types=[
                {"id": TaskTypes.RUNNING, "title": "Running"},
                {"id": TaskTypes.TERMINATED, "title": "Terminated"},
            ],
        )
        return web.Response(body=output, content_type="text/html")


async def show_about_page(request: web.Request) -> web.Response:
    ctx: WebUIContext = request.app["ctx"]
    nav_info, nav_items = get_navigation_info(request.path)
    template = ctx.jenv.get_template("about.html")
    output = template.render(
        navigation=nav_items,
        page={
            "title": nav_info.title,
        },
    )
    return web.Response(body=output, content_type="text/html")


async def show_trace_page(request: web.Request) -> web.Response:
    ctx: WebUIContext = request.app["ctx"]
    template = ctx.jenv.get_template("trace.html")
    async with check_params(request, TaskIdParams) as params:
        if request.path.startswith("/trace-running"):
            trace_data = ctx.monitor.format_running_task_stack(params.task_id)
        elif request.path.startswith("/trace-terminated"):
            trace_data = ctx.monitor.format_terminated_task_stack(params.task_id)
        else:
            raise RuntimeError("should not reach here")
        output = template.render(
            navigation=nav_menus,
            page={
                "title": f"Task trace for {params.task_id}",
            },
            trace_data=trace_data,
        )
        return web.Response(body=output, content_type="text/html")


async def get_version(request: web.Request) -> web.Response:
    return web.json_response(
        data={
            "value": version("aiomonitor"),
        }
    )


async def get_task_count(request: web.Request) -> web.Response:
    async with check_params(request, TaskTypeParams) as params:
        ctx: WebUIContext = request.app["ctx"]
        if params.task_type == TaskTypes.RUNNING:
            count = len(asyncio.all_tasks(ctx.monitor._monitored_loop))
        elif params.task_type == TaskTypes.TERMINATED:
            count = len(ctx.monitor._terminated_history)
        else:
            raise RuntimeError("should not reach here")
        return web.json_response(
            data={
                "value": count,
            }
        )


async def get_live_task_list(request: web.Request) -> web.Response:
    ctx: WebUIContext = request.app["ctx"]
    async with check_params(request, ListFilterParams) as params:
        tasks = ctx.monitor.format_running_task_list(
            params.filter,
            params.persistent,
        )
        return web.json_response(
            data={
                "tasks": [
                    {
                        "task_id": t.task_id,
                        "state": t.state,
                        "name": t.name,
                        "coro": t.coro,
                        "created_location": t.created_location,
                        "since": t.since,
                        "is_root": t.created_location == "-",
                    }
                    for t in tasks
                ]
            }
        )


async def get_terminated_task_list(request: web.Request) -> web.Response:
    ctx: WebUIContext = request.app["ctx"]
    async with check_params(request, ListFilterParams) as params:
        tasks = ctx.monitor.format_terminated_task_list(
            params.filter,
            params.persistent,
        )
        return web.json_response(
            data={
                "tasks": [
                    {
                        "task_id": t.task_id,
                        "name": t.name,
                        "coro": t.coro,
                        "started_since": t.started_since,
                        "terminated_since": t.terminated_since,
                    }
                    for t in tasks
                ]
            }
        )


async def cancel_task(request: web.Request) -> web.Response:
    ctx: WebUIContext = request.app["ctx"]
    async with check_params(request, TaskIdParams) as params:
        try:
            coro_repr = await ctx.monitor.cancel_monitored_task(params.task_id)
            return web.json_response(
                data={
                    "msg": f"Successfully cancelled {params.task_id}",
                    "detail": coro_repr,
                },
            )
        except ValueError as e:
            return web.json_response(
                status=404,
                data={"msg": repr(e)},
            )


async def init_webui(monitor: Monitor) -> web.Application:
    jenv = Environment(
        loader=PackageLoader("aiomonitor.webui"), autoescape=select_autoescape()
    )
    app = web.Application()
    app["ctx"] = WebUIContext(
        monitor=monitor,
        jenv=jenv,
    )
    app.router.add_route("GET", "/", show_list_page)
    app.router.add_route("GET", "/about", show_about_page)
    app.router.add_route("GET", "/trace-running", show_trace_page)
    app.router.add_route("GET", "/trace-terminated", show_trace_page)
    app.router.add_route("GET", "/api/version", get_version)
    app.router.add_route("POST", "/api/task-count", get_task_count)
    app.router.add_route("POST", "/api/live-tasks", get_live_task_list)
    app.router.add_route("POST", "/api/terminated-tasks", get_terminated_task_list)
    app.router.add_route("DELETE", "/api/task", cancel_task)
    app.router.add_static("/static", importlib.resources.files(__package__) / "static")
    return app
