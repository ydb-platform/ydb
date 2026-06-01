from dataclasses import asdict

from aiohttp import web

from ydb.tools.mnc.agent.services.features import FeatureStatus, features_service
from ydb.tools.mnc.agent.services.tasks import task_service


features_service.set_feature_status("tasks", FeatureStatus.ENABLED)

routes = web.RouteTableDef()


@routes.get("/tasks")
async def get_tasks(request):
    tasks = task_service.get_all_tasks()
    return web.json_response(
        {
            "tasks": [asdict(task.to_schema()) for task in tasks.values()],
            "stats": asdict(task_service.get_task_stats()),
        }
    )


@routes.get("/tasks/{task_id}")
async def get_task(request):
    task_id = request.match_info["task_id"]
    task = task_service.get_task(task_id)
    if not task:
        raise web.HTTPNotFound(text="Task not found")
    return web.json_response(asdict(task.to_schema()))


@routes.get("/tasks/stats/stats")
async def get_task_stats(request):
    return web.json_response(asdict(task_service.get_task_stats()))
