from typing import List
from fastapi import APIRouter, HTTPException

from ydb.tools.mnc.agent.services.tasks import task_service
from ydb.tools.mnc.agent.services.features import features_service, FeatureStatus
from ydb.tools.mnc.agent.schemas.task import TaskSchema, TaskStatsSchema

features_service.set_feature_status("tasks", FeatureStatus.ENABLED)


router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("", response_model=List[TaskSchema])
async def get_tasks():
    """Get all tasks."""
    tasks = task_service.get_all_tasks()
    return {
        "tasks": [
            {
                "id": task.task_id,
                "type": task.__class__.__name__,
                "status": task.status.value,
                "created_at": task.created_at,
                "started_at": task.started_at,
                "completed_at": task.completed_at,
                "result": task.result,
                "error": task.error,
                "delay": task.delay
            }
            for task in tasks.values()
        ],
        "stats": task_service.get_task_stats()
    }


@router.get("/{task_id}", response_model=TaskSchema)
async def get_task(task_id: str):
    """Get task by ID."""
    task = task_service.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return task.to_schema()


@router.get("/stats/stats", response_model=TaskStatsSchema)
async def get_task_stats():
    """Get task service statistics."""
    return task_service.get_task_stats()
