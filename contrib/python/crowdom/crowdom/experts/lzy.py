from dataclasses import dataclass
from typing import Dict, Optional

from lzy.api import op, LzyRemoteEnv
from lzy.api.whiteboard import whiteboard, view
import toloka.client as toloka

from . import registration

# TODO: E2E test: register -> revoke -> ... -> get [for different (task_id, lang) cases]


@whiteboard(tags=['expert_registration'], namespace='default')
@dataclass
class Whiteboard:
    pool: toloka.Pool
    task_id: str
    lang: str
    name_to_code: Dict[str, str]
    name_to_worker_id: Dict[str, str] = None


@op
def approve_expected_codes(client: toloka.TolokaClient, pool_id: str, name_to_code: Dict[str, str]) -> Dict[str, str]:
    return registration.approve_expected_codes(client, pool_id, name_to_code)


def register_experts(
    client: toloka.TolokaClient,
    name_to_code: Dict[str, str],
    pool: toloka.Pool,
    task_id: Optional[str] = None,
    lang: Optional[str] = None,
) -> Dict[str, str]:
    wb = Whiteboard(pool=pool, task_id=task_id, lang=lang, name_to_code=name_to_code)

    env = LzyRemoteEnv()
    with env.workflow('expert_registration', whiteboard=wb):
        wb.name_to_worker_id = approve_expected_codes(client, pool.id, name_to_code)

    return wb.name_to_worker_id


def get_registered_experts(
    client: toloka.TolokaClient,
    task_id: Optional[str] = None,
    lang: Optional[str] = None,
) -> Dict[str, str]:
    # in whiteboards, we store information about expert names, but we also could revoke some registrations, so we have
    # to match registrations from whiteboards with user skills
    env = LzyRemoteEnv()
    name_to_worker_id = {}
    for whiteboard in env.whiteboards([Whiteboard]):
        if task_id and whiteboard.task_id != task_id:
            continue
        if lang and whiteboard.lang != lang:
            continue
        name_to_worker_id.update(whiteboard.name_to_worker_id)
    skills = registration.get_skills(task_id, lang, client)
    worker_ids = {}
    for skill in skills:
        worker_ids |= [user_skill.user_id for user_skill in client.get_user_skills(skill_id=skill.id)]
    return {name: worker_id for name, worker_id in name_to_worker_id.items() if worker_id in worker_ids}
