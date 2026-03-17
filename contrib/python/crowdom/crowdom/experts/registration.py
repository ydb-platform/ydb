from collections import Counter
import datetime
from typing import List, Dict, Optional, Tuple

import toloka.client as toloka
import toloka.client.project.template_builder as tb
import uuid


CODE_LENGTH = 10
PROJECT_PRIVATE_COMMENT = 'expert-registration'
EXPERT_SKILL = 'expert'


def generate_code() -> str:
    return uuid.uuid4().hex[:10]


def generate_codes(worker_names: List[str]) -> Dict[str, str]:
    return {name: generate_code() for name in worker_names}


def get_skill_name(task_id: Optional[str], lang: Optional[str]) -> str:
    return f"{EXPERT_SKILL}{'_' + task_id if task_id is not None else ''}{'_' + lang if lang is not None else ''}"


def get_skill_names(task_id: Optional[str], lang: Optional[str]) -> List[str]:
    skill_names = []
    task_ids = {task_id, None}
    langs = {lang, None}
    for task_id in task_ids:
        for lang in langs:
            skill_names.append(get_skill_name(task_id, lang))
    return sorted(skill_names)


def get_skills(
    task_id: Optional[str],
    lang: Optional[str],
    client: toloka.TolokaClient,
) -> List[Tuple[str, toloka.Skill]]:
    skill_names = get_skill_names(task_id, lang)
    skills = [next(client.get_skills(name=skill_name), None) for skill_name in skill_names]
    return [(name, skill) for name, skill in zip(skill_names, skills) if skill is not None]


def create_registration_project(client: toloka.TolokaClient) -> toloka.Project:
    project = toloka.Project(
        public_name=f"Welcome to {client.get_requester().public_name['EN']}!",
        public_description='Welcome! You will need to enter a code',
        public_instructions='Enter supplied code',
        private_comment=PROJECT_PRIVATE_COMMENT,
        task_spec=toloka.project.task_spec.TaskSpec(
            input_spec={'unused': toloka.project.field_spec.StringSpec(required=False, hidden=True)},
            output_spec={
                'code': toloka.project.field_spec.StringSpec(
                    required=True, min_length=CODE_LENGTH, max_length=CODE_LENGTH
                )
            },
            view_spec=toloka.project.TemplateBuilderViewSpec(
                view=tb.ListViewV1(
                    items=[
                        tb.TextViewV1(content='Enter code:'),
                        tb.TextFieldV1(
                            data=tb.OutputData(path='code'),
                            validation=tb.SchemaConditionV1(
                                schema={
                                    'type': 'string',
                                    'pattern': f'[a-zA-Z0-9]{{{CODE_LENGTH}}}',
                                    'minLength': CODE_LENGTH,
                                    'maxLength': CODE_LENGTH,
                                },
                                hint=f'Code length should be {CODE_LENGTH}, '
                                'you can use only latin alphabet and digits',
                            ),
                        ),
                    ]
                )
            ),
        ),
    )

    return client.create_project(project)


def create_registration_pool(
    client: toloka.TolokaClient,
    expected_date: str = '14.05.1405',
    task_id: Optional[str] = None,
    lang: Optional[str] = None,
) -> toloka.Pool:
    project_id = None
    for project in client.get_projects(status='ACTIVE'):
        if project.private_comment == PROJECT_PRIVATE_COMMENT:
            project_id = project.id
            break
    if project_id is None:
        project_id = create_registration_project(client).id
    skill_name = get_skill_name(task_id, lang)
    expert_skill = next(client.get_skills(name=skill_name), None)
    if expert_skill is None:
        expert_skill = client.create_skill(name=skill_name, hidden=True)
    date = datetime.datetime.strptime(expected_date, '%d.%m.%Y')
    for pool in client.get_pools(project_id=project_id, status='OPEN'):
        if pool.private_comment == skill_name:
            return pool
    pool = toloka.Pool(
        project_id=project_id,
        public_description=skill_name,
        private_comment=skill_name,
        private_name='Pool for expert promotion',
        auto_accept_solutions=False,
        may_contain_adult_content=False,
        reward_per_assignment=0.0,
        assignment_max_duration_seconds=120,
        will_expire=datetime.datetime.utcnow() + datetime.timedelta(days=365),
        filter=(
            (toloka.filter.Skill(expert_skill.id) == None)  # noqa
            & (toloka.filter.DateOfBirth < int((date + datetime.timedelta(days=2)).timestamp()))
            & (toloka.filter.DateOfBirth > int((date - datetime.timedelta(days=2)).timestamp()))
        ),
    )
    pool.set_mixer_config(golden_tasks_count=1)

    pool.quality_control.add_action(
        collector=toloka.collectors.AnswerCount(),
        conditions=[toloka.conditions.AssignmentsAcceptedCount > 0],
        action=toloka.actions.SetSkill(skill_id=expert_skill.id, skill_value=1),
    )

    pool = client.create_pool(pool)
    client.create_task(
        toloka.task.Task(
            input_values={'unused': ''},
            known_solutions=[toloka.task.BaseTask.KnownSolution(output_values={'code': 'AAAAAAAAAA'})],
            pool_id=pool.id,
            infinite_overlap=True,
        )
    )
    client.open_pool(pool.id)
    return pool


def get_entered_code(assignment: toloka.Assignment) -> str:
    return assignment.solutions[0].output_values['code']


def approve_expected_codes(client: toloka.TolokaClient, pool_id: str, name_to_code: Dict[str, str]) -> Dict[str, str]:
    name_to_worker_id = {}
    assignments = list(client.get_assignments(pool_id=pool_id, status=[toloka.Assignment.SUBMITTED]))
    code_counter = Counter(get_entered_code(assignment) for assignment in assignments)
    code_to_name = {v: k for k, v in name_to_code.items()}
    for assignment in assignments:
        worker_id = assignment.user_id
        code = get_entered_code(assignment)
        if code not in code_to_name:
            print(f'Unexpected code {code} from worker {worker_id}')
            client.reject_assignment(assignment_id=assignment.id, public_comment='Unexpected code')
        elif code_counter[code] > 1:
            print(f'More than one answer for code {code}, this one from {worker_id}')
            client.reject_assignment(assignment_id=assignment.id, public_comment='Duplicated code')
        else:
            name_to_worker_id[code_to_name[code]] = worker_id
            client.accept_assignment(assignment_id=assignment.id, public_comment='')
    return name_to_worker_id


def revoke_expert_access(
    client: toloka.TolokaClient, worker_id: str, task_id: Optional[str] = None, lang: Optional[str] = None
):
    skill_name = get_skill_name(task_id, lang)
    skill = next(client.get_skills(name=skill_name), None)
    if skill is None:
        print(f'Warning: no such skill found: {skill_name}')
    else:
        found_skills = list(client.get_user_skills(user_id=worker_id, skill_id=skill.id))
        if not found_skills:
            print(f'Warning: no {skill_name} skill found for worker {worker_id}')
        else:
            for skill in found_skills:
                client.delete_user_skill(skill.id)
