from collections import defaultdict
from contextlib import AbstractContextManager
from dataclasses import dataclass, is_dataclass
from typing import List, Type, Tuple, Optional, Dict, Iterable, Any

import toloka.client as toloka

import crowdom.base as base

obj_primitive_field_types = {str, int, bool, list}

validation_enabled = True


class DisableValidation(AbstractContextManager):
    def __enter__(self):
        global validation_enabled
        validation_enabled = False

    def __exit__(self, exc_type, exc, exc_tb):
        global validation_enabled
        validation_enabled = True


def obj_to_values(obj: base.Object) -> dict:
    if issubclass(type(obj), base.Class):
        assert isinstance(obj.value, str), 'class enum must have string values'
        return {base.CLASS_OBJ_FIELD: obj.value}
    values = {}
    for field_name, field_value in obj.__dict__.items():
        if any(isinstance(field_value, t) for t in obj_primitive_field_types):
            values[field_name] = field_value
        else:
            raise ValueError(f'object field "{field_name}" has unsupported type {type(field_value)}')
    return values


def values_to_obj(values: dict, obj_cls: Type[base.Object]) -> base.Object:
    if issubclass(obj_cls, base.Class):
        return obj_cls(values[base.CLASS_OBJ_FIELD])
    fields = {}
    for field_name, field_value in values.items():
        field_type = _get_all_annotations(obj_cls).get(field_name)
        if field_type is None:
            continue  # additional toloka values is OK
        if field_type in obj_primitive_field_types:
            assert field_type == type(field_value), (
                f'obj field type {field_type} does not match to values type ' f'{type(field_value)}'
            )
            fields[field_name] = field_value
        else:
            raise ValueError(f'value {field_name} has unsupported type {type(field_value)}')
    return obj_cls(**fields)


def _get_all_annotations(obj_cls: Type[base.Object]) -> dict:
    """
    Get all class annotations, including annotations from superclasses.
    In case of annotations collision, superclass annotations will be overridden.
    """
    result = {}
    for c in obj_cls.mro()[::-1]:
        if hasattr(c, '__annotations__'):
            result.update(c.__annotations__)
    if len(result) == 0:
        raise AttributeError('class must have annotations for serialization')
    return result


@dataclass(frozen=True)
class ObjectMapping:
    obj_meta: base.ObjectMeta
    obj_task_fields: Tuple[Tuple[str, str], ...]

    @property
    def obj_type(self) -> base.ObjectT:
        return self.obj_meta.type

    def to_values(self, obj: Optional[base.Object]) -> Dict[str, Any]:
        if validation_enabled:
            if obj is None:
                assert not self.obj_meta.required, f'object {self.obj_meta} is required'
            else:
                assert (
                    obj.__class__ == self.obj_type
                ), f'passed {obj.__class__} does not correspond to mapping {self.obj_type}'
        if obj is None:
            return {}
        obj_values = obj_to_values(obj)
        values = {}
        for obj_field, task_field in self.obj_task_fields:
            values[task_field] = obj_values[obj_field]
        return values

    def to_toloka_spec(self) -> Dict[str, toloka.primitives.base.BaseTolokaObject]:
        type_to_toloka_spec = {
            str: toloka.project.StringSpec,
            bool: toloka.project.BooleanSpec,
            int: toloka.project.IntegerSpec,
            float: toloka.project.FloatSpec,
            list: toloka.project.JsonSpec,
        }

        if issubclass(self.obj_type, base.Label):
            assert len(self.obj_task_fields) == 1
            allowed_values = list(sorted(self.obj_type.possible_values()))
            assert len(allowed_values) > 0
            return {
                self.obj_task_fields[0][1]: type_to_toloka_spec[type(allowed_values[0])](allowed_values=allowed_values)
            }

        spec = {}
        for obj_field, task_field in self.obj_task_fields:
            obj_field_type = self.obj_type.__annotations__[obj_field]
            if obj_field_type == str and self.obj_type.is_media():
                toloka_spec_type = toloka.project.UrlSpec
            else:
                toloka_spec_type = type_to_toloka_spec[obj_field_type]
            spec[task_field] = toloka_spec_type(
                required=self.obj_meta.required, hidden=issubclass(self.obj_type, base.Metadata)
            )
        return spec

    def from_values(self, values: Dict[str, Any]) -> Optional[base.Object]:
        task_fields = set(task_field for _, task_field in self.obj_task_fields)
        if task_fields & values.keys() != task_fields:
            assert (
                not self.obj_meta.required
            ), f'missing fields {task_fields - values.keys()} in required object {self.obj_meta}'
            return None
        return values_to_obj(
            values={obj_field: values[task_field] for obj_field, task_field in self.obj_task_fields},
            obj_cls=self.obj_type,
        )

    def validate(self):
        if is_dataclass(self.obj_type):
            for obj_field, _ in self.obj_task_fields:
                assert (
                    obj_field in self.obj_type.__annotations__
                ), f'{self.obj_type} does not have attribute "{obj_field}"'
        elif issubclass(self.obj_type, base.Class):
            # assert self.obj_task_fields == ((base.CLASS_OBJ_FIELD, base.CLASS_TASK_FIELD),)
            # variant above is stricter, but we have some deps, which use self-defined mapping
            assert len(self.obj_task_fields) == 1 and self.obj_task_fields[0][0] == base.CLASS_OBJ_FIELD
        else:
            assert False, f'{self.obj_type} is not a dataclass nor an options class'


TASK_ID_FIELD = 'id'
Objects = Tuple[Optional[base.Object], ...]


class TaskID:
    objects: Objects
    id: str

    def __init__(self, objects: Objects):
        self.objects = objects
        # We need to somehow map the task objects to the corresponding toloka.Task on assignments. It would be possible
        # to assign a random ID to each object, but then some operations like feedback loop will lose reentrability.
        # This approach can be used only with the support of reentrability from the execution environment (it will be
        # possible in lzy). While this is not the case, we are counting on the implementation of __repr__() @dataclass,
        # which serializes the actual data of the object in the string. There is a problem with number of digits in
        # floats string representation, but since we need to reproduce the results when restarting on the same
        # environment, this seems OK.
        self.id = ' '.join(repr(obj) for obj in objects)

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __repr__(self) -> str:
        return self.id


TaskSingleSolution = Tuple[Objects, Objects]


@dataclass(frozen=True)
class TaskMapping:
    input_mapping: Tuple[ObjectMapping, ...]
    output_mapping: Tuple[ObjectMapping, ...]

    def to_task(self, objects: Objects) -> toloka.Task:
        if validation_enabled:
            self.validate_objects(objects)
        return toloka.Task(input_values=self.toloka_values(objects))

    def to_control_task(self, control_objects: TaskSingleSolution) -> toloka.Task:
        input_objects, output_objects = control_objects
        task = self.to_task(input_objects)
        self.validate_objects(output_objects, output=True)
        task.known_solutions = [
            toloka.Task.KnownSolution(
                output_values=self.toloka_values(output_objects, output=True), correctness_weight=1
            )
        ]
        task.infinite_overlap = True
        return task

    def to_solution(self, objects: Objects) -> toloka.solution.Solution:
        # mostly for tests, because solutions are created in Toloka usually
        self.validate_objects(objects, output=True)
        return toloka.solution.Solution(output_values=self.toloka_values(objects, output=True))

    @staticmethod
    def objects_mapping_to_toloka_spec(
        mapping: Tuple[ObjectMapping, ...],
    ) -> Dict[str, toloka.primitives.base.BaseTolokaObject]:
        spec = {}
        for object_mapping in mapping:
            object_spec = object_mapping.to_toloka_spec()
            assert not spec.keys() & object_spec
            spec.update(object_spec)
        return spec

    def to_toloka_input_spec(self) -> Dict[str, toloka.primitives.base.BaseTolokaObject]:
        spec = self.objects_mapping_to_toloka_spec(self.input_mapping)
        assert TASK_ID_FIELD not in spec
        spec[TASK_ID_FIELD] = toloka.project.StringSpec(hidden=True)
        return spec

    def to_toloka_output_spec(self) -> Dict[str, toloka.primitives.base.BaseTolokaObject]:
        return self.objects_mapping_to_toloka_spec(self.output_mapping)

    def toloka_values(self, objects: Objects, output: bool = False) -> Dict[str, Any]:
        mapping = self.output_mapping if output else self.input_mapping
        values = {}
        for obj, obj_mapping in zip(objects, mapping):
            values.update(obj_mapping.to_values(obj))
        if not output:
            values[TASK_ID_FIELD] = self.task_id(objects).id
        return values

    def task_id(self, objects: Objects) -> TaskID:
        if validation_enabled:
            self.validate_objects(objects)
        return TaskID(objects)

    def from_task(self, task: toloka.Task) -> Objects:
        return self.from_task_values(task.input_values)

    def from_task_values(self, values: Dict[str, Any]) -> Objects:
        return tuple(mapping.from_values(values) for mapping in self.input_mapping)

    def from_solution(self, solution: toloka.solution.Solution) -> Objects:
        return self.from_solution_values(solution.output_values)

    def from_solution_values(self, values: Dict[str, Any]) -> Objects:
        return tuple(mapping.from_values(values) for mapping in self.output_mapping)

    def validate(self):
        self.validate_mapping_list(self.input_mapping)
        self.validate_mapping_list(self.output_mapping)

    @staticmethod
    def validate_mapping_list(mapping_list: Tuple[ObjectMapping, ...]):
        fields = []
        for mapping in mapping_list:
            mapping.validate()
            fields += [task_field for _, task_field in mapping.obj_task_fields]
        assert len(fields) == len(set(fields)), f'fields intersection in mapping list: {fields}'

    def validate_objects(self, objects: Objects, output: bool = False):
        mapping = self.output_mapping if output else self.input_mapping
        assert len(objects) == len(mapping), f'expected length {len(mapping)}, got {len(objects)}'
        for obj, obj_mapping in zip(objects, mapping):
            assert (
                obj_mapping.obj_type == obj.__class__ or not obj_mapping.obj_meta.required and obj is None
            ), f'expected type {obj_mapping.obj_type}, got {obj.__class__}'


TaskMultipleSolutions = Tuple[Objects, List[Tuple[Objects, toloka.Assignment]]]
TaskMultipleSolutionOptions = Tuple[Objects, List[Objects]]
AssignmentSolutions = Tuple[toloka.Assignment, List[TaskSingleSolution]]


# solutions grouped by assignments, assignments order is preserved
def get_assignments_solutions(
    assignments: List[toloka.Assignment],
    mapping: TaskMapping,
    with_control_tasks: bool = False,
) -> List[AssignmentSolutions]:
    assignments_solutions = []
    for assignment in assignments:
        solutions = []
        for task_id, input_objects, output_objects in iterate_assignment(assignment, mapping, with_control_tasks):
            solutions.append((input_objects, output_objects))
        assignments_solutions.append((assignment, solutions))
    return assignments_solutions


# solutions grouped by input objects, input objects order is preserved
def get_solutions(
    assignments: List[toloka.Assignment],
    mapping: TaskMapping,
    pool_input_objects: List[Objects],
) -> List[TaskMultipleSolutions]:
    task_id_to_output_objects = defaultdict(list)
    for assignment in assignments:
        for task_id, _, output_objects in iterate_assignment(assignment, mapping):
            task_id_to_output_objects[task_id].append((output_objects, assignment))
    return [
        (input_objects, task_id_to_output_objects[mapping.task_id(input_objects).id])
        for input_objects in pool_input_objects
    ]


def iterate_assignment(
    assignment: toloka.Assignment,
    mapping: TaskMapping,
    with_control_tasks: bool = False,
) -> Iterable[Tuple[str, Objects, Objects]]:
    return (
        (task.input_values[TASK_ID_FIELD], mapping.from_task(task), mapping.from_solution(solution))
        for task, solution in zip(assignment.tasks, assignment.solutions)
        if (with_control_tasks or not task.known_solutions)
    )


def iterate_assignment_tasks(
    assignment: toloka.Assignment,
    mapping: TaskMapping,
    with_control_tasks: bool = True,
) -> Iterable[Tuple[str, Objects]]:
    return (
        (task.input_values[TASK_ID_FIELD], mapping.from_task(task))
        for task in assignment.tasks
        if (with_control_tasks or not task.known_solutions)
    )


def generate_feedback_loop_mapping(
    input_objects_mapping: Tuple[ObjectMapping, ...],
    markup_objects_mapping: Tuple[ObjectMapping, ...],
    check_objects_mapping: Tuple[ObjectMapping, ...],
) -> Tuple[TaskMapping, TaskMapping]:
    # first object in check output objects must be evaluation object
    assert issubclass(check_objects_mapping[0].obj_type, base.Evaluation)

    for mapping in input_objects_mapping + markup_objects_mapping + check_objects_mapping:
        mapping.validate()

    return TaskMapping(
        input_mapping=input_objects_mapping,
        output_mapping=markup_objects_mapping,
    ), TaskMapping(input_mapping=input_objects_mapping + markup_objects_mapping, output_mapping=check_objects_mapping)


def check_project_is_suitable(project: toloka.Project, task_mapping: TaskMapping) -> bool:
    # toloka specs are unhashable
    def is_included(
        inner: Dict[str, toloka.primitives.base.BaseTolokaObject],
        outer: Dict[str, toloka.primitives.base.BaseTolokaObject],
    ) -> bool:
        # TODO: allow transition from required=True to required=False; the reverse is prohibited
        return all(key in outer and outer[key] == inner[key] for key in inner)

    generated_input_spec = task_mapping.to_toloka_input_spec()
    generated_output_spec = task_mapping.to_toloka_output_spec()

    return is_included(generated_input_spec, project.task_spec.input_spec) and is_included(
        generated_output_spec, project.task_spec.output_spec
    )
