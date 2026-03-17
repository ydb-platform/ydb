__all__ = [
    'field_spec',
    'task_spec',
    'template_builder',
    'view_spec',
    'AdditionalLanguage',
    'ArrayBooleanSpec',
    'ArrayCoordinatesSpec',
    'ArrayFileSpec',
    'ArrayFloatSpec',
    'ArrayIntegerSpec',
    'ArrayStringSpec',
    'ArrayUrlSpec',
    'BooleanSpec',
    'ClassicViewSpec',
    'CoordinatesSpec',
    'FileSpec',
    'FloatSpec',
    'IntegerSpec',
    'JsonSpec',
    'LocalizationConfig',
    'Project',
    'ProjectCheckResponse',
    'ProjectUpdateDifferenceLevel',
    'StringSpec',
    'TemplateBuilderViewSpec',
    'UrlSpec',
]
import datetime
import toloka.client.primitives.base
import toloka.client.project.localization
import toloka.client.project.task_spec
import toloka.client.quality_control
import toloka.util._extendable_enum
import typing

from toloka.client.project import (
    field_spec,
    task_spec,
    template_builder,
    view_spec,
)
from toloka.client.project.field_spec import (
    ArrayBooleanSpec,
    ArrayCoordinatesSpec,
    ArrayFileSpec,
    ArrayFloatSpec,
    ArrayIntegerSpec,
    ArrayStringSpec,
    ArrayUrlSpec,
    BooleanSpec,
    CoordinatesSpec,
    FileSpec,
    FloatSpec,
    IntegerSpec,
    JsonSpec,
    StringSpec,
    UrlSpec,
)
from toloka.client.project.localization import (
    AdditionalLanguage,
    LocalizationConfig,
)
from toloka.client.project.view_spec import (
    ClassicViewSpec,
    TemplateBuilderViewSpec,
)


class Project(toloka.client.primitives.base.BaseTolokaObject):
    """Top-level object in Toloka that describes one requester's objective.

    If your task is complex, consider to [decompose](https://toloka.ai/docs/guide/solution-architecture) it into several projects.
    For example, one project finds images with some objects, another project describes image segmentation, and the third project checks this segmentation.

    In a project, you set properties for tasks and responses:
    * Input data parameters describe what kind of input data you have: images, text, and other.
    * Output data parameters describe Tolokers' responses. They are used to validate a data type, range of values, string length, and so on.
    * Task interface. To learn how to define the appearance of tasks, see [Task interface](https://toloka.ai/docs/guide/spec) in the guide.

    You upload [tasks](toloka.client.task.Task.md) to project [pools](toloka.client.pool.Pool.md) and [training pools](toloka.client.training.Training.md).
    They are grouped into [task suites](toloka.client.task_suite.TaskSuite.md) and assigned to Tolokers.

    Attributes:
        public_name: The name of the project. Visible to Tolokers.
        public_description: The description of the project. Visible to Tolokers.
        public_instructions: Instructions for Tolokers describe what to do in the tasks. You can use any HTML markup in the instructions.
        private_comment: Comments about the project. Visible only to the requester.
        task_spec: Input and output data specification and the task interface.
            The interface can be defined with HTML, CSS, and JS or using the [Template Builder](https://toloka.ai/docs/template-builder/) components.
        assignments_issuing_type: Settings for assigning tasks. Default value: `AUTOMATED`.
        assignments_issuing_view_config: The configuration of a task view on a map. Provide it if `assignments_issuing_type=MAP_SELECTOR`.
        assignments_automerge_enabled: [Merging tasks](https://toloka.ai/docs/api/tasks) control.
        max_active_assignments_count: The number of task suites simultaneously assigned to a Toloker. Note, that Toloka counts assignments having the `ACTIVE` status only.
        quality_control: [Quality control](https://toloka.ai/docs/guide/project-qa) rules.
        localization_config: Translations to other languages.
        metadata: Additional information about the project.
        id: The ID of the project. Read-only field.
        status: A project status. Read-only field.
        created: The UTC date and time when the project was created. Read-only field.

    Example:
        Creating a new project.

        >>> new_project = toloka.client.project.Project(
        >>>     assignments_issuing_type=toloka.client.project.Project.AssignmentsIssuingType.AUTOMATED,
        >>>     public_name='My best project',
        >>>     public_description='Describe the picture',
        >>>     public_instructions='Describe in a few words what is happening in the image.',
        >>>     task_spec=toloka.client.project.task_spec.TaskSpec(
        >>>         input_spec={'image': toloka.client.project.field_spec.UrlSpec()},
        >>>         output_spec={'result': toloka.client.project.field_spec.StringSpec()},
        >>>         view_spec=project_interface,
        >>>     ),
        >>> )
        >>> new_project = toloka_client.create_project(new_project)
        >>> print(new_project.id)
        ...
    """

    class AssignmentsIssuingType(toloka.util._extendable_enum.ExtendableStrEnum):
        """Settings for assigning tasks.

        Attributes:
            AUTOMATED: A Toloker is assigned a task suite from a pool. You can configure the order
                of assigning task suites.
            MAP_SELECTOR: A Toloker chooses a task suite on the map.
                A task view on the map is configured with the `Project.assignments_issuing_view_config` parameters.
        """

        AUTOMATED = 'AUTOMATED'
        MAP_SELECTOR = 'MAP_SELECTOR'

    class ProjectStatus(toloka.util._extendable_enum.ExtendableStrEnum):
        """A project status.

        Attributes:
            ACTIVE: The project is active.
            ARCHIVED: The project is archived.
        """

        ACTIVE = 'ACTIVE'
        ARCHIVED = 'ARCHIVED'

    class AssignmentsIssuingViewConfig(toloka.client.primitives.base.BaseTolokaObject):
        """Task view on the map.

        These parameters are used when `Project.assignments_issuing_type` is set to `MAP_SELECTOR`.

        Attributes:
            title_template: The name of a task. Tolokers see it in the task preview mode.
            description_template: The brief description of a task. Tolokers see it in the task preview mode.
            map_provider: A map provider.
        """

        class MapProvider(toloka.util._extendable_enum.ExtendableStrEnum):
            """A map provider.

            Attributes:
                GOOGLE: Google Maps.
                YANDEX: Yandex Maps.
            """

            YANDEX = 'YANDEX'
            GOOGLE = 'GOOGLE'

        def __init__(
            self,
            *,
            title_template: typing.Optional[str] = None,
            description_template: typing.Optional[str] = None,
            map_provider: typing.Optional[MapProvider] = None
        ) -> None:
            """Method generated by attrs for class Project.AssignmentsIssuingViewConfig.
            """
            ...

        _unexpected: typing.Optional[typing.Dict[str, typing.Any]]
        title_template: typing.Optional[str]
        description_template: typing.Optional[str]
        map_provider: typing.Optional[MapProvider]

    def __attrs_post_init__(self): ...

    def set_default_language(self, language: str):
        """Sets the main language used in the project parameters.

        You must set the default language if you want to translate the project to other languages.
        Args:
            language: Two-letter [ISO 639-1](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes) language code in upper case.
        """
        ...

    def add_requester_translation(
        self,
        language: str,
        public_name: typing.Optional[str] = None,
        public_description: typing.Optional[str] = None,
        public_instructions: typing.Optional[str] = None
    ):
        """Adds a project interface translation to other language.

        To translate to several languages call this method for each translation.
        A subsequent call with the same `language` updates the translation of passed parameters and doesn't touch omitted parameters.

        Args:
            language: Two-letter [ISO 639-1](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes) language code in upper case.
            public_name: A translated project name.
            public_description: A translated project description.
            public_instructions: Translated instructions for Tolokers.

        Examples:
            How to add russian translation to the project.

            >>> project = toloka.client.Project(
            >>>     public_name='Cats vs dogs',
            >>>     public_description='A simple image classification',
            >>>     public_instructions='Determine which animal is in an image',
            >>>     ...
            >>> )
            >>> project.set_default_language('EN')
            >>> project.add_requester_translation(
            >>>     language='RU',
            >>>     public_name='Кошки или собаки'
            >>>     public_description='Простая классификация изображений'
            >>> )
            >>> project.add_requester_translation(language='RU', public_instructions='Определите, какое животное изображено')
        """
        ...

    def __init__(
        self,
        *,
        public_name: typing.Optional[str] = None,
        public_description: typing.Optional[str] = None,
        task_spec: typing.Optional[toloka.client.project.task_spec.TaskSpec] = None,
        assignments_issuing_type: typing.Union[AssignmentsIssuingType, str] = AssignmentsIssuingType.AUTOMATED,
        assignments_issuing_view_config: typing.Optional[AssignmentsIssuingViewConfig] = None,
        assignments_automerge_enabled: typing.Optional[bool] = None,
        max_active_assignments_count: typing.Optional[int] = None,
        quality_control: typing.Optional[toloka.client.quality_control.QualityControl] = None,
        metadata: typing.Optional[typing.Dict[str, typing.List[str]]] = None,
        status: typing.Optional[ProjectStatus] = None,
        created: typing.Optional[datetime.datetime] = None,
        id: typing.Optional[str] = None,
        public_instructions: typing.Optional[str] = None,
        private_comment: typing.Optional[str] = None,
        localization_config: typing.Optional[toloka.client.project.localization.LocalizationConfig] = None
    ) -> None:
        """Method generated by attrs for class Project.
        """
        ...

    _unexpected: typing.Optional[typing.Dict[str, typing.Any]]
    public_name: typing.Optional[str]
    public_description: typing.Optional[str]
    task_spec: typing.Optional[toloka.client.project.task_spec.TaskSpec]
    assignments_issuing_type: AssignmentsIssuingType
    assignments_issuing_view_config: typing.Optional[AssignmentsIssuingViewConfig]
    assignments_automerge_enabled: typing.Optional[bool]
    max_active_assignments_count: typing.Optional[int]
    quality_control: typing.Optional[toloka.client.quality_control.QualityControl]
    metadata: typing.Optional[typing.Dict[str, typing.List[str]]]
    status: typing.Optional[ProjectStatus]
    created: typing.Optional[datetime.datetime]
    id: typing.Optional[str]
    public_instructions: typing.Optional[str]
    private_comment: typing.Optional[str]
    localization_config: typing.Optional[toloka.client.project.localization.LocalizationConfig]


class ProjectUpdateDifferenceLevel(toloka.util._extendable_enum.ExtendableStrEnum):
    """The level of a project update difference.

    Attributes:
        EQUAL: The update does not change the project.
        BREAKING_CHANGE: The update is a breaking change.
        NON_BREAKING_CHANGE: The update is not a breaking change.
    """

    EQUAL = 'EQUAL'
    BREAKING_CHANGE = 'BREAKING_CHANGE'
    NON_BREAKING_CHANGE = 'NON_BREAKING_CHANGE'


class ProjectCheckResponse(toloka.client.primitives.base.BaseTolokaObject):
    """The result of a check whether a project update is a breaking change.

    Attributes:
        difference_level: The level of a project update difference.
    """

    def __init__(self, *, difference_level: typing.Optional[ProjectUpdateDifferenceLevel] = None) -> None:
        """Method generated by attrs for class ProjectCheckResponse.
        """
        ...

    _unexpected: typing.Optional[typing.Dict[str, typing.Any]]
    difference_level: typing.Optional[ProjectUpdateDifferenceLevel]
