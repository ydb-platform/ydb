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
from enum import unique
from typing import Optional, Dict, List

from . import field_spec
from . import task_spec
from . import template_builder
from . import view_spec

from ..primitives.base import BaseTolokaObject
from ..project.field_spec import (
    BooleanSpec,
    StringSpec,
    IntegerSpec,
    FloatSpec,
    UrlSpec,
    FileSpec,
    CoordinatesSpec,
    JsonSpec,
    ArrayBooleanSpec,
    ArrayStringSpec,
    ArrayIntegerSpec,
    ArrayFloatSpec,
    ArrayUrlSpec,
    ArrayFileSpec,
    ArrayCoordinatesSpec,
)
from ..project.localization import LocalizationConfig, AdditionalLanguage
from ..project.view_spec import ClassicViewSpec, TemplateBuilderViewSpec
from ..project.task_spec import TaskSpec
from ..quality_control import QualityControl
from ...util._codegen import attribute
from ...util._extendable_enum import ExtendableStrEnum


class Project(BaseTolokaObject):
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

    @unique
    class AssignmentsIssuingType(ExtendableStrEnum):
        """Settings for assigning tasks.

        Attributes:
            AUTOMATED: A Toloker is assigned a task suite from a pool. You can configure the order
                of assigning task suites.
            MAP_SELECTOR: A Toloker chooses a task suite on the map.
                A task view on the map is configured with the `Project.assignments_issuing_view_config` parameters.
        """

        AUTOMATED = 'AUTOMATED'
        MAP_SELECTOR = 'MAP_SELECTOR'

    @unique
    class ProjectStatus(ExtendableStrEnum):
        """A project status.

        Attributes:
            ACTIVE: The project is active.
            ARCHIVED: The project is archived.
        """

        ACTIVE = 'ACTIVE'
        ARCHIVED = 'ARCHIVED'

    class AssignmentsIssuingViewConfig(BaseTolokaObject):
        """Task view on the map.

        These parameters are used when `Project.assignments_issuing_type` is set to `MAP_SELECTOR`.

        Attributes:
            title_template: The name of a task. Tolokers see it in the task preview mode.
            description_template: The brief description of a task. Tolokers see it in the task preview mode.
            map_provider: A map provider.
        """

        @unique
        class MapProvider(ExtendableStrEnum):
            """A map provider.

            Attributes:
                GOOGLE: Google Maps.
                YANDEX: Yandex Maps.
            """

            YANDEX = 'YANDEX'
            GOOGLE = 'GOOGLE'

        title_template: str
        description_template: str
        map_provider: Optional[MapProvider] = None

    QualityControl = QualityControl

    public_name: str  # public
    public_description: str  # public
    task_spec: TaskSpec  # public
    assignments_issuing_type: AssignmentsIssuingType = attribute(default=AssignmentsIssuingType.AUTOMATED,
                                                                 required=True,
                                                                 autocast=True)  # AssignmentsIssuingType  # public

    assignments_issuing_view_config: AssignmentsIssuingViewConfig
    assignments_automerge_enabled: bool
    max_active_assignments_count: int
    quality_control: QualityControl

    metadata: Dict[str, List[str]]
    status: ProjectStatus = attribute(readonly=True)
    created: datetime.datetime = attribute(readonly=True)

    id: str = attribute(readonly=True)

    public_instructions: str  # public
    private_comment: str
    localization_config: LocalizationConfig

    def __attrs_post_init__(self):
        # TODO: delegate this check to API
        if self.assignments_issuing_type == Project.AssignmentsIssuingType.MAP_SELECTOR:
            assert self.assignments_issuing_view_config is not None

    def set_default_language(self, language: str):
        """Sets the main language used in the project parameters.

        You must set the default language if you want to translate the project to other languages.
        Args:
            language: Two-letter [ISO 639-1](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes) language code in upper case.
        """
        if self.localization_config is None:
            self.localization_config = LocalizationConfig()
        self.localization_config.default_language = language

    def add_requester_translation(self, language: str, public_name: Optional[str] = None, public_description: Optional[str] = None, public_instructions: Optional[str] = None):
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
        assert language

        if self.localization_config is None:
            self.localization_config = LocalizationConfig()
        if self.localization_config.additional_languages is None:
            self.localization_config.additional_languages = []

        for translation in self.localization_config.additional_languages:
            if translation.language == language:
                current_translation = translation
                break
        else:
            current_translation = AdditionalLanguage(language=language)
            self.localization_config.additional_languages.append(current_translation)

        if public_name is not None:
            current_translation.public_name = AdditionalLanguage.FieldTranslation(value=public_name)
        if public_description is not None:
            current_translation.public_description = AdditionalLanguage.FieldTranslation(value=public_description)
        if public_instructions is not None:
            current_translation.public_instructions = AdditionalLanguage.FieldTranslation(value=public_instructions)


class ProjectUpdateDifferenceLevel(ExtendableStrEnum):
    """The level of a project update difference.

    Attributes:
        EQUAL: The update does not change the project.
        BREAKING_CHANGE: The update is a breaking change.
        NON_BREAKING_CHANGE: The update is not a breaking change.
    """

    EQUAL = 'EQUAL'
    BREAKING_CHANGE = 'BREAKING_CHANGE'
    NON_BREAKING_CHANGE = 'NON_BREAKING_CHANGE'


class ProjectCheckResponse(BaseTolokaObject):
    """The result of a check whether a project update is a breaking change.

    Attributes:
        difference_level: The level of a project update difference.
    """
    difference_level: ProjectUpdateDifferenceLevel = attribute(origin='differenceLevel')
