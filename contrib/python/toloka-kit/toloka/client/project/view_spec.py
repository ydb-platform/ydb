__all__ = [
    'ViewSpec',
    'ClassicViewSpec',
    'TemplateBuilderViewSpec'
]

import json

from copy import deepcopy
from enum import unique
from typing import List

from .template_builder import TemplateBuilder
from ..primitives.base import BaseTolokaObject
from ...util import traverse_dicts_recursively
from ...util._codegen import attribute, expand
from ...util._extendable_enum import ExtendableStrEnum


class ViewSpec(BaseTolokaObject, spec_enum='Type', spec_field='type'):
    """The description of a task interface.

    You can choose the type of description:
        * HTML, CSS, and JS — use [ClassicViewSpec](toloka.client.project.view_spec.ClassicViewSpec.md)
        * Template Builder components — use [TemplateBuilderViewSpec](toloka.client.project.view_spec.TemplateBuilderViewSpec.md)

    Args:
        settings: Common interface elements.
    """

    @unique
    class Type(ExtendableStrEnum):
        """A `ViewSpec` type.

        Attributes:
            CLASSIC: A task interface is defined with HTML, CSS and JS.
            TEMPLATE_BUILDER: A task interface is defined with the [TemplateBuilder](toloka.client.project.template_builder.TemplateBuilder.md) components.
        """
        CLASSIC = 'classic'
        TEMPLATE_BUILDER = 'tb'

    CLASSIC = Type.CLASSIC
    TEMPLATE_BUILDER = Type.TEMPLATE_BUILDER

    class Settings(BaseTolokaObject):
        """Common interface elements.

        Attributes:
            show_finish: Show the **Exit** button. The default is to show the button.
            show_fullscreen: Show the **Expand to fullscreen** button. The default is to show the button.
            show_instructions: Show the **Instructions** button. The default is to show the button.
            show_message: Show the **Message for the requester** button. The default is to show the button.
            show_reward: Show the price per task page. The default is to show the price.
            show_skip: Show the **Skip** button. The default is to show the button.
            show_submit: Show the **Submit** button. The default is to show the button.
            show_timer: Show the timer. The default is to show the timer.
            show_title: Show the project name on the top of a page. The default is to show the name.
        """

        show_finish: bool = attribute(origin='showFinish')
        show_fullscreen: bool = attribute(origin='showFullscreen')
        show_instructions: bool = attribute(origin='showInstructions')
        show_message: bool = attribute(origin='showMessage')
        show_reward: bool = attribute(origin='showReward')
        show_skip: bool = attribute(origin='showSkip')
        show_submit: bool = attribute(origin='showSubmit')
        show_timer: bool = attribute(origin='showTimer')
        show_title: bool = attribute(origin='showTitle')

    settings: Settings


class ClassicViewSpec(ViewSpec, spec_value=ViewSpec.CLASSIC):
    """A task interface defined with HTML, CSS and JS.

    For more information, see the [guide](https://toloka.ai/docs/guide/spec).

    Attributes:
        markup: HTML markup of the task interface.
        styles: CSS for the task interface.
        script: JavaScript code for the task interface.
        assets: Links to external files.
    """

    class Assets(BaseTolokaObject):
        """Links to external files.

        You can link:
            * CSS libraries
            * JavaScript libraries
            * Toloka assets — libraries that can be linked using the `$TOLOKA_ASSETS` path:
                * `$TOLOKA_ASSETS/js/toloka-handlebars-templates.js` — [Handlebars template engine](http://handlebarsjs.com/).
                * `$TOLOKA_ASSETS/js/image-annotation.js` — Image labeling interface. Note, that this library requires Handlebars and must be linked after it.
                    For more information, see [Image with area selection](https://toloka.ai/docs/guide/t-components/image-annotation).

            Add items in the order they should be linked.

        Attributes:
            style_urls: Links to CSS libraries.
            script_urls: Links to JavaScript libraries and Toloka assets.

        Examples:
            >>> from toloka.client.project.view_spec import ClassicViewSpec
            >>> view_spec = ClassicViewSpec(
            >>>     ...,
            >>>     assets = ClassicViewSpec.Assets(
            >>>         script_urls = [
            >>>             "$TOLOKA_ASSETS/js/toloka-handlebars-templates.js",
            >>>             "$TOLOKA_ASSETS/js/image-annotation.js",
            >>>         ]
            >>>     )
            >>> )
        """
        style_urls: List[str]
        script_urls: List[str]

    script: str
    markup: str
    styles: str
    assets: Assets


class TemplateBuilderViewSpec(ViewSpec, spec_value=ViewSpec.TEMPLATE_BUILDER):
    """A task interface defined with the [TemplateBuilder](toloka.client.project.template_builder.TemplateBuilder.md).

    See also [Template Builder](https://toloka.ai/docs/template-builder/) in the guide.

    Attributes:
        view: A top level component like [SideBySideLayoutV1](toloka.client.project.template_builder.layouts.SideBySideLayoutV1.md).
        plugins: An array of plugins.
        vars: Reusable data. It is referenced with the [RefComponent](toloka.client.project.template_builder.base.RefComponent.md).
        core_version: The default template components version. Most likely, you do not need to change this parameter.
        infer_data_spec:
            * `True` – The specifications of input and output data are generated automatically depending on the task interface settings.
            * `False` – You configure the specifications manually, if:
                * You don't want the specification to be affected by changes in instructions or other project parameters.
                * You have to change automatically generated specifications to suite your needs.

    Example:
        Creating a simple interface based on [ListViewV1](toloka.client.project.template_builder.view.ListViewV1.md):

        >>> import toloka.client.project.template_builder as tb
        >>> project_interface = toloka.client.project.view_spec.TemplateBuilderViewSpec(
        >>>     view=tb.view.ListViewV1(
        >>>         items=[header, output_field, radiobuttons],
        >>>         validation=some_validation,
        >>>     ),
        >>>     plugins=[plugin1, plugin2]
        >>> )
        >>> # add 'project_interface' to 'toloka.project.Project' instance
        ...
    """

    config: TemplateBuilder
    core_version: str = '1.0.0'
    infer_data_spec: bool = attribute(default=False, origin='inferDataSpec')

    def unstructure(self):
        data = super().unstructure()
        lock = {'core': data.pop('core_version')}

        for dct in traverse_dicts_recursively(data['config']):
            if 'type' not in dct or 'version' not in dct:
                continue

            comp_type = dct['type']
            if comp_type.startswith('data.'):
                continue

            comp_version = dct.pop('version')
            if comp_version != lock.setdefault(comp_type, comp_version):
                raise RuntimeError(f'Different versions of the same component: {comp_type}')

        data['lock'] = lock
        data['config'] = json.dumps(data['config'], indent=4, ensure_ascii=False)
        return data

    @classmethod
    def structure(cls, data: dict):
        data_copy = deepcopy(data)
        lock = data_copy.pop('lock')

        data_copy['config'] = json.loads(data['config'])
        if lock:
            data_copy['core_version'] = lock['core']

            for dct in traverse_dicts_recursively(data_copy['config']):
                if dct.get('type') in lock:
                    dct['version'] = lock[dct['type']]

        return super().structure(data_copy)


TemplateBuilderViewSpec.__init__ = expand('config')(TemplateBuilderViewSpec.__init__)
