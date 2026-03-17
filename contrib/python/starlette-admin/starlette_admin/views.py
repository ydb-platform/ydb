import inspect
from abc import abstractmethod
from collections import OrderedDict
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from jinja2 import Template
from starlette.requests import Request
from starlette.responses import Response
from starlette.templating import Jinja2Templates
from starlette_admin._types import ExportType, RequestAction, RowActionsDisplayType
from starlette_admin.actions import action, link_row_action, row_action
from starlette_admin.exceptions import ActionFailed
from starlette_admin.fields import (
    BaseField,
    CollectionField,
    FileField,
    HasOne,
    RelationField,
)
from starlette_admin.helpers import extract_fields, not_none
from starlette_admin.i18n import get_locale, gettext, ngettext
from starlette_admin.i18n import lazy_gettext as _


class BaseView:
    """
    Base class for all views

    Attributes:
        label: Label of the view to be displayed.
        icon: Icon to be displayed for this model in the admin. Only FontAwesome names are supported.
    """

    label: str = ""
    icon: Optional[str] = None

    def title(self, request: Request) -> str:
        """Return the title of the view to be displayed in the browser tab"""
        return self.label

    def is_active(self, request: Request) -> bool:
        """Return true if the current view is active"""
        return False

    def is_accessible(self, request: Request) -> bool:
        """
        Override this method to add permission checks.
        Return True if current user can access this view
        """
        return True


class DropDown(BaseView):
    """
    Group views inside a dropdown

    Example:
        ```python
        admin.add_view(
            DropDown(
                "Resources",
                icon="fa fa-list",
                views=[
                    ModelView(User),
                    Link(label="Home Page", url="/"),
                    CustomView(label="Dashboard", path="/dashboard", template_path="dashboard.html"),
                ],
            )
        )
        ```
    """

    def __init__(
        self,
        label: str,
        views: List[Union[Type[BaseView], BaseView]],
        icon: Optional[str] = None,
        always_open: bool = True,
    ) -> None:
        self.label = label
        self.icon = icon
        self.always_open = always_open
        self.views: List[BaseView] = [
            (v if isinstance(v, BaseView) else v()) for v in views
        ]

    def is_active(self, request: Request) -> bool:
        return any(v.is_active(request) for v in self.views)

    def is_accessible(self, request: Request) -> bool:
        return any(v.is_accessible(request) for v in self.views)


class Link(BaseView):
    """
    Add arbitrary hyperlinks to the menu

    Example:
        ```python
        admin.add_view(Link(label="Home Page", icon="fa fa-link", url="/"))
        ```
    """

    def __init__(
        self,
        label: str = "",
        icon: Optional[str] = None,
        url: str = "/",
        target: Optional[str] = "_self",
    ):
        self.label = label
        self.icon = icon
        self.url = url
        self.target = target


class CustomView(BaseView):
    """
    Add your own views (not tied to any particular model). For example,
    a custom home page that displays some analytics data.

    Attributes:
        path: Route path
        template_path: Path to template file
        methods: HTTP methods
        name: Route name
        add_to_menu: Display to menu or not

    Example:
        ```python
        admin.add_view(CustomView(label="Home", icon="fa fa-home", path="/home", template_path="home.html"))
        ```
    """

    def __init__(
        self,
        label: str,
        icon: Optional[str] = None,
        path: str = "/",
        template_path: str = "index.html",
        name: Optional[str] = None,
        methods: Optional[List[str]] = None,
        add_to_menu: bool = True,
    ):
        self.label = label
        self.icon = icon
        self.path = path
        self.template_path = template_path
        self.name = name
        self.methods = methods
        self.add_to_menu = add_to_menu

    async def render(self, request: Request, templates: Jinja2Templates) -> Response:
        """Default methods to render view. Override this methods to add your custom logic."""
        return templates.TemplateResponse(
            request=request,
            name=self.template_path,
            context={"title": self.title(request)},
        )

    def is_active(self, request: Request) -> bool:
        return request.scope["path"] == request.scope["root_path"] + self.path


class BaseModelView(BaseView):
    """
    Base administrative view.
    Derive from this class to implement your administrative interface piece.

    Attributes:
        identity: Unique identity to identify the model associated to this view.
            Will be used for URL of the endpoints.
        name: Name of the view to be displayed
        fields: List of fields
        pk_attr: Primary key field name
        form_include_pk (bool): Indicates whether the primary key should be
            included in create and edit forms. Default to False.
        exclude_fields_from_list: List of fields to exclude in List page.
        exclude_fields_from_detail: List of fields to exclude in Detail page.
        exclude_fields_from_create: List of fields to exclude from creation page.
        exclude_fields_from_edit: List of fields to exclude from editing page.
        searchable_fields: List of searchable fields.
        sortable_fields: List of sortable fields.
        export_fields: List of fields to include in exports.
        fields_default_sort: Initial order (sort) to apply to the table.
            Should be a sequence of field names or a tuple of
            (field name, True/False to indicate the sort direction).
            For example:
            `["title",  ("created_at", False), ("price", True)]` will sort
             by `title` ascending, `created_at` ascending and `price` descending.
        export_types: A list of available export filetypes. Available
            exports are `['csv', 'excel', 'pdf', 'print']`. Only `pdf` is
            disabled by default.
        column_visibility: Enable/Disable
            [column visibility](https://datatables.net/extensions/buttons/built-in#Column-visibility)
            extension
        search_builder: Enable/Disable [search builder](https://datatables.net/extensions/searchbuilder/)
            extension
        page_size: Default number of items to display in List page pagination.
            Default value is set to `10`.
        page_size_options: Pagination choices displayed in List page.
            Default value is set to `[10, 25, 50, 100]`. Use `-1`to display All
        responsive_table: Enable/Disable [responsive](https://datatables.net/extensions/responsive/)
            extension
        save_state: Enable/Disable [state saving](https://datatables.net/examples/basic_init/state_save.html)
        datatables_options: Dict of [Datatables options](https://datatables.net/reference/option/).
            These will overwrite any default options set for the datatable.
        list_template: List view template. Default is `list.html`.
        detail_template: Details view template. Default is `detail.html`.
        create_template: Edit view template. Default is `create.html`.
        edit_template: Edit view template. Default is `edit.html`.
        actions: List of actions
        additional_js_links: A list of additional JavaScript files to include.
        additional_css_links: A list of additional CSS files to include.


    """

    identity: Optional[str] = None
    name: Optional[str] = None
    fields: Sequence[BaseField] = []
    pk_attr: Optional[str] = None
    form_include_pk: bool = False
    exclude_fields_from_list: Sequence[str] = []
    exclude_fields_from_detail: Sequence[str] = []
    exclude_fields_from_create: Sequence[str] = []
    exclude_fields_from_edit: Sequence[str] = []
    searchable_fields: Optional[Sequence[str]] = None
    sortable_fields: Optional[Sequence[str]] = None
    fields_default_sort: Optional[Sequence[Union[Tuple[str, bool], str]]] = None
    export_types: Sequence[ExportType] = [
        ExportType.CSV,
        ExportType.EXCEL,
        ExportType.PRINT,
    ]
    export_fields: Optional[Sequence[str]] = None
    column_visibility: bool = True
    search_builder: bool = True
    page_size: int = 10
    page_size_options: Sequence[int] = [10, 25, 50, 100]
    responsive_table: bool = False
    save_state: bool = True
    datatables_options: ClassVar[Dict[str, Any]] = {}
    list_template: str = "list.html"
    detail_template: str = "detail.html"
    create_template: str = "create.html"
    edit_template: str = "edit.html"
    actions: Optional[Sequence[str]] = None
    row_actions: Optional[Sequence[str]] = None
    additional_js_links: Optional[List[str]] = None
    additional_css_links: Optional[List[str]] = None
    row_actions_display_type: RowActionsDisplayType = RowActionsDisplayType.ICON_LIST

    _find_foreign_model: Callable[[str], "BaseModelView"]

    def __init__(self) -> None:  # noqa: C901
        fringe = list(self.fields)
        all_field_names = []
        while len(fringe) > 0:
            field = fringe.pop(0)
            if not hasattr(field, "_name"):
                field._name = field.name  # type: ignore
            if isinstance(field, CollectionField):
                for f in field.fields:
                    f._name = f"{field._name}.{f.name}"  # type: ignore
                fringe.extend(field.fields)
            name = field._name  # type: ignore
            if name == self.pk_attr and not self.form_include_pk:
                field.exclude_from_create = True
                field.exclude_from_edit = True
            if name in self.exclude_fields_from_list:
                field.exclude_from_list = True
            if name in self.exclude_fields_from_detail:
                field.exclude_from_detail = True
            if name in self.exclude_fields_from_create:
                field.exclude_from_create = True
            if name in self.exclude_fields_from_edit:
                field.exclude_from_edit = True
            if not isinstance(field, CollectionField):
                all_field_names.append(name)
                field.searchable = (self.searchable_fields is None) or (
                    name in self.searchable_fields
                )
                field.orderable = (self.sortable_fields is None) or (
                    name in self.sortable_fields
                )
        if self.searchable_fields is None:
            self.searchable_fields = all_field_names[:]
        if self.sortable_fields is None:
            self.sortable_fields = all_field_names[:]
        if self.export_fields is None:
            self.export_fields = all_field_names[:]
        if self.fields_default_sort is None:
            self.fields_default_sort = [self.pk_attr]  # type: ignore[list-item]

        # Actions
        self._actions: Dict[str, Dict[str, str]] = OrderedDict()
        self._row_actions: Dict[str, Dict[str, str]] = OrderedDict()
        self._actions_handlers: Dict[
            str, Callable[[Request, Sequence[Any]], Awaitable]
        ] = OrderedDict()
        self._row_actions_handlers: Dict[str, Callable[[Request, Any], Awaitable]] = (
            OrderedDict()
        )
        self._init_actions()

    def is_active(self, request: Request) -> bool:
        return request.path_params.get("identity", None) == self.identity

    def _init_actions(self) -> None:
        self._init_batch_actions()
        self._init_row_actions()
        self._validate_actions()

    def _init_batch_actions(self) -> None:
        """
        This method initializes batch and row actions, collects their handlers,
        and validates that all specified actions exist.
        """
        for _method_name, method in inspect.getmembers(
            self, predicate=inspect.ismethod
        ):
            if hasattr(method, "_action"):
                name = method._action.get("name")
                self._actions[name] = method._action
                self._actions_handlers[name] = method

        if self.actions is None:
            self.actions = list(self._actions_handlers.keys())

    def _init_row_actions(self) -> None:
        for _method_name, method in inspect.getmembers(
            self, predicate=inspect.ismethod
        ):
            if hasattr(method, "_row_action"):
                name = method._row_action.get("name")
                self._row_actions[name] = method._row_action
                self._row_actions_handlers[name] = method

        if self.row_actions is None:
            self.row_actions = list(self._row_actions_handlers.keys())

    def _validate_actions(self) -> None:
        for action_name in not_none(self.actions):
            if action_name not in self._actions:
                raise ValueError(f"Unknown action with name `{action_name}`")
        for action_name in not_none(self.row_actions):
            if action_name not in self._row_actions:
                raise ValueError(f"Unknown row action with name `{action_name}`")

    async def is_action_allowed(self, request: Request, name: str) -> bool:
        """
        Verify if action with `name` is allowed.
        Override this method to allow or disallow actions based
        on some condition.

        Args:
            name: Action name
            request: Starlette request
        """
        if name == "delete":
            return self.can_delete(request)
        return True

    async def is_row_action_allowed(self, request: Request, name: str) -> bool:
        """
        Verify if the row action with `name` is allowed.
        Override this method to allow or disallow row actions based
        on some condition.

        Args:
            name: Row action name
            request: Starlette request
        """
        if name == "delete":
            return self.can_delete(request)
        if name == "edit":
            return self.can_edit(request)
        if name == "view":
            return self.can_view_details(request)
        return True

    async def get_all_actions(self, request: Request) -> List[Dict[str, Any]]:
        """Return a list of allowed batch actions"""
        actions = []
        for action_name in not_none(self.actions):
            if await self.is_action_allowed(request, action_name):
                actions.append(self._actions.get(action_name, {}))
        return actions

    async def get_all_row_actions(self, request: Request) -> List[Dict[str, Any]]:
        """Return a list of allowed row actions"""
        row_actions = []
        for row_action_name in not_none(self.row_actions):
            if await self.is_row_action_allowed(request, row_action_name):
                _row_action = self._row_actions.get(row_action_name, {})
                if (
                    request.state.action == RequestAction.LIST
                    and not _row_action.get("exclude_from_list")
                ) or (
                    request.state.action == RequestAction.DETAIL
                    and not _row_action.get("exclude_from_detail")
                ):
                    row_actions.append(_row_action)
        return row_actions

    async def handle_action(
        self, request: Request, pks: List[Any], name: str
    ) -> Union[str, Response]:
        """
        Handle action with `name`.
        Raises:
            ActionFailed: to display meaningfully error
        """
        handler = self._actions_handlers.get(name, None)
        if handler is None:
            raise ActionFailed("Invalid action")
        if not await self.is_action_allowed(request, name):
            raise ActionFailed("Forbidden")
        handler_return = await handler(request, pks)
        custom_response = self._actions[name]["custom_response"]
        if isinstance(handler_return, Response) and not custom_response:
            raise ActionFailed(
                "Set custom_response to true, to be able to return custom response"
            )
        return handler_return

    async def handle_row_action(
        self, request: Request, pk: Any, name: str
    ) -> Union[str, Response]:
        """
        Handle row action with `name`.
        Raises:
            ActionFailed: to display meaningfully error
        """
        handler = self._row_actions_handlers.get(name, None)
        if handler is None:
            raise ActionFailed("Invalid row action")
        if not await self.is_row_action_allowed(request, name):
            raise ActionFailed("Forbidden")
        handler_return = await handler(request, pk)
        custom_response = self._row_actions[name]["custom_response"]
        if isinstance(handler_return, Response) and not custom_response:
            raise ActionFailed(
                "Set custom_response to true, to be able to return custom response"
            )
        return handler_return

    @action(
        name="delete",
        text=_("Delete"),
        confirmation=_("Are you sure you want to delete selected items?"),
        submit_btn_text=_("Yes, delete all"),
        submit_btn_class="btn-danger",
    )
    async def delete_action(self, request: Request, pks: List[Any]) -> str:
        affected_rows = await self.delete(request, pks)
        return ngettext(
            "Item was successfully deleted",
            "%(count)d items were successfully deleted",
            affected_rows or 0,
        ) % {"count": affected_rows}

    @link_row_action(
        name="view",
        text=_("View"),
        icon_class="fa-solid fa-eye",
        exclude_from_detail=True,
    )
    def row_action_1_view(self, request: Request, pk: Any) -> str:
        route_name = request.app.state.ROUTE_NAME
        return str(
            request.url_for(route_name + ":detail", identity=self.identity, pk=pk)
        )

    @link_row_action(
        name="edit",
        text=_("Edit"),
        icon_class="fa-solid fa-edit",
        action_btn_class="btn-primary",
    )
    def row_action_2_edit(self, request: Request, pk: Any) -> str:
        route_name = request.app.state.ROUTE_NAME
        return str(request.url_for(route_name + ":edit", identity=self.identity, pk=pk))

    @row_action(
        name="delete",
        text=_("Delete"),
        confirmation=_("Are you sure you want to delete this item?"),
        icon_class="fa-solid fa-trash",
        submit_btn_text="Yes, delete",
        submit_btn_class="btn-danger",
        action_btn_class="btn-danger",
    )
    async def row_action_3_delete(self, request: Request, pk: Any) -> str:
        await self.delete(request, [pk])
        return gettext("Item was successfully deleted")

    @abstractmethod
    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: Union[Dict[str, Any], str, None] = None,
        order_by: Optional[List[str]] = None,
    ) -> Sequence[Any]:
        """
        Find all items
        Parameters:
            request: The request being processed
            where: Can be dict for complex query
                ```json
                 {"and":[{"id": {"gt": 5}},{"name": {"startsWith": "ban"}}]}
                ```
                or plain text for full search
            skip: should return values start from position skip+1
            limit: number of maximum items to return
            order_by: order data clauses in form `["id asc", "name desc"]`
        """
        raise NotImplementedError()

    @abstractmethod
    async def count(
        self,
        request: Request,
        where: Union[Dict[str, Any], str, None] = None,
    ) -> int:
        """
        Count items
        Parameters:
            request: The request being processed
            where: Can be dict for complex query
                ```json
                 {"and":[{"id": {"gt": 5}},{"name": {"startsWith": "ban"}}]}
                ```
                or plain text for full search
        """
        raise NotImplementedError()

    @abstractmethod
    async def find_by_pk(self, request: Request, pk: Any) -> Any:
        """
        Find one item
        Parameters:
            request: The request being processed
            pk: Primary key
        """
        raise NotImplementedError()

    @abstractmethod
    async def find_by_pks(self, request: Request, pks: List[Any]) -> Sequence[Any]:
        """
        Find many items
        Parameters:
            request: The request being processed
            pks: List of Primary key
        """
        raise NotImplementedError()

    async def before_create(
        self, request: Request, data: Dict[str, Any], obj: Any
    ) -> None:
        """
        This hook is called before a new item is created.

        Args:
            request: The request being processed.
            data: Dict values contained converted form data.
            obj: The object about to be created.
        """

    @abstractmethod
    async def create(self, request: Request, data: Dict) -> Any:
        """
        Create item
        Parameters:
            request: The request being processed
            data: Dict values contained converted form data
        Returns:
            Any: Created Item
        """
        raise NotImplementedError()

    async def after_create(self, request: Request, obj: Any) -> None:
        """
        This hook is called after a new item is successfully created.

        Args:
            request: The request being processed.
            obj: The newly created object.
        """

    async def before_edit(
        self, request: Request, data: Dict[str, Any], obj: Any
    ) -> None:
        """
        This hook is called before an item is edited.

        Args:
            request: The request being processed.
            data: Dict values contained converted form data
            obj: The object about to be edited.
        """

    @abstractmethod
    async def edit(self, request: Request, pk: Any, data: Dict[str, Any]) -> Any:
        """
        Edit item
        Parameters:
            request: The request being processed
            pk: Primary key
            data: Dict values contained converted form data
        Returns:
            Any: Edited Item
        """
        raise NotImplementedError()

    async def after_edit(self, request: Request, obj: Any) -> None:
        """
        This hook is called after an item is successfully edited.

        Args:
            request: The request being processed.
            obj: The edited object.
        """

    async def before_delete(self, request: Request, obj: Any) -> None:
        """
        This hook is called before an item is deleted.

        Args:
            request: The request being processed.
            obj: The object about to be deleted.
        """

    @abstractmethod
    async def delete(self, request: Request, pks: List[Any]) -> Optional[int]:
        """
        Bulk delete items
        Parameters:
            request: The request being processed
            pks: List of primary keys
        """
        raise NotImplementedError()

    async def after_delete(self, request: Request, obj: Any) -> None:
        """
        This hook is called after an item is successfully deleted.

        Args:
            request: The request being processed.
            obj: The deleted object.
        """

    def can_view_details(self, request: Request) -> bool:
        """Permission for viewing full details of Item. Return True by default"""
        return True

    def can_create(self, request: Request) -> bool:
        """Permission for creating new Items. Return True by default"""
        return True

    def can_edit(self, request: Request) -> bool:
        """Permission for editing Items. Return True by default"""
        return True

    def can_delete(self, request: Request) -> bool:
        """Permission for deleting Items. Return True by default"""
        return True

    async def serialize_field_value(
        self, value: Any, field: BaseField, action: RequestAction, request: Request
    ) -> Any:
        """
        Format output value for each field.

        !!! important

            The returned value should be json serializable

        Parameters:
            value: attribute of item returned by `find_all` or `find_by_pk`
            field: Starlette Admin field for this attribute
            action: Specify where the data will be used. Possible values are
                `VIEW` for detail page, `EDIT` for editing page and `API`
                for listing page and select2 data.
            request: The request being processed
        """
        if value is None:
            return await field.serialize_none_value(request, action)
        return await field.serialize_value(request, value, action)

    async def serialize(
        self,
        obj: Any,
        request: Request,
        action: RequestAction,
        include_relationships: bool = True,
        include_select2: bool = False,
    ) -> Dict[str, Any]:
        obj_serialized: Dict[str, Any] = {}
        obj_meta: Dict[str, Any] = {}
        for field in self.get_fields_list(request, action):
            if isinstance(field, RelationField) and include_relationships:
                value = getattr(obj, field.name, None)
                foreign_model = self._find_foreign_model(field.identity)  # type: ignore
                if value is None:
                    obj_serialized[field.name] = None
                elif isinstance(field, HasOne):
                    if action == RequestAction.EDIT:
                        obj_serialized[field.name] = (
                            await foreign_model.get_serialized_pk_value(request, value)
                        )
                    else:
                        obj_serialized[field.name] = await foreign_model.serialize(
                            value, request, action, include_relationships=False
                        )
                else:
                    if action == RequestAction.EDIT:
                        obj_serialized[field.name] = [
                            (await foreign_model.get_serialized_pk_value(request, obj))
                            for obj in value
                        ]
                    else:
                        obj_serialized[field.name] = [
                            await foreign_model.serialize(
                                v, request, action, include_relationships=False
                            )
                            for v in value
                        ]
            elif not isinstance(field, RelationField):
                value = await field.parse_obj(request, obj)
                obj_serialized[field.name] = await self.serialize_field_value(
                    value, field, action, request
                )
        if include_select2:
            obj_meta["select2"] = {
                "selection": await self.select2_selection(obj, request),
                "result": await self.select2_result(obj, request),
            }
        obj_meta["repr"] = await self.repr(obj, request)

        # Make sure the primary key is always available
        pk_attr = not_none(self.pk_attr)
        if pk_attr not in obj_serialized:
            pk_value = await self.get_serialized_pk_value(request, obj)
            obj_serialized[pk_attr] = pk_value

        pk = await self.get_pk_value(request, obj)
        route_name = request.app.state.ROUTE_NAME
        obj_meta["detailUrl"] = str(
            request.url_for(route_name + ":detail", identity=self.identity, pk=pk)
        )
        obj_serialized["_meta"] = obj_meta
        return obj_serialized

    async def repr(self, obj: Any, request: Request) -> str:
        """Return a string representation of the given object that can be displayed in the admin interface.

        If the object has a custom representation method `__admin_repr__`, it is used to generate the string. Otherwise,
        the value of the object's primary key attribute is used.

        Args:
            obj: The object to represent.
            request: The request being processed

        Example:
            For example, the following implementation for a `User` model will display
            the user's full name instead of their primary key in the admin interface:

            ```python
            class User:
                id: int
                first_name: str
                last_name: str

                def __admin_repr__(self, request: Request):
                    return f"{self.last_name} {self.first_name}"
            ```
        """
        repr_method = getattr(obj, "__admin_repr__", None)
        if repr_method is None:
            return str(await self.get_pk_value(request, obj))
        if inspect.iscoroutinefunction(repr_method):
            return await repr_method(request)
        return repr_method(request)

    async def select2_result(self, obj: Any, request: Request) -> str:
        """Returns an HTML-formatted string that represents the search results for a Select2 search box.

        By default, this method returns a string that contains all the object's attributes in a list except
        relation and file attributes.

        If the object has a custom representation method `__admin_select2_repr__`, it is used to generate the
        HTML-formatted string.

        !!! note

            The returned value should be valid HTML.

        !!! danger

            Escape your database value to avoid Cross-Site Scripting (XSS) attack.
            You can use Jinja2 Template render with `autoescape=True`.
            For more information [click here](https://owasp.org/www-community/attacks/xss/)

        Parameters:
            obj: The object returned by the `find_all` or `find_by_pk` method.
            request: The request being processed

        Example:
            Here is an example implementation for a `User` model
            that includes the user's name and photo:

            ```python
            class User:
                id: int
                name: str
                photo_url: str

                def __admin_select2_repr__(self, request: Request) -> str:
                    return f'<div><img src="{escape(photo_url)}"><span>{escape(self.name)}</span></div>'
            ```

        """
        template_str = (
            "<span>{%for col in fields %}{%if obj[col]%}<strong>{{col}}:"
            " </strong>{{obj[col]}} {%endif%}{%endfor%}</span>"
        )
        fields = [
            field.name
            for field in self.get_fields_list(request)
            if (
                not isinstance(field, (RelationField, FileField))
                and not field.exclude_from_detail
            )
        ]
        html_repr_method = getattr(
            obj,
            "__admin_select2_repr__",
            lambda request: Template(template_str, autoescape=True).render(
                obj=obj, fields=fields
            ),
        )
        if inspect.iscoroutinefunction(html_repr_method):
            return await html_repr_method(request)
        return html_repr_method(request)

    async def select2_selection(self, obj: Any, request: Request) -> str:
        """
        Returns the HTML representation of an item selected by a user in a Select2 component.
        By default, it simply calls `select2_result()`.

        !!! note

            The returned value should be valid HTML.

        !!! danger

            Escape your database value to avoid Cross-Site Scripting (XSS) attack.
            You can use Jinja2 Template render with `autoescape=True`.
            For more information [click here](https://owasp.org/www-community/attacks/xss/)

        Parameters:
            obj: item returned by `find_all` or `find_by_pk`
            request: The request being processed

        """
        return await self.select2_result(obj, request)

    async def get_pk_value(self, request: Request, obj: Any) -> Any:
        return getattr(obj, not_none(self.pk_attr))

    async def get_serialized_pk_value(self, request: Request, obj: Any) -> Any:
        """
        Return serialized value of the primary key.

        !!! note

            The returned value should be JSON-serializable.

        Parameters:
            request: The request being processed
            obj: object to get primary key of

        Returns:
            Any: Serialized value of a PK.
        """
        return await self.get_pk_value(request, obj)

    def _length_menu(self) -> Any:
        return [
            self.page_size_options,
            [(_("All") if i < 0 else i) for i in self.page_size_options],
        ]

    def _search_columns_selector(self) -> List[str]:
        return [f"{name}:name" for name in self.searchable_fields]  # type: ignore

    def _export_columns_selector(self) -> List[str]:
        return [f"{name}:name" for name in self.export_fields]  # type: ignore

    def get_fields_list(
        self,
        request: Request,
        action: RequestAction = RequestAction.LIST,
    ) -> Sequence[BaseField]:
        """Return a list of field instances to display in the specified view action.
        This function excludes fields with corresponding exclude flags, which are
        determined by the `exclude_fields_from_*` attributes.

        Parameters:
             request: The request being processed.
             action: The type of action being performed on the view.
        """
        return extract_fields(self.fields, action)

    def _additional_css_links(
        self, request: Request, action: RequestAction
    ) -> Sequence[str]:
        links = self.additional_css_links or []
        for field in self.get_fields_list(request, action):
            for link in field.additional_css_links(request, action) or []:
                if link not in links:
                    links.append(link)
        return links

    def _additional_js_links(
        self, request: Request, action: RequestAction
    ) -> Sequence[str]:
        links = self.additional_js_links or []
        for field in self.get_fields_list(request, action):
            for link in field.additional_js_links(request, action) or []:
                if link not in links:
                    links.append(link)
        return links

    async def _configs(self, request: Request) -> Dict[str, Any]:
        locale = get_locale()
        return {
            "label": self.label,
            "pageSize": self.page_size,
            "lengthMenu": self._length_menu(),
            "searchColumns": self._search_columns_selector(),
            "exportColumns": self._export_columns_selector(),
            "fieldsDefaultSort": dict(
                (it, False) if isinstance(it, str) else it
                for it in self.fields_default_sort  # type: ignore[union-attr]
            ),
            "exportTypes": self.export_types,
            "columnVisibility": self.column_visibility,
            "searchBuilder": self.search_builder,
            "responsiveTable": self.responsive_table,
            "stateSave": self.save_state,
            "fields": [f.dict() for f in self.get_fields_list(request)],
            "pk": self.pk_attr,
            "locale": locale,
            "apiUrl": request.url_for(
                f"{request.app.state.ROUTE_NAME}:api", identity=self.identity
            ),
            "actionUrl": request.url_for(
                f"{request.app.state.ROUTE_NAME}:action", identity=self.identity
            ),
            "rowActionUrl": request.url_for(
                f"{request.app.state.ROUTE_NAME}:row-action", identity=self.identity
            ),
            "dt_i18n_url": request.url_for(
                f"{request.app.state.ROUTE_NAME}:statics", path=f"i18n/dt/{locale}.json"
            ),
            "datatablesOptions": self.datatables_options,
        }
