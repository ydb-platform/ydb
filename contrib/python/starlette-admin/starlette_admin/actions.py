from typing import Any, Awaitable, Callable, Optional

from starlette_admin.i18n import lazy_gettext as _


def action(
    name: str,
    text: str,
    confirmation: Optional[str] = None,
    submit_btn_class: Optional[str] = "btn-primary",
    submit_btn_text: Optional[str] = _("Yes, Proceed"),
    icon_class: Optional[str] = None,
    form: Optional[str] = None,
    custom_response: Optional[bool] = False,
) -> Callable[[Callable[..., Awaitable[str]]], Any]:
    """
    Decorator to add custom batch actions to your [ModelView][starlette_admin.views.BaseModelView]

    Args:
        name: unique action name for your ModelView
        text: Action text
        confirmation: Confirmation text. If not provided, action will be executed
                      unconditionally.
        submit_btn_text: Submit button text
        submit_btn_class: Submit button variant (ex. `btn-primary`, `btn-ghost-info`,
                `btn-outline-danger`, ...)
        icon_class: Icon class (ex. `fa-lite fa-folder`, `fa-duotone fa-circle-right`, ...)
        form: Custom form to collect data from user
        custom_response: Set to True when you want to return a custom Starlette response
            from your action instead of a string.


    !!! usage

        ```python
        class ArticleView(ModelView):
            actions = ['make_published', 'redirect']

            @action(
                name="make_published",
                text="Mark selected articles as published",
                confirmation="Are you sure you want to mark selected articles as published ?",
                submit_btn_text="Yes, proceed",
                submit_btn_class="btn-success",
                form='''
                <form>
                    <div class="mt-3">
                        <input type="text" class="form-control" name="example-text-input" placeholder="Enter value">
                    </div>
                </form>
                '''
            )
            async def make_published_action(self, request: Request, pks: List[Any]) -> str:
                # Write your logic here

                data: FormData =  await request.form()
                user_input = data.get("example-text-input")

                if ... :
                    # Display meaningfully error
                    raise ActionFailed("Sorry, We can't proceed this action now.")
                # Display successfully message
                return "{} articles were successfully marked as published".format(len(pks))

            # For custom response
            @action(
                name="redirect",
                text="Redirect",
                custom_response=True,
                confirmation="Fill the form",
                form='''
                <form>
                    <div class="mt-3">
                        <input type="text" class="form-control" name="value" placeholder="Enter value">
                    </div>
                </form>
                '''
             )
            async def redirect_action(self, request: Request, pks: List[Any]) -> Response:
                data = await request.form()
                return RedirectResponse(f"https://example.com/?value={data['value']}")
        ```
    """

    def wrap(f: Callable[..., Awaitable[str]]) -> Callable[..., Awaitable[str]]:
        f._action = {  # type: ignore
            "name": name,
            "text": text,
            "confirmation": confirmation,
            "submit_btn_text": submit_btn_text,
            "submit_btn_class": submit_btn_class,
            "icon_class": icon_class,
            "form": form if form is not None else "",
            "custom_response": custom_response,
        }
        return f

    return wrap


def row_action(
    name: str,
    text: str,
    confirmation: Optional[str] = None,
    action_btn_class: Optional[str] = None,
    submit_btn_class: Optional[str] = "btn-primary",
    submit_btn_text: Optional[str] = _("Yes, Proceed"),
    icon_class: Optional[str] = None,
    form: Optional[str] = None,
    custom_response: Optional[bool] = False,
    exclude_from_list: bool = False,
    exclude_from_detail: bool = False,
) -> Callable[[Callable[..., Awaitable[str]]], Any]:
    """
    Decorator to add custom row actions to your [ModelView][starlette_admin.views.BaseModelView]

    Args:
        name: Unique row action name for the ModelView.
        text: Action text displayed to users.
        confirmation: Confirmation text; if provided, the action will require confirmation.
        action_btn_class: Action button variant for detail page (ex. `btn-success`, `btn-outline`, ...)
        submit_btn_class: Submit button variant (ex. `btn-primary`, `btn-ghost-info`, `btn-outline-danger`, ...)
        submit_btn_text: Text for the submit button.
        icon_class: Icon class (ex. `fa-lite fa-folder`, `fa-duotone fa-circle-right`, ...)
        form: Custom HTML to collect data from the user.
        custom_response: Set to True when you want to return a custom Starlette response
            from your action instead of a string.
        exclude_from_list: Set to True to exclude the action from the list view.
        exclude_from_detail: Set to True to exclude the action from the detail view.


    !!! usage

        ```python
        @row_action(
            name="make_published",
            text="Mark as published",
            confirmation="Are you sure you want to mark this article as published ?",
            icon_class="fas fa-check-circle",
            submit_btn_text="Yes, proceed",
            submit_btn_class="btn-success",
            action_btn_class="btn-info",
        )
        async def make_published_row_action(self, request: Request, pk: Any) -> str:
            session: Session = request.state.session
            article = await self.find_by_pk(request, pk)
            if article.status == Status.Published:
                raise ActionFailed("The article is already marked as published.")
            article.status = Status.Published
            session.add(article)
            session.commit()
            return f"The article was successfully marked as published."
        ```
    """

    def wrap(f: Callable[..., Awaitable[str]]) -> Callable[..., Awaitable[str]]:
        f._row_action = {  # type: ignore
            "name": name,
            "text": text,
            "confirmation": confirmation,
            "action_btn_class": action_btn_class,
            "submit_btn_text": submit_btn_text,
            "submit_btn_class": submit_btn_class,
            "icon_class": icon_class,
            "form": form if form is not None else "",
            "custom_response": custom_response,
            "exclude_from_list": exclude_from_list,
            "exclude_from_detail": exclude_from_detail,
        }
        return f

    return wrap


def link_row_action(
    name: str,
    text: str,
    action_btn_class: Optional[str] = None,
    icon_class: Optional[str] = None,
    exclude_from_list: bool = False,
    exclude_from_detail: bool = False,
) -> Callable[[Callable[..., str]], Any]:
    """
    Decorator to add custom row link actions to a ModelView for URL redirection.

    !!! note

        This decorator is designed to create row actions that redirect to a URL, making it ideal for cases where a
        row action should simply navigate users to a website or internal page.

    Args:
        name: Unique row action name for the ModelView.
        text: Action text displayed to users.
        action_btn_class: Action button variant for detail page (ex. `btn-success`, `btn-outline`, ...)
        icon_class: Icon class (ex. `fa-lite fa-folder`, `fa-duotone fa-circle-right`, ...)
        exclude_from_list: Set to True to exclude the action from the list view.
        exclude_from_detail: Set to True to exclude the action from the detail view.


    !!! usage

        ```python
        @link_row_action(
            name="go_to_example",
            text="Go to example.com",
            icon_class="fas fa-arrow-up-right-from-square",
        )
        def go_to_example_row_action(self, request: Request, pk: Any) -> str:
            return f"https://example.com/?pk={pk}"
        ```

    """

    def wrap(f: Callable[..., str]) -> Callable[..., str]:
        f._row_action = {  # type: ignore
            "name": name,
            "text": text,
            "action_btn_class": action_btn_class,
            "icon_class": icon_class,
            "is_link": True,
            "exclude_from_list": exclude_from_list,
            "exclude_from_detail": exclude_from_detail,
        }
        return f

    return wrap
