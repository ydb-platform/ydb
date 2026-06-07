from textual.events import Key
from textual.widgets import Button, Checkbox, Input


class OperationFormInput(Input):
    def _move(self, event: Key, step: int) -> None:
        form = self.parent
        while form is not None:
            if hasattr(form, "move_operation_form_focus_from"):
                if form.move_operation_form_focus_from(self.id, step):
                    event.stop()
                return
            form = form.parent

    def key_up(self, event: Key) -> None:
        self._move(event, -1)

    def key_down(self, event: Key) -> None:
        self._move(event, 1)


class OperationFormCheckbox(Checkbox):
    def _move(self, event: Key, step: int) -> None:
        form = self.parent
        while form is not None:
            if hasattr(form, "move_operation_form_focus_from"):
                if form.move_operation_form_focus_from(self.id, step):
                    event.stop()
                return
            form = form.parent

    def key_up(self, event: Key) -> None:
        self._move(event, -1)

    def key_down(self, event: Key) -> None:
        self._move(event, 1)


class OperationFormButton(Button):
    def _move(self, event: Key, step: int) -> None:
        form = self.parent
        while form is not None:
            if hasattr(form, "move_operation_form_focus_from"):
                if form.move_operation_form_focus_from(self.id, step):
                    event.stop()
                return
            form = form.parent

    def key_up(self, event: Key) -> None:
        self._move(event, -1)

    def key_down(self, event: Key) -> None:
        self._move(event, 1)
