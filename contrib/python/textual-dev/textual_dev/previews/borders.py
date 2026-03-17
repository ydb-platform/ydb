from typing import cast

from textual.app import App, ComposeResult
from textual.css.constants import VALID_BORDER
from textual.css.types import EdgeType
from textual.widgets import Label, OptionList
from textual.widgets.option_list import Option

TEXT = """I must not fear.
Fear is the mind-killer.
Fear is the little-death that brings total obliteration.
I will face my fear.
I will permit it to pass over me and through me.
And when it has gone past, I will turn the inner eye to see its path.
Where the fear has gone there will be nothing. Only I will remain."""


class BorderApp(App[None]):
    """Demonstrates the border styles."""

    CSS = """
    Screen {
        align: center middle;
        overflow: auto;
    }
    OptionList#sidebar {
        width: 20;
        dock: left;
        height: 1fr;
    }
    #text {
        margin: 2 4;
        padding: 2 4;
        border: solid $border;
        height: auto;
        background: $panel;
        color: $text;
        border-title-align: center;
    }
    """

    def compose(self) -> ComposeResult:
        yield OptionList(
            *[Option(border, id=border) for border in sorted(VALID_BORDER) if border],
            id="sidebar",
        )

        self.text = Label(TEXT, id="text")
        self.text.shrink = True
        self.text.border_title = "solid"
        self.text.border_subtitle = "border subtitle"
        yield self.text

    def on_mount(self) -> None:
        self.theme_changed_signal.subscribe(self, self.update_border)

    @property
    def sidebar(self) -> OptionList:
        return self.query_one("#sidebar", OptionList)

    def on_option_list_option_highlighted(
        self, event: OptionList.OptionHighlighted
    ) -> None:
        border_name = event.option.id
        self.text.border_title = border_name
        self.update_border(self.app.current_theme)

    def update_border(self, _) -> None:
        self.text.styles.border = (
            cast(
                EdgeType,
                self.sidebar.get_option_at_index(self.sidebar.highlighted or 0).id,
            ),
            self.theme_variables["border"],
        )


if __name__ == "__main__":
    BorderApp().run()
