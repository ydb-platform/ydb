from textual._color_constants import COLOR_NAME_TO_RGB
from textual.app import App, ComposeResult
from textual.color import Color
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.design import ColorSystem
from textual.widget import Widget
from textual.widgets import Footer, Label, OptionList, Static, TabbedContent
from textual.widgets.option_list import Option

try:
    from textual.lazy import Lazy
except ImportError:

    def Lazy(widget: Widget) -> Widget:  # type: ignore
        return widget


class ColorBar(Static):
    pass


class ColorItem(Horizontal):
    pass


class ColorGroup(Vertical):
    pass


class Content(Vertical):
    pass


class ColorsView(VerticalScroll):
    pass


class ColorTabs(TabbedContent):
    pass


class NamedColorsView(ColorsView):
    def compose(self) -> ComposeResult:
        with ColorGroup(id=f"group-named"):
            for name, rgb in COLOR_NAME_TO_RGB.items():
                color = Color(*rgb)
                with ColorItem() as ci:
                    ci.styles.background = name
                    yield ColorBar(name, classes="text")
                    yield ColorBar(f"{color.hex6}", classes="text")
                    yield ColorBar(f"{color.rgb}", classes="text text-left")


class ThemeColorsView(ColorsView, can_focus=False):
    def compose(self) -> ComposeResult:
        LEVELS = [
            "darken-3",
            "darken-2",
            "darken-1",
            "",
            "lighten-1",
            "lighten-2",
            "lighten-3",
        ]

        for color_name in ColorSystem.COLOR_NAMES:
            with ColorGroup(id=f"group-{color_name}"):
                yield Label(f'"{color_name}"')
                for level in LEVELS:
                    color = f"{color_name}-{level}" if level else color_name
                    with ColorItem(classes=color):
                        yield ColorBar(f"${color}", classes="text label")
                        yield ColorBar("$text-muted", classes="muted")
                        yield ColorBar("$text-disabled", classes="disabled")


class ColorsApp(App[None]):
    CSS_PATH = "colors.tcss"

    BINDINGS = [
        ("[", "previous_theme", "Previous theme"),
        ("]", "next_theme", "Next theme"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.theme_names = [
            theme for theme in self.available_themes if theme != "textual-ansi"
        ]

    def compose(self) -> ComposeResult:
        yield Footer()
        with ColorTabs("Theme Colors", "Named Colors"):
            with Content(id="theme"):
                sidebar = OptionList(
                    *[
                        Option(color_name, id=color_name)
                        for color_name in ColorSystem.COLOR_NAMES
                    ],
                    id="sidebar",
                )
                sidebar.border_title = "Theme Colors"
                yield sidebar
                yield ThemeColorsView()
            yield Lazy(NamedColorsView())

    def on_option_list_option_highlighted(
        self, event: OptionList.OptionHighlighted
    ) -> None:
        self.query(ColorGroup).remove_class("-active")
        group = self.query_one(f"#group-{event.option.id}", ColorGroup)
        group.add_class("-active")
        group.scroll_visible(top=True, speed=150)

    def action_next_theme(self) -> None:
        themes = self.theme_names
        index = themes.index(self.current_theme.name)
        self.theme = themes[(index + 1) % len(themes)]
        self.notify_new_theme(self.current_theme.name)

    def action_previous_theme(self) -> None:
        themes = self.theme_names
        index = themes.index(self.current_theme.name)
        self.theme = themes[(index - 1) % len(themes)]
        self.notify_new_theme(self.current_theme.name)

    def notify_new_theme(self, theme_name: str) -> None:
        self.clear_notifications()
        self.notify(f"Theme is {theme_name}")


if __name__ == "__main__":
    ColorsApp().run()
