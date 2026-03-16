from rich.cells import cell_len
from rich.console import Console, ConsoleOptions, RenderResult, Style
from rich.padding import Padding
from rich.panel import Panel as RichPanel
from rich.segment import Segment
from rich.text import Text


# this is a custom version of Rich's panel, where we override
# the __rich_console__ magic method to just render a basic panel
class Panel(RichPanel):
    def __rich_console__(
        self, console: "Console", options: "ConsoleOptions"
    ) -> "RenderResult":
        # copied from Panel.__rich_console__
        _padding = Padding.unpack(self.padding)
        renderable = (
            Padding(self.renderable, _padding) if any(_padding) else self.renderable
        )
        style = console.get_style(self.style)
        partial_border_style = console.get_style(self.border_style)
        border_style = style + partial_border_style
        width = (
            options.max_width
            if self.width is None
            else min(options.max_width, self.width)
        )

        safe_box: bool = console.safe_box if self.safe_box is None else self.safe_box
        box = self.box.substitute(options, safe=safe_box)

        def align_text(
            text: Text, width: int, align: str, character: str, style: Style
        ) -> Text:
            """Gets new aligned text.

            Args:
                text (Text): Title or subtitle text.
                width (int): Desired width.
                align (str): Alignment.
                character (str): Character for alignment.
                style (Style): Border style

            Returns:
                Text: New text instance
            """
            text = text.copy()
            text.truncate(width)
            excess_space = width - cell_len(text.plain)
            if text.style:
                text.stylize(console.get_style(text.style))

            if excess_space:
                if align == "left":
                    return Text.assemble(
                        text,
                        (character * excess_space, style),
                        no_wrap=True,
                        end="",
                    )
                elif align == "center":
                    left = excess_space // 2
                    return Text.assemble(
                        (character * left, style),
                        text,
                        (character * (excess_space - left), style),
                        no_wrap=True,
                        end="",
                    )
                else:
                    return Text.assemble(
                        (character * excess_space, style),
                        text,
                        no_wrap=True,
                        end="",
                    )
            return text

        title_text = self._title
        if title_text is not None:
            title_text.stylize_before(partial_border_style)

        child_width = (
            width - 2
            if self.expand
            else console.measure(
                renderable, options=options.update_width(width - 2)
            ).maximum
        )
        child_height = self.height or options.height or None
        if child_height:
            child_height -= 2
        if title_text is not None:
            child_width = min(
                options.max_width - 2, max(child_width, title_text.cell_len + 2)
            )

        width = child_width + 2
        child_options = options.update(
            width=child_width, height=child_height, highlight=self.highlight
        )
        lines = console.render_lines(renderable, child_options, style=style)

        line_start = Segment(box.mid_left, border_style)
        line_end = Segment(f"{box.mid_right}", border_style)
        new_line = Segment.line()
        if title_text is None or width <= 4:
            yield Segment(box.get_top([width - 2]), border_style)
        else:
            title_text = align_text(
                title_text,
                width - 4,
                self.title_align,
                box.top,
                border_style,
            )
            # changed from `box.top_left + box.top` to just `box.top_left``
            yield Segment(box.top_left, border_style)
            yield from console.render(title_text, child_options.update_width(width - 4))
            # changed from `box.top + box.top_right` to `box.top * 2 + box.top_right``
            yield Segment(box.top * 2 + box.top_right, border_style)

        yield new_line
        for line in lines:
            yield line_start
            yield from line
            yield line_end
            yield new_line

        subtitle_text = self._subtitle
        if subtitle_text is not None:
            subtitle_text.stylize_before(partial_border_style)

        if subtitle_text is None or width <= 4:
            yield Segment(box.get_bottom([width - 2]), border_style)
        else:
            subtitle_text = align_text(
                subtitle_text,
                width - 4,
                self.subtitle_align,
                box.bottom,
                border_style,
            )
            yield Segment(box.bottom_left + box.bottom, border_style)
            yield from console.render(
                subtitle_text, child_options.update_width(width - 4)
            )
            yield Segment(box.bottom + box.bottom_right, border_style)

        yield new_line
