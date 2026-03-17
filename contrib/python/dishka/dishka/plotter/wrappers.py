from dishka import AsyncContainer, Container
from .d2 import D2Renderer
from .mermaid import MermaidRenderer
from .transform import Transformer


def render_mermaid(container: AsyncContainer | Container) -> str:
    return MermaidRenderer().render(
        Transformer().transform(container),
    )


def render_d2(container: AsyncContainer | Container) -> str:
    return D2Renderer().render(
        Transformer().transform(container),
    )
