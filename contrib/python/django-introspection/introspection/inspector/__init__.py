# flake8: noqa: F401
from .inspector import AppInspector


def title(name: str) -> None:
    print("========================================================")
    print("                     " + name)
    print("========================================================")


def subtitle(name: str) -> None:
    print(f"-------------- {name} --------------")
