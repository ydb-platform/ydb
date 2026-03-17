from functools import singledispatch
from typing import Any, Optional

import pytest

import argclass


def test_subparsers():
    class Subparser(argclass.Parser):
        foo: str = argclass.Argument()

    class Parser(argclass.Parser):
        subparser = Subparser()

    parser = Parser()
    parser.parse_args(["subparser", "--foo=bar"])
    assert parser.subparser.foo == "bar"


def test_two_subparsers():
    class Subparser(argclass.Parser):
        spam: str = argclass.Argument()

    class Parser(argclass.Parser):
        foo = Subparser()
        bar = Subparser()

    parser = Parser()
    parser.parse_args(["bar", "--spam=egg"])
    assert parser.bar.spam == "egg"

    with pytest.raises(AttributeError):
        _ = parser.foo.spam


def test_two_simple_subparsers():
    class Subparser(argclass.Parser):
        spam: str

    class Parser(argclass.Parser):
        foo = Subparser()
        bar = Subparser()

    parser = Parser()
    parser.parse_args(["foo", "--spam=egg"])
    assert parser.foo.spam == "egg"

    with pytest.raises(AttributeError):
        _ = parser.bar.spam


def test_current_subparsers():
    class AddressPortGroup(argclass.Group):
        address: str = argclass.Argument(default="127.0.0.1")
        port: int

    class CommitCommand(argclass.Parser):
        comment: str = argclass.Argument()

    class PushCommand(argclass.Parser):
        comment: str = argclass.Argument()

    class Parser(argclass.Parser):
        log_level: int = argclass.LogLevel
        endpoint = AddressPortGroup(
            title="Endpoint options",
            defaults=dict(port=8080),
        )
        commit: Optional[CommitCommand] = CommitCommand()
        push: Optional[PushCommand] = PushCommand()

    state: dict = {}

    @singledispatch
    def handle_subparser(subparser: Any) -> None:
        raise NotImplementedError(
            f"Unexpected subparser type {subparser.__class__!r}",
        )

    @handle_subparser.register(type(None))
    def handle_none(_: None) -> None:
        Parser().print_help()
        exit(12)

    @handle_subparser.register(CommitCommand)
    def handle_commit(subparser: CommitCommand) -> None:
        state["commit"] = subparser

    @handle_subparser.register(PushCommand)
    def handle_push(subparser: PushCommand) -> None:
        state["push"] = subparser

    parser = Parser()
    parser.parse_args([])

    with pytest.raises(SystemExit) as e:
        handle_subparser(parser.current_subparser)

    assert e.value.code == 12

    parser.parse_args(["commit"])
    handle_subparser(parser.current_subparser)
    assert "commit" in state
    assert "push" not in state

    parser.parse_args(["push"])
    handle_subparser(parser.current_subparser)
    assert "push" in state


def test_nested_subparsers() -> None:
    class Group(argclass.Group):
        value: int = argclass.Argument()

    class SubSubParser(argclass.Parser):
        str_value: str = argclass.Argument()
        group: Group = Group()

    class SubParser(argclass.Parser):
        subsub: Optional[SubSubParser] = SubSubParser()
        val: str = argclass.Argument()

    class Parser(argclass.Parser):
        sub: Optional[SubParser] = SubParser()
        sub2: Optional[SubParser] = SubParser()

    parser = Parser()
    args = parser.parse_args(
        [
            "sub",
            "--val=lol",
            "subsub",
            "--group-value=2",
            "--str-value=kek",
        ]
    )

    assert args.sub is not None
    assert args.sub.val == "lol"
    assert args.sub.subsub is not None
    assert args.sub.subsub.str_value == "kek"
    assert args.sub.subsub.group.value == 2
    with pytest.raises(AttributeError):
        print(args.sub2.val)  # type: ignore[union-attr]


def test_call() -> None:
    class SubParser(argclass.Parser):
        _flag = False

        def __call__(self):
            self.__class__._flag = not self.__class__._flag

    class Parser(argclass.Parser):
        subparser1 = SubParser()
        subparser2 = SubParser()
        flag: bool = True

    parser = Parser()

    parser.parse_args(["subparser1"])
    parser()
    assert parser.subparser1._flag

    parser.parse_args(["subparser2"])
    parser()
    assert not parser.subparser2._flag
