"""@private"""

import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, TypedDict, Union

from langfuse.api.resources.commons.types.dataset import (
    Dataset,  # noqa: F401
)

# these imports need to stay here, otherwise imports from our clients wont work
from langfuse.api.resources.commons.types.dataset_item import DatasetItem  # noqa: F401

# noqa: F401
from langfuse.api.resources.commons.types.dataset_run import DatasetRun  # noqa: F401

# noqa: F401
from langfuse.api.resources.commons.types.dataset_status import (  # noqa: F401
    DatasetStatus,
)
from langfuse.api.resources.commons.types.map_value import MapValue  # noqa: F401
from langfuse.api.resources.commons.types.observation import Observation  # noqa: F401
from langfuse.api.resources.commons.types.trace_with_full_details import (  # noqa: F401
    TraceWithFullDetails,
)

# noqa: F401
from langfuse.api.resources.dataset_items.types.create_dataset_item_request import (  # noqa: F401
    CreateDatasetItemRequest,
)
from langfuse.api.resources.dataset_run_items.types.create_dataset_run_item_request import (  # noqa: F401
    CreateDatasetRunItemRequest,
)

# noqa: F401
from langfuse.api.resources.datasets.types.create_dataset_request import (  # noqa: F401
    CreateDatasetRequest,
)
from langfuse.api.resources.prompts import Prompt, Prompt_Chat, Prompt_Text
from langfuse.logger import langfuse_logger


class ModelUsage(TypedDict):
    unit: Optional[str]
    input: Optional[int]
    output: Optional[int]
    total: Optional[int]
    input_cost: Optional[float]
    output_cost: Optional[float]
    total_cost: Optional[float]


class ChatMessageDict(TypedDict):
    role: str
    content: str


class ChatMessagePlaceholderDict(TypedDict):
    role: str
    content: str


class ChatMessageWithPlaceholdersDict_Message(TypedDict):
    type: Literal["message"]
    role: str
    content: str


class ChatMessageWithPlaceholdersDict_Placeholder(TypedDict):
    type: Literal["placeholder"]
    name: str


ChatMessageWithPlaceholdersDict = Union[
    ChatMessageWithPlaceholdersDict_Message,
    ChatMessageWithPlaceholdersDict_Placeholder,
]


class TemplateParser:
    OPENING = "{{"
    CLOSING = "}}"

    @staticmethod
    def _parse_next_variable(
        content: str, start_idx: int
    ) -> Optional[Tuple[str, int, int]]:
        """Returns (variable_name, start_pos, end_pos) or None if no variable found"""
        var_start = content.find(TemplateParser.OPENING, start_idx)
        if var_start == -1:
            return None

        var_end = content.find(TemplateParser.CLOSING, var_start)
        if var_end == -1:
            return None

        variable_name = content[
            var_start + len(TemplateParser.OPENING) : var_end
        ].strip()
        return (variable_name, var_start, var_end + len(TemplateParser.CLOSING))

    @staticmethod
    def find_variable_names(content: str) -> List[str]:
        names = []
        curr_idx = 0

        while curr_idx < len(content):
            result = TemplateParser._parse_next_variable(content, curr_idx)
            if not result:
                break
            names.append(result[0])
            curr_idx = result[2]

        return names

    @staticmethod
    def compile_template(content: str, data: Optional[Dict[str, Any]] = None) -> str:
        if data is None:
            return content

        result_list = []
        curr_idx = 0

        while curr_idx < len(content):
            result = TemplateParser._parse_next_variable(content, curr_idx)

            if not result:
                result_list.append(content[curr_idx:])
                break

            variable_name, var_start, var_end = result
            result_list.append(content[curr_idx:var_start])

            if variable_name in data:
                result_list.append(
                    str(data[variable_name]) if data[variable_name] is not None else ""
                )
            else:
                result_list.append(content[var_start:var_end])

            curr_idx = var_end

        return "".join(result_list)


class BasePromptClient(ABC):
    name: str
    version: int
    config: Dict[str, Any]
    labels: List[str]
    tags: List[str]
    commit_message: Optional[str]

    def __init__(self, prompt: Prompt, is_fallback: bool = False):
        self.name = prompt.name
        self.version = prompt.version
        self.config = prompt.config
        self.labels = prompt.labels
        self.tags = prompt.tags
        self.commit_message = prompt.commit_message
        self.is_fallback = is_fallback

    @abstractmethod
    def compile(
        self, **kwargs: Union[str, Any]
    ) -> Union[
        str,
        Sequence[
            Union[
                Dict[str, Any],
                ChatMessageDict,
                ChatMessageWithPlaceholdersDict_Placeholder,
            ]
        ],
    ]:
        pass

    @property
    @abstractmethod
    def variables(self) -> List[str]:
        pass

    @abstractmethod
    def __eq__(self, other: object) -> bool:
        pass

    @abstractmethod
    def get_langchain_prompt(self) -> Any:
        pass

    @staticmethod
    def _get_langchain_prompt_string(content: str) -> str:
        json_escaped_content = BasePromptClient._escape_json_for_langchain(content)

        return re.sub(r"{{\s*(\w+)\s*}}", r"{\g<1>}", json_escaped_content)

    @staticmethod
    def _escape_json_for_langchain(text: str) -> str:
        """Escapes every curly-brace that is part of a JSON object by doubling it.

        A curly brace is considered “JSON-related” when, after skipping any
        immediate whitespace, the next non-whitespace character is a single
        or double quote.

        Braces that are already doubled (e.g. {{variable}} placeholders) are
        left untouched.

        Parameters
        ----------
        text : str
            The input string that may contain JSON snippets.

        Returns:
        -------
        str
            The string with JSON-related braces doubled.
        """
        out = []  # collected characters
        stack = []  # True = “this { belongs to JSON”, False = normal “{”
        i, n = 0, len(text)

        while i < n:
            ch = text[i]

            # ---------- opening brace ----------
            if ch == "{":
                # leave existing “{{ …” untouched
                if i + 1 < n and text[i + 1] == "{":
                    out.append("{{")
                    i += 2
                    continue

                # look ahead to find the next non-space character
                j = i + 1
                while j < n and text[j].isspace():
                    j += 1

                is_json = j < n and text[j] in {"'", '"'}
                out.append("{{" if is_json else "{")
                stack.append(is_json)  # remember how this “{” was treated
                i += 1
                continue

            # ---------- closing brace ----------
            elif ch == "}":
                # leave existing “… }}” untouched
                if i + 1 < n and text[i + 1] == "}":
                    out.append("}}")
                    i += 2
                    continue

                is_json = stack.pop() if stack else False
                out.append("}}" if is_json else "}")
                i += 1
                continue

            # ---------- any other character ----------
            out.append(ch)
            i += 1

        return "".join(out)


class TextPromptClient(BasePromptClient):
    def __init__(self, prompt: Prompt_Text, is_fallback: bool = False):
        super().__init__(prompt, is_fallback)
        self.prompt = prompt.prompt

    def compile(self, **kwargs: Union[str, Any]) -> str:
        return TemplateParser.compile_template(self.prompt, kwargs)

    @property
    def variables(self) -> List[str]:
        """Return all the variable names in the prompt template."""
        return TemplateParser.find_variable_names(self.prompt)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return (
                self.name == other.name
                and self.version == other.version
                and self.prompt == other.prompt
                and self.config == other.config
            )

        return False

    def get_langchain_prompt(self, **kwargs: Union[str, Any]) -> str:
        """Convert Langfuse prompt into string compatible with Langchain PromptTemplate.

        This method adapts the mustache-style double curly braces {{variable}} used in Langfuse
        to the single curly brace {variable} format expected by Langchain.

        kwargs: Optional keyword arguments to precompile the template string. Variables that match
                the provided keyword arguments will be precompiled. Remaining variables must then be
                handled by Langchain's prompt template.

        Returns:
            str: The string that can be plugged into Langchain's PromptTemplate.
        """
        prompt = (
            TemplateParser.compile_template(self.prompt, kwargs)
            if kwargs
            else self.prompt
        )

        return self._get_langchain_prompt_string(prompt)


class ChatPromptClient(BasePromptClient):
    def __init__(self, prompt: Prompt_Chat, is_fallback: bool = False):
        super().__init__(prompt, is_fallback)
        self.prompt: List[ChatMessageWithPlaceholdersDict] = []

        for p in prompt.prompt:
            # Handle objects with attributes (normal case)
            if hasattr(p, "type") and hasattr(p, "name") and p.type == "placeholder":
                self.prompt.append(
                    ChatMessageWithPlaceholdersDict_Placeholder(
                        type="placeholder",
                        name=p.name,
                    ),
                )
            elif hasattr(p, "role") and hasattr(p, "content"):
                self.prompt.append(
                    ChatMessageWithPlaceholdersDict_Message(
                        type="message",
                        role=p.role,  # type: ignore
                        content=p.content,  # type: ignore
                    ),
                )

    def compile(
        self,
        **kwargs: Union[str, Any],
    ) -> Sequence[
        Union[
            Dict[str, Any], ChatMessageDict, ChatMessageWithPlaceholdersDict_Placeholder
        ]
    ]:
        """Compile the prompt with placeholders and variables.

        Args:
            **kwargs: Can contain both placeholder values (list of chat messages) and variable values.
                     Placeholders are resolved first, then variables are substituted.

        Returns:
            List of compiled chat messages as plain dictionaries, with unresolved placeholders kept as-is.
        """
        compiled_messages: List[
            Union[
                Dict[str, Any],
                ChatMessageDict,
                ChatMessageWithPlaceholdersDict_Placeholder,
            ]
        ] = []
        unresolved_placeholders: List[ChatMessageWithPlaceholdersDict_Placeholder] = []

        for chat_message in self.prompt:
            if chat_message["type"] == "message":
                # For regular messages, compile variables and add to output
                message_obj = chat_message  # type: ignore
                compiled_messages.append(
                    ChatMessageDict(
                        role=message_obj["role"],  # type: ignore
                        content=TemplateParser.compile_template(
                            message_obj["content"],  # type: ignore
                            kwargs,
                        ),
                    ),
                )
            elif chat_message["type"] == "placeholder":
                placeholder_name = chat_message["name"]
                if placeholder_name in kwargs:
                    placeholder_value = kwargs[placeholder_name]
                    if isinstance(placeholder_value, list):
                        for msg in placeholder_value:
                            if isinstance(msg, dict):
                                # Preserve all fields from the original message, such as tool calls
                                compiled_msg = dict(msg)  # type: ignore
                                # Ensure role and content are always present
                                compiled_msg["role"] = msg.get("role", "NOT_GIVEN")
                                compiled_msg["content"] = (
                                    TemplateParser.compile_template(
                                        msg.get("content", ""),  # type: ignore
                                        kwargs,
                                    )
                                )
                                compiled_messages.append(compiled_msg)
                            else:
                                compiled_messages.append(
                                    ChatMessageDict(
                                        role="NOT_GIVEN",
                                        content=str(placeholder_value),
                                    )
                                )
                                no_role_content_in_placeholder = f"Placeholder '{placeholder_name}' should contain a list of chat messages with 'role' and 'content' fields. Appended as string."
                                langfuse_logger.warning(no_role_content_in_placeholder)
                    else:
                        compiled_messages.append(
                            ChatMessageDict(
                                role="NOT_GIVEN",
                                content=str(placeholder_value),
                            ),
                        )
                        placeholder_not_a_list = f"Placeholder '{placeholder_name}' must contain a list of chat messages, got {type(placeholder_value)}"
                        langfuse_logger.warning(placeholder_not_a_list)
                else:
                    # Keep unresolved placeholder in the compiled messages
                    compiled_messages.append(chat_message)
                    unresolved_placeholders.append(chat_message["name"])  # type: ignore

        if unresolved_placeholders:
            unresolved_placeholders_message = f"Placeholders {unresolved_placeholders} have not been resolved. Pass them as keyword arguments to compile()."
            langfuse_logger.warning(unresolved_placeholders_message)

        return compiled_messages  # type: ignore

    @property
    def variables(self) -> List[str]:
        """Return all the variable names in the chat prompt template."""
        variables = []
        # Variables from prompt messages
        for chat_message in self.prompt:
            if chat_message["type"] == "message":
                variables.extend(
                    TemplateParser.find_variable_names(chat_message["content"]),
                )
        return variables

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return (
                self.name == other.name
                and self.version == other.version
                and len(self.prompt) == len(other.prompt)
                and all(
                    # chatmessage equality
                    (
                        m1["type"] == "message"
                        and m2["type"] == "message"
                        and m1["role"] == m2["role"]
                        and m1["content"] == m2["content"]
                    )
                    or
                    # placeholder equality
                    (
                        m1["type"] == "placeholder"
                        and m2["type"] == "placeholder"
                        and m1["name"] == m2["name"]
                    )
                    for m1, m2 in zip(self.prompt, other.prompt)
                )
                and self.config == other.config
            )

        return False

    def get_langchain_prompt(
        self, **kwargs: Union[str, Any]
    ) -> List[Union[Tuple[str, str], Any]]:
        """Convert Langfuse prompt into string compatible with Langchain ChatPromptTemplate.

        It specifically adapts the mustache-style double curly braces {{variable}} used in Langfuse
        to the single curly brace {variable} format expected by Langchain.
        Placeholders are filled-in from kwargs and unresolved placeholders are returned as Langchain MessagesPlaceholder.

        kwargs: Optional keyword arguments to precompile the template string. Variables that match
                the provided keyword arguments will be precompiled. Remaining variables must then be
                handled by Langchain's prompt template.
                Can also contain placeholders (list of chat messages) which will be resolved prior to variable
                compilation.

        Returns:
            List of messages in the format expected by Langchain's ChatPromptTemplate:
            (role, content) tuples for regular messages or MessagesPlaceholder objects for unresolved placeholders.
        """
        compiled_messages = self.compile(**kwargs)
        langchain_messages: List[Union[Tuple[str, str], Any]] = []

        for msg in compiled_messages:
            if isinstance(msg, dict) and "type" in msg and msg["type"] == "placeholder":  # type: ignore
                # unresolved placeholder -> add LC MessagesPlaceholder
                placeholder_name = msg["name"]  # type: ignore
                try:
                    from langchain_core.prompts.chat import MessagesPlaceholder  # noqa: PLC0415, I001

                    langchain_messages.append(
                        MessagesPlaceholder(variable_name=placeholder_name),
                    )
                except ImportError as e:
                    import_error = "langchain_core is required to use get_langchain_prompt() with unresolved placeholders."
                    raise ImportError(import_error) from e
            else:
                if isinstance(msg, dict) and "role" in msg and "content" in msg:
                    langchain_messages.append(
                        (
                            msg["role"],  # type: ignore
                            self._get_langchain_prompt_string(msg["content"]),  # type: ignore
                        ),
                    )

        return langchain_messages


PromptClient = Union[TextPromptClient, ChatPromptClient]
