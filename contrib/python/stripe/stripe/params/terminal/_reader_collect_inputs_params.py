# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class ReaderCollectInputsParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    inputs: List["ReaderCollectInputsParamsInput"]
    """
    List of inputs to be collected from the customer using the Reader. Maximum 5 inputs.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """


class ReaderCollectInputsParamsInput(TypedDict):
    custom_text: "ReaderCollectInputsParamsInputCustomText"
    """
    Customize the text which will be displayed while collecting this input
    """
    required: NotRequired[bool]
    """
    Indicate that this input is required, disabling the skip button
    """
    selection: NotRequired["ReaderCollectInputsParamsInputSelection"]
    """
    Options for the `selection` input
    """
    toggles: NotRequired[List["ReaderCollectInputsParamsInputToggle"]]
    """
    List of toggles to be displayed and customization for the toggles
    """
    type: Literal[
        "email", "numeric", "phone", "selection", "signature", "text"
    ]
    """
    The type of input to collect
    """


class ReaderCollectInputsParamsInputCustomText(TypedDict):
    description: NotRequired[str]
    """
    The description which will be displayed when collecting this input
    """
    skip_button: NotRequired[str]
    """
    Custom text for the skip button. Maximum 14 characters.
    """
    submit_button: NotRequired[str]
    """
    Custom text for the submit button. Maximum 30 characters.
    """
    title: str
    """
    The title which will be displayed when collecting this input
    """


class ReaderCollectInputsParamsInputSelection(TypedDict):
    choices: List["ReaderCollectInputsParamsInputSelectionChoice"]
    """
    List of choices for the `selection` input
    """


class ReaderCollectInputsParamsInputSelectionChoice(TypedDict):
    id: str
    """
    The unique identifier for this choice
    """
    style: NotRequired[Literal["primary", "secondary"]]
    """
    The style of the button which will be shown for this choice. Can be `primary` or `secondary`.
    """
    text: str
    """
    The text which will be shown on the button for this choice
    """


class ReaderCollectInputsParamsInputToggle(TypedDict):
    default_value: NotRequired[Literal["disabled", "enabled"]]
    """
    The default value of the toggle. Can be `enabled` or `disabled`.
    """
    description: NotRequired[str]
    """
    The description which will be displayed for the toggle. Maximum 50 characters. At least one of title or description must be provided.
    """
    title: NotRequired[str]
    """
    The title which will be displayed for the toggle. Maximum 50 characters. At least one of title or description must be provided.
    """
