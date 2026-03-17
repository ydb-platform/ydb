from collections import defaultdict
from typing import Any, Dict, List, Optional, Type, Union

try:
    from pydantic.v1 import BaseModel, Field, StrictStr, ValidationError, root_validator
except ImportError:
    from pydantic import BaseModel, Field, StrictStr, ValidationError, root_validator  # type: ignore

from wasabi import msg


def validate(schema: Type[BaseModel], obj: Dict[str, Any]) -> List[str]:
    """Validate data against a given pydantic schema.

    obj (Dict[str, Any]): JSON-serializable data to validate.
    schema (pydantic.BaseModel): The schema to validate against.
    RETURNS (List[str]): A list of error messages, if available.
    """
    try:
        schema(**obj)
        return []
    except ValidationError as e:
        errors = e.errors()
        data = defaultdict(list)
        for error in errors:
            err_loc = " -> ".join([str(p) for p in error.get("loc", [])])
            data[err_loc].append(error.get("msg"))
        return [f"[{loc}] {', '.join(msg)}" for loc, msg in data.items()]  # type: ignore[arg-type]


# Project config Schema


class ProjectConfigAssetGitItem(BaseModel):
    # fmt: off
    repo: StrictStr = Field(..., title="URL of Git repo to download from")
    path: StrictStr = Field(..., title="File path or sub-directory to download (used for sparse checkout)")
    branch: StrictStr = Field("master", title="Branch to clone from")
    # fmt: on


class ProjectConfigAssetURL(BaseModel):
    # fmt: off
    dest: StrictStr = Field(..., title="Destination of downloaded asset")
    url: Optional[StrictStr] = Field(None, title="URL of asset")
    checksum: Optional[str] = Field(None, title="MD5 hash of file", regex=r"([a-fA-F\d]{32})")
    description: StrictStr = Field("", title="Description of asset")
    # fmt: on


class ProjectConfigAssetGit(BaseModel):
    # fmt: off
    git: ProjectConfigAssetGitItem = Field(..., title="Git repo information")
    checksum: Optional[str] = Field(None, title="MD5 hash of file", regex=r"([a-fA-F\d]{32})")
    description: Optional[StrictStr] = Field(None, title="Description of asset")
    # fmt: on


class ProjectConfigCommand(BaseModel):
    # fmt: off
    name: StrictStr = Field(..., title="Name of command")
    help: Optional[StrictStr] = Field(None, title="Command description")
    script: List[StrictStr] = Field([], title="List of CLI commands to run, in order")
    deps: List[StrictStr] = Field([], title="File dependencies required by this command")
    outputs: List[StrictStr] = Field([], title="Outputs produced by this command")
    outputs_no_cache: List[StrictStr] = Field([], title="Outputs not tracked by DVC (DVC only)")
    no_skip: bool = Field(False, title="Never skip this command, even if nothing changed")
    # fmt: on

    class Config:
        title = "A single named command specified in a project config"
        extra = "forbid"


class ProjectConfigSchema(BaseModel):
    # fmt: off
    vars: Dict[StrictStr, Any] = Field({}, title="Optional variables to substitute in commands")
    env: Dict[StrictStr, Any] = Field({}, title="Optional variable names to substitute in commands, mapped to environment variable names")
    assets: List[Union[ProjectConfigAssetURL, ProjectConfigAssetGit]] = Field([], title="Data assets")
    workflows: Dict[StrictStr, List[StrictStr]] = Field({}, title="Named workflows, mapped to list of project commands to run in order")
    commands: List[ProjectConfigCommand] = Field([], title="Project command shortucts")
    title: Optional[str] = Field(None, title="Project title")
    # fmt: on

    class Config:
        title = "Schema for project configuration file"

    @root_validator(pre=True)
    def check_legacy_keys(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        if "spacy_version" in obj:
            msg.warn(
                "Your project configuration file includes a `spacy_version` key, "
                "which is now deprecated. Weasel will not validate your version of spaCy.",
            )
        if "check_requirements" in obj:
            msg.warn(
                "Your project configuration file includes a `check_requirements` key, "
                "which is now deprecated. Weasel will not validate your requirements.",
            )
        return obj
