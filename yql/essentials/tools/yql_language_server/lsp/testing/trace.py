from typing import Any

from pydantic import BaseModel, RootModel, model_validator


class LspMessage(BaseModel):
    jsonrpc: str
    id: int | str | None = None

    def to_json(self) -> dict:
        return self.model_dump(exclude_none=True)

    @classmethod
    def from_json(cls, obj: dict) -> 'LspMessage':
        if 'method' in obj:
            return LspRequest.model_validate(obj)
        elif 'result' in obj or 'error' in obj:
            return LspResponse.model_validate(obj)
        else:
            raise ValueError(f'Cannot determine LSP message type: {obj!r}')


class LspRequest(LspMessage):
    method: str
    params: Any | None = None


class LspResponse(LspMessage):
    result: Any | None = None
    error: Any | None = None


class LspTrace(RootModel[list[LspMessage]]):
    root: list[LspMessage]

    @model_validator(mode='before')
    @classmethod
    def _parse_messages(cls, data: Any) -> Any:
        if not isinstance(data, list):
            return data
        return [LspMessage.from_json(item) if isinstance(item, dict) else item for item in data]

    @property
    def messages(self) -> list[LspMessage]:
        return self.root

    def to_json(self) -> list[dict]:
        return [msg.to_json() for msg in self.root]

    @classmethod
    def from_json(cls, obj: list[dict]) -> 'LspTrace':
        return cls.model_validate(obj)
