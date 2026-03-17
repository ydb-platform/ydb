from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field


class EmailMessage(BaseModel):
    message: str = Field(..., description='The email message text.')
    subject: str = Field(..., description='The subject line of the email.')
    to: List[str] = Field(..., description='A list of email addresses.')
