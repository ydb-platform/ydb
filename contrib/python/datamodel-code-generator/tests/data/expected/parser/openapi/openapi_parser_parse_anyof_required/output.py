from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class EmailMessage(BaseModel):
    bcc: Optional[List[str]] = Field(
        None, description='A list of "blind carbon copy" email addresses.'
    )
    cc: Optional[List[str]] = Field(
        None, description='A list of "carbon copy" email addresses.'
    )
    message: str = Field(..., description='The email message text.')
    subject: str = Field(..., description='The subject line of the email.')
    to: Optional[List[str]] = Field(None, description='A list of email addresses.')
