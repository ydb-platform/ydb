from __future__ import annotations

import warnings

from fastapi import APIRouter

warnings.warn(
    "InferringRouter is deprecated, as its functionality is now provided in fastapi.APIRouter", DeprecationWarning
)

InferringRouter = APIRouter
