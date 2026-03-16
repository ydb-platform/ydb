from enum import Enum


class ModelType(str, Enum):
    Document = "Document"
    View = "View"
    UnionDoc = "UnionDoc"


class DetectionInterface:
    @classmethod
    def get_model_type(cls) -> ModelType:
        return ModelType.Document
