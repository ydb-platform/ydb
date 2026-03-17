from typing import Optional, Any

from qdrant_client.http import models
from qdrant_client.embed.models import NumericVector


class BuiltinEmbedder:
    _SUPPORTED_MODELS = ("Qdrant/Bm25",)

    def __init__(self, **kwargs: Any) -> None:
        pass

    def embed(
        self,
        model_name: str,
        texts: Optional[list[str]] = None,
        options: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> NumericVector:
        if texts is None:
            if "images" in kwargs:
                raise ValueError(
                    "Image processing is only available with cloud inference of FastEmbed"
                )

            raise ValueError("Texts must be provided for the inference")

        if not self.is_supported_sparse_model(model_name):
            raise ValueError(
                f"Model {model_name} is not supported in {self.__class__.__name__}. "
                f"Did you forget to enable cloud inference or install FastEmbed for local inference?"
            )

        return [models.Document(text=text, options=options, model=model_name) for text in texts]

    @classmethod
    def is_supported_text_model(cls, model_name: str) -> bool:
        """Mock embedder interface, only sparse text model Qdrant/Bm25 is supported

        Args:
            model_name (str): The name of the model to check.

        Returns:
            bool: True if the model is supported, False otherwise.
        """
        return False  # currently only Qdrant/Bm25 is supported

    @classmethod
    def is_supported_image_model(cls, model_name: str) -> bool:
        """Mock embedder interface, only sparse text model Qdrant/Bm25 is supported

        Args:
            model_name (str): The name of the model to check.

        Returns:
            bool: True if the model is supported, False otherwise.
        """
        return False  # currently only Qdrant/Bm25 is supported

    @classmethod
    def is_supported_late_interaction_text_model(cls, model_name: str) -> bool:
        """Mock embedder interface, only sparse text model Qdrant/Bm25 is supported

        Args:
            model_name (str): The name of the model to check.

        Returns:
            bool: True if the model is supported, False otherwise.
        """
        return False  # currently only Qdrant/Bm25 is supported

    @classmethod
    def is_supported_late_interaction_multimodal_model(cls, model_name: str) -> bool:
        """Mock embedder interface, only sparse text model Qdrant/Bm25 is supported

        Args:
            model_name (str): The name of the model to check.

        Returns:
            bool: True if the model is supported, False otherwise.
        """
        return False  # currently only Qdrant/Bm25 is supported

    @classmethod
    def is_supported_sparse_model(cls, model_name: str) -> bool:
        """Checks if the model is supported. Only `Qdrant/Bm25` is supported

        Args:
            model_name (str): The name of the model to check.

        Returns:
            bool: True if the model is supported, False otherwise.
        """
        return model_name.lower() in [model.lower() for model in cls._SUPPORTED_MODELS]
