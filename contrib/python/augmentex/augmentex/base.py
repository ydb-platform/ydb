import random
import json
from abc import ABC, abstractmethod
from typing import List, Union, Dict

import numpy as np
import library.python.resource

from augmentex.variables import SUPPORT_LANGUAGES, SUPPORT_PLATFORMS


class BaseAug(ABC):
    def __init__(
        self,
        min_aug: int = 1,
        max_aug: int = 5,
        random_seed: int = None,
        lang: str = "rus",
        platform: str = "pc",
    ) -> None:
        """
        Args:
            min_aug (int, optional): The minimum amount of augmentation. Defaults to 1.
            max_aug (int, optional): The maximum amount of augmentation. Defaults to 5.
            random_seed (int, optional): Random seed. Default to None.
            lang (str, optional): Language of texts. Default to 'rus'.
            platform (str, optional): Type of platform where statistic was collected. Defaults to 'pc'.
        """
        self.min_aug = min_aug
        self.max_aug = max_aug
        self.random_seed = random_seed
        self.lang = lang
        self.platform = platform

        if self.random_seed:
            self.__fix_random_seed(self.random_seed)

        if self.lang not in SUPPORT_LANGUAGES:
            raise ValueError(
                f"""Augmentex support only {', '.join(SUPPORT_LANGUAGES)} languages.
                You put {self.lang}."""
            )
        if self.platform not in SUPPORT_PLATFORMS:
            raise ValueError(
                f"""Augmentex support only {', '.join(SUPPORT_PLATFORMS)} platforms.
                You put {self.platform}."""
            )

    def _read_json(self, path: str) -> Dict:
        """Read JSON to Dict.

        Args:
            path (str): Path to file.

        Returns:
            Dict: dict with data.
        """
        data = json.loads(library.python.resource.resfs_read(path).decode("utf-8"))

        return data

    def __fix_random_seed(self, random_seed: int) -> None:
        """Fixing random seed.

        Args:
            random_seed (int): Integer digit.
        """
        random.seed(random_seed)
        np.random.seed(random_seed)

    def __augs_count(self, size: int, rate: float) -> int:
        """Counts the number of augmentations and performs circumcision by the maximum or minimum number.

        Args:
            size (int): The number of units (chars or words) in the text.
            rate (float): The percentage of units to which augmentation will be applied.

        Returns:
            int: The amount of augmentation.
        """
        cnt = 0
        if size > 1:
            cnt = int(rate * size)

        return cnt

    def __get_random_idx(self, inputs: List[str], aug_count: int) -> List[int]:
        """Randomly select indexes for augmentation

        Args:
            inputs (List[str]): List of units.
            aug_count (int): The amount of augmentation.

        Returns:
            List[int]: List of indices.
        """
        token_idxes = [i for i in range(len(inputs))]
        aug_idxs = random.sample(token_idxes, aug_count)

        return aug_idxs

    def _aug_indexing(
        self, inputs: List[str], rate: float, clip: bool = False
    ) -> List[int]:
        """
        Args:
            inputs (List[str]): List of units.
            rate (float): The percentage of units to which augmentation will be applied.
            clip (bool): Takes into account the maximum and minimum values. Defaults to False.

        Returns:
            List[int]: List of indices.
        """
        aug_count = self.__augs_count(len(inputs), rate)
        if clip:
            aug_count = max(aug_count, self.min_aug)
            aug_count = min(aug_count, self.max_aug)

        aug_idxs = self.__get_random_idx(inputs, aug_count)

        return aug_idxs

    def aug_batch(
        self,
        batch: List[str],
        batch_prob: float = 1.0,
        action: Union[None, str] = None,
    ) -> List[str]:
        """The use of augmentation to several lines

        Args:
            batch (List[str]): List of lines for augmentation.
            batch_prob (float, optional): The percentage of units to which augmentation will be applied. Defaults to 1.0.
            action (Union[None, str], optional): Indicates what action will be applied. Defaults to None. If None, then a random action is chosen.

        Returns:
            List[str]: List of augmented lines.
        """
        aug_batch = batch.copy()
        aug_idxs = self._aug_indexing(aug_batch, batch_prob)
        for idx in aug_idxs:
            aug_batch[idx] = self.augment(aug_batch[idx], action)

        return aug_batch

    @abstractmethod
    def augment(self, text, action):
        pass
