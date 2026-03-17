import random
from augmentex.base import BaseAug

import numpy as np


class PuncAug(BaseAug):
    def __init__(
        self,
        aug_rate=0.3,
    ):
        super().__init__()
        self.aug_rate = aug_rate
        self.__actions = ["replace", "delete", "multiply", "swap"]

    def actions_list(self):
        return self.__actions

    def _replace(self, char):
        typo_char = np.random.choice(self.typo_dict.get(char, char))
        return typo_char

    def _delete(self):
        return ""

    def _multiply(self, char):
        n = np.random.randint(1, self.mult_num)
        return char * n

    def augment(self, text, action):
        typo_text_arr = list(text)
        aug_idxs = self.aug_idxs(typo_text_arr, self.aug_rate)
        for idx in aug_idxs:
            if action == "replace":
                typo_text_arr[idx] = self._replace(typo_text_arr[idx])
            elif action == "delete":
                typo_text_arr[idx] = self._delete()
            elif action == "multiply":
                typo_text_arr[idx] = self._multiply(typo_text_arr[idx])
            elif action == "swap":
                sw = max(0, idx - 1)
                typo_text_arr[sw], typo_text_arr[idx] = (
                    typo_text_arr[idx],
                    typo_text_arr[sw],
                )
            else:
                raise NameError(
                    """These type of augmentation is not available, please try TypoAug.actions_list() to see
                available augmentations"""
                )
        return "".join(typo_text_arr)

    def random(self, text):
        action = np.random.choice(self.__actions)
        new = self.augment(text, action)
        return new

    def __call__(self, text):
        new = self.random(text)
        return new
