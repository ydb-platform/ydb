import os
from typing import List, Union

import numpy as np

from augmentex.base import BaseAug
from augmentex.preprocessor import ComputeStatistic
from augmentex.variables import CHAR_ACTIONS


class CharAug(BaseAug):
    """Augmentation at the character level."""

    def __init__(
        self,
        unit_prob: float = 0.3,
        min_aug: int = 1,
        max_aug: int = 5,
        mult_num: int = 5,
        random_seed: Union[int, None] = None,
        lang: str = "rus",
        platform: str = "pc",
        correct_texts_path: Union[str, None] = None,
        error_texts_path: Union[str, None] = None,
    ) -> None:
        """
        Args:
            unit_prob (float, optional): Percentage of the phrase to which augmentations will be applied. Defaults to 0.3.
            min_aug (int, optional): The minimum amount of augmentation. Defaults to 1.
            max_aug (int, optional): The maximum amount of augmentation. Defaults to 5.
            mult_num (int, optional): Maximum repetitions of characters. Defaults to 5.
            random_seed (int, optional): Random seed. Default to None.
            lang (str, optional): Language of texts. Default to 'rus'.
            platform (str, optional): Type of platform where statistic was collected. Defaults to 'pc'.
            correct_texts_path (str, optional): Path to txt file with correct texts. Defaults to None.
            error_texts_path (str, optional): Path to txt file with error texts. Default to None.
        """
        super().__init__(
            min_aug=min_aug,
            max_aug=max_aug,
            random_seed=random_seed,
            lang=lang,
            platform=platform,
        )
        dir_path = "contrib/python/augmentex/augmentex"

        self.typo_dict = self._read_json(
            os.path.join(dir_path, "static_data", f"{self.platform}_typos_chars.json")
        )
        self.shift_dict = self._read_json(
            os.path.join(dir_path, "static_data", "shift.json")
        )
        self.vocab = self._read_json(
            os.path.join(dir_path, "static_data", self.lang, "vocab.json")
        )

        if correct_texts_path is not None or error_texts_path is not None:
            cs = ComputeStatistic(correct_texts_path, error_texts_path, self.lang)
            self.orfo_dict = cs.compute_char_statistic()
        else:
            self.orfo_dict = self._read_json(
                os.path.join(
                    dir_path, "static_data", self.lang, self.platform, "orfo_chars.json"
                )
            )

        self.mult_num = mult_num
        self.unit_prob = unit_prob

    @property
    def actions_list(self) -> List[str]:
        """
        Returns:
            List[str]: A list of possible methods.
        """

        return CHAR_ACTIONS

    def __typo(self, char: str) -> str:
        """A method that simulates a typo by an adjacent key.

        Args:
            char (str): A symbol from the word.

        Returns:
            str: A new symbol.
        """
        typo_char = np.random.choice(self.typo_dict.get(char, [char]))

        return typo_char

    def __shift(self, char: str) -> str:
        """Changes the case of the symbol.

        Args:
            char (str): A symbol from the word.

        Returns:
            str: The same character but with a different case.
        """
        shift_char = self.shift_dict.get(char, char)

        return shift_char

    def __orfo(self, char: str) -> str:
        """Changes the symbol depending on the error statistics.

        Args:
            char (str): A symbol from the word.

        Returns:
            str: A new symbol.
        """
        if self.orfo_dict.get(char, None) == None:
            orfo_char = char
        else:
            orfo_char = np.random.choice(self.vocab, p=self.orfo_dict.get(char, None))

        return orfo_char

    def __delete(self) -> str:
        """Deletes a random character.

        Returns:
            str: Empty string.
        """

        return ""

    def __insert(self, char: str) -> str:
        """Inserts a random character.

        Args:
            char (str): A symbol from the word.

        Returns:
            str: A symbol + new symbol.
        """

        return char + np.random.choice(self.vocab)

    def __multiply(self, char: str) -> str:
        """Repeats a randomly selected character.

        Args:
            char (str): A symbol from the word.

        Returns:
            str: A symbol from the word matmul n times.
        """
        if char in [" ", ",", ".", "?", "!", "-"]:
            return char
        else:
            n = np.random.randint(1, self.mult_num)
            return char * n

    # def _clean_punc(self, text: str) -> str:
    #     """Clears the text from punctuation.

    #     Args:
    #         text (str): Original text.

    #     Returns:
    #         str: Text without punctuation.
    #     """
    #     return text.translate(str.maketrans("", "", string.punctuation))

    def augment(self, text, action=None):
        if action is None:
            action = np.random.choice(CHAR_ACTIONS)

        typo_text_arr = list(text)
        aug_idxs = self._aug_indexing(typo_text_arr, self.unit_prob, clip=True)
        for idx in aug_idxs:
            if action == "typo":
                typo_text_arr[idx] = self.__typo(typo_text_arr[idx])
            elif action == "shift":
                typo_text_arr[idx] = self.__shift(typo_text_arr[idx])
            elif action == "delete":
                typo_text_arr[idx] = self.__delete()
            elif action == "insert":
                typo_text_arr[idx] = self.__insert(typo_text_arr[idx])
            elif action == "orfo":
                typo_text_arr[idx] = self.__orfo(typo_text_arr[idx])
            elif action == "multiply":
                typo_text_arr[idx] = self.__multiply(typo_text_arr[idx])
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
