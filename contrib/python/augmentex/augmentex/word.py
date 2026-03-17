import os
import re
from typing import List, Union

import numpy as np

from augmentex.base import BaseAug
from augmentex.preprocessor import ComputeStatistic
from augmentex.variables import WORD_ACTIONS


class WordAug(BaseAug):
    """Augmentation at the level of words."""

    def __init__(
        self,
        min_aug: int = 1,
        max_aug: int = 5,
        unit_prob: float = 0.3,
        random_seed: int = None,
        lang: str = "rus",
        platform: str = "pc",
        correct_texts_path: Union[str, None] = None,
        error_texts_path: Union[str, None] = None,
    ) -> None:
        """
        Args:
            min_aug (int, optional): The minimum amount of augmentation. Defaults to 1.
            max_aug (int, optional): The maximum amount of augmentation. Defaults to 5.
            unit_prob (float, optional): Percentage of the phrase to which augmentations will be applied. Defaults to 0.3.
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

        self.stopwords = self._read_json(
            os.path.join(dir_path, "static_data", self.lang, "stopwords.json")
        )
        self.text2emoji_map = self._read_json(
            os.path.join(dir_path, "static_data", self.lang, "text2emoji.json")
        )

        if correct_texts_path is not None or error_texts_path is not None:
            cs = ComputeStatistic(correct_texts_path, error_texts_path, self.lang)
            self.orfo_dict, self.ngram_dict = cs.compute_word_statistic()
        else:
            self.orfo_dict = self._read_json(
                os.path.join(
                    dir_path, "static_data", self.lang, self.platform, "orfo_words.json"
                )
            )
            self.ngram_dict = self._read_json(
                os.path.join(
                    dir_path,
                    "static_data",
                    self.lang,
                    self.platform,
                    "orfo_ngrams.json",
                )
            )

        self.unit_prob = unit_prob

    @property
    def actions_list(self) -> List[str]:
        """
        Returns:
            List[str]: A list of possible methods.
        """

        return WORD_ACTIONS

    def __ngram(self, word: str, n: int = 3) -> str:
        if len(word) > 3:
            word_ngrams = [word[i : i + n] for i in range(len(word) - n + 1)]
            random_ngram = np.random.choice(word_ngrams)
            ngram_probas = self.ngram_dict.get(
                random_ngram.lower(), [[random_ngram], [1.0]]
            )
            ngram_for_replace = np.random.choice(ngram_probas[0], p=ngram_probas[1])
            word = word.replace(random_ngram, ngram_for_replace)

        return word

    def __reverse_case(self, word: str) -> str:
        """Changes the case of the first letter to the reverse.

        Args:
            word (str): The initial word.

        Returns:
            str: A word with a different case of the first letter.
        """
        if len(word):
            if word[0].isupper():
                word = word.lower()
            else:
                word = word.capitalize()

        return word

    def __text2emoji(self, word: str) -> str:
        """Replace word to emoji.

        Args:
            word (str): A word with the correct spelling.

        Returns:
            str: Emoji that matches this word.
        """
        word = re.findall("[а-яА-ЯёЁa-zA-Z0-9']+|[.,!?;-]+", word)
        words = self.text2emoji_map.get(word[0].lower(), [word[0]])
        word[0] = np.random.choice(words)

        return "".join(word)

    def __split(self, word: str) -> str:
        """Divides a word character-by-character.

        Args:
            word (str): A word with the correct spelling.

        Returns:
            str: Word with spaces.
        """
        word = " ".join(list(word))

        return word

    def __replace(self, word: str) -> str:
        """Replaces a word with the correct spelling with a word with spelling errors.

        Args:
            word (str): A word with the correct spelling.

        Returns:
            str: A misspelled word.
        """
        word = re.findall("[а-яА-ЯёЁa-zA-Z0-9']+|[.,!?;]+", word)
        word_probas = self.orfo_dict.get(word[0].lower(), [[word[0]], [1.0]])
        word[0] = np.random.choice(word_probas[0], p=word_probas[1])

        return "".join(word)

    def __delete(self) -> str:
        """Deletes a random word.

        Returns:
            str: Empty string.
        """

        return ""

    def __stopword(self, word: str) -> str:
        """Adds a stop word before the word.

        Args:
            word (str): Just word.

        Returns:
            str: Stopword + word.
        """
        stopword = np.random.choice(self.stopwords)

        return " ".join([stopword, word])

    def augment(self, text: str, action: str = None) -> str:
        """Modifies the phrase according to the action.

        Args:
            text (str): Text phrase.
            action (str, optional): The action to apply to the phrase.

        Returns:
            str: Modified phrase.
        """
        if action is None:
            action = np.random.choice(WORD_ACTIONS)

        aug_sent_arr = text.split()
        aug_idxs = self._aug_indexing(aug_sent_arr, self.unit_prob, clip=True)
        for idx in aug_idxs:
            if action == "delete":
                aug_sent_arr[idx] = self.__delete()
            elif action == "reverse":
                aug_sent_arr[idx] = self.__reverse_case(aug_sent_arr[idx])
            elif action == "swap":
                swap_idx = np.random.randint(0, len(aug_sent_arr) - 1)
                aug_sent_arr[swap_idx], aug_sent_arr[idx] = (
                    aug_sent_arr[idx],
                    aug_sent_arr[swap_idx],
                )
            elif action == "stopword":
                aug_sent_arr[idx] = self.__stopword(aug_sent_arr[idx])
            elif action == "ngram":
                aug_sent_arr[idx] = self.__ngram(aug_sent_arr[idx])
            elif action == "replace":
                aug_sent_arr[idx] = self.__replace(aug_sent_arr[idx])
            elif action == "text2emoji":
                aug_sent_arr[idx] = self.__text2emoji(aug_sent_arr[idx])
            elif action == "split":
                aug_sent_arr[idx] = self.__split(aug_sent_arr[idx])
            else:
                raise NameError(
                    """These type of augmentation is not available, please check EDAAug.actions_list() to see
                available augmentations"""
                )

        return re.sub(" +", " ", " ".join(aug_sent_arr).strip())
