import os
import re
import json
import library.python.resource
from typing import List, Dict, Tuple, Union
from collections import defaultdict
from pylev import levenshtein

from augmentex.variables import SUPPORT_LANGUAGES


class ComputeStatistic:
    """A class for counting spelling errors."""

    def __init__(
        self, correct_texts_path: str, error_texts_path: str, lang: str
    ) -> None:
        """
        Args:
            correct_texts_path (str): Path to txt file with correct texts.
            error_texts_path (str): Path to txt file with error texts.
            lang (str): Language of texts.
        """
        dir_path = "contrib/python/augmentex/augmentex"
        self.lang = lang

        self.__check_txt_file(correct_texts_path)
        self.__check_txt_file(error_texts_path)

        if self.lang not in SUPPORT_LANGUAGES:
            raise ValueError(
                f"""Augmentex support only {', '.join(SUPPORT_LANGUAGES)} languages.
                You put {self.lang}."""
            )

        self.vocab = self.__read_json(
            os.path.join(dir_path, "static_data", self.lang, "vocab.json")
        )
        self.correct_texts = self.__read_txt_file(correct_texts_path)
        self.error_texts = self.__read_txt_file(error_texts_path)

        if len(self.correct_texts) != len(self.error_texts):
            raise ValueError(
                f"Correct texts and texts with errors must be same length. Your length is {len(self.correct_texts)} and {len(self.error_texts)} respectively."
            )

    def __read_json(self, path: str) -> Dict:
        """Read JSON to Dict.

        Args:
            path (str): Path to file.

        Returns:
            Dict: dict with data.
        """
        data = json.loads(library.python.resource.resfs_read(path).decode("utf-8"))

        return data

    def __check_txt_file(self, path: str) -> None:
        """Checking the file for existence and extension.

        Args:
            path (str): Path to file.
        """
        if type(path) != str:
            raise ValueError(f"Path must be str!")
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File {path} does not exists!")
        if not path.endswith(".txt"):
            raise ValueError(f"File extension must be .txt")

    def __read_txt_file(self, path: str) -> List[str]:
        """Read .txt file.

        Args:
            path (str): Path to file.

        Returns:
            List[str]: List with phrases.
        """
        with open(path, "r") as handle:
            data = handle.readlines()

        return data

    def __preprocess(self, data: List[str]) -> List[str]:
        """Tokenize and lower texts.

        Args:
            data (List[str]): List with phrases.

        Returns:
            List[str]: List with preprocessed phrases.
        """
        if self.lang == "rus":
            regex = r"[а-яё]+"
        else:
            regex = r"[a-z]+"

        data = list(map(lambda x: " ".join(re.findall(regex, x.lower())), data))

        return data

    def __filter(
        self, correct_texts: List[str], error_texts: List[str]
    ) -> List[Tuple[str, str]]:
        """Matches the correct word with the word with an error.

        Args:
            correct_texts (List[str]): List with correct texts.
            error_texts (List[str]): List with error texts.

        Returns:
            List[Tuple[str, str]]: List with pairs of words.
        """
        pairs = []
        for i in range(len(correct_texts)):
            if correct_texts[i] != error_texts[i]:
                correct_texts_i_split = correct_texts[i].split()
                error_texts_i_split = error_texts[i].split()
                if len(correct_texts_i_split) == len(error_texts_i_split):
                    for word_idx in range(len(correct_texts_i_split)):
                        true_word = correct_texts_i_split[word_idx]
                        broke_word = error_texts_i_split[word_idx]
                        if (
                            true_word != broke_word
                            and levenshtein(true_word, broke_word) <= 2
                        ):
                            pairs.append((true_word, broke_word))

        return pairs

    def __compute_word_statistic(
        self, pairs: List[Tuple[str, str]]
    ) -> Dict[str, List[Union[List[str], List[float]]]]:
        """From pairs of words , it compiles statistics on the use of words with errors.

        Args:
            pairs (List[Tuple[str, str]]): List with pairs of words.

        Returns:
            Dict[str, List[Union[List[str], List[float]]]]: Statistics for Augmentex.
        """
        count_pairs = defaultdict(int)
        for pair in pairs:
            count_pairs[f"{pair[0]}_{pair[1]}"] += 1

        word_statistic = {}
        for pair in list(count_pairs.items()):
            true_word = pair[0].split("_")[0]
            broke_word = pair[0].split("_")[1]
            count = pair[1]
            if word_statistic.get(true_word, False):
                word_statistic[true_word][0].append(broke_word)
                word_statistic[true_word][1].append(count)
            else:
                word_statistic[true_word] = [[broke_word], [count]]

        for item in word_statistic.items():
            item[1][1] = [float(i) / sum(item[1][1]) for i in item[1][1]]

        return word_statistic

    def __compute_char_statistic(
        self, pairs: List[Tuple[str, str]]
    ) -> Dict[str, List[float]]:
        """From pairs of words , statistics on the use of the wrong char are compiled.

        Args:
            pairs (List[Tuple[str, str]]): List with pairs of words.

        Returns:
            Dict[str, List[float]]: Statistics for Augmentex.
        """
        unique_pairs = list(set(pairs))
        char_pairs = []
        for pair in unique_pairs:
            true_word = pair[0]
            broke_word = pair[1]
            if len(true_word) == len(broke_word):
                for char_id in range(len(true_word)):
                    true_char = true_word[char_id]
                    broke_char = broke_word[char_id]
                    if true_char != broke_char:
                        char_pairs.append((true_char, broke_char))

        count_char_pairs = defaultdict(int)
        for pair in char_pairs:
            count_char_pairs[f"{pair[0]}_{pair[1]}"] += 1

        char_statistic = {}
        for pair in list(count_char_pairs.items()):
            tmp = {char: 0 for char in self.vocab}
            true_char = pair[0].split("_")[0]
            broke_char = pair[0].split("_")[1]
            count = pair[1]

            if true_char not in self.vocab:
                continue
            if broke_char not in self.vocab:
                continue

            if char_statistic.get(true_char, False):
                char_statistic[true_char][broke_char] = count
            else:
                tmp[broke_char] = count
                char_statistic[true_char] = tmp

        char_statistic = {
            k: [float(i) / sum(list(v.values())) for i in list(v.values())]
            for k, v in char_statistic.items()
        }

        return char_statistic

    def __compute_ngram_statistic(
        self, pairs: List[Tuple[str, str]], n: int = 3
    ) -> Dict[str, List[Union[List[str], List[float]]]]:
        """From pairs of words , it compiles statistics on the use of ngrams with errors.

        Args:
            pairs (List[Tuple[str, str]]): List with pairs of words.

        Returns:
            Dict[str, List[Union[List[str], List[float]]]]: Statistics for Augmentex.
        """
        unique_pairs = list(set(pairs))
        ngram_pairs = []
        for pair in unique_pairs:
            true_word = pair[0]
            broke_word = pair[1]
            if len(true_word) == len(broke_word):
                if len(true_word) < n:
                    continue
                true_word_ngram = [
                    true_word[i : i + n] for i in range(len(true_word) - n + 1)
                ]
                broke_word_ngram = [
                    broke_word[i : i + n] for i in range(len(broke_word) - n + 1)
                ]
                for gram_id in range(len(true_word_ngram)):
                    if true_word_ngram[gram_id] != broke_word_ngram[gram_id]:
                        ngram_pairs.append(
                            (true_word_ngram[gram_id], broke_word_ngram[gram_id])
                        )

        count_ngram_pairs = defaultdict(int)
        for pair in ngram_pairs:
            count_ngram_pairs[f"{pair[0]}_{pair[1]}"] += 1

        ngram_statistic = {}
        for pair in list(count_ngram_pairs.items()):
            true_gram = pair[0].split("_")[0]
            broke_gram = pair[0].split("_")[1]
            count = pair[1]
            if ngram_statistic.get(true_gram, False):
                ngram_statistic[true_gram][0].append(broke_gram)
                ngram_statistic[true_gram][1].append(count)
            else:
                ngram_statistic[true_gram] = [[broke_gram], [count]]

        for item in ngram_statistic.items():
            item[1][1] = [float(i) / sum(item[1][1]) for i in item[1][1]]

        return ngram_statistic

    def compute_char_statistic(self) -> Dict[str, List[Union[List[str], List[float]]]]:
        """Calculates char statistics.

        Returns:
            Dict[str, List[Union[List[str], List[float]]]]: Statistics for Augmentex.
        """
        preprocess_correct_texts = self.__preprocess(self.correct_texts)
        preprocess_error_texts = self.__preprocess(self.error_texts)

        pairs = self.__filter(preprocess_correct_texts, preprocess_error_texts)

        char_statistic = self.__compute_char_statistic(pairs)

        return char_statistic

    def compute_word_statistic(self) -> Dict[str, List[Union[List[str], List[float]]]]:
        """Calculates word statistics.

        Returns:
            Dict[str, List[Union[List[str], List[float]]]]: Statistics for Augmentex.
        """
        preprocess_correct_texts = self.__preprocess(self.correct_texts)
        preprocess_error_texts = self.__preprocess(self.error_texts)

        pairs = self.__filter(preprocess_correct_texts, preprocess_error_texts)

        word_statistic = self.__compute_word_statistic(pairs)
        ngram_statistic = self.__compute_ngram_statistic(pairs)

        return word_statistic, ngram_statistic
