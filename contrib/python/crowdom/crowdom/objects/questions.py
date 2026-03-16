import abc
from collections import defaultdict
from enum import Enum
from typing import List, Dict, Type, Tuple

from ..base import Class


class Question(Class):
    # user-defined class. contains both the question and all possible answers for it
    # possible answers are stored in enum values, question_label returns the question itself

    @classmethod
    @abc.abstractmethod
    def question_label(cls) -> Dict[str, str]:
        ...


class CombinedAnswer(Class):
    # auto-generated class. uniquely combines all possible answer values from several questions
    question_mapping: Dict['CombinedAnswer', Question]

    def get_original_answer(self) -> Question:
        # one of the answer options from original Question class, can be used is Results
        return self.question_mapping[self]


def get_combined_classes(
    questions: List[Type[Question]],
) -> Tuple[Type[Class], Type[CombinedAnswer], List[Tuple[Class, List[Class]]]]:
    # we need to combine all possible answers into a single CombinedAnswer Class with unique answer variants
    # and combine all possible questions into a single Class

    question_fields = {}  # {field_name -> field_value} for new Question Enum
    question_labels = {}  # {question_name -> question_label}

    answer_fields = {}  # {field_name -> field_value} for new CombinedAnswer Enum
    answer_values = []  # all Enum instances with possible answers from all given Questions

    question_answers_dict = defaultdict(list)
    for i, question in enumerate(questions):
        question_name = question.__name__
        enum_name = question_name.upper()
        enum_value = question_name.lower()
        question_fields[enum_name] = enum_value
        question_labels[enum_name] = question.question_label()
        for answer in list(question):

            new_value = f'{enum_value}__{answer.value}'  # ensuring new values are unique
            new_name = f'{enum_name}__{answer.name}'  # and new names also

            answer_values.append(answer)
            answer_fields[new_name] = new_value
            question_answers_dict[enum_name].append(new_name)

    GeneratedAnswer = Enum('GeneratedAnswer', answer_fields)

    question_mapping = {}
    for answer, new_answer_enum_value in zip(answer_values, list(GeneratedAnswer)):
        question_mapping[new_answer_enum_value] = answer

    setattr(GeneratedAnswer, 'question_mapping', question_mapping)

    # todo: maybe we should check that set of languages is the same for all question classes
    def answer_labels() -> Dict[GeneratedAnswer, Dict[str, str]]:
        return {answer: answer.get_original_answer().get_label().lang_to_text for answer in list(GeneratedAnswer)}

    setattr(GeneratedAnswer, 'labels', answer_labels)

    GeneratedAnswer.__bases__ = (
        CombinedAnswer,
        Class,
    ) + GeneratedAnswer.__bases__

    CombinedQuestion = Enum('CombinedQuestion', question_fields)

    def questions_labels() -> Dict[CombinedQuestion, Dict[str, str]]:
        return {question: question_labels[question.name.upper()] for question in list(CombinedQuestion)}

    setattr(CombinedQuestion, 'labels', questions_labels)

    CombinedQuestion.__bases__ = (Class,) + CombinedQuestion.__bases__

    question_answers = [
        (
            getattr(CombinedQuestion, question_name),
            [getattr(GeneratedAnswer, answer_name) for answer_name in answer_names],
        )
        for question_name, answer_names in question_answers_dict.items()
    ]
    return CombinedQuestion, GeneratedAnswer, question_answers
