from typing import Any, Callable, Dict, Iterable, Optional
from spacy.scorer import PRFScore, ROCAUCScore
from spacy.tokens import Doc
from spacy.training import Example
from spacy.util import SimpleFrozenList


def score_cats_v1(
    examples: Iterable[Example],
    attr: str,
    *,
    getter: Callable[[Doc, str], Any] = getattr,
    labels: Iterable[str] = SimpleFrozenList(),
    multi_label: bool = True,
    positive_label: Optional[str] = None,
    threshold: Optional[float] = None,
    **cfg,
) -> Dict[str, Any]:
    """Returns PRF and ROC AUC scores for a doc-level attribute with a
    dict with scores for each label like Doc.cats. The reported overall
    score depends on the scorer settings.

    examples (Iterable[Example]): Examples to score
    attr (str): The attribute to score.
    getter (Callable[[Doc, str], Any]): Defaults to getattr. If provided,
        getter(doc, attr) should return the values for the individual doc.
    labels (Iterable[str]): The set of possible labels. Defaults to [].
    multi_label (bool): Whether the attribute allows multiple labels.
        Defaults to True. When set to False (exclusive labels), missing
        gold labels are interpreted as 0.0.
    positive_label (str): The positive label for a binary task with
        exclusive classes. Defaults to None.
    threshold (float): Cutoff to consider a prediction "positive". Defaults
        to 0.5 for multi-label, and 0.0 (i.e. whatever's highest scoring)
        otherwise.
    RETURNS (Dict[str, Any]): A dictionary containing the scores, with
        inapplicable scores as None:
        for all:
            attr_score (one of attr_micro_f / attr_macro_f / attr_macro_auc),
            attr_score_desc (text description of the overall score),
            attr_micro_p,
            attr_micro_r,
            attr_micro_f,
            attr_macro_p,
            attr_macro_r,
            attr_macro_f,
            attr_macro_auc,
            attr_f_per_type,
            attr_auc_per_type
    """
    if threshold is None:
        threshold = 0.5 if multi_label else 0.0
    f_per_type = {label: PRFScore() for label in labels}
    auc_per_type = {label: ROCAUCScore() for label in labels}
    labels = set(labels)
    if labels:
        for eg in examples:
            labels.update(eg.predicted.cats.keys())
            labels.update(eg.reference.cats.keys())
    for example in examples:
        # Through this loop, None in the gold_cats indicates missing label.
        pred_cats = getter(example.predicted, attr)
        gold_cats = getter(example.reference, attr)

        for label in labels:
            pred_score = pred_cats.get(label, 0.0)
            gold_score = gold_cats.get(label)
            if not gold_score and not multi_label:
                gold_score = 0.0
            if gold_score is not None:
                auc_per_type[label].score_set(pred_score, gold_score)
        if multi_label:
            for label in labels:
                pred_score = pred_cats.get(label, 0.0)
                gold_score = gold_cats.get(label)
                if gold_score is not None:
                    if pred_score >= threshold and gold_score > 0:
                        f_per_type[label].tp += 1
                    elif pred_score >= threshold and gold_score == 0:
                        f_per_type[label].fp += 1
                    elif pred_score < threshold and gold_score > 0:
                        f_per_type[label].fn += 1
        elif pred_cats and gold_cats:
            # Get the highest-scoring for each.
            pred_label, pred_score = max(pred_cats.items(), key=lambda it: it[1])
            gold_label, gold_score = max(gold_cats.items(), key=lambda it: it[1])
            if pred_label == gold_label and pred_score >= threshold:
                f_per_type[pred_label].tp += 1
            else:
                f_per_type[gold_label].fn += 1
                if pred_score >= threshold:
                    f_per_type[pred_label].fp += 1
        elif gold_cats:
            gold_label, gold_score = max(gold_cats, key=lambda it: it[1])
            if gold_score > 0:
                f_per_type[gold_label].fn += 1
        elif pred_cats:
            pred_label, pred_score = max(pred_cats.items(), key=lambda it: it[1])
            if pred_score >= threshold:
                f_per_type[pred_label].fp += 1
    micro_prf = PRFScore()
    for label_prf in f_per_type.values():
        micro_prf.tp += label_prf.tp
        micro_prf.fn += label_prf.fn
        micro_prf.fp += label_prf.fp
    n_cats = len(f_per_type) + 1e-100
    macro_p = sum(prf.precision for prf in f_per_type.values()) / n_cats
    macro_r = sum(prf.recall for prf in f_per_type.values()) / n_cats
    macro_f = sum(prf.fscore for prf in f_per_type.values()) / n_cats
    # Limit macro_auc to those labels with gold annotations,
    # but still divide by all cats to avoid artificial boosting of datasets with missing labels
    macro_auc = (
        sum(auc.score if auc.is_binary() else 0.0 for auc in auc_per_type.values())
        / n_cats
    )
    results: Dict[str, Any] = {
        f"{attr}_score": None,
        f"{attr}_score_desc": None,
        f"{attr}_micro_p": micro_prf.precision,
        f"{attr}_micro_r": micro_prf.recall,
        f"{attr}_micro_f": micro_prf.fscore,
        f"{attr}_macro_p": macro_p,
        f"{attr}_macro_r": macro_r,
        f"{attr}_macro_f": macro_f,
        f"{attr}_macro_auc": macro_auc,
        f"{attr}_f_per_type": {k: v.to_dict() for k, v in f_per_type.items()},
        f"{attr}_auc_per_type": {
            k: v.score if v.is_binary() else None for k, v in auc_per_type.items()
        },
    }
    if len(labels) == 2 and not multi_label and positive_label:
        positive_label_f = results[f"{attr}_f_per_type"][positive_label]["f"]
        results[f"{attr}_score"] = positive_label_f
        results[f"{attr}_score_desc"] = f"F ({positive_label})"
    elif not multi_label:
        results[f"{attr}_score"] = results[f"{attr}_macro_f"]
        results[f"{attr}_score_desc"] = "macro F"
    else:
        results[f"{attr}_score"] = results[f"{attr}_macro_auc"]
        results[f"{attr}_score_desc"] = "macro AUC"
    return results


def textcat_score_v1(examples: Iterable[Example], **kwargs) -> Dict[str, Any]:
    return score_cats_v1(
        examples,
        "cats",
        multi_label=False,
        **kwargs,
    )


def make_textcat_scorer_v1():
    return textcat_score_v1


def textcat_multilabel_score_v1(
    examples: Iterable[Example], **kwargs
) -> Dict[str, Any]:
    return score_cats_v1(
        examples,
        "cats",
        multi_label=True,
        **kwargs,
    )


def make_textcat_multilabel_scorer_v1():
    return textcat_multilabel_score_v1
