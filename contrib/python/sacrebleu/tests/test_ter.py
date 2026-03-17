import pytest
import sacrebleu

from argparse import Namespace

EPSILON = 1e-3

test_cases = [
    (['aaaa bbbb cccc dddd'], ['aaaa bbbb cccc dddd'], 0),  # perfect match
    (['dddd eeee ffff'], ['aaaa bbbb cccc'], 1),  # no overlap
    ([''], [''], 1),  # corner case, empty strings
    (['d e f g h a b c'], ['a b c d e f g h'], 1/8),  # a single shift fixes MT
    (
        [
            'wählen Sie " Bild neu berechnen , " um beim Ändern der Bildgröße Pixel hinzuzufügen oder zu entfernen , damit das Bild ungefähr dieselbe Größe aufweist wie die andere Größe .',
            'wenn Sie alle Aufgaben im aktuellen Dokument aktualisieren möchten , wählen Sie im Menü des Aufgabenbedienfelds die Option " Alle Aufgaben aktualisieren . "',
            'klicken Sie auf der Registerkarte " Optionen " auf die Schaltfläche " Benutzerdefiniert " und geben Sie Werte für " Fehlerkorrektur-Level " und " Y / X-Verhältnis " ein .',
            'Sie können beispielsweise ein Dokument erstellen , das ein Auto über die Bühne enthält .',
            'wählen Sie im Dialogfeld " Neu aus Vorlage " eine Vorlage aus und klicken Sie auf " Neu . "',
        ],
        [
            'wählen Sie " Bild neu berechnen , " um beim Ändern der Bildgröße Pixel hinzuzufügen oder zu entfernen , damit die Darstellung des Bildes in einer anderen Größe beibehalten wird .',
            'wenn Sie alle Aufgaben im aktuellen Dokument aktualisieren möchten , wählen Sie im Menü des Aufgabenbedienfelds die Option " Alle Aufgaben aktualisieren . "',
            'klicken Sie auf der Registerkarte " Optionen " auf die Schaltfläche " Benutzerdefiniert " und geben Sie für " Fehlerkorrektur-Level " und " Y / X-Verhältnis " niedrigere Werte ein .',
            'Sie können beispielsweise ein Dokument erstellen , das ein Auto enthalt , das sich über die Bühne bewegt .',
            'wählen Sie im Dialogfeld " Neu aus Vorlage " eine Vorlage aus und klicken Sie auf " Neu . "',
        ],
        0.136  # realistic example from WMT dev data (2019)
    ),
]


@pytest.mark.parametrize("hypotheses, references, expected_score", test_cases)
def test_ter(hypotheses, references, expected_score):
    args = Namespace(tokenize=sacrebleu.DEFAULT_TOKENIZER)
    metric = sacrebleu.metrics.TER(args)
    score = metric.corpus_score(hypotheses, [references]).score
    assert abs(score - expected_score) < EPSILON
