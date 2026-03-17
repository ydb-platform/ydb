import justext


def test_words_should_be_split_by_br_tag():
    paragraphs = justext.justext('abc<br/>def becoming abcdef', justext.get_stoplist("English"))

    assert [p.text for p in paragraphs] == ["abc def becoming abcdef"]
