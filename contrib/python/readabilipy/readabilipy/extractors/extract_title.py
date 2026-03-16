from itertools import permutations
from .extract_element import extract_element


def extract_title(html):
    """Return the article title from the article HTML"""

    # List of xpaths for HTML tags that could contain a title
    # Tuple scores reflect confidence in these xpaths and the preference used for extraction
    xpaths = [
        ('//header[@class="entry-header"]/h1[@class="entry-title"]//text()', 4),
        ('//meta[@property="og:title"]/@content', 4),
        ('//h1[@class="entry-title"]//text()', 3),
        ('//h1[@itemprop="headline"]//text()', 3),
        ('//h2[@itemprop="headline"]//text()', 2),
        ('//meta[contains(@itemprop, "headline")]/@content', 2),
        ('//body/title//text()', 1),
        ('//div[@class="postarea"]/h2/a//text()', 1),
        ('//h1[@class="post__title"]//text()', 1),
        ('//h1[@class="title"]//text()', 1),
        ('//head/title//text()', 1),
        ('//header/h1//text()', 1),
        ('//meta[@name="dcterms.title"]/@content', 1),
        ('//meta[@name="fb_title"]/@content', 1),
        ('//meta[@name="sailthru.title"]/@content', 1),
        ('//meta[@name="title"]/@content', 1),
    ]

    extracted_titles = extract_element(html, xpaths, process_dict_fn=combine_similar_titles)
    if not extracted_titles:
        return None
    return max(extracted_titles, key=lambda x: extracted_titles[x].get('score'))


def combine_similar_titles(extracted_strings):
    """Take a dictionary with titles and nested dicts with scores and combine scores for titles which we decide are the same."""

    # Iterate through each possible pair of title keys, including both permutations of each pair
    for title_pair in permutations(extracted_strings, 2):
        # If the first title is a subset of the second then combine their scores, taking the shorter one as the key
        if title_pair[0] in title_pair[1]:
            extracted_strings[title_pair[0]]['score'] += extracted_strings[title_pair[1]]['score']
            extracted_strings[title_pair[0]]['xpaths'] += extracted_strings[title_pair[1]]['xpaths']
        # If the first title is identical to the second (ignoring case) then combine their scores, taking the one with more capitals as the key
        elif title_pair[0].lower() == title_pair[1].lower():
            if len([c for c in title_pair[0] if c.isupper()]) > len([c for c in title_pair[1] if c.isupper()]):
                extracted_strings[title_pair[0]]['score'] += extracted_strings[title_pair[1]]['score']
                extracted_strings[title_pair[0]]['xpaths'] += extracted_strings[title_pair[1]]['xpaths']
    for score_xpath_dict in extracted_strings.values():
        score_xpath_dict['xpaths'].sort()
    return extracted_strings
