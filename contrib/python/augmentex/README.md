<p align="center">
    <a href="https://github.com/ai-forever/augmentex/blob/main/LICENSE">
    <img alt="License" src="https://img.shields.io/badge/License-MIT-yellow.svg">
    </a>
    <a href="https://github.com/ai-forever/augmentex/releases">
    <img alt="Release" src="https://img.shields.io/badge/release-v1.2.1-blue">
    </a>
    <a href="https://arxiv.org/abs/2308.09435">
    <img alt="Paper" src="https://img.shields.io/badge/arXiv-2308.09435-red">
    </a>
</p>

# Augmentex — a library for augmenting texts with errors
Augmentex introduces rule-based and common statistic (empowered by [KartaSlov](https://kartaslov.ru) project) 
approach to insert errors in text. It is fully described again in the [Paper](https://www.dialog-21.ru/media/5914/martynovnplusetal056.pdf)
and in this 🗣️[Talk](https://youtu.be/yFfkV0Qjuu0?si=XmKfocCSLnKihxS_).

## Contents
- [Augmentex — a library for augmenting texts with errors](#augmentex--a-library-for-augmenting-texts-with-errors)
  - [Contents](#contents)
  - [Installation](#installation)
  - [Implemented functionality](#implemented-functionality)
  - [Usage](#usage)
    - [**Word level**](#word-level)
    - [**Character level**](#character-level)
    - [**Batch processing**](#batch-processing)
    - [**Compute your own statistics**](#compute-your-own-statistics)
    - [**Google Colab example**](#google-colab-example)
  - [Contributing](#contributing)
    - [Issue](#issue)
    - [Pull request](#pull-request)
  - [References](#references)
  - [Authors](#authors)

## Installation
```commandline
pip install augmentex
```

## Implemented functionality
We collected statistics from different languages and from different input sources. This table shows what functionality the library currently supports.

|             | Russian     | English     |
| -----------:|:-----------:|:-----------:|
| PC keyboard |      ✅     |      ✅     |
| Mobile kb   |      ✅     |      ❌     |

In the future, it is planned to scale the functionality to new languages and various input sources.

## Usage
🖇️ Augmentex allows you to operate on two levels of granularity when it comes to text corruption and offers you sets of 
specific methods suited for particular level:
- **Word level**:
  - _replace_ - replace a random word with its incorrect counterpart;
  - _delete_ - delete random word;
  - _swap_ - swap two random words;
  - _stopword_ - add random words from stop-list;
  - _split_ - add spaces between letters to the word;
  - _reverse_ - change a case of the first letter of a random word;
  - _text2emoji_ - change the word to the corresponding emoji.
- **Character level**:
  - _shift_ - randomly swaps upper / lower case in a string;
  - _orfo_ - substitute correct characters with their common incorrect counterparts;
  - _typo_ - substitute correct characters as if they are mistyped on a keyboard;
  - _delete_ - delete random character;
  - _insert_ - insert random character;
  - _multiply_ - multiply random character;
  - _swap_ - swap two adjacent characters.

### **Word level**
```python
from augmentex import WordAug

word_aug = WordAug(
    unit_prob=0.4, # Percentage of the phrase to which augmentations will be applied
    min_aug=1, # Minimum number of augmentations
    max_aug=5, # Maximum number of augmentations
    lang="eng", # supports: "rus", "eng"
    platform="pc", # supports: "pc", "mobile"
    random_seed=42,
    )
```

1. Replace a random word with its incorrect counterpart;
```python
text = "Screw you guys, I am going home. (c)"
word_aug.augment(text=text, action="replace")
# Screw to guys, I to going com. (c)
```

2. Delete random word;
```python
text = "Screw you guys, I am going home. (c)"
word_aug.augment(text=text, action="delete")
# you I am home. (c)
```

3. Swap two random words;
```python
text = "Screw you guys, I am going home. (c)"
word_aug.augment(text=text, action="swap")
# Screw I guys, am home. going you (c)
```

4. Add random words from stop-list;
```python
text = "Screw you guys, I am going home. (c)"
word_aug.augment(text=text, action="stopword")
# like Screw you guys, I am going completely home. by the way (c)
```

5. Adds spaces between letters to the word;
```python
text = "Screw you guys, I am going home. (c)"
word_aug.augment(text=text, action="split")
# Screw y o u guys, I am going h o m e . (c)
```

6. Change a case of the first letter of a random word;
```python
text = "Screw you guys, I am going home. (c)"
word_aug.augment(text=text, action="reverse")
# Screw You guys, i Am going home. (c)
```

7. Changes the word to the corresponding emoji.
```python
text = "Screw you guys, I am going home. (c)"
word_aug.augment(text=text, action="text2emoji")
# Screw you guys, I am going home. (c)
```

8. Replaces ngram in a word with erroneous ones.
```python
text = "Screw you guys, I am going home. (c)"
word_aug.augment(text=text, action="ngram")
# Scren you guys, I am going home. (c)
```

### **Character level**
```python
from augmentex import CharAug

char_aug = CharAug(
    unit_prob=0.3, # Percentage of the phrase to which augmentations will be applied
    min_aug=1, # Minimum number of augmentations
    max_aug=5, # Maximum number of augmentations
    mult_num=3, # Maximum number of repetitions of characters (only for the multiply method)
    lang="eng", # supports: "rus", "eng"
    platform="pc", # supports: "pc", "mobile"
    random_seed=42,
    )
```

1. Randomly swaps upper / lower case in a string;
```python
text = "Screw you guys, I am going home. (c)"
char_aug.augment(text=text, action="shift")
# Screw YoU guys, I am going Home. (C)
```

2. Substitute correct characters with their common incorrect counterparts;
```python
text = "Screw you guys, I am going home. (c)"
char_aug.augment(text=text, action="orfo")
# Sedew you guya, I am going home. (c)
```

3. Substitute correct characters as if they are mistyped on a keyboard;
```python
text = "Screw you guys, I am going home. (c)"
char_aug.augment(text=text, action="typo")
# Sxrew you gugs, I am going home. (x)
```

4. Delete random character;
```python
text = "Screw you guys, I am going home. (c)"
char_aug.augment(text=text, action="delete")
# crew you guys Iam goinghme. (c)
```

5. Insert random character;
```python
text = "Screw you guys, I am going home. (c)"
char_aug.augment(text=text, action="insert")
# Screw you ughuys, I vam gcoing hxome. (c)
```

6. Multiply random character;
```python
text = "Screw you guys, I am going home. (c)"
char_aug.augment(text=text, action="multiply")
# Screw yyou guyss, I am ggoinng home. (c)
```

7. Swap two adjacent characters.
```python
text = "Screw you guys, I am going home. (c)"
char_aug.augment(text=text, action="swap")
# Srcewy ou guys,I  am oging hmoe. (c)
```

### **Batch processing**
📁 For batch text processing, you need to call the `aug_batch` method instead of the `augment` method and pass a list of strings to it.

```python
from augmentex import WordAug

word_aug = WordAug(
    unit_prob=0.4, # Percentage of the phrase to which augmentations will be applied
    min_aug=1, # Minimum number of augmentations
    max_aug=5, # Maximum number of augmentations
    lang="eng", # supports: "rus", "eng"
    platform="pc", # supports: "pc", "mobile"
    random_seed=42,
    )

text_list = ["Screw you guys, I am going home. (c)"] * 10
word_aug.aug_batch(text_list, batch_prob=0.5) # without action

text_list = ["Screw you guys, I am going home. (c)"] * 10
word_aug.aug_batch(text_list, batch_prob=0.5, action="replace") # with action
```

### **Compute your own statistics**
📊 If you want to use your own statistics for the _replace_ and _orfo_ methods, then you will need to specify two paths to parallel corpora with texts without errors and with errors.

Example of txt files:
<table style="width: 100%;">
<tbody style="
"><tr style="
">
<th> texts_without_errors.txt </th>
<th> texts_with_errors.txt </th>
</tr>
<tr style="
">
<td style="
    width: 1%;
">
<p dir="auto">some text without errors 1<br>
some text without errors 2<br>
some text without errors 3<br>
...</p>
</td>
<td style="
    width: 1%;
">
<p dir="auto">some text with errors 1<br>
some text with errors 2<br>
some text with errors 3<br>
...</p>
</td>
</tr>
</tbody></table>

```python
from augmentex import WordAug

word_aug = WordAug(
    unit_prob=0.4, # Percentage of the phrase to which augmentations will be applied
    min_aug=1, # Minimum number of augmentations
    max_aug=5, # Maximum number of augmentations
    lang="eng", # supports: "rus", "eng"
    platform="pc", # supports: "pc", "mobile"
    random_seed=42,
    correct_texts_path="correct_texts.txt",
    error_texts_path="error_texts.txt",
    )
```

### **Google Colab example**
You can familiarize yourself with the usage in the example [![Try In Colab!](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1azYUsAd1ofvBI_sPrMftX_ioaspvjEOg?usp=sharing)

## Contributing
### Issue
- If you see an open issue and are willing to do it, add yourself to the performers and write about how much time it will take to fix it. See the pull request module below.
- If you want to add something new or if you find a bug, you should start by creating a new issue and describing the problem/feature. Don't forget to include the appropriate labels.

### Pull request
How to make a pull request.
1. Clone the repository;
2. Create a new branch, for example `git checkout -b issue-id-short-name`;
3. Make changes to the code (make sure you are definitely working in the new branch);
4. `git push`;
5. Create a pull request to the `develop` branch;
6. Add a brief description of the work done;
7. Expect comments from the authors.

## References
- [SAGE](https://github.com/ai-forever/sage) — superlib, developed jointly with our friends by the AGI NLP team, which provides advanced spelling corruptions and spell checking techniques, including using Augmentex.

## Authors
- [Aleksandr Abramov](https://github.com/Ab1992ao) — Source code and algorithm author;
- [Mark Baushenko](https://github.com/e0xextazy) — Source code lead developer.
