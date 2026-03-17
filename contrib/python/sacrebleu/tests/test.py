import sacrebleu

segment = "Consistency is the last refuge of the unimaginative"
score = sacrebleu.corpus_chrf([segment], [segment], 6, 3.0)
assert(score == 1.0)

ref = "AAAAAA"
sys = "BBBB"
score = sacrebleu.corpus_chrf([sys], [ref], 3, 3.0)
assert(score == 0.0)

ref = ""
sys = ""
score = sacrebleu.corpus_chrf([sys], [ref], 6, 3)
print(score)
#assert(score == 1.0)

ref = "A"
sys = ""
score = sacrebleu.corpus_chrf([sys], [ref], 6, 3)
assert(score == 0.0)

ref = ""
sys = "A"
score = sacrebleu.corpus_chrf([sys], [ref], 6, 3)
assert(score == 0.0)

ref = "AB"
sys = "AA"
score = sacrebleu.corpus_chrf([sys], [ref], 6, 3)
assert(score == 0.25)

# segment_a = self.tokenize("A")
# segment_b = self.tokenize("A")
ref = "A"
sys = "A"
score = sacrebleu.corpus_chrf([sys], [ref], 6, 3)
assert(score == 1.0)
# scorer = CharacterFScorer('n=6,beta=3')
# scorer.set_reference(segment_a)
# self.assertEqual(scorer.score(segment_b), 1.0)

ref = "risk assessment has to be undertaken by those who are qualified and expert in that area - that is the scientists ."
sys = " risk assessment must be made of those who are qualified and expertise in the sector - these are the scientists ."
score = sacrebleu.corpus_chrf([sys], [ref], 6, 3)
print(score)
assert('{0:.5f}'.format(score) == '0.63362')
