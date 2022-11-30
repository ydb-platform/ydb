#pragma once

class Perfect_Hash
{
private:
	static inline unsigned int hash (const char *str, unsigned int len);

public:
	static struct XMLTagHashPair *in_word_set (const char *str, unsigned int len);
};
