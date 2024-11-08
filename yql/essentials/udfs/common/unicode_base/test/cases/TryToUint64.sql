SELECT
    Unicode::TryToUint64("hell", 10);

SELECT
    Unicode::TryToUint64("01238", 8);

SELECT
    Unicode::TryToUint64("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);

SELECT
    Unicode::TryToUint64("0x1234abcd", 16),
    Unicode::TryToUint64("0X4", 16),
    Unicode::TryToUint64("0644", 8),
    Unicode::TryToUint64("0101010", 16),
    Unicode::TryToUint64("0101010", 2),
    Unicode::TryToUint64("0101010", 10),
    Unicode::TryToUint64("101", 10);
