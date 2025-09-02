SELECT
    String::ReplaceFirst("gasas", "as", "z"),
    String::ReplaceFirst("gasas", "a", "zzz"),
    String::ReplaceFirst("gasas", "a", ""),
    String::ReplaceFirst("gasas", "e", "z"),
    String::ReplaceLast("gasas", "as", "z"),
    String::ReplaceLast("gasas", "a", "zzz"),
    String::ReplaceLast("gasas", "a", ""),
    String::ReplaceLast("gasas", "k", "ey");

