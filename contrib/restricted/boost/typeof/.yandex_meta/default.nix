self: super: with self; {
  boost_typeof = stdenv.mkDerivation rec {
    pname = "boost_typeof";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "typeof";
      rev = "boost-${version}";
      hash = "sha256-FebbLKrqYrKOS4lHcf9HZNwnzBRWuHjF6D81RvU8M38=";
    };
  };
}
