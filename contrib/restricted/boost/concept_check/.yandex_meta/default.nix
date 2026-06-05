self: super: with self; {
  boost_concept_check = stdenv.mkDerivation rec {
    pname = "boost_concept_check";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "concept_check";
      rev = "boost-${version}";
      hash = "sha256-xXBkNTd+1j2jkPsczcslqd8MO7LLrOl7LrFVaRLXYjY=";
    };
  };
}
