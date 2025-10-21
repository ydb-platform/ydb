self: super: with self; {
  boost_concept_check = stdenv.mkDerivation rec {
    pname = "boost_concept_check";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "concept_check";
      rev = "boost-${version}";
      hash = "sha256-nsf7frCia0fgBpfimW+IqznnVfMSmar9KEKB5sccPPI=";
    };
  };
}
