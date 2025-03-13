self: super: with self; {
  boost_concept_check = stdenv.mkDerivation rec {
    pname = "boost_concept_check";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "concept_check";
      rev = "boost-${version}";
      hash = "sha256-W/LAEqMIFXsPBgDJ3ENcsaFAiRODRKrPXrGIMwLbvO4=";
    };
  };
}
