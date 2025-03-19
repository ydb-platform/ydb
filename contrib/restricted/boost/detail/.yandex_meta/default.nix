self: super: with self; {
  boost_detail = stdenv.mkDerivation rec {
    pname = "boost_detail";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "detail";
      rev = "boost-${version}";
      hash = "sha256-BORKOSwA7WhzEEK2Y7IF8tSgVbAIRj7mAJ020P4T75E=";
    };
  };
}
