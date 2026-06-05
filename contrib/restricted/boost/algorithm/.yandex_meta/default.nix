self: super: with self; {
  boost_algorithm = stdenv.mkDerivation rec {
    pname = "boost_algorithm";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "algorithm";
      rev = "boost-${version}";
      hash = "sha256-d76GEKpoisH2sgT0f5cDlIn2LGulzjHaYS2waZMUhD8=";
    };
  };
}
