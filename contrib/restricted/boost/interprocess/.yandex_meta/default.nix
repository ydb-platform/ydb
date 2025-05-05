self: super: with self; {
  boost_interprocess = stdenv.mkDerivation rec {
    pname = "boost_interprocess";
    version = "1.86.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "interprocess";
      rev = "boost-${version}";
      hash = "sha256-X8Ab1Mv7EArhtg6/c+vC3mk1lqOVvqsd+rvfP3MJnbA=";
    };
  };
}
