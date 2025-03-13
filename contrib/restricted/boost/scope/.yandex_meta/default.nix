self: super: with self; {
  boost_scope = stdenv.mkDerivation rec {
    pname = "boost_scope";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "scope";
      rev = "boost-${version}";
      hash = "sha256-gY6BOYlMXWGuATot1bgokPd7jQI1CMNRvl5XJ71pW2s=";
    };
  };
}
