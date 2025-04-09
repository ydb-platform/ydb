self: super: with self; {
  boost_coroutine = stdenv.mkDerivation rec {
    pname = "boost_coroutine";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "coroutine";
      rev = "boost-${version}";
      hash = "sha256-wPHezql6B5Ewe2KVQsVHXf0cBsVVp6Xre20KoUjbN14=";
    };
  };
}
