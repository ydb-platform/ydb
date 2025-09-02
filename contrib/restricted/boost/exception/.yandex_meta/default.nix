self: super: with self; {
  boost_exception = stdenv.mkDerivation rec {
    pname = "boost_exception";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "exception";
      rev = "boost-${version}";
      hash = "sha256-oziBPE66j9nntT4eLj3lh8+xL3hjWu0B4UGhjuUoqI8=";
    };
  };
}
