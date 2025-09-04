self: super: with self; {
  boost_move = stdenv.mkDerivation rec {
    pname = "boost_move";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "move";
      rev = "boost-${version}";
      hash = "sha256-eXS9ooLEzzItTcfTH0lzoyjDykr+dCGzJcTj/v9V8k4=";
    };
  };
}
