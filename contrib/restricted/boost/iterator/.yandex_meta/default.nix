self: super: with self; {
  boost_iterator = stdenv.mkDerivation rec {
    pname = "boost_iterator";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "iterator";
      rev = "boost-${version}";
      hash = "sha256-AWy3/louiVD3uwu/IoYfKPi8273IoIGs6nXWs2AZo+g=";
    };
  };
}
