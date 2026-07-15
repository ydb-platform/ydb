self: super: with self; {
  boost_context = stdenv.mkDerivation rec {
    pname = "boost_context";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "context";
      rev = "boost-${version}";
      hash = "sha256-I4WsIxf4osQyS+IAEiwMo81k51LAj4cIQmc6kevpR88=";
    };
  };
}
