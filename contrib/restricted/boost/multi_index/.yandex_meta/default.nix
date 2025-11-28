self: super: with self; {
  boost_multi_index = stdenv.mkDerivation rec {
    pname = "boost_multi_index";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "multi_index";
      rev = "boost-${version}";
      hash = "sha256-DoLjlX6/SlMaoahtriwjeARHxMTx4uS68wBVRPw6JZU=";
    };
  };
}
