self: super: with self; {
  boost_proto = stdenv.mkDerivation rec {
    pname = "boost_proto";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "proto";
      rev = "boost-${version}";
      hash = "sha256-708hd2eOvo9rqvOA58aMW8lfRWSEHibTxSyVZxUgy/I=";
    };
  };
}
