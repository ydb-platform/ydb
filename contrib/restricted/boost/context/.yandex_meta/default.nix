self: super: with self; {
  boost_context = stdenv.mkDerivation rec {
    pname = "boost_context";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "context";
      rev = "boost-${version}";
      hash = "sha256-KYEjmXFVNiP8TD3TB+JS5CwvfHPdly3qDKDNaW9w0N4=";
    };
  };
}
