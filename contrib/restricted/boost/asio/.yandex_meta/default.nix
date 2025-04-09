self: super: with self; {
  boost_asio = stdenv.mkDerivation rec {
    pname = "boost_asio";
    version = "1.85.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "asio";
      rev = "boost-${version}";
      hash = "sha256-vYYYna5TXBCAAO1NkoxaUidgErKbCNP06S8uHijzIDE=";
    };
  };
}
