self: super: with self; {
  boost_asio = stdenv.mkDerivation rec {
    pname = "boost_asio";
    version = "1.86.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "asio";
      rev = "boost-${version}";
      hash = "sha256-dxORb6fczR6vtZKyMN+iQKcaDbN4cqcPPM07B/S1ovY=";
    };
  };
}
