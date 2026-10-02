self: super: with self; {
  apache-orc-format = stdenv.mkDerivation rec {
    name = "apache-orc-format";
    version = "1.1.1";

    src = fetchFromGitHub {
      owner = "apache";
      repo = "orc-format";
      rev = "v${version}";
      hash = "sha256-IH8wKEfElSpCqXEwMpCNpaea6B68Rj/lZygSOuHQUYQ=";
    };
  };
}
