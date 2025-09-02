self: super: with self; {
  apache-orc-format = stdenv.mkDerivation rec {
    name = "apache-orc-format";
    version = "1.1.0";

    src = fetchFromGitHub {
      owner = "apache";
      repo = "orc-format";
      rev = "v${version}";
      hash = "sha256-n35WbL0jUFGm/RlJVtMJcnqOdXgzn1X8dAOooE5rwzQ=";
    };
  };
}
