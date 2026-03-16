self: super: with self; rec {
  pname = "jsoncpp";
  version = "1.9.6";

  src = fetchFromGitHub {
    owner = "open-source-parsers";
    repo = "jsoncpp";
    rev = version;
    hash = "sha256-3msc3B8NyF8PUlNaAHdUDfCpcUmz8JVW2X58USJ5HRw=";
  };

  # In nixpkgs original src is modified during unpackPhase,
  # thus preventing src from being overriden.
  #
  # There is no need in such modification as everything works fine without it.
  unpackPhase = "";

  patches = [];
}
