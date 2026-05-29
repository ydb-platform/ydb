pkgs: attrs: with pkgs; rec {
  version = "2023.0.0";

  src = fetchFromGitHub {
      owner = "uxlfoundation";
      repo = "oneTBB";
      rev = "v${version}";
      hash = "sha256-algLvvVHHCQTiwxNX7gM6vfpSadR7fu49CBtfx2LSPk=";
  };

  patches = [];

  nativeBuildInputs = [ cmake ];
}
