pkgs: attrs: with pkgs; rec {
  version = "2022.3.0";

  src = fetchFromGitHub {
      owner = "uxlfoundation";
      repo = "oneTBB";
      rev = "v${version}";
      hash = "sha256-HIHF6KHlEI4rgQ9Epe0+DmNe1y95K9iYa4V/wFnJfEU=";
  };

  patches = [];

  nativeBuildInputs = [ cmake ];
}
