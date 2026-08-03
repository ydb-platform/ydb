pkgs: attrs: with pkgs; rec {
  version = "2023.1.0";

  src = fetchFromGitHub {
      owner = "uxlfoundation";
      repo = "oneTBB";
      rev = "v${version}";
      hash = "sha256-7C6h2wcQW/t3J6/PX+aoRFHeHpy0s0km9ZLvxvOBdpY=";
  };

  patches = [];

  nativeBuildInputs = [ cmake ];
}
