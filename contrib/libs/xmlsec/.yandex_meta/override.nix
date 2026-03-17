self: super: with self; rec {
  version = "1.2.37";

  src = fetchurl {
    url = "https://www.aleksey.com/xmlsec/download/xmlsec1-${version}.tar.gz";
    hash = "sha256-X437y20eVr3dC17C4Ao9DKU0Kp9Xwk3/3lx5ayvihxw=";
  };

  patches = [];
}
